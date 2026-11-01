"""Airflow REST API 客户端（stdlib urllib，零第三方依赖）。

对接 Airflow 3.3 `/api/v2`，JWT Bearer 认证：
1. POST /auth/token 换取 access_token；
2. 携带 Bearer Token 调用 DAG / DAG Run / 任务实例 / 日志接口；
3. 401 时自动刷新 Token 重试一次。
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger("bridge.airflow_client")

TERMINAL_STATES = {"success", "failed", "upstream_failed", "skipped"}
FAILURE_STATES = {"failed", "upstream_failed"}


class AirflowApiError(RuntimeError):
    """Airflow REST 调用异常。"""


class AirflowRestClient:
    def __init__(self, base_url: str, username: str, password: str, timeout: int = 20):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = timeout
        self._token: Optional[str] = None

    # ------------------------------------------------------------------ #
    # 底层 HTTP
    # ------------------------------------------------------------------ #
    def _http(
        self,
        method: str,
        path: str,
        body: Optional[dict] = None,
        headers: Optional[dict] = None,
        raw_response: bool = False,
        _retried: bool = False,
    ) -> Any:
        url = f"{self.base_url}{path}"
        headers = dict(headers or {})
        data = None
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            headers.setdefault("Content-Type", "application/json")
        if self._token and path != "/auth/token":
            headers["Authorization"] = f"Bearer {self._token}"

        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as resp:
                payload = resp.read()
                if raw_response:
                    return payload.decode("utf-8", errors="replace")
                if not payload:
                    return {}
                return json.loads(payload.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace")
            if exc.code == 401 and not _retried and path != "/auth/token":
                self._token = None
                self._authenticate()
                return self._http(method, path, body, headers, raw_response, _retried=True)
            raise AirflowApiError(f"HTTP {exc.code} {method} {path}: {err_body}") from exc
        except urllib.error.URLError as exc:
            raise AirflowApiError(f"网络无法连接 Airflow {url}: {exc.reason}") from exc

    def _authenticate(self) -> None:
        # 注意：Airflow 3 的 token 路由挂在根路径 /auth/token（不属于 /api/v2 命名空间），
        # 而 DAG / DagRun 等业务接口都在 /api/v2 下；混用前缀会得到 405。
        result = self._http(
            "POST", "/auth/token",
            body={"username": self.username, "password": self.password},
        )
        self._token = result["access_token"]
        log.info("Airflow JWT Token 已获取")

    def _ensure_auth(self) -> None:
        if not self._token:
            self._authenticate()

    # ------------------------------------------------------------------ #
    # DAG 元数据 / 控制
    # ------------------------------------------------------------------ #
    def list_dags(self) -> List[dict]:
        self._ensure_auth()
        result = self._http("GET", "/api/v2/dags?limit=100")
        return result.get("dags", [])

    def get_dag(self, dag_id: str) -> dict:
        self._ensure_auth()
        return self._http("GET", f"/api/v2/dags/{urllib.parse.quote(dag_id)}")

    def unpause_dag(self, dag_id: str) -> dict:
        self._ensure_auth()
        return self._http(
            "PATCH", f"/api/v2/dags/{urllib.parse.quote(dag_id)}", body={"is_paused": False}
        )

    # ------------------------------------------------------------------ #
    # DAG Run 触发 / 查询 / 轮询
    # ------------------------------------------------------------------ #
    def trigger_dag(self, dag_id: str, conf: Optional[dict] = None) -> dict:
        self._ensure_auth()
        # Airflow 3 的 POST dagRuns 强制要求 logical_date（UTC ISO8601），
        # 不再像 2.x 那样由服务端自动生成。
        payload: Dict[str, Any] = {
            "logical_date": datetime.now(timezone.utc).isoformat(),
            "conf": conf or {},
        }
        result = self._http(
            "POST", f"/api/v2/dags/{urllib.parse.quote(dag_id)}/dagRuns", body=payload
        )
        log.info("已触发 DAG %s, run_id=%s", dag_id, result.get("dag_run_id"))
        return result

    def get_dag_run(self, dag_id: str, run_id: str) -> dict:
        self._ensure_auth()
        return self._http(
            "GET", f"/api/v2/dags/{urllib.parse.quote(dag_id)}/dagRuns/{urllib.parse.quote(run_id)}"
        )

    def list_task_instances(self, dag_id: str, run_id: str) -> List[dict]:
        self._ensure_auth()
        result = self._http(
            "GET",
            f"/api/v2/dags/{urllib.parse.quote(dag_id)}/dagRuns/{urllib.parse.quote(run_id)}/taskInstances",
        )
        return result.get("task_instances", [])

    def get_task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
        self._ensure_auth()
        # Airflow 3 日志路由为 .../taskInstances/{task_id}/logs/{try_number}，
        # try_number 在路径段中（不是查询参数）；JSON 响应 content 为日志行数组。
        result = self._http(
            "GET",
            f"/api/v2/dags/{urllib.parse.quote(dag_id)}/dagRuns/{urllib.parse.quote(run_id)}"
            f"/taskInstances/{urllib.parse.quote(task_id)}/logs/{int(try_number)}?full_content=true",
        )
        content = result.get("content") or []
        if isinstance(content, list):
            return "\n".join(str(line) for line in content)
        return str(content)

    def wait_for_finish(
        self,
        dag_id: str,
        run_id: str,
        timeout: float = 180,
        poll_interval: float = 3,
        on_poll=None,
    ) -> Dict[str, Any]:
        """轮询 DAG Run 直到终态，返回 run 快照 + 任务实例列表。"""
        deadline = time.time() + timeout
        last_state = None
        while time.time() < deadline:
            run = self.get_dag_run(dag_id, run_id)
            state = run.get("state")
            if state != last_state:
                log.info("DAG Run 状态: %s", state)
                last_state = state
            if on_poll:
                on_poll(state)
            if state in TERMINAL_STATES:
                tasks = self.list_task_instances(dag_id, run_id)
                return {"run": run, "tasks": tasks}
            time.sleep(poll_interval)
        raise AirflowApiError(f"等待 DAG Run 终态超时（{timeout}s）: {dag_id}/{run_id}")
