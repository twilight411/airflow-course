"""LLM 网关编排器：意图 -> 校验 -> 触发 Airflow -> 轮询监控 -> 异常汇总。

这是大模型与 Airflow 之间的接口层：
- 上游对接自然语言对话（CLI / 后续可接 Web、IM 机器人）；
- 下游通过 AirflowRestClient 调用 REST API；
- 负责 DAG 解暂停、触发、状态轮询、失败任务日志采集与人类可读汇报生成。
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from .airflow_rest_client import AirflowRestClient, AirflowApiError, FAILURE_STATES
from .config import Settings
from .intent import Intent, find_dag, load_registry
from .intent_parser import IntentParser

log = logging.getLogger("bridge.orchestrator")
CST = timezone(timedelta(hours=8))


class Orchestrator:
    def __init__(
        self,
        airflow: AirflowRestClient,
        parser: IntentParser,
        registry: Optional[dict] = None,
        settings: Optional[Settings] = None,
    ):
        self.airflow = airflow
        self.parser = parser
        self.registry = registry or load_registry()
        self.settings = settings

    # ------------------------------------------------------------------ #
    # 能力查询
    # ------------------------------------------------------------------ #
    def list_capabilities(self) -> List[Dict[str, Any]]:
        return [
            {
                "dag_id": d["dag_id"],
                "name": d["name"],
                "description": d["description"],
                "schedule": d.get("default_schedule_note"),
            }
            for d in self.registry["dags"]
        ]

    # ------------------------------------------------------------------ #
    # 主流程
    # ------------------------------------------------------------------ #
    def handle(self, text: str, auto_confirm: bool = True) -> Dict[str, Any]:
        started = time.perf_counter()
        intent = self.parser.parse(text)
        report: Dict[str, Any] = {
            "ok": False,
            "request": text,
            "parsed_intent": self._intent_to_dict(intent),
            "timestamp": datetime.now(CST).isoformat(),
        }

        if not intent.valid:
            report["error"] = intent.error or "意图解析失败"
            return report

        meta = find_dag(self.registry, intent.dag_id)
        if meta is None:
            report["error"] = f"注册表中不存在 DAG: {intent.dag_id}"
            return report

        report["dag_meta"] = {"dag_id": meta["dag_id"], "name": meta["name"]}
        if intent.schedule_hint:
            report["schedule_hint"] = intent.schedule_hint

        try:
            # 1. 确保 DAG 处于运行状态（仅当确实处于暂停时才发起 PATCH，
            #    Airflow 3 的 PATCH 会触发 DAG bundle 重载，已启用时反复调用会徒增等待）
            current = self.airflow.get_dag(intent.dag_id)
            if current.get("is_paused"):
                self.airflow.unpause_dag(intent.dag_id)
            # 2. 通过 REST API 触发 DAG Run
            triggered = self.airflow.trigger_dag(intent.dag_id, intent.conf)
            run_id = triggered["dag_run_id"]
            report["run_id"] = run_id
            log.info("DAG Run 已触发: %s/%s", intent.dag_id, run_id)

            # 3. 轮询等待终态并采集任务状态
            timeout = self.settings.wait_timeout if self.settings else 180
            poll_interval = self.settings.poll_interval if self.settings else 3
            snapshot = self.airflow.wait_for_finish(
                intent.dag_id, run_id,
                timeout=timeout, poll_interval=poll_interval,
            )
            run = snapshot["run"]
            tasks = snapshot["tasks"]
            report["final_state"] = run.get("state")
            report["tasks"] = [
                {
                    "task_id": t["task_id"],
                    "state": t["state"],
                    "try_number": t.get("try_number"),
                    "start_date": t.get("start_date"),
                    "end_date": t.get("end_date"),
                }
                for t in tasks
            ]

            failed = [t for t in tasks if t["state"] in FAILURE_STATES]
            report["failed_tasks"] = [t["task_id"] for t in failed]
            if failed:
                report["failure_summary"] = self._collect_failure_details(intent.dag_id, run_id, failed)
            report["ok"] = run.get("state") not in FAILURE_STATES
            report["elapsed_seconds"] = round(time.perf_counter() - started, 2)
            return report

        except AirflowApiError as exc:
            report["error"] = str(exc)
            report["elapsed_seconds"] = round(time.perf_counter() - started, 2)
            return report

    # ------------------------------------------------------------------ #
    # 状态查询（CLI: status <run_id>）
    # ------------------------------------------------------------------ #
    def query_status(self, dag_id: str, run_id: str) -> Dict[str, Any]:
        run = self.airflow.get_dag_run(dag_id, run_id)
        tasks = self.airflow.list_task_instances(dag_id, run_id)
        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "state": run.get("state"),
            "tasks": [{"task_id": t["task_id"], "state": t["state"]} for t in tasks],
        }

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #
    def _collect_failure_details(self, dag_id: str, run_id: str, failed: List[dict]) -> str:
        summaries: List[str] = []
        for task in failed:
            task_id = task["task_id"]
            try_number = task.get("try_number") or 1
            try:
                log_text = self.airflow.get_task_log(dag_id, run_id, task_id, try_number)
                tail = self._extract_traceback_tail(log_text)
                summaries.append(f"[{task_id}] {tail}")
            except AirflowApiError as exc:
                summaries.append(f"[{task_id}] 日志获取失败: {exc}")
        return "\n".join(summaries)

    @staticmethod
    def _extract_traceback_tail(log_text: str, max_chars: int = 1500) -> str:
        """从 Airflow 3 结构化 JSON 日志中提取失败异常摘要。

        Airflow 3 任务日志每行是一个 JSON 事件；真正的异常在
        ``error_detail.exc_type / exc_value`` 与调用栈帧中，不再是传统纯文本 Traceback。
        解析失败时回退到关键词过滤与末尾若干行。
        """
        import ast as _ast
        import json as _json

        summaries: list[str] = []
        fallback: list[str] = []
        for raw in log_text.splitlines():
            line = raw.strip()
            if not line:
                continue
            fallback.append(line)
            event: Any = None
            try:
                event = _json.loads(line)
            except (_json.JSONDecodeError, ValueError):
                try:
                    # Airflow 3 部分日志项以 Python dict 字面量（单引号）返回，兼容解析
                    event = _ast.literal_eval(line)
                except (ValueError, SyntaxError):
                    if "Traceback" in line or "Error" in line or "raise " in line:
                        summaries.append(line)
                    continue
            if not isinstance(event, dict):
                continue
            error_detail = event.get("error_detail")
            if isinstance(error_detail, list) and error_detail:
                error_detail = error_detail[0]
            if event.get("event") == "Task failed with exception" and isinstance(error_detail, dict):
                exc_type = error_detail.get("exc_type", "Exception")
                exc_value = error_detail.get("exc_value", "")
                frames = error_detail.get("frames") or []
                last = frames[-1] if frames else {}
                summaries.append(f"{exc_type}: {exc_value}")
                if last:
                    summaries.append(
                        f"  出错位置: {last.get('filename')}:{last.get('lineno')} "
                        f"({last.get('name')})"
                    )
            elif event.get("level") == "error" and event.get("event"):
                summaries.append(str(event["event"]))

        if summaries:
            snippet = "\n".join(summaries[-8:])
        else:
            snippet = "\n".join(fallback[-12:])
        return snippet[-max_chars:]

    @staticmethod
    def _intent_to_dict(intent: Intent) -> Dict[str, Any]:
        return {
            "dag_id": intent.dag_id,
            "skill": intent.skill,
            "conf": intent.conf,
            "rationale": intent.rationale,
            "confidence": intent.confidence,
            "source": intent.source,
            "schedule_hint": intent.schedule_hint,
        }
