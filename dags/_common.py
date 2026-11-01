"""DAG 公共引导与回调：sys.path 引导、成功/失败监控落盘。

所有课程 DAG 统一通过本模块完成路径引导，避免重复样板代码；
on_success_callback / on_failure_callback 把每次运行的状态快照写入
output/status/，供 LLM 网关与人工监控复盘。
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

# Airflow 容器内项目根目录；宿主机测试时自动回退到当前文件上两级
_CONTAINER_ROOT = "/opt/airflow"
if _CONTAINER_ROOT not in sys.path and Path(_CONTAINER_ROOT).is_dir():
    sys.path.insert(0, _CONTAINER_ROOT)
else:
    _host_root = str(Path(__file__).resolve().parent.parent)
    if _host_root not in sys.path:
        sys.path.insert(0, _host_root)

log = logging.getLogger("course.dags.common")

CST = timezone(timedelta(hours=8))
OUTPUT_DIR = os.environ.get("EMAIL_OUTPUT_DIR", "/opt/airflow/output")


def get_conf(context: Dict[str, Any]) -> Dict[str, Any]:
    """安全读取 DAG run 的运行参数 conf。"""
    dag_run = context.get("dag_run")
    if dag_run is not None and getattr(dag_run, "conf", None):
        return dict(dag_run.conf)
    return {}


def _snapshot_path(dag_id: str, run_id: Optional[str]) -> Path:
    status_dir = Path(OUTPUT_DIR) / "status"
    status_dir.mkdir(parents=True, exist_ok=True)
    safe = f"{dag_id}__{run_id or 'manual'}"
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in safe)
    return status_dir / f"{safe}.json"


def _status_snapshot(context: Dict[str, Any], status: str) -> Dict[str, Any]:
    dag_run = context.get("dag_run")
    ti = context.get("task_instance") or context.get("ti")
    exception = context.get("exception")
    dag_id = dag_run.dag_id if dag_run else context.get("dag").dag_id
    run_id = dag_run.run_id if dag_run else None

    # Airflow 3 的 DagRun SDK 对象不再提供 get_task_instances()；
    # 回调按任务先后触发，这里读取上一次快照合并各任务状态，形成累积执行画像。
    task_states: Dict[str, str] = {}
    prev_path = _snapshot_path(dag_id, run_id)
    if prev_path.exists():
        try:
            prev = json.loads(prev_path.read_text(encoding="utf-8"))
            task_states.update(prev.get("task_states", {}))
        except Exception:  # pragma: no cover - 快照损坏不影响任务执行
            log.warning("历史状态快照读取失败，忽略: %s", prev_path)
    if ti is not None:
        task_states[ti.task_id] = ti.state or "unknown"

    # 已有失败任务时整体状态保持 FAILED，后续成功任务不能覆盖失败结论
    if any(state in ("failed", "upstream_failed") for state in task_states.values()):
        status = "FAILED"

    logical_date = getattr(dag_run, "logical_date", None) or getattr(
        dag_run, "execution_date", None
    )
    return {
        "status": status,
        "dag_id": dag_id,
        "run_id": run_id,
        "logical_date": str(logical_date),
        "conf": get_conf(context),
        "task_states": task_states,
        "error": str(exception) if exception is not None else None,
        "snapshot_at": datetime.now(CST).isoformat(),
    }


def _write_snapshot(snapshot: Dict[str, Any]) -> str:
    path = _snapshot_path(snapshot["dag_id"], snapshot["run_id"])
    existing: Dict[str, Any] = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # pragma: no cover - 快照损坏不影响本次任务执行
            log.warning("历史状态快照读取失败，忽略: %s", path)

    # 并行任务的成功回调可能同时读旧快照；写入前再次合并已有任务状态，避免后写覆盖先写。
    merged_states = dict(existing.get("task_states", {}))
    merged_states.update(snapshot.get("task_states", {}))
    snapshot["task_states"] = merged_states
    if any(state in ("failed", "upstream_failed") for state in merged_states.values()):
        snapshot["status"] = "FAILED"

    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp_path, path)
    return str(path)


def on_success_callback(context: Dict[str, Any]) -> None:
    """DAG 整体成功回调：记录成功快照与耗时。"""
    snapshot = _status_snapshot(context, "SUCCESS")
    path = _write_snapshot(snapshot)
    log.info("DAG 运行成功，状态快照已写入: %s", path)


def on_failure_callback(context: Dict[str, Any]) -> None:
    """DAG/任务失败回调：记录失败快照、异常堆栈摘要，供异常处理演示。"""
    snapshot = _status_snapshot(context, "FAILED")
    path = _write_snapshot(snapshot)
    log.error("DAG/任务运行失败，状态快照已写入: %s；异常: %s", path, snapshot["error"])
