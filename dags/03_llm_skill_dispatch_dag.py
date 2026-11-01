"""
DAG 03 —— LLM 网关驱动的通用 Skill 调度 DAG（任务 3 执行侧）
================================================================

本 DAG 不配置定时调度，只接受 LLM 网关通过 Airflow REST API 触发：
自然语言 -> 大模型意图解析 -> 选择已注册 Skill -> 网关 POST dagRun(conf)
-> 本 DAG 的 route_execute 任务按 conf.skill 路由到对应 Skill 执行 -> 回传状态。

conf 结构：
{
  "skill": "send_email" | "collect_data" | "archive",
  "payload": { ...Skill 业务参数... },
  "conf":    { ...邮件收件人/主题等运行时覆盖参数... }
}

已注册 Skill 与 llm_bridge/intent_registry.json 保持一一对应；
未注册 Skill 直接抛出 SkillNotFoundError，由失败回调记录异常快照。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from _common import get_conf, on_success_callback, on_failure_callback, OUTPUT_DIR
from skills.data_collector.collector import collect_report_payload
from skills.email_sender import run_email_task
from skills.archiver import archive_run

log = logging.getLogger("course.dag.llm_dispatch")
CST = timezone(timedelta(hours=8))


class SkillNotFoundError(Exception):
    """LLM 选择了注册表之外的 Skill。"""


def _registry():
    """Skill 注册表：skill 名称 -> 执行函数。"""
    return {
        "send_email": lambda payload, conf, run_id: run_email_task(payload, conf, run_id),
        "collect_data": lambda payload, conf, run_id: collect_report_payload(conf or payload),
        "archive": lambda payload, conf, run_id: archive_run(payload, OUTPUT_DIR, run_id),
    }


def route_execute(**kwargs) -> Dict[str, Any]:
    conf = get_conf(kwargs)
    skill_name = conf.get("skill")
    payload = conf.get("payload") or {}
    runtime_conf = conf.get("conf") or {}
    run_id = kwargs["dag_run"].run_id

    registry = _registry()
    if skill_name not in registry:
        raise SkillNotFoundError(
            f"未注册的 Skill: {skill_name!r}；可选: {sorted(registry.keys())}"
        )

    log.info("LLM 路由 -> Skill=%s, run_id=%s", skill_name, run_id)
    result = registry[skill_name](payload, runtime_conf, run_id)
    kwargs["ti"].xcom_push(key="dispatch_result", value={"skill": skill_name, "result": result})
    return {"skill": skill_name, "status": "EXECUTED", "result": result}


default_args = {
    "owner": "course",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_failure_callback,
    "on_success_callback": on_success_callback,
}

with DAG(
    dag_id="llm_skill_dispatch",
    description="任务3：由 LLM 网关通过 REST API 触发的通用 Skill 调度 DAG",
    default_args=default_args,
    schedule=None,  # 仅手动 / API 触发
    start_date=datetime(2026, 11, 1, tzinfo=CST),
    catchup=False,
    max_active_runs=1,
    tags=["course", "llm", "dispatch", "api"],
    params={
        "skill": "send_email",
        "payload": {},
        "conf": {},
    },
    doc_md=__doc__,
) as dag:

    t_route = PythonOperator(
        task_id="route_execute",
        python_callable=route_execute,
    )
