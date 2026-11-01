"""
DAG 01 —— 定时邮件汇报工作流（任务 2 主 DAG）
=================================================

业务流程：
    prepare_report_data  ->  render_email_draft  ->  send_report_email  ->  archive_and_verify
        (data_collector Skill)  (email_sender Skill 渲染/存草稿)  (email_sender Skill 发送)  (archiver Skill)

覆盖知识点：
- 任务依赖：线性链式依赖（>> 运算符）；
- 定时调度：cron 表达式 0 9 * * *（每天 09:00，Asia/Shanghai）；
- 执行监控：XCom 传递上下文、任务状态快照回调、归档目录可追溯；
- 异常处理：retries + 指数退避重试、失败回调、execution_timeout、ALL_DONE 兜底汇总任务；
- Skill 集成：三个独立 Skill 被封装为 PythonOperator，可被其他 DAG / LLM 网关复用。

运行参数（手动 Trigger DAG w/ config 或 REST API conf 覆盖）：
{
  "to": "teacher@example.com",
  "subject": "项目进度汇报",
  "recipient_name": "张老师",
  "text_body": "自定义正文（可选）",
  "highlights": ["事项一", "事项二"]
}
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from _common import (  # type: ignore
    get_conf,
    on_success_callback,
    on_failure_callback,
    OUTPUT_DIR,
)
from skills.data_collector.collector import collect_report_payload
from skills.email_sender import render_report, build_message, save_draft, run_email_task
from skills.archiver import archive_run

log = logging.getLogger("course.dag.email_daily_report")
CST = timezone(timedelta(hours=8))

default_args = {
    "owner": "course",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_failure_callback,
    "on_success_callback": on_success_callback,
}


# --------------------------------------------------------------------------- #
# 任务实现
# --------------------------------------------------------------------------- #
def prepare_report_data(**kwargs) -> dict:
    """Skill: data_collector —— 生成日报结构化上下文并写入 XCom。"""
    conf = get_conf(kwargs)
    payload = collect_report_payload(conf)
    kwargs["ti"].xcom_push(key="report_payload", value=payload)
    log.info("报表数据准备完成: %s", payload["report_date"])
    return {"report_date": payload["report_date"], "metrics": payload["metrics"]}


def render_email_draft(**kwargs) -> dict:
    """Skill: email_sender 渲染阶段 —— 生成主题/正文并保存 .eml 草稿。"""
    ti = kwargs["ti"]
    conf = get_conf(kwargs)
    payload = ti.xcom_pull(task_ids="prepare_report_data", key="report_payload")
    dag_run = kwargs["dag_run"]

    rendered = render_report(payload)
    subject = conf.get("subject") or rendered["subject"]
    text_body = conf.get("text_body") or rendered["text"]
    html_body = conf.get("html_body") or rendered["html"]

    from skills.email_sender.config import load_email_config, recipients_from_conf

    config = load_email_config()
    recipients = recipients_from_conf(conf, config)
    message = build_message(recipients, subject, text_body, html_body, config=config)
    draft_path = save_draft(message, OUTPUT_DIR, dag_run.run_id)

    meta = {
        "recipients": recipients,
        "subject": subject,
        "draft_path": draft_path,
        "message_id": message["Message-ID"],
    }
    ti.xcom_push(key="email_meta", value=meta)
    log.info("邮件草稿渲染完成: %s", subject)
    return meta


def send_report_email(**kwargs) -> dict:
    """Skill: email_sender 发送阶段 —— SMTP 真实发送或 DRY_RUN 模拟发送。"""
    ti = kwargs["ti"]
    conf = get_conf(kwargs)
    payload = ti.xcom_pull(task_ids="prepare_report_data", key="report_payload")
    result = run_email_task(payload, conf, run_id=kwargs["dag_run"].run_id)
    ti.xcom_push(key="send_result", value=result)
    return result


def archive_and_verify(**kwargs) -> dict:
    """Skill: archiver —— 校验发送记录并归档草稿、汇总与索引。"""
    ti = kwargs["ti"]
    send_result = ti.xcom_pull(task_ids="send_report_email", key="send_result")
    archive_result = archive_run(send_result, OUTPUT_DIR, kwargs["dag_run"].run_id)
    return archive_result


def final_summary(**kwargs) -> dict:
    """兜底汇总任务：无论上游成功/失败都执行（trigger_rule=ALL_DONE），输出执行画像。

    Airflow 3 的 DagRun SDK 对象不再提供 get_task_instances()，
    这里通过上游 XCom 是否存在来推断成功状态，避免跨模型查询耦合。
    """
    ti = kwargs["ti"]
    upstream_ids = [
        "prepare_report_data", "render_email_draft",
        "send_report_email", "archive_and_verify",
    ]
    task_states: Dict[str, str] = {}
    for task_id in upstream_ids:
        try:
            xcom_value = ti.xcom_pull(task_ids=task_id)
            task_states[task_id] = "success" if xcom_value is not None else "skipped/empty"
        except Exception:  # 上游失败时 XCom 不可用，如实标记
            task_states[task_id] = "failed"
    task_states["final_summary"] = ti.state or "running"

    summary = {
        "dag_id": kwargs["dag_run"].dag_id,
        "run_id": kwargs["dag_run"].run_id,
        "task_states": task_states,
        "finished_at": datetime.now(CST).isoformat(),
    }
    ti.xcom_push(key="dag_summary", value=summary)
    log.info("DAG 执行画像: %s", task_states)
    return summary


# --------------------------------------------------------------------------- #
# DAG 定义
# --------------------------------------------------------------------------- #
with DAG(
    dag_id="email_daily_report",
    description="任务2：data_collector + email_sender + archiver 三 Skill 编排的定时邮件汇报工作流",
    default_args=default_args,
    schedule="0 9 * * *",
    start_date=datetime(2026, 11, 1, tzinfo=CST),
    catchup=False,
    max_active_runs=1,
    tags=["course", "skill", "email", "scheduled"],
    params={
        "to": "",
        "subject": "",
        "recipient_name": "老师",
        "text_body": "",
        "highlights": [],
    },
    doc_md=__doc__,
) as dag:

    t_prepare = PythonOperator(
        task_id="prepare_report_data",
        python_callable=prepare_report_data,
    )

    t_render = PythonOperator(
        task_id="render_email_draft",
        python_callable=render_email_draft,
    )

    t_send = PythonOperator(
        task_id="send_report_email",
        python_callable=send_report_email,
    )

    t_archive = PythonOperator(
        task_id="archive_and_verify",
        python_callable=archive_and_verify,
    )

    t_summary = PythonOperator(
        task_id="final_summary",
        python_callable=final_summary,
        trigger_rule="all_done",
    )

    # 线性依赖链；汇总任务挂在最后，ALL_DONE 保证异常场景也能输出状态画像
    t_prepare >> t_render >> t_send >> t_archive >> t_summary
