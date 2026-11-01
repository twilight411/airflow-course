"""
DAG 02 —— 多 Skill 并行分支流水线（拓展探索 DAG）
=====================================================

在 DAG 01 线性依赖的基础上，本 DAG 演示：
- 并行任务分支：邮件渲染 与 附件摘要准备 两条分支并行执行；
- 分支汇合：join 节点等待两条分支都成功后才发送（join 节点隐式 ALL_SUCCESS）；
- 更多 Skill 编排：data_collector / email_sender / archiver 复用同一套 Skill 实现。

流程：
                         ┌─> render_email_draft ─┐
    prepare_report_data ─┤                         ├─> send_report_email ─> archive_and_verify
                         └─> prepare_attachment ──┘

调度：每天 18:00（Asia/Shanghai），可手动触发并通过 conf 覆盖收件人与正文。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from _common import get_conf, on_success_callback, on_failure_callback, OUTPUT_DIR
from skills.data_collector.collector import collect_report_payload
from skills.email_sender import run_email_task
from skills.archiver import archive_run

log = logging.getLogger("course.dag.skill_pipeline")
CST = timezone(timedelta(hours=8))

default_args = {
    "owner": "course",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_failure_callback,
    "on_success_callback": on_success_callback,
}


def prepare_report_data(**kwargs) -> dict:
    conf = get_conf(kwargs)
    payload = collect_report_payload(conf)
    kwargs["ti"].xcom_push(key="report_payload", value=payload)
    return {"report_date": payload["report_date"]}


def prepare_attachment(**kwargs) -> dict:
    """并行分支 B：生成附件摘要文件（模拟附件准备 Skill）。"""
    ti = kwargs["ti"]
    payload = ti.xcom_pull(task_ids="prepare_report_data", key="report_payload")
    att_dir = Path(OUTPUT_DIR) / "attachments"
    att_dir.mkdir(parents=True, exist_ok=True)
    att_path = att_dir / f"{kwargs['dag_run'].run_id}.txt"
    lines = [f"报表日期: {payload['report_date']} {payload['weekday']}", "核心指标:"]
    lines += [f"  - {k}: {v}" for k, v in payload["metrics"].items()]
    att_path.write_text("\n".join(lines), encoding="utf-8")
    log.info("附件摘要已生成: %s", att_path)
    return {"attachment_path": str(att_path)}


def render_email_draft(**kwargs) -> dict:
    """并行分支 A：渲染邮件草稿（与附件分支并行）。"""
    from skills.email_sender import render_report, build_message, save_draft
    from skills.email_sender.config import load_email_config, recipients_from_conf

    ti = kwargs["ti"]
    conf = get_conf(kwargs)
    payload = ti.xcom_pull(task_ids="prepare_report_data", key="report_payload")
    rendered = render_report(payload)
    config = load_email_config()
    recipients = recipients_from_conf(conf, config)
    message = build_message(
        recipients, conf.get("subject") or rendered["subject"],
        conf.get("text_body") or rendered["text"],
        conf.get("html_body") or rendered["html"], config=config,
    )
    draft_path = save_draft(message, OUTPUT_DIR, kwargs["dag_run"].run_id)
    return {"draft_path": draft_path, "subject": message["Subject"]}


def send_report_email(**kwargs) -> dict:
    """汇合节点：两条分支均成功后执行发送。"""
    ti = kwargs["ti"]
    conf = get_conf(kwargs)
    payload = ti.xcom_pull(task_ids="prepare_report_data", key="report_payload")
    result = run_email_task(payload, conf, run_id=kwargs["dag_run"].run_id)
    ti.xcom_push(key="send_result", value=result)
    return result


def archive_and_verify(**kwargs) -> dict:
    ti = kwargs["ti"]
    send_result = ti.xcom_pull(task_ids="send_report_email", key="send_result")
    return archive_run(send_result, OUTPUT_DIR, kwargs["dag_run"].run_id)


with DAG(
    dag_id="skill_pipeline_evening",
    description="拓展：并行分支多 Skill 流水线，演示分支并行与汇合依赖",
    default_args=default_args,
    schedule="0 18 * * *",
    start_date=datetime(2026, 11, 1, tzinfo=CST),
    catchup=False,
    max_active_runs=1,
    tags=["course", "skill", "pipeline", "parallel"],
    doc_md=__doc__,
) as dag:

    t_prepare = PythonOperator(task_id="prepare_report_data", python_callable=prepare_report_data)
    t_attach = PythonOperator(task_id="prepare_attachment", python_callable=prepare_attachment)
    t_render = PythonOperator(task_id="render_email_draft", python_callable=render_email_draft)
    t_send = PythonOperator(task_id="send_report_email", python_callable=send_report_email)
    t_archive = PythonOperator(task_id="archive_and_verify", python_callable=archive_and_verify)

    t_prepare >> [t_attach, t_render] >> t_send >> t_archive
