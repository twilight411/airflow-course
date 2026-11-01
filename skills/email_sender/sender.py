"""email_sender Skill 执行体：渲染邮件、保存草稿、SMTP 发送、落盘记录。

仅依赖 Python 标准库（smtplib / email / json / logging），可同时运行于：
- Airflow Celery Worker 容器内；
- 宿主机单元测试环境。

异常模型：
- 配置/收件人非法 -> ValueError（任务直接失败，不进入 SMTP 重试）；
- SMTP 连接、登录、发送阶段失败 -> EmailSkillError（Airflow retries 负责重试）；
- DRY_RUN 模式下不触达网络，仅落盘草稿与发送记录，状态记为 DRY_RUN_OK。
"""

from __future__ import annotations

import json
import logging
import os
import ssl
import smtplib
import time
import uuid
from datetime import datetime, timezone, timedelta
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import EmailConfig, load_email_config, recipients_from_conf

log = logging.getLogger("skill.email_sender")

CST = timezone(timedelta(hours=8))


class EmailSkillError(Exception):
    """SMTP 发送阶段的可重试异常。"""


# --------------------------------------------------------------------------- #
# 模板渲染
# --------------------------------------------------------------------------- #
def render_report(context: Dict[str, Any]) -> Dict[str, str]:
    """根据报表上下文生成主题、纯文本与 HTML 正文。

    context 关键字段：
      report_date, report_title, greeting, recipient_name,
      metrics: {指标名: 数值}, highlights: [str], closing
    """
    report_date = context.get("report_date") or datetime.now(CST).strftime("%Y-%m-%d")
    title = context.get("report_title") or f"{report_date} 自动化工作流日报"
    greeting = context.get("greeting") or "各位老师、同学："
    recipient_name = context.get("recipient_name") or "老师"
    highlights: List[str] = context.get("highlights") or []
    metrics: Dict[str, Any] = context.get("metrics") or {}
    closing = context.get("closing") or "本邮件由 Airflow 自动化工作流通过 email_sender Skill 发送，如有疑问请回复本邮件。"

    metric_lines = "\n".join(f"  - {k}: {v}" for k, v in metrics.items())
    highlight_lines = "\n".join(f"  {i + 1}. {h}" for i, h in enumerate(highlights))

    text = (
        f"{greeting}\n\n"
        f"以下是 {report_date} 的工作流执行情况汇报：\n\n"
        f"一、核心指标\n{metric_lines or '  - （本次无指标数据）'}\n\n"
        f"二、重点事项\n{highlight_lines or '  - （本次无重点事项）'}\n\n"
        f"{closing}\n\n"
        f"—— Airflow 自动化工作流 · email_sender Skill\n"
    )

    metric_rows = "".join(
        f"<tr><td>{k}</td><td style='text-align:right'>{v}</td></tr>" for k, v in metrics.items()
    )
    highlight_items = "".join(f"<li>{h}</li>" for h in highlights)
    html = f"""<html><body style="font-family:'Microsoft YaHei',Arial,sans-serif;line-height:1.8;color:#222">
  <h2 style="color:#1f4e79">{title}</h2>
  <p>{greeting} {recipient_name}</p>
  <p>以下是 <b>{report_date}</b> 的工作流执行情况汇报：</p>
  <h3>一、核心指标</h3>
  <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse">
    <thead><tr><th>指标</th><th>数值</th></tr></thead><tbody>{metric_rows}</tbody>
  </table>
  <h3>二、重点事项</h3>
  <ul>{highlight_items or '<li>（本次无重点事项）</li>'}</ul>
  <p style="color:#666;font-size:12px">{closing}</p>
  <hr style="border:none;border-top:1px solid #ddd">
  <p style="color:#999;font-size:12px">Airflow 自动化工作流 · email_sender Skill 自动生成</p>
</body></html>"""

    return {"subject": title, "text": text, "html": html}


# --------------------------------------------------------------------------- #
# 邮件组装 / 草稿落盘 / 记录
# --------------------------------------------------------------------------- #
def build_message(
    recipients: List[str],
    subject: str,
    text_body: str,
    html_body: Optional[str] = None,
    cc: Optional[List[str]] = None,
    config: Optional[EmailConfig] = None,
) -> EmailMessage:
    message = EmailMessage()
    config = config or load_email_config()
    message["From"] = config.sender or config.username
    message["To"] = ", ".join(recipients)
    if cc:
        message["Cc"] = ", ".join(cc)
    message["Subject"] = subject
    message["Date"] = formatdate(localtime=True)
    message["Message-ID"] = make_msgid(domain="airflow-course.local")

    message.set_content(text_body)
    if html_body:
        message.add_alternative(html_body, subtype="html")
    return message


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_draft(message: EmailMessage, output_dir: str, run_id: str) -> str:
    """把组装好的邮件保存为 .eml 草稿，返回绝对路径。"""
    safe_run = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in run_id)
    drafts = _ensure_dir(Path(output_dir) / "drafts")
    stamp = datetime.now(CST).strftime("%Y%m%d-%H%M%S")
    draft_path = drafts / f"{stamp}_{safe_run}.eml"
    draft_path.write_bytes(message.as_bytes())
    log.info("草稿已保存: %s", draft_path)
    return str(draft_path)


def append_record(record: Dict[str, Any], output_dir: str) -> str:
    records = _ensure_dir(Path(output_dir)) / "send_records.jsonl"
    with records.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    return str(records)


# --------------------------------------------------------------------------- #
# SMTP 发送
# --------------------------------------------------------------------------- #
def _open_smtp(config: EmailConfig):
    if config.secure == "ssl":
        context = ssl.create_default_context()
        client = smtplib.SMTP_SSL(config.host, config.port, timeout=20, context=context)
        return client
    client = smtplib.SMTP(config.host, config.port, timeout=20)
    client.ehlo()
    if config.secure == "starttls":
        context = ssl.create_default_context()
        client.starttls(context=context)
        client.ehlo()
    return client


def send_message(
    message: EmailMessage,
    config: Optional[EmailConfig] = None,
    run_id: str = "manual",
) -> Dict[str, Any]:
    """发送邮件；DRY_RUN 模式仅记录模拟发送结果。

    返回结构化结果字典，供 XCom 与监控任务消费。
    """
    config = config or load_email_config()
    started = time.perf_counter()

    if config.dry_run:
        draft_path = save_draft(message, config.output_dir, run_id)
        elapsed = round((time.perf_counter() - started) * 1000, 2)
        result: Dict[str, Any] = {
            "status": "DRY_RUN_OK",
            "mode": "dry-run",
            "reason": config.dry_run_reason,
            "recipients": message["To"],
            "subject": message["Subject"],
            "message_id": message["Message-ID"],
            "draft_path": draft_path,
            "duration_ms": elapsed,
            "timestamp": datetime.now(CST).isoformat(),
            "run_id": run_id,
        }
        append_record(result, config.output_dir)
        log.info("DRY_RUN 模拟发送完成: %s", result["subject"])
        return result

    if not config.credentials_ready:
        raise EmailSkillError("SMTP 凭据不完整，无法真实发送")

    client = None
    try:
        client = _open_smtp(config)
        if config.username:
            client.login(config.username, config.password)
        refused = client.send_message(message)
        if refused:
            raise EmailSkillError(f"以下收件人被服务器拒绝: {refused}")
        elapsed = round((time.perf_counter() - started) * 1000, 2)
        result = {
            "status": "SENT",
            "mode": "smtp",
            "reason": None,
            "recipients": message["To"],
            "subject": message["Subject"],
            "message_id": message["Message-ID"],
            "draft_path": None,
            "duration_ms": elapsed,
            "timestamp": datetime.now(CST).isoformat(),
            "run_id": run_id,
        }
        append_record(result, config.output_dir)
        log.info("真实邮件已发送: %s -> %s", result["subject"], result["recipients"])
        return result
    except (smtplib.SMTPException, OSError, ssl.SSLError) as exc:
        elapsed = round((time.perf_counter() - started) * 1000, 2)
        error_record = {
            "status": "FAILED",
            "mode": "smtp",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "recipients": message["To"],
            "subject": message["Subject"],
            "duration_ms": elapsed,
            "timestamp": datetime.now(CST).isoformat(),
            "run_id": run_id,
        }
        append_record(error_record, config.output_dir)
        log.exception("SMTP 发送失败")
        raise EmailSkillError(f"邮件发送失败 [{type(exc).__name__}]: {exc}") from exc
    finally:
        if client is not None:
            try:
                client.quit()
            except Exception:  # pragma: no cover - 关闭阶段异常不影响主流程
                pass


# --------------------------------------------------------------------------- #
# 高层任务入口（PythonOperator 直接调用）
# --------------------------------------------------------------------------- #
def run_email_task(
    payload: Dict[str, Any],
    conf: Optional[Dict[str, Any]] = None,
    run_id: str = "manual",
) -> Dict[str, Any]:
    """组装 -> 渲染 -> 保存草稿 -> 发送 的完整 Skill 调用链，返回汇总结果。"""
    config = load_email_config()
    recipients = recipients_from_conf(conf, config)

    # conf 允许覆盖主题 / 正文，便于 LLM 网关传入自然语言生成的内容
    merged_context = dict(payload)
    if conf:
        if conf.get("subject"):
            merged_context["report_title"] = conf["subject"]
        if conf.get("text_body"):
            merged_context["custom_text_body"] = conf["text_body"]
        if conf.get("html_body"):
            merged_context["custom_html_body"] = conf["html_body"]
        if conf.get("recipient_name"):
            merged_context["recipient_name"] = conf["recipient_name"]

    rendered = render_report(merged_context)
    subject = conf.get("subject") if conf and conf.get("subject") else rendered["subject"]
    text_body = merged_context.get("custom_text_body") or rendered["text"]
    html_body = merged_context.get("custom_html_body") or rendered["html"]

    message = build_message(recipients, subject, text_body, html_body, config=config)
    draft_path = save_draft(message, config.output_dir, run_id)

    send_result = send_message(message, config=config, run_id=run_id)
    send_result["draft_path"] = send_result.get("draft_path") or draft_path
    send_result["recipients"] = recipients
    return {
        "skill": "email_sender",
        "run_id": run_id,
        "config_mode": "dry-run" if config.dry_run else "smtp",
        "send_result": send_result,
    }
