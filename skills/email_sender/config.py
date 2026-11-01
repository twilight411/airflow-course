"""邮件 Skill 配置加载与校验。

所有配置来自环境变量（由 docker-compose.yaml 从 .env 注入容器），
DAG run 的 conf 可以覆盖收件人等运行时参数。

设计约定：
- 未配置 SMTP 账号 / 授权码时，无论 EMAIL_DRY_RUN 取何值，都强制进入
  DRY_RUN 模式，把邮件渲染后落盘并写模拟发送记录，避免演示现场因缺密钥失败；
- 配置完整且 EMAIL_DRY_RUN=false 时走真实 SMTP 发送。
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import List, Optional

EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


@dataclass
class EmailConfig:
    host: str
    port: int
    secure: str  # "ssl" | "starttls" | "none"
    username: str
    password: str
    sender: str
    default_to: List[str]
    dry_run: bool
    dry_run_reason: Optional[str]
    output_dir: str

    @property
    def credentials_ready(self) -> bool:
        return bool(self.username and self.password and self.sender)


def _split_emails(raw: str) -> List[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.replace(";", ",").split(",") if item.strip()]


def load_email_config(env: Optional[dict] = None) -> EmailConfig:
    env = env if env is not None else os.environ

    host = env.get("EMAIL_SMTP_HOST", "smtp.qq.com").strip()
    port = int(env.get("EMAIL_SMTP_PORT", "465"))
    secure = env.get("EMAIL_SMTP_SECURE", "ssl").strip().lower()
    username = env.get("EMAIL_SMTP_USER", "").strip()
    password = env.get("EMAIL_SMTP_PASSWORD", "").strip()
    sender = env.get("EMAIL_SMTP_FROM", "").strip() or username
    default_to = _split_emails(env.get("EMAIL_DEFAULT_TO", ""))
    output_dir = env.get("EMAIL_OUTPUT_DIR", "/opt/airflow/output").strip()

    declared_dry_run = env.get("EMAIL_DRY_RUN", "true").strip().lower() != "false"

    reason: Optional[str] = None
    if declared_dry_run:
        reason = "EMAIL_DRY_RUN=true（配置显式要求模拟发送）"
    elif not (username and password and sender):
        reason = "SMTP 账号/授权码未配置，自动降级为模拟发送"

    return EmailConfig(
        host=host,
        port=port,
        secure=secure,
        username=username,
        password=password,
        sender=sender,
        default_to=default_to,
        dry_run=reason is not None,
        dry_run_reason=reason,
        output_dir=output_dir,
    )


def recipients_from_conf(conf: Optional[dict], config: EmailConfig) -> List[str]:
    """优先使用 DAG run conf 中的收件人；否则回落到环境默认收件人。"""
    conf = conf or {}
    raw_to = conf.get("to") or conf.get("recipient") or conf.get("recipients")
    recipients = _split_emails(raw_to) if isinstance(raw_to, str) else list(raw_to or [])
    if not recipients:
        recipients = list(config.default_to)
    if not recipients:
        # 演示兜底：dry-run 模式下没有真实收件人也能完成链路，收件地址用占位值记录
        recipients = ["demo-recipient@example.com"]
    for addr in recipients:
        if not EMAIL_PATTERN.match(addr):
            raise ValueError(f"收件人邮箱格式不合法: {addr!r}")
    return recipients
