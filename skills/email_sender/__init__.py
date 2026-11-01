"""email_sender Skill：SMTP 邮件渲染、发送、落盘与记录。"""

from .config import EmailConfig, load_email_config, recipients_from_conf
from .sender import (
    EmailSkillError,
    build_message,
    render_report,
    save_draft,
    send_message,
    append_record,
    run_email_task,
)

__all__ = [
    "EmailConfig",
    "load_email_config",
    "recipients_from_conf",
    "EmailSkillError",
    "build_message",
    "render_report",
    "save_draft",
    "send_message",
    "append_record",
    "run_email_task",
]
