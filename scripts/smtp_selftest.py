"""宿主机独立 QQ SMTP 自测：不启动 Airflow，直接调用邮件 Skill。

脚本只从本地被 .gitignore 排除的根目录 .env 读取配置；输出中不打印 SMTP 授权码。
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def load_local_env() -> None:
    env_file = ROOT / ".env"
    for raw in env_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())
    # 宿主机自测写到本机挂载产物目录；容器运行时仍使用 /opt/airflow/output。
    os.environ["EMAIL_OUTPUT_DIR"] = str(ROOT / "output")


def main() -> int:
    load_local_env()
    from skills.email_sender import run_email_task

    cst = timezone(timedelta(hours=8))
    now = datetime.now(cst)
    payload = {
        "report_date": now.strftime("%Y-%m-%d"),
        "report_title": "Airflow 课程工程 SMTP 自测",
        "greeting": "自测收件人您好：",
        "recipient_name": "本人测试邮箱",
        "metrics": {
            "工程": "airflow-course",
            "链路": "宿主机 Skill 独立自测",
            "发送时间": now.strftime("%Y-%m-%d %H:%M:%S"),
        },
        "highlights": [
            "这是一封 Airflow 课程工程通过 QQ SMTP 发出的真实测试邮件。",
            "收到本邮件说明发件邮箱、授权码、收件人链路配置正确。",
        ],
    }
    conf = {
        "subject": "Airflow 课程工程 SMTP 自测",
        "recipient_name": "本人测试邮箱",
    }
    run_id = f"smtp-selftest-{now.strftime('%Y%m%d-%H%M%S')}"
    result = run_email_task(payload=payload, conf=conf, run_id=run_id)
    send_result = result["send_result"]

    if send_result.get("status") != "SENT":
        print(json.dumps({"ok": False, "result": result}, ensure_ascii=False, indent=2))
        return 1

    summary = {
        "ok": True,
        "config_mode": result["config_mode"],
        "status": send_result["status"],
        "recipients": send_result["recipients"],
        "subject": send_result["subject"],
        "message_id": send_result["message_id"],
        "draft_path": send_result.get("draft_path"),
        "duration_ms": send_result["duration_ms"],
        "timestamp": send_result["timestamp"],
        "run_id": run_id,
    }
    evidence = ROOT / "logs" / "smtp-selftest.json"
    evidence.parent.mkdir(exist_ok=True)
    evidence.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
