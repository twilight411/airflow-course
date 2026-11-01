"""邮件 Skill 离线单元测试：DRY_RUN 渲染、落盘、记录与参数校验。

运行：python tests/test_email_skill.py  （也兼容 pytest 自动发现）
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _setup_env(tmp: Path) -> None:
    os.environ["EMAIL_SMTP_HOST"] = "smtp.qq.com"
    os.environ["EMAIL_SMTP_PORT"] = "465"
    os.environ["EMAIL_SMTP_SECURE"] = "ssl"
    os.environ["EMAIL_SMTP_USER"] = ""
    os.environ["EMAIL_SMTP_PASSWORD"] = ""
    os.environ["EMAIL_SMTP_FROM"] = ""
    os.environ["EMAIL_DEFAULT_TO"] = ""
    os.environ["EMAIL_DRY_RUN"] = "true"
    os.environ["EMAIL_OUTPUT_DIR"] = str(tmp)


def test_dry_run_send_writes_draft_and_record(tmp_path: Path) -> None:
    _setup_env(tmp_path)
    from skills.email_sender import run_email_task

    payload = {
        "report_date": "2026-11-01",
        "report_title": "测试日报",
        "metrics": {"成功任务数": 11},
        "highlights": ["Skill 链路测试"],
    }
    result = run_email_task(
        payload,
        conf={"to": "demo@example.com", "subject": "单元测试邮件", "recipient_name": "老师"},
        run_id="test-run-001",
    )
    send = result["send_result"]
    assert send["status"] == "DRY_RUN_OK"
    assert Path(send["draft_path"]).exists()
    record = tmp_path / "send_records.jsonl"
    assert record.exists()
    last_line = record.read_text(encoding="utf-8").strip().splitlines()[-1]
    assert "DRY_RUN_OK" in last_line


def test_invalid_recipient_raises(tmp_path: Path) -> None:
    _setup_env(tmp_path)
    from skills.email_sender import run_email_task

    payload = {"report_date": "2026-11-01", "metrics": {}, "highlights": []}
    try:
        run_email_task(payload, conf={"to": "not-an-email"}, run_id="test-bad-addr")
        raise AssertionError("非法邮箱应当抛出 ValueError")
    except ValueError:
        pass


def test_missing_credentials_forces_dry_run(tmp_path: Path) -> None:
    os.environ["EMAIL_DRY_RUN"] = "false"  # 声明真实发送，但凭据为空
    os.environ["EMAIL_SMTP_USER"] = ""
    os.environ["EMAIL_SMTP_PASSWORD"] = ""
    os.environ["EMAIL_SMTP_FROM"] = ""
    os.environ["EMAIL_OUTPUT_DIR"] = str(tmp_path)
    from skills.email_sender.config import load_email_config

    cfg = load_email_config()
    assert cfg.dry_run is True
    assert "授权码未配置" in (cfg.dry_run_reason or "")


if __name__ == "__main__":
    import tempfile as _tf
    with _tf.TemporaryDirectory() as d:
        test_dry_run_send_writes_draft_and_record(Path(d))
    with _tf.TemporaryDirectory() as d:
        test_invalid_recipient_raises(Path(d))
    with _tf.TemporaryDirectory() as d:
        test_missing_credentials_forces_dry_run(Path(d))
    print("test_email_skill.py 全部通过")
