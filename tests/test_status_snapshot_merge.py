"""DAG 状态快照并发合并测试：并行任务回调不能互相覆盖任务状态。"""

from __future__ import annotations

import importlib
import json
import logging
import os
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "dags"))


def _context(task_id: str, state: str) -> dict:
    return {
        "dag_run": SimpleNamespace(
            dag_id="demo_pipeline",
            run_id="scheduled__2026-11-01T01:00:00+00:00",
            logical_date="2026-11-01 01:00:00+00:00",
            conf={},
        ),
        "task_instance": SimpleNamespace(task_id=task_id, state=state),
        "exception": None,
    }


def _load_common(tmp_path: Path):
    os.environ["EMAIL_OUTPUT_DIR"] = str(tmp_path)
    if "_common" in sys.modules:
        del sys.modules["_common"]
    common = importlib.import_module("_common")
    logging.getLogger("course.dags.common").setLevel(logging.CRITICAL)
    return common


def test_parallel_callbacks_preserve_all_task_states(tmp_path: Path) -> None:
    common = _load_common(tmp_path)

    common.on_success_callback(_context("prepare_report_data", "success"))
    common.on_success_callback(_context("render_email_draft", "success"))
    common.on_success_callback(_context("prepare_attachment", "success"))
    common.on_success_callback(_context("send_report_email", "success"))
    common.on_success_callback(_context("archive_and_verify", "success"))

    snapshot_path = tmp_path / "status" / (
        "demo_pipeline__scheduled__2026-11-01T01_00_00_00_00.json"
    )
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))

    assert snapshot["status"] == "SUCCESS"
    assert snapshot["task_states"] == {
        "prepare_report_data": "success",
        "render_email_draft": "success",
        "prepare_attachment": "success",
        "send_report_email": "success",
        "archive_and_verify": "success",
    }


def test_failed_state_is_not_overwritten_by_later_success(tmp_path: Path) -> None:
    common = _load_common(tmp_path)

    common.on_failure_callback(_context("route_execute", "failed"))
    common.on_success_callback(_context("cleanup_summary", "success"))

    snapshot_path = tmp_path / "status" / (
        "demo_pipeline__scheduled__2026-11-01T01_00_00_00_00.json"
    )
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))

    assert snapshot["status"] == "FAILED"
    assert snapshot["task_states"]["route_execute"] == "failed"
    assert snapshot["task_states"]["cleanup_summary"] == "success"


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as directory:
        test_parallel_callbacks_preserve_all_task_states(Path(directory))
    with tempfile.TemporaryDirectory() as directory:
        test_failed_state_is_not_overwritten_by_later_success(Path(directory))
    print("test_status_snapshot_merge.py 全部通过")
