"""归档 Skill 并发记录匹配测试：避免多个 DAG 同时运行时串用最后一条记录。"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from skills.archiver import archive_run, verify_last_record


def _seed_two_runs(tmp: Path) -> None:
    drafts = tmp / "drafts"
    drafts.mkdir(parents=True, exist_ok=True)
    draft_a = drafts / "a.eml"
    draft_b = drafts / "b.eml"
    draft_a.write_text("draft A", encoding="utf-8")
    draft_b.write_text("draft B", encoding="utf-8")

    records = [
        {
            "status": "DRY_RUN_OK",
            "run_id": "run-a",
            "message_id": "msg-a",
            "draft_path": str(draft_a),
        },
        {
            "status": "DRY_RUN_OK",
            "run_id": "run-b",
            "message_id": "msg-b",
            "draft_path": str(draft_b),
        },
    ]
    (tmp / "send_records.jsonl").write_text(
        "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
        encoding="utf-8",
    )


def test_archive_matches_current_run_not_global_last(tmp_path: Path) -> None:
    _seed_two_runs(tmp_path)

    summary = {
        "skill": "email_sender",
        "run_id": "run-a",
        "send_result": {
            "status": "DRY_RUN_OK",
            "message_id": "msg-a",
            "draft_path": str(tmp_path / "drafts" / "a.eml"),
            "recipients": ["a@example.com"],
        },
    }
    result = archive_run(summary, str(tmp_path), "run-a")
    archived = json.loads(
        Path(result["archive_dir"]).joinpath("run_summary.json").read_text(encoding="utf-8")
    )

    assert archived["send_record"]["message_id"] == "msg-a"
    assert archived["send_record"]["run_id"] == "run-a"
    archived_files = sorted(p.name for p in Path(result["archive_dir"]).iterdir())
    assert "a.eml" in archived_files
    assert "b.eml" not in archived_files

    record_b = verify_last_record(str(tmp_path), run_id="run-b")
    assert record_b["message_id"] == "msg-b"


def test_dispatch_archive_branch_keeps_last_record_fallback(tmp_path: Path) -> None:
    _seed_two_runs(tmp_path)

    # DAG03 archive Skill 本身不发邮件，保留校验最近一条发送记录的历史语义。
    result = archive_run({}, str(tmp_path), "run-c")
    archived = json.loads(
        Path(result["archive_dir"]).joinpath("run_summary.json").read_text(encoding="utf-8")
    )

    assert archived["send_record"]["status"] == "DRY_RUN_OK"
    assert archived["send_record"]["run_id"] == "run-b"


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as directory:
        test_archive_matches_current_run_not_global_last(Path(directory))
    with tempfile.TemporaryDirectory() as directory:
        test_dispatch_archive_branch_keeps_last_record_fallback(Path(directory))
    print("test_archiver_record_match.py 全部通过")
