"""archiver Skill：运行结果归档与交付校验。

职责：
1. 校验发送记录存在且状态为成功（SENT / DRY_RUN_OK）；
2. 把邮件草稿、发送结果、运行汇总复制到 output/archive/<run_id>/；
3. 追加全局归档索引 index.jsonl，供监控与复盘查询。

并发说明：多个邮件 DAG 可能在相近时间运行，全局 send_records.jsonl 的最后一条
不一定属于当前 DAG Run。邮件 DAG 归档时优先按当前 run_id / message_id 匹配记录；
匹配不到时以上游 XCom 中的 send_result 为准，避免把其他 DAG 的草稿归档到本 Run。
"""

from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

log = logging.getLogger("skill.archiver")

CST = timezone(timedelta(hours=8))
SUCCESS_STATES = {"SENT", "DRY_RUN_OK"}


def _safe_run_id(run_id: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in run_id)


def _load_records(output_dir: str) -> List[Dict[str, Any]]:
    """读取全部发送记录，保留顺序，便于按 run_id / message_id 精确匹配。"""
    record_file = Path(output_dir) / "send_records.jsonl"
    if not record_file.exists():
        raise FileNotFoundError(f"发送记录不存在: {record_file}")
    lines = record_file.read_text(encoding="utf-8").strip().splitlines()
    if not lines:
        raise ValueError("发送记录为空，无法校验交付状态")
    return [json.loads(line) for line in lines if line.strip()]


def verify_last_record(
    output_dir: str,
    run_id: Optional[str] = None,
    message_id: Optional[str] = None,
) -> Dict[str, Any]:
    """校验发送记录状态。

    - 传入 run_id 或 message_id 时，先从后向前匹配当前 Run 的记录；
    - 未传入标识（DAG03 纯归档 Skill）时保持历史语义，校验全局最后一条记录；
    - 传入标识但未匹配到时回落到最后一条，由调用方根据 message_id 决定是否改用 XCom 结果。
    """
    records = _load_records(output_dir)
    matched: Optional[Dict[str, Any]] = None
    if run_id or message_id:
        for record in reversed(records):
            same_run = bool(run_id and record.get("run_id") == run_id)
            same_message = bool(message_id and record.get("message_id") == message_id)
            if same_run or same_message:
                matched = record
                break
    if matched is None:
        matched = records[-1]

    if matched.get("status") not in SUCCESS_STATES:
        raise RuntimeError(
            f"发送记录状态为 {matched.get('status')!r}，交付校验未通过: {matched.get('error')}"
        )
    log.info("交付校验通过: %s", matched.get("status"))
    return matched


def archive_run(
    summary: Dict[str, Any],
    output_dir: str,
    run_id: str,
) -> Dict[str, Any]:
    """归档一次 DAG 运行的全部产物，返回归档目录与索引信息。"""
    summary = summary or {}
    send_result = summary.get("send_result") if isinstance(summary, dict) else None
    summary_run_id = summary.get("run_id") or run_id

    if isinstance(send_result, dict):
        # 邮件 DAG：先尝试从全局记录中定位当前 Run；若并发串扰导致匹配到其他记录，
        # 以上游 XCom 中的发送结果为准，保证归档草稿属于本次 Run。
        verified = verify_last_record(
            output_dir,
            run_id=summary_run_id,
            message_id=send_result.get("message_id"),
        )
        if verified.get("message_id") != send_result.get("message_id"):
            verified = {**send_result, "run_id": summary_run_id, "record_source": "xcom"}
    else:
        # DAG03 archive Skill：本身不发邮件，沿用“校验最近一次发送记录”的课程语义。
        verified = verify_last_record(output_dir)

    safe_run = _safe_run_id(run_id)
    archive_dir = Path(output_dir) / "archive" / safe_run
    archive_dir.mkdir(parents=True, exist_ok=True)

    merged = {
        "archived_at": datetime.now(CST).isoformat(),
        "run_id": run_id,
        "send_record": verified,
        "summary": summary,
    }
    (archive_dir / "run_summary.json").write_text(
        json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    draft_path = verified.get("draft_path")
    if draft_path and Path(draft_path).exists():
        shutil.copy2(draft_path, archive_dir / Path(draft_path).name)

    index_file = Path(output_dir) / "archive" / "index.jsonl"
    with index_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "run_id": run_id,
            "archived_at": merged["archived_at"],
            "status": verified["status"],
            "archive_dir": str(archive_dir),
        }, ensure_ascii=False) + "\n")

    log.info("运行产物已归档: %s", archive_dir)
    return {"archive_dir": str(archive_dir), "verified_record": verified}
