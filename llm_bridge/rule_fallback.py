"""无大模型 Key（或大模型解析失败）时的中文规则兜底解析器。

职责：在离线环境下完成“关键词 -> DAG/Skill 选择 + 参数抽取”，
保证自然语言驱动链路不因为缺 Key 而中断；规则命中置信度低于真实大模型。
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

from .intent import Intent, load_registry, find_dag

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
RECIPIENT_RE = re.compile(r"([一-龥A-Za-z]{1,12}(?:老师|同学|教授|主任|同事))")
SUBJECT_RE = re.compile(r"(?:主题|标题|题目)\s*(?:是|为|[:：])\s*(.{2,40})")
BODY_RE = re.compile(r"(?:正文|内容|要说的话)\s*(?:是|为|[:：])\s*(.+)")
HIGHLIGHT_RE = re.compile(r"(?:重点|事项)\s*(?:如下|是|[:：])\s*(.+)")


def _extract_subject(text: str) -> Optional[str]:
    m = SUBJECT_RE.search(text)
    if m:
        return m.group(1).strip().rstrip("。！!？?")
    m = re.search(r"发[一封]?(?:汇报|通知|提醒|邮件)[^，,。.]{0,20}?", text)
    return None


EMAIL_KEYWORDS = ["邮件", "汇报", "通知", "提醒", "日报", "发给", "发送", "主题", "正文"]
COLLECT_KEYWORDS = [
    "采集数据", "统计报表", "收集指标", "数据准备",
    "采集指标", "采集", "指标统计", "报表数据", "统计数据",
]
PARALLEL_KEYWORDS = ["并行", "附件", "流水线", "分支"]
ARCHIVE_KEYWORDS = ["归档", "存档", "整理记录"]


def _choose_dag(text: str) -> tuple[str, str, bool]:
    """按关键词优先级匹配 DAG/Skill；未命中任何能力信号时 matched=False。"""
    if any(k in text for k in ARCHIVE_KEYWORDS):
        return "llm_skill_dispatch", "archive", True
    if any(k in text for k in COLLECT_KEYWORDS):
        return "llm_skill_dispatch", "collect_data", True
    if any(k in text for k in PARALLEL_KEYWORDS):
        return "skill_pipeline_evening", "send_email_parallel", True
    email_match = bool(EMAIL_RE.search(text)) or bool(RECIPIENT_RE.search(text))
    if email_match or any(k in text for k in EMAIL_KEYWORDS):
        return "email_daily_report", "send_email", True
    return "", "", False


def _extract_highlights(text: str) -> list:
    m = HIGHLIGHT_RE.search(text)
    if not m:
        return []
    clause = m.group(1)
    parts = re.split(r"[、，,；;]", clause)
    return [p.strip() for p in parts if p.strip()]


def _schedule_hint(text: str) -> Optional[dict]:
    m = re.search(r"每天\s*(早上|上午|中午|下午|晚上)?\s*(\d{1,2})\s*点(\d{0,2})?", text)
    if not m:
        return None
    hour = int(m.group(2))
    minute = int(m.group(3) or 0)
    return {
        "cron": f"{minute} {hour} * * *",
        "note": f"规则解析到定时需求：每天 {hour:02d}:{minute:02d}；"
                "Airflow 的定时规则定义在 DAG 文件 schedule 参数中，本次先立即触发一次。",
    }


def parse_by_rule(text: str) -> Intent:
    registry = load_registry()
    dag_id, skill, matched = _choose_dag(text)
    if not matched:
        return Intent(
            dag_id="",
            skill="",
            rationale="规则兜底未在注册表能力目录中识别到可执行任务，拒绝触发未知工作流",
            confidence=0.0,
            source="rule-fallback",
            error="未识别到与已注册 DAG/Skill 匹配的需求；可输入 list 查看当前支持的任务范围",
        )
    dag_meta = find_dag(registry, dag_id)

    conf: Dict[str, Any] = {}
    email_match = EMAIL_RE.search(text)
    if email_match:
        conf["to"] = email_match.group(0)
    recipient = RECIPIENT_RE.search(text)
    if recipient:
        conf["recipient_name"] = recipient.group(1)
    subject = _extract_subject(text)
    if subject:
        conf["subject"] = subject
    body = BODY_RE.search(text)
    if body:
        conf["text_body"] = body.group(1).strip()
    highlights = _extract_highlights(text)
    if highlights:
        conf["highlights"] = highlights

    if skill in {"archive", "collect_data"}:
        # 通用调度 DAG 的 conf 需要嵌套 skill/payload/conf
        conf = {"skill": skill, "payload": {}, "conf": conf}

    rationale = f"规则关键词匹配选择 DAG={dag_id}（{dag_meta['name'] if dag_meta else '未知'}）"
    return Intent(
        dag_id=dag_id,
        skill=skill,
        conf=conf,
        rationale=rationale,
        confidence=0.55,
        source="rule-fallback",
        schedule_hint=_schedule_hint(text),
    )
