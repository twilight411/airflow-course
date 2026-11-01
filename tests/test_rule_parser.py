"""规则兜底解析器单元测试：中文需求 -> DAG/Skill 选择与参数抽取。"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_bridge.rule_fallback import parse_by_rule


def test_email_request_selects_report_dag() -> None:
    intent = parse_by_rule("给 zhang@whu.edu.cn 张老师发一封项目进度汇报邮件，主题是 课程作业进展")
    assert intent.dag_id == "email_daily_report"
    assert intent.skill == "send_email"
    assert intent.conf["to"] == "zhang@whu.edu.cn"
    assert intent.conf["recipient_name"] == "张老师"
    assert intent.conf["subject"] == "课程作业进展"
    assert intent.source == "rule-fallback"


def test_parallel_keyword_selects_pipeline_dag() -> None:
    intent = parse_by_rule("走并行流水线，附上附件摘要后给 team@example.com 发送晚间汇总")
    assert intent.dag_id == "skill_pipeline_evening"


def test_archive_intent_routes_dispatch_dag() -> None:
    intent = parse_by_rule("把刚才的发送记录归档整理一下")
    assert intent.dag_id == "llm_skill_dispatch"
    assert intent.conf["skill"] == "archive"


def test_collect_data_intent() -> None:
    intent = parse_by_rule("帮我采集今天的运行指标，统计报表数据")
    assert intent.dag_id == "llm_skill_dispatch"
    assert intent.conf["skill"] == "collect_data"


def test_highlights_extraction() -> None:
    intent = parse_by_rule(
        "给 teacher@example.com 发邮件，重点：完成 Airflow 部署、接入邮件 Skill、调试大模型网关"
    )
    assert len(intent.conf["highlights"]) == 3


def test_schedule_hint_extraction() -> None:
    intent = parse_by_rule("每天早上 9 点给老师发日报")
    assert intent.schedule_hint is not None
    assert intent.schedule_hint["cron"] == "0 9 * * *"


def test_unmatched_request_is_rejected() -> None:
    intent = parse_by_rule("帮我订一张去火星的火箭票")
    assert not intent.valid
    assert intent.error


if __name__ == "__main__":
    test_email_request_selects_report_dag()
    test_parallel_keyword_selects_pipeline_dag()
    test_archive_intent_routes_dispatch_dag()
    test_collect_data_intent()
    test_highlights_extraction()
    test_schedule_hint_extraction()
    test_unmatched_request_is_rejected()
    print("test_rule_parser.py 全部通过")
