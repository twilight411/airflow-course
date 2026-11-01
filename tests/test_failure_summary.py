"""网关失败日志摘要提取测试：兼容 Airflow 3 结构化 dict 字面量日志。"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_bridge.orchestrator import Orchestrator


SAMPLE_LOG = """{'timestamp': '2026-11-01T10:34:02.325149Z', 'event': 'Task failed with exception', 'level': 'error', 'error_detail': [{'exc_type': 'SkillNotFoundError', 'exc_value': "未注册的 Skill: 'bad'；可选: ['archive', 'collect_data', 'send_email']", 'frames': [{'filename': '/opt/airflow/dags/03_llm_skill_dispatch_dag.py', 'lineno': 60, 'name': 'route_execute'}], 'is_group': False, 'exceptions': []}]}
{'timestamp': '2026-11-01T10:34:02.346579Z', 'event': 'DAG/任务运行失败，状态快照已写入', 'level': 'error'}
"""


def test_structured_airflow3_exception_is_extracted() -> None:
    summary = Orchestrator._extract_traceback_tail(SAMPLE_LOG)
    assert "SkillNotFoundError" in summary
    assert "未注册的 Skill" in summary
    assert "03_llm_skill_dispatch_dag.py:60" in summary


def test_plain_text_traceback_fallback() -> None:
    text = "Traceback (most recent call last):\nValueError: 收件人邮箱格式非法"
    summary = Orchestrator._extract_traceback_tail(text)
    assert "ValueError" in summary


if __name__ == "__main__":
    test_structured_airflow3_exception_is_extracted()
    test_plain_text_traceback_fallback()
    print("test_failure_summary.py 全部通过")
