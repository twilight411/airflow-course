"""
LLM 自然语言编排网关 —— 交互式命令行入口
==========================================

用法：
    python llm_bridge/cli.py                 # 交互式对话
    python llm_bridge/cli.py --once "给张老师发一封项目进度汇报邮件"
    python llm_bridge/cli.py --no-llm        # 强制规则兜底（不调用大模型）
    python llm_bridge/cli.py --json          # 机器可读 JSON 输出

内置命令：
    list                          查看可调度 DAG / Skill 能力目录
    status <dag_id> <run_id>      查询某次运行状态
    help                          帮助
    exit / quit                   退出
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any, Dict, List, Optional

# 允许直接 `python llm_bridge/cli.py` 运行
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from llm_bridge.config import load_settings
from llm_bridge.deepseek_client import DeepSeekClient
from llm_bridge.airflow_rest_client import AirflowRestClient
from llm_bridge.intent_parser import IntentParser
from llm_bridge.orchestrator import Orchestrator


def build_orchestrator(force_no_llm: bool = False) -> Orchestrator:
    settings = load_settings()
    llm = DeepSeekClient(
        api_key=None if force_no_llm else settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
        model=settings.deepseek_model,
    )
    airflow = AirflowRestClient(
        base_url=settings.airflow_base_url,
        username=settings.airflow_username,
        password=settings.airflow_password,
    )
    parser = IntentParser(llm)
    return Orchestrator(airflow, parser, settings=settings)


# --------------------------------------------------------------------------- #
# 输出格式化
# --------------------------------------------------------------------------- #
STATE_EMOJI = {
    "success": "✅", "failed": "❌", "running": "🔄", "queued": "⏳",
    "up_for_retry": "🔁", "upstream_failed": "⚠️", "skipped": "⏭️",
}


def _print_report(report: Dict[str, Any], as_json: bool = False) -> None:
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return

    intent = report.get("parsed_intent", {})
    print("\n" + "=" * 72)
    print(f"【需求】{report.get('request')}")
    print("-" * 72)
    print(f"意图来源: {intent.get('source')}    置信度: {intent.get('confidence')}")
    print(f"目标 DAG: {report.get('dag_meta', {}).get('dag_id') or intent.get('dag_id')}")
    if intent.get("rationale"):
        print(f"选择理由: {intent['rationale']}")
    if intent.get("conf"):
        print(f"调度参数: {json.dumps(intent['conf'], ensure_ascii=False)}")
    if report.get("schedule_hint"):
        print(f"定时提示: {report['schedule_hint'].get('note')}")

    if report.get("error"):
        print(f"\n执行失败: {report['error']}")
        print("=" * 72)
        return

    print(f"\nDAG Run : {report.get('run_id')}")
    print(f"最终状态: {STATE_EMOJI.get(report.get('final_state'), 'ℹ️')} {report.get('final_state')}")
    print("任务状态:")
    for task in report.get("tasks", []):
        icon = STATE_EMOJI.get(task["state"], "ℹ️")
        print(f"  {icon} {task['task_id']:<24} {task['state']}")

    if report.get("failure_summary"):
        print("\n异常汇总:")
        print(report["failure_summary"])
    print(f"耗时: {report.get('elapsed_seconds')}s")
    print("=" * 72)


def _print_capabilities(orch: Orchestrator) -> None:
    print("\n可调度能力目录：")
    for cap in orch.list_capabilities():
        print(f"\n● {cap['dag_id']}  —  {cap['name']}")
        print(f"  {cap['description']}")
        if cap.get("schedule"):
            print(f"  调度: {cap['schedule']}")


def handle_builtin(text: str, orch: Orchestrator) -> bool:
    """处理 list / status / help 等内置命令；返回 True 表示已消费，不进入任务触发。"""
    if text in {"exit", "quit", "退出"}:
        print("再见！")
        return True
    if text == "help":
        print(__doc__)
        return True
    if text == "list":
        _print_capabilities(orch)
        return True
    if text.startswith("status"):
        parts = text.split()
        if len(parts) == 3:
            print(json.dumps(orch.query_status(parts[1], parts[2]), ensure_ascii=False, indent=2))
        else:
            print("用法: status <dag_id> <run_id>")
        return True
    return False


def interactive(orch: Orchestrator, as_json: bool, force_no_llm: bool = False) -> None:
    settings = load_settings()
    if force_no_llm:
        mode = "规则兜底离线（强制 --no-llm）"
    else:
        mode = "DeepSeek 在线" if settings.llm_enabled else "规则兜底离线"
    print("=" * 72)
    print(" LLM 自然语言 -> Airflow 工作流 编排网关")
    print(f" 模式: {mode}    Airflow: {settings.airflow_base_url}")
    print(" 输入自然语言需求；输入 list 查看能力；status <dag_id> <run_id> 查状态；exit 退出")
    print("=" * 72)

    while True:
        try:
            text = input("\n请输入需求 > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n再见！")
            break
        if not text:
            continue
        if handle_builtin(text, orch):
            if text in {"exit", "quit", "退出"}:
                break
            continue

        report = orch.handle(text)
        _print_report(report, as_json=as_json)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="LLM -> Airflow 自然语言编排网关")
    parser.add_argument("--once", help="单次执行一句自然语言需求后退出")
    parser.add_argument("--no-llm", action="store_true", help="禁用大模型，强制规则兜底解析")
    parser.add_argument("--json", action="store_true", help="以 JSON 输出报告")
    parser.add_argument("--debug", action="store_true", help="输出调试日志")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    orch = build_orchestrator(force_no_llm=args.no_llm)

    if args.once:
        if handle_builtin(args.once, orch):
            return 0
        report = orch.handle(args.once)
        _print_report(report, as_json=args.json)
        return 0 if report.get("ok") else 1

    interactive(orch, as_json=args.json, force_no_llm=args.no_llm)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
