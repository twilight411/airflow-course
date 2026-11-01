"""意图解析器：大模型优先，规则兜底。

链路：自然语言文本 -> DeepSeek 结构化 JSON -> 注册表校验/归一化 -> Intent；
大模型不可用或输出不合法时，自动切换 rule_fallback 规则引擎。
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from .deepseek_client import DeepSeekClient
from .intent import Intent, load_registry, find_dag, all_dag_ids
from .rule_fallback import parse_by_rule

log = logging.getLogger("bridge.parser")


def _build_system_prompt(registry: Dict[str, Any]) -> str:
    blocks = []
    for dag in registry["dags"]:
        block = [f"- dag_id: {dag['dag_id']}", f"  名称: {dag['name']}", f"  说明: {dag['description']}"]
        if dag.get("default_schedule"):
            block.append(f"  默认调度: {dag['default_schedule_note']}")
        if dag.get("params_schema"):
            block.append(f"  参数 schema: {json.dumps(dag['params_schema'], ensure_ascii=False)}")
        if dag.get("example_conf"):
            block.append(f"  示例 conf: {json.dumps(dag['example_conf'], ensure_ascii=False)}")
        blocks.append("\n".join(block))
    catalog = "\n".join(blocks)

    return f"""你是 Airflow 自动化工作流的调度决策器。你的任务：把用户的自然语言需求映射到下面注册表中**已经存在**的 DAG，禁止编造 dag_id。

可用 DAG 目录：
{catalog}

输出严格符合以下 JSON（不要输出 Markdown 代码块、不要解释）：
{{
  "dag_id": "注册表中的 dag_id",
  "skill": "对应技能名，如 send_email / collect_data / archive",
  "conf": {{ ...按参数 schema 抽取的运行参数... }},
  "rationale": "中文一句话说明选择理由",
  "confidence": 0.0到1.0之间的小数,
  "schedule_hint": null 或 {{"cron": "分 时 日 月 周", "note": "说明"}}
}}

规则：
1. 邮件/汇报/通知/提醒默认选择 email_daily_report；附件、并行、流水线选择 skill_pipeline_evening；只做归档选 llm_skill_dispatch(skill=archive)；只做数据采集选 llm_skill_dispatch(skill=collect_data)。
2. 选择 llm_skill_dispatch 时，conf 必须为 {{"skill": "send_email|collect_data|archive", "payload": {{}}, "conf": {{...}}}}。
3. 从文本中抽取邮箱地址到 conf.to；称呼对象到 conf.recipient_name；主题到 conf.subject；正文到 conf.text_body；分条事项放入 conf.highlights 数组。
4. 用户只要求“现在执行一次”时 schedule_hint 为 null；只有明确说每天/每周固定时间才填 cron（北京时间）。
5. 无法判断时 dag_id 填 null、confidence 填 0.1，禁止编造或强行选择邮件 DAG；下游会拒绝不可执行的请求。"""


class IntentParser:
    def __init__(self, llm: Optional[DeepSeekClient], registry: Optional[dict] = None):
        self.llm = llm
        self.registry = registry or load_registry()

    def parse(self, text: str) -> Intent:
        if self.llm and self.llm.enabled:
            intent = self._parse_by_llm(text)
            if intent and intent.valid:
                return intent
            log.info("大模型结果不可用，切换规则兜底")
        return parse_by_rule(text)

    def _parse_by_llm(self, text: str) -> Optional[Intent]:
        prompt = _build_system_prompt(self.registry)
        raw = self.llm.chat_json(prompt, text)
        if not raw:
            return None
        try:
            dag_id = raw["dag_id"]
            if dag_id not in all_dag_ids(self.registry):
                return None
            skill = raw.get("skill") or "send_email"
            conf = raw.get("conf") or {}
            intent = Intent(
                dag_id=dag_id,
                skill=skill,
                conf=conf if isinstance(conf, dict) else {},
                rationale=raw.get("rationale", ""),
                confidence=float(raw.get("confidence", 0.7)),
                source="deepseek",
                schedule_hint=raw.get("schedule_hint"),
            )
            # 注册表交叉校验
            meta = find_dag(self.registry, dag_id)
            if meta and meta.get("skill_dispatch"):
                if intent.conf.get("skill") not in meta.get("available_skills", []):
                    return None
            return intent
        except (KeyError, TypeError, ValueError) as exc:
            log.warning("大模型输出归一化失败: %s", exc)
            return None
