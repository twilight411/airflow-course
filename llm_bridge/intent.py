"""意图数据结构与注册表加载。"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

REGISTRY_PATH = Path(__file__).resolve().parent / "intent_registry.json"


@dataclass
class Intent:
    """一次自然语言请求解析出的调度意图。"""

    dag_id: str
    skill: str
    conf: Dict[str, Any] = field(default_factory=dict)
    rationale: str = ""
    confidence: float = 0.0
    source: str = "unknown"  # deepseek | rule-fallback
    schedule_hint: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    @property
    def valid(self) -> bool:
        return self.error is None and bool(self.dag_id)


def load_registry(path: Optional[Path] = None) -> Dict[str, Any]:
    reg_path = path or REGISTRY_PATH
    return json.loads(reg_path.read_text(encoding="utf-8"))


def find_dag(registry: Dict[str, Any], dag_id: str) -> Optional[Dict[str, Any]]:
    for item in registry.get("dags", []):
        if item["dag_id"] == dag_id:
            return item
    return None


def all_dag_ids(registry: Dict[str, Any]) -> List[str]:
    return [item["dag_id"] for item in registry.get("dags", [])]
