"""注册表与 DAG 文件一致性测试：防止 LLM 选择了不存在的 DAG / Skill。"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_bridge.intent import load_registry


def test_registry_dag_ids_exist_in_dags_folder() -> None:
    registry = load_registry()
    dag_text = "\n".join(
        p.read_text(encoding="utf-8") for p in (ROOT / "dags").glob("*.py")
    )
    for dag in registry["dags"]:
        assert f'dag_id="{dag["dag_id"]}"' in dag_text, f"DAG 文件中缺少 {dag['dag_id']}"


def test_dispatch_skills_match_dag03_registry() -> None:
    registry = load_registry()
    dispatch = next(d for d in registry["dags"] if d["dag_id"] == "llm_skill_dispatch")
    dag03 = (ROOT / "dags" / "03_llm_skill_dispatch_dag.py").read_text(encoding="utf-8")
    for skill in dispatch["available_skills"]:
        assert f'"{skill}"' in dag03, f"DAG03 未注册 Skill: {skill}"


if __name__ == "__main__":
    test_registry_dag_ids_exist_in_dags_folder()
    test_dispatch_skills_match_dag03_registry()
    print("test_registry_consistency.py 全部通过")
