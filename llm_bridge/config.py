"""LLM 网关配置加载：读取 llm_bridge/.env（优先）与项目根 .env。

仅使用标准库解析 .env，避免在宿主机额外安装 python-dotenv。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

BRIDGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BRIDGE_DIR.parent


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip('"').strip("'")
        # 已存在的进程环境变量优先级更高
        os.environ.setdefault(key, value)


_load_env_file(BRIDGE_DIR / ".env")
_load_env_file(PROJECT_ROOT / ".env")


@dataclass
class Settings:
    deepseek_api_key: Optional[str]
    deepseek_base_url: str
    deepseek_model: str
    airflow_base_url: str
    airflow_username: str
    airflow_password: str
    poll_interval: float
    wait_timeout: float

    @property
    def llm_enabled(self) -> bool:
        return bool(self.deepseek_api_key)


def load_settings() -> Settings:
    return Settings(
        deepseek_api_key=os.environ.get("DEEPSEEK_API_KEY") or None,
        deepseek_base_url=os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/"),
        deepseek_model=os.environ.get("DEEPSEEK_MODEL", "deepseek-chat"),
        airflow_base_url=os.environ.get("AIRFLOW_BASE_URL", "http://127.0.0.1:18080").rstrip("/"),
        airflow_username=os.environ.get("AIRFLOW_USERNAME", "airflow"),
        airflow_password=os.environ.get("AIRFLOW_PASSWORD", "airflow"),
        poll_interval=float(os.environ.get("POLL_INTERVAL", "3")),
        wait_timeout=float(os.environ.get("WAIT_TIMEOUT", "180")),
    )
