"""DeepSeek（OpenAI 兼容协议）大模型客户端，标准库 urllib 实现。

接口：POST {base_url}/chat/completions，强制 JSON 输出（response_format=json_object）。
无 API Key 或调用失败时返回 None，由上层切换到规则兜底解析，保证链路可演示。
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

log = logging.getLogger("bridge.deepseek")


class DeepSeekClient:
    def __init__(self, api_key: Optional[str], base_url: str, model: str, timeout: int = 30):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def chat_json(self, system_prompt: str, user_message: str) -> Optional[Dict[str, Any]]:
        """调用大模型并解析为 JSON；失败返回 None。"""
        if not self.enabled:
            return None
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }
        request = urllib.request.Request(
            f"{self.base_url}/chat/completions",
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            content = payload["choices"][0]["message"]["content"]
            return json.loads(content)
        except (urllib.error.URLError, KeyError, IndexError, json.JSONDecodeError) as exc:
            log.warning("大模型调用失败，将切换规则兜底: %s", exc)
            return None
