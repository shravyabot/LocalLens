from __future__ import annotations

import json
from typing import Any

import requests

from locallens.config import Settings


class OllamaClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def is_available(self) -> bool:
        try:
            response = requests.get(
                f"{self.settings.ollama_base_url}/api/tags",
                timeout=2.5,
            )
            return response.ok
        except requests.RequestException:
            return False

    def generate_json(self, prompt: str) -> dict[str, Any]:
        response = requests.post(
            f"{self.settings.ollama_base_url}/api/generate",
            json={
                "model": self.settings.ollama_generation_model,
                "prompt": prompt,
                "stream": False,
                "format": "json",
                "options": {
                    "temperature": 0.2,
                    "num_ctx": 2048,
                },
            },
            timeout=240,
        )
        response.raise_for_status()
        payload = response.json()
        return json.loads(payload.get("response", "{}"))
