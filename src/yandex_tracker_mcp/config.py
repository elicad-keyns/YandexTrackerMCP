from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def _boolean(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class Settings:
    backend: str
    tracker_token: str | None
    auth_type: str
    org_id: str | None
    org_header: str
    default_queue: str | None
    api_url: str
    mcp_api_key: str | None
    allow_insecure_no_auth: bool
    mcp_host: str
    mcp_port: int
    mcp_public_url: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            backend=os.getenv("TRACKER_BACKEND", "yandex").strip().lower(),
            tracker_token=os.getenv("YANDEX_TRACKER_TOKEN"),
            auth_type=os.getenv("YANDEX_AUTH_TYPE", "oauth").strip().lower(),
            org_id=os.getenv("YANDEX_ORG_ID"),
            org_header=os.getenv("YANDEX_ORG_HEADER", "X-Org-ID").strip(),
            default_queue=os.getenv("YANDEX_DEFAULT_QUEUE"),
            api_url=os.getenv(
                "YANDEX_TRACKER_API_URL", "https://api.tracker.yandex.net/v3"
            ).rstrip("/"),
            mcp_api_key=os.getenv("MCP_API_KEY"),
            allow_insecure_no_auth=_boolean("ALLOW_INSECURE_NO_AUTH"),
            mcp_host=os.getenv("MCP_HOST", "0.0.0.0").strip(),
            mcp_port=int(os.getenv("MCP_PORT", "8788")),
            mcp_public_url=os.getenv("MCP_PUBLIC_URL", "http://localhost:8788").rstrip("/"),
        )

    @property
    def tracker_authorization(self) -> str:
        prefix = "OAuth" if self.auth_type == "oauth" else "Bearer"
        return f"{prefix} {self.tracker_token}"

    def validate_for_startup(self) -> None:
        if self.backend not in {"yandex", "mock"}:
            raise ValueError("TRACKER_BACKEND must be 'yandex' or 'mock'.")
        if not self.mcp_api_key and not self.allow_insecure_no_auth:
            raise ValueError(
                "MCP_API_KEY is required. Set ALLOW_INSECURE_NO_AUTH=true only for local tests."
            )
        if self.backend == "mock":
            return
        if self.auth_type not in {"oauth", "iam"}:
            raise ValueError("YANDEX_AUTH_TYPE must be 'oauth' or 'iam'.")
        if not self.tracker_token:
            raise ValueError("YANDEX_TRACKER_TOKEN is required.")
        if self.auth_type == "oauth" and re.fullmatch(
            r"[0-9a-fA-F]{32}", self.tracker_token.strip()
        ):
            raise ValueError(
                "YANDEX_TRACKER_TOKEN looks like an OAuth Client Secret. "
                "Generate a user OAuth token with scripts/oauth_url.py instead."
            )
        if not self.org_id:
            raise ValueError("YANDEX_ORG_ID is required.")
        if self.org_header not in {"X-Org-ID", "X-Cloud-Org-ID"}:
            raise ValueError("YANDEX_ORG_HEADER must be X-Org-ID or X-Cloud-Org-ID.")
        if self.org_header == "X-Org-ID" and not self.org_id.strip().isdigit():
            raise ValueError(
                "YANDEX_ORG_ID for X-Org-ID must be the numeric organization ID from "
                "Tracker Administration, not the OAuth Client ID."
            )
        if not self.default_queue or self.default_queue.strip().upper() == "YOURQUEUE":
            raise ValueError(
                "YANDEX_DEFAULT_QUEUE must contain a real Tracker queue key, for example TEST."
            )
