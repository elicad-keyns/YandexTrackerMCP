from __future__ import annotations

import base64
import json
import logging
import re
import sqlite3
import time
import uuid
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from .config import Settings
from .tracker import TrackerGateway


logger = logging.getLogger(__name__)


class PipelineError(RuntimeError):
    """A safe composition-pipeline error that can be returned to an MCP client."""


@dataclass(frozen=True, slots=True)
class DocumentDeliveryResult:
    status: str
    delivered_chats: int = 0
    error: str | None = None


class PipelineStore:
    def __init__(self, database_path: str) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection, connection:
            connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS pipeline_searches (
                    id TEXT PRIMARY KEY,
                    queue TEXT NOT NULL,
                    query TEXT,
                    max_issues INTEGER NOT NULL,
                    issues_json TEXT NOT NULL,
                    issues_count INTEGER NOT NULL,
                    filters_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pipeline_summaries (
                    id TEXT PRIMARY KEY,
                    search_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    focus TEXT,
                    markdown TEXT NOT NULL,
                    aggregates_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(search_id) REFERENCES pipeline_searches(id)
                );
                CREATE TABLE IF NOT EXISTS report_artifacts (
                    id TEXT PRIMARY KEY,
                    summary_id TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    telegram_status TEXT NOT NULL,
                    delivered_chats INTEGER NOT NULL DEFAULT 0,
                    delivery_error TEXT,
                    FOREIGN KEY(summary_id) REFERENCES pipeline_summaries(id)
                );
                CREATE INDEX IF NOT EXISTS idx_pipeline_searches_created
                    ON pipeline_searches(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_pipeline_summaries_search
                    ON pipeline_summaries(search_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_report_artifacts_summary
                    ON report_artifacts(summary_id, created_at DESC);
                """
            )
            search_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(pipeline_searches)")
            }
            if "filters_json" not in search_columns:
                connection.execute(
                    "ALTER TABLE pipeline_searches "
                    "ADD COLUMN filters_json TEXT NOT NULL DEFAULT '{}'"
                )

    def save_search(self, snapshot: dict[str, Any]) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                """
                INSERT INTO pipeline_searches (
                    id, queue, query, max_issues, issues_json, issues_count,
                    filters_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot["search_id"],
                    snapshot["queue"],
                    snapshot.get("query"),
                    snapshot["max_issues"],
                    json.dumps(snapshot["issues"], ensure_ascii=False),
                    snapshot["issues_found"],
                    json.dumps(snapshot["filters"], ensure_ascii=False),
                    snapshot["created_at"],
                ),
            )

    def get_search(self, search_id: str) -> dict[str, Any]:
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                "SELECT * FROM pipeline_searches WHERE id = ?", (search_id,)
            ).fetchone()
        if row is None:
            raise PipelineError(f"Pipeline search '{search_id}' was not found.")
        result = dict(row)
        result["search_id"] = result.pop("id")
        result["issues"] = json.loads(result.pop("issues_json"))
        result["filters"] = json.loads(result.pop("filters_json"))
        result["issues_found"] = result.pop("issues_count")
        return result

    def save_summary(self, summary: dict[str, Any]) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                """
                INSERT INTO pipeline_summaries (
                    id, search_id, title, focus, markdown, aggregates_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    summary["summary_id"],
                    summary["search_id"],
                    summary["title"],
                    summary.get("focus"),
                    summary["markdown"],
                    json.dumps(summary["aggregates"], ensure_ascii=False),
                    summary["created_at"],
                ),
            )

    def get_summary(self, summary_id: str) -> dict[str, Any]:
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                "SELECT * FROM pipeline_summaries WHERE id = ?", (summary_id,)
            ).fetchone()
        if row is None:
            raise PipelineError(f"Pipeline summary '{summary_id}' was not found.")
        result = dict(row)
        result["summary_id"] = result.pop("id")
        result["aggregates"] = json.loads(result.pop("aggregates_json"))
        return result

    def save_artifact(self, artifact: dict[str, Any]) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                """
                INSERT INTO report_artifacts (
                    id, summary_id, filename, file_path, size_bytes, created_at,
                    telegram_status, delivered_chats, delivery_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact["artifact_id"],
                    artifact["summary_id"],
                    artifact["filename"],
                    artifact["file_path"],
                    artifact["size_bytes"],
                    artifact["created_at"],
                    artifact["telegram_status"],
                    artifact["delivered_chats"],
                    artifact.get("delivery_error"),
                ),
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        return connection


class BotDocumentNotifier:
    def __init__(self, settings: Settings) -> None:
        self._url = settings.bot_service_url
        self._api_key = settings.bot_service_api_key
        self._client = httpx.AsyncClient(timeout=60.0)

    async def send_document(
        self,
        *,
        artifact_id: str,
        summary_id: str,
        filename: str,
        content: bytes,
        caption: str,
    ) -> DocumentDeliveryResult:
        if not self._url:
            return DocumentDeliveryResult(
                status="not_configured",
                error="TELEGRAM_BOT_SERVICE_URL is not configured.",
            )
        started = time.monotonic()
        logger.info(
            "Sending report document to bot service: artifact_id=%s summary_id=%s "
            "filename=%s size_bytes=%d",
            artifact_id,
            summary_id,
            filename,
            len(content),
        )
        try:
            response = await self._client.post(
                f"{self._url}/notify-document",
                headers={"Authorization": f"Bearer {self._api_key}"},
                json={
                    "artifact_id": artifact_id,
                    "summary_id": summary_id,
                    "filename": filename,
                    "caption": caption,
                    "content_base64": base64.b64encode(content).decode("ascii"),
                },
            )
            response.raise_for_status()
            payload = response.json()
            status = str(payload.get("status") or "unknown")
            delivered_chats = int(payload.get("delivered_chats", 0))
            error = str(payload["error"]) if payload.get("error") else None
            if status == "delivered" and delivered_chats == 0:
                status = "no_subscribers"
                error = error or "Bot service has no active Telegram subscribers."
            logger.info(
                "Bot document response: artifact_id=%s status=%s delivered_chats=%d "
                "duration_ms=%d error=%s",
                artifact_id,
                status,
                delivered_chats,
                int((time.monotonic() - started) * 1000),
                error,
            )
            return DocumentDeliveryResult(status, delivered_chats, error)
        except (httpx.HTTPError, ValueError, TypeError) as error:
            logger.exception(
                "Bot document delivery failed: artifact_id=%s duration_ms=%d",
                artifact_id,
                int((time.monotonic() - started) * 1000),
            )
            return DocumentDeliveryResult(status="failed", error=str(error))


class PipelineRuntime:
    def __init__(
        self,
        settings: Settings,
        gateway: TrackerGateway,
        notifier: BotDocumentNotifier | None = None,
    ) -> None:
        self.settings = settings
        self.gateway = gateway
        self.store = PipelineStore(settings.scheduler_database)
        self.reports_directory = Path(settings.reports_directory)
        self.notifier = notifier or BotDocumentNotifier(settings)

    async def start(self) -> None:
        self.store.initialize()
        self.reports_directory.mkdir(parents=True, exist_ok=True)
        logger.info(
            "MCP composition pipeline ready: database=%s reports_directory=%s",
            self.settings.scheduler_database,
            self.reports_directory,
        )

    async def search(
        self,
        *,
        queue: str,
        query: str | None,
        max_issues: int,
        open_only: bool = False,
        critical_only: bool = False,
    ) -> dict[str, Any]:
        started = time.monotonic()
        structured_filter = open_only or critical_only
        effective_query = None if structured_filter else query
        if structured_filter and query:
            logger.warning(
                "Ignoring free-form Tracker query because structured pipeline filters are set: "
                "queue=%s open_only=%s critical_only=%s query=%r",
                queue,
                open_only,
                critical_only,
                query[:500],
            )
        fetch_limit = 100 if structured_filter else max_issues
        issues = await self.gateway.search_issues(queue, effective_query, fetch_limit)
        if open_only:
            issues = [issue for issue in issues if not _issue_is_closed(issue)]
        if critical_only:
            issues = [issue for issue in issues if _issue_is_critical(issue)]
        issues = issues[:max_issues]
        snapshot = {
            "search_id": uuid.uuid4().hex,
            "queue": queue,
            "query": effective_query,
            "filters": {
                "open_only": open_only,
                "critical_only": critical_only,
            },
            "max_issues": max_issues,
            "issues_found": len(issues),
            "issues": issues,
            "created_at": _utc_now(),
        }
        self.store.save_search(snapshot)
        logger.info(
            "Pipeline search saved: search_id=%s queue=%s filters=%s issues=%d duration_ms=%d",
            snapshot["search_id"],
            queue,
            snapshot["filters"],
            len(issues),
            int((time.monotonic() - started) * 1000),
        )
        return snapshot

    async def summarize(
        self, *, search_id: str, focus: str | None, title: str | None
    ) -> dict[str, Any]:
        snapshot = self.store.get_search(search_id)
        summary = build_tracker_summary(snapshot, focus=focus, title=title)
        self.store.save_summary(summary)
        logger.info(
            "Pipeline summary saved: summary_id=%s search_id=%s total=%d",
            summary["summary_id"],
            search_id,
            summary["aggregates"]["total"],
        )
        return summary

    async def save_report(
        self,
        *,
        summary_id: str,
        filename: str | None,
        send_to_telegram: bool,
    ) -> dict[str, Any]:
        summary = self.store.get_summary(summary_id)
        artifact_id = uuid.uuid4().hex
        safe_filename = _safe_markdown_filename(filename or summary["title"], artifact_id)
        dated_directory = self.reports_directory / datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dated_directory.mkdir(parents=True, exist_ok=True)
        file_path = dated_directory / safe_filename
        content = summary["markdown"].encode("utf-8")
        file_path.write_bytes(content)
        logger.info(
            "Pipeline report file saved: artifact_id=%s summary_id=%s path=%s size_bytes=%d",
            artifact_id,
            summary_id,
            file_path,
            len(content),
        )

        if send_to_telegram:
            delivery = await self.notifier.send_document(
                artifact_id=artifact_id,
                summary_id=summary_id,
                filename=safe_filename,
                content=content,
                caption=summary["title"],
            )
        else:
            delivery = DocumentDeliveryResult(status="not_requested")

        artifact = {
            "artifact_id": artifact_id,
            "summary_id": summary_id,
            "filename": safe_filename,
            "file_path": str(file_path),
            "size_bytes": len(content),
            "created_at": _utc_now(),
            "telegram_status": delivery.status,
            "delivered_chats": delivery.delivered_chats,
            "delivery_error": delivery.error,
        }
        self.store.save_artifact(artifact)
        logger.info(
            "Pipeline artifact completed: artifact_id=%s telegram_status=%s "
            "delivered_chats=%d error=%s",
            artifact_id,
            delivery.status,
            delivery.delivered_chats,
            delivery.error,
        )
        return artifact


def build_tracker_summary(
    snapshot: dict[str, Any], *, focus: str | None, title: str | None
) -> dict[str, Any]:
    issues = snapshot["issues"]
    today = date.today()
    status_counts: dict[str, int] = {}
    open_issues: list[dict[str, Any]] = []
    overdue: list[dict[str, Any]] = []
    critical: list[dict[str, Any]] = []
    unassigned: list[dict[str, Any]] = []

    for issue in issues:
        status = issue.get("status") if isinstance(issue.get("status"), dict) else {}
        status_key = str(status.get("key") or "unknown").lower()
        status_name = str(status.get("display") or status_key)
        status_counts[status_name] = status_counts.get(status_name, 0) + 1
        is_closed = status_key in {"closed", "resolved", "cancelled", "canceled"}
        if not is_closed:
            open_issues.append(issue)
        deadline = _issue_deadline(issue)
        if not is_closed and deadline and deadline < today:
            overdue.append(issue)
        priority = issue.get("priority") if isinstance(issue.get("priority"), dict) else {}
        if str(priority.get("key") or "").lower() in {"critical", "blocker"}:
            critical.append(issue)
        if not is_closed and not issue.get("assignee"):
            unassigned.append(issue)

    report_title = (title or f"Сводка задач очереди {snapshot['queue']}").strip()
    aggregates = {
        "total": len(issues),
        "open": len(open_issues),
        "closed": len(issues) - len(open_issues),
        "overdue": len(overdue),
        "critical": len(critical),
        "unassigned": len(unassigned),
        "by_status": status_counts,
    }
    lines = [
        f"# {report_title}",
        "",
        f"Сформировано: {_utc_now()}",
        f"Очередь: `{snapshot['queue']}`",
        f"Снимок поиска: `{snapshot['search_id']}`",
    ]
    if snapshot.get("query"):
        lines.append(f"Запрос Tracker: `{snapshot['query']}`")
    filters = snapshot.get("filters") or {}
    if filters.get("open_only"):
        lines.append("Фильтр: только незакрытые задачи")
    if filters.get("critical_only"):
        lines.append("Фильтр: только критический приоритет")
    if focus:
        lines.extend(["", "## Фокус", "", focus.strip()])
    lines.extend(
        [
            "",
            "## Итоги",
            "",
            "| Показатель | Количество |",
            "|---|---:|",
            f"| Всего | {aggregates['total']} |",
            f"| Открытые | {aggregates['open']} |",
            f"| Закрытые | {aggregates['closed']} |",
            f"| Просроченные | {aggregates['overdue']} |",
            f"| Критические | {aggregates['critical']} |",
            f"| Без исполнителя | {aggregates['unassigned']} |",
            "",
            "## По статусам",
            "",
        ]
    )
    if status_counts:
        lines.extend(f"- {name}: {count}" for name, count in sorted(status_counts.items()))
    else:
        lines.append("Задачи не найдены.")

    attention = _unique_issues(overdue + critical + unassigned)
    lines.extend(["", "## Требуют внимания", ""])
    if attention:
        lines.extend(_issue_markdown(issue) for issue in attention[:20])
    else:
        lines.append("Задач, требующих особого внимания, не найдено.")

    lines.extend(["", "## Найденные задачи", ""])
    if issues:
        lines.extend(_issue_markdown(issue) for issue in issues[:100])
    else:
        lines.append("Задачи не найдены.")

    return {
        "summary_id": uuid.uuid4().hex,
        "search_id": snapshot["search_id"],
        "title": report_title,
        "focus": focus.strip() if focus else None,
        "aggregates": aggregates,
        "markdown": "\n".join(lines).strip() + "\n",
        "created_at": _utc_now(),
    }


def _issue_deadline(issue: dict[str, Any]) -> date | None:
    raw = issue.get("deadline") or issue.get("dueDate")
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def _issue_is_closed(issue: dict[str, Any]) -> bool:
    status = issue.get("status") if isinstance(issue.get("status"), dict) else {}
    status_key = str(status.get("key") or "").lower()
    return status_key in {
        "closed",
        "resolved",
        "cancelled",
        "canceled",
        "completed",
        "done",
    }


def _issue_is_critical(issue: dict[str, Any]) -> bool:
    priority = issue.get("priority") if isinstance(issue.get("priority"), dict) else {}
    return str(priority.get("key") or "").lower() in {"critical", "blocker"}


def _unique_issues(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    seen = set()
    for issue in issues:
        key = str(issue.get("key") or issue.get("id") or "")
        if key in seen:
            continue
        seen.add(key)
        result.append(issue)
    return result


def _issue_markdown(issue: dict[str, Any]) -> str:
    key = str(issue.get("key") or "без ключа")
    summary = str(issue.get("summary") or "Без названия").replace("\n", " ")
    status = issue.get("status") if isinstance(issue.get("status"), dict) else {}
    status_name = str(status.get("display") or status.get("key") or "без статуса")
    return f"- **{key}** — {summary} ({status_name})"


def _safe_markdown_filename(value: str, artifact_id: str) -> str:
    raw_stem = Path(value.strip()).stem
    stem = re.sub(r"[^\w.-]+", "_", raw_stem, flags=re.UNICODE).strip("._")
    if not stem:
        stem = "tracker-report"
    return f"{stem[:80]}-{artifact_id[:8]}.md"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
