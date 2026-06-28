from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from yandex_tracker_mcp.pipeline import (
    DocumentDeliveryResult,
    PipelineRuntime,
    build_tracker_summary,
)

from test_tracker import settings


class FakeGateway:
    async def search_issues(self, queue, query, max_results):
        assert queue == "TEST"
        assert query is None
        return [
            {
                "id": "1",
                "key": "TEST-1",
                "summary": "Critical overdue task",
                "status": {"key": "open", "display": "Открыта"},
                "priority": {"key": "critical", "display": "Критический"},
                "deadline": "2020-01-01",
                "assignee": None,
            },
            {
                "id": "2",
                "key": "TEST-2",
                "summary": "Resolved task",
                "status": {"key": "resolved", "display": "Решена"},
                "priority": {"key": "normal", "display": "Средний"},
                "assignee": {"id": "user-1", "display": "Tester"},
            },
        ][:max_results]


class FakeDocumentNotifier:
    def __init__(self) -> None:
        self.documents = []

    async def send_document(self, **document):
        self.documents.append(document)
        return DocumentDeliveryResult(status="delivered", delivered_chats=1)


@pytest.mark.asyncio
async def test_composition_pipeline_passes_ids_and_saves_document(tmp_path) -> None:
    notifier = FakeDocumentNotifier()
    runtime = PipelineRuntime(
        replace(
            settings(backend="mock"),
            scheduler_database=str(tmp_path / "pipeline.db"),
            reports_directory=str(tmp_path / "reports"),
        ),
        gateway=FakeGateway(),
        notifier=notifier,
    )
    await runtime.start()

    search = await runtime.search(
        queue="TEST",
        query='"Status": Open AND "Priority": Critical',
        max_issues=100,
        open_only=True,
        critical_only=True,
    )
    summary = await runtime.summarize(
        search_id=search["search_id"],
        focus="Просроченные и критические",
        title="Демонстрация композиции MCP",
    )
    artifact = await runtime.save_report(
        summary_id=summary["summary_id"],
        filename="day-19-report",
        send_to_telegram=True,
    )
    loaded_artifact = await runtime.get_artifact(artifact["artifact_id"])

    assert search["issues_found"] == 1
    assert search["query"] is None
    assert search["filters"] == {"open_only": True, "critical_only": True}
    assert summary["search_id"] == search["search_id"]
    assert summary["aggregates"] == {
        "total": 1,
        "open": 1,
        "closed": 0,
        "overdue": 1,
        "critical": 1,
        "unassigned": 1,
        "by_status": {"Открыта": 1},
    }
    assert artifact["summary_id"] == summary["summary_id"]
    assert artifact["telegram_status"] == "delivered"
    assert artifact["delivered_chats"] == 1
    report_path = Path(artifact["file_path"])
    assert report_path.exists()
    assert "TEST-1" in report_path.read_text(encoding="utf-8")
    assert notifier.documents[0]["content"] == report_path.read_bytes()
    assert loaded_artifact["artifact_id"] == artifact["artifact_id"]
    assert loaded_artifact["summary_id"] == summary["summary_id"]
    assert loaded_artifact["markdown"] == report_path.read_text(encoding="utf-8")


def test_summary_is_built_from_persisted_search_snapshot() -> None:
    summary = build_tracker_summary(
        {
            "search_id": "search-1",
            "queue": "TEST",
            "query": None,
            "filters": {"open_only": False, "critical_only": False},
            "issues": [],
        },
        focus=None,
        title=None,
    )

    assert summary["search_id"] == "search-1"
    assert summary["aggregates"]["total"] == 0
    assert "Задачи не найдены" in summary["markdown"]
