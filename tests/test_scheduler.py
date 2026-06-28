from __future__ import annotations

from dataclasses import replace

import httpx
import pytest

from yandex_tracker_mcp.scheduler import (
    BotServiceNotifier,
    DeliveryResult,
    ScheduledReportJob,
    SchedulerRuntime,
    build_report,
)
from yandex_tracker_mcp.tracker import CreateIssueCommand, MockTrackerGateway

from test_tracker import settings


class FakeNotifier:
    def __init__(self) -> None:
        self.reports = []

    async def send_report(self, report):
        self.reports.append(report)
        return DeliveryResult(status="delivered", delivered_chats=1)


@pytest.mark.asyncio
async def test_bot_notifier_does_not_claim_delivery_with_zero_recipients() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/notify"
        return httpx.Response(
            200,
            json={"status": "delivered", "delivered_chats": 0, "failed_chats": 0},
        )

    notifier = BotServiceNotifier(
        settings(
            bot_service_url="http://telegram-bot:8791",
            bot_service_api_key="service-secret",
        )
    )
    await notifier._client.aclose()
    notifier._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    try:
        result = await notifier.send_report(
            {
                "id": "report-1",
                "job_id": "job-1",
                "body": "Report body",
            }
        )
    finally:
        await notifier._client.aclose()

    assert result.status == "no_subscribers"
    assert result.delivered_chats == 0
    assert "no active" in (result.error or "")


def test_build_report_aggregates_attention_items() -> None:
    job = ScheduledReportJob(
        id="job-1",
        name="Daily",
        queue="TEST",
        schedule_type="cron",
        schedule_value="0 9 * * 1-5",
        timezone="Europe/Moscow",
        query=None,
        max_issues=100,
        enabled=True,
        deleted=False,
        created_at="now",
        updated_at="now",
    )
    report = build_report(
        job,
        [
            {
                "key": "TEST-1",
                "summary": "Critical task",
                "status": {"key": "open", "display": "Открыта"},
                "priority": {"key": "critical", "display": "Критический"},
                "deadline": "2020-01-01",
                "assignee": None,
            }
        ],
    )
    assert report["aggregates"]["total"] == 1
    assert report["aggregates"]["overdue"] == 1
    assert report["aggregates"]["critical"] == 1
    assert "TEST-1" in report["body"]


@pytest.mark.asyncio
async def test_scheduler_persists_and_delivers_report(tmp_path) -> None:
    gateway = MockTrackerGateway()
    await gateway.create_issue(
        CreateIssueCommand(
            summary="Scheduled demo",
            queue="TEST",
            description=None,
            issue_type="task",
            priority="normal",
            assignee=None,
            parent=None,
            tags=(),
            followers=(),
            unique="scheduled-demo",
            notify=False,
        )
    )
    notifier = FakeNotifier()
    runtime = SchedulerRuntime(
        replace(
            settings(backend="mock"),
            scheduler_database=str(tmp_path / "scheduler.db"),
        ),
        gateway=gateway,
        notifier=notifier,
    )
    await runtime.start()
    try:
        job = await runtime.create_job(
            name="Every hour",
            queue="TEST",
            schedule_type="interval",
            schedule_value="60",
            timezone_name="Europe/Moscow",
            query=None,
            max_issues=100,
        )
        report = await runtime.run_job(job["id"])
        latest = await runtime.latest_report(job["id"])
        assert report["aggregates"]["total"] == 1
        assert report["delivery_status"] == "delivered"
        assert latest["id"] == report["id"]
        assert len(notifier.reports) == 1
        await runtime.delete_job(job["id"])
        assert await runtime.list_jobs() == []
        assert (await runtime.latest_report(job["id"]))["id"] == report["id"]
    finally:
        await runtime.stop()
