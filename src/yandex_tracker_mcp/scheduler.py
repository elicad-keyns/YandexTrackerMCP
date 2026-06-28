from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from .config import Settings
from .tracker import TrackerGateway, create_gateway


logger = logging.getLogger(__name__)


class SchedulerError(RuntimeError):
    """A safe scheduler error that can be returned to an MCP client."""


@dataclass(frozen=True, slots=True)
class ScheduledReportJob:
    id: str
    name: str
    queue: str
    schedule_type: str
    schedule_value: str
    timezone: str
    query: str | None
    max_issues: int
    enabled: bool
    deleted: bool
    created_at: str
    updated_at: str
    next_run_at: str | None = None


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    status: str
    delivered_chats: int = 0
    error: str | None = None


class ReportNotifier(Protocol):
    async def send_report(self, report: dict[str, Any]) -> DeliveryResult: ...


class BotServiceNotifier:
    def __init__(self, settings: Settings) -> None:
        self._url = settings.bot_service_url
        self._api_key = settings.bot_service_api_key
        self._client = httpx.AsyncClient(timeout=30.0)

    async def send_report(self, report: dict[str, Any]) -> DeliveryResult:
        if not self._url:
            error = "TELEGRAM_BOT_SERVICE_URL is not configured."
            logger.warning(
                "Telegram delivery skipped: report_id=%s reason=%s", report.get("id"), error
            )
            return DeliveryResult(status="not_configured", error=error)
        report_id = str(report.get("id") or "unknown")
        started = time.monotonic()
        logger.info(
            "Sending report to bot service: report_id=%s job_id=%s url=%s body_chars=%d",
            report_id,
            report.get("job_id"),
            f"{self._url}/notify",
            len(str(report.get("body") or "")),
        )
        try:
            response = await self._client.post(
                f"{self._url}/notify",
                headers={"Authorization": f"Bearer {self._api_key}"},
                json=report,
            )
            response.raise_for_status()
            payload = response.json()
            delivered_chats = int(payload.get("delivered_chats", 0))
            failed_chats = int(payload.get("failed_chats", 0))
            status = str(payload.get("status") or "unknown")
            error = str(payload["error"]) if payload.get("error") else None
            # Compatibility with an older bot version which incorrectly returned
            # status=delivered when its subscriber list was empty.
            if status == "delivered" and delivered_chats == 0:
                status = "no_subscribers"
                error = error or "Bot service has no active Telegram subscribers."
            logger.info(
                "Bot service response: report_id=%s http_status=%d status=%s "
                "delivered_chats=%d failed_chats=%d duration_ms=%d error=%s",
                report_id,
                response.status_code,
                status,
                delivered_chats,
                failed_chats,
                int((time.monotonic() - started) * 1000),
                error,
            )
            return DeliveryResult(status=status, delivered_chats=delivered_chats, error=error)
        except (httpx.HTTPError, ValueError, TypeError) as error:
            response_text = None
            if isinstance(error, httpx.HTTPStatusError):
                response_text = error.response.text[:1000]
            logger.exception(
                "Bot service delivery failed: report_id=%s duration_ms=%d response=%s",
                report_id,
                int((time.monotonic() - started) * 1000),
                response_text,
            )
            return DeliveryResult(status="failed", error=str(error))


class SchedulerStore:
    def __init__(self, database_path: str) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS scheduled_jobs (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    queue TEXT NOT NULL,
                    schedule_type TEXT NOT NULL,
                    schedule_value TEXT NOT NULL,
                    timezone TEXT NOT NULL,
                    query TEXT,
                    max_issues INTEGER NOT NULL,
                    enabled INTEGER NOT NULL,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    next_run_at TEXT
                );
                CREATE TABLE IF NOT EXISTS job_runs (
                    id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    report_id TEXT,
                    error TEXT,
                    FOREIGN KEY(job_id) REFERENCES scheduled_jobs(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS reports (
                    id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT NOT NULL,
                    aggregates_json TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    delivery_status TEXT NOT NULL,
                    delivered_chats INTEGER NOT NULL DEFAULT 0,
                    delivery_error TEXT,
                    FOREIGN KEY(job_id) REFERENCES scheduled_jobs(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_reports_job_generated
                    ON reports(job_id, generated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_job_runs_job_started
                    ON job_runs(job_id, started_at DESC);
                """
            )
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(scheduled_jobs)")
            }
            if "deleted" not in columns:
                connection.execute(
                    "ALTER TABLE scheduled_jobs ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0"
                )

    def save_job(self, job: ScheduledReportJob) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO scheduled_jobs (
                    id, name, queue, schedule_type, schedule_value, timezone,
                    query, max_issues, enabled, deleted, created_at, updated_at, next_run_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name,
                    queue=excluded.queue,
                    schedule_type=excluded.schedule_type,
                    schedule_value=excluded.schedule_value,
                    timezone=excluded.timezone,
                    query=excluded.query,
                    max_issues=excluded.max_issues,
                    enabled=excluded.enabled,
                    deleted=excluded.deleted,
                    updated_at=excluded.updated_at,
                    next_run_at=excluded.next_run_at
                """,
                (
                    job.id,
                    job.name,
                    job.queue,
                    job.schedule_type,
                    job.schedule_value,
                    job.timezone,
                    job.query,
                    job.max_issues,
                    int(job.enabled),
                    int(job.deleted),
                    job.created_at,
                    job.updated_at,
                    job.next_run_at,
                ),
            )

    def get_job(self, job_id: str) -> ScheduledReportJob:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM scheduled_jobs WHERE id = ? AND deleted = 0", (job_id,)
            ).fetchone()
        if row is None:
            raise SchedulerError(f"Scheduled job '{job_id}' was not found.")
        return _row_to_job(row)

    def list_jobs(self) -> list[ScheduledReportJob]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM scheduled_jobs WHERE deleted = 0 ORDER BY created_at DESC"
            ).fetchall()
        return [_row_to_job(row) for row in rows]

    def set_job_state(
        self, job_id: str, enabled: bool, next_run_at: str | None = None
    ) -> ScheduledReportJob:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE scheduled_jobs
                SET enabled = ?, next_run_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (int(enabled), next_run_at, _utc_now(), job_id),
            )
            if cursor.rowcount == 0:
                raise SchedulerError(f"Scheduled job '{job_id}' was not found.")
        return self.get_job(job_id)

    def update_next_run(self, job_id: str, next_run_at: str | None) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE scheduled_jobs SET next_run_at = ?, updated_at = ? WHERE id = ?",
                (next_run_at, _utc_now(), job_id),
            )

    def delete_job(self, job_id: str) -> None:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE scheduled_jobs
                SET deleted = 1, enabled = 0, next_run_at = NULL, updated_at = ?
                WHERE id = ? AND deleted = 0
                """,
                (_utc_now(), job_id),
            )
            if cursor.rowcount == 0:
                raise SchedulerError(f"Scheduled job '{job_id}' was not found.")

    def start_run(self, job_id: str) -> str:
        run_id = uuid.uuid4().hex
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO job_runs (id, job_id, started_at, status)
                VALUES (?, ?, ?, 'running')
                """,
                (run_id, job_id, _utc_now()),
            )
        return run_id

    def finish_run(
        self,
        run_id: str,
        status: str,
        report_id: str | None = None,
        error: str | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE job_runs
                SET finished_at = ?, status = ?, report_id = ?, error = ?
                WHERE id = ?
                """,
                (_utc_now(), status, report_id, error, run_id),
            )

    def save_report(self, report: dict[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO reports (
                    id, job_id, title, body, aggregates_json, generated_at,
                    delivery_status, delivered_chats, delivery_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report["id"],
                    report["job_id"],
                    report["title"],
                    report["body"],
                    json.dumps(report["aggregates"], ensure_ascii=False),
                    report["generated_at"],
                    report["delivery_status"],
                    report.get("delivered_chats", 0),
                    report.get("delivery_error"),
                ),
            )

    def update_report_delivery(self, report_id: str, delivery: DeliveryResult) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE reports
                SET delivery_status = ?, delivered_chats = ?, delivery_error = ?
                WHERE id = ?
                """,
                (delivery.status, delivery.delivered_chats, delivery.error, report_id),
            )

    def latest_report(self, job_id: str | None = None) -> dict[str, Any] | None:
        query = "SELECT * FROM reports"
        params: tuple[Any, ...] = ()
        if job_id:
            query += " WHERE job_id = ?"
            params = (job_id,)
        query += " ORDER BY generated_at DESC LIMIT 1"
        with self._connect() as connection:
            row = connection.execute(query, params).fetchone()
        return _row_to_report(row) if row else None

    def report_history(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM reports ORDER BY generated_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [_row_to_report(row) for row in rows]

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        return connection


class SchedulerRuntime:
    def __init__(
        self,
        settings: Settings,
        gateway: TrackerGateway | None = None,
        notifier: ReportNotifier | None = None,
    ) -> None:
        self.settings = settings
        self.store = SchedulerStore(settings.scheduler_database)
        self.gateway = gateway or create_gateway(settings)
        self.notifier = notifier or BotServiceNotifier(settings)
        self.scheduler = AsyncIOScheduler(timezone=settings.scheduler_timezone)

    async def start(self) -> None:
        logger.info(
            "Starting scheduler: database=%s timezone=%s bot_service_configured=%s",
            self.settings.scheduler_database,
            self.settings.scheduler_timezone,
            bool(self.settings.bot_service_url),
        )
        self.store.initialize()
        self.scheduler.start()
        jobs = self.store.list_jobs()
        logger.info("Scheduler database initialized: persisted_jobs=%d", len(jobs))
        restored = 0
        for job in jobs:
            if job.enabled:
                scheduled = self._schedule(job)
                restored += 1
                logger.info(
                    "Restored scheduled job: job_id=%s name=%r type=%s value=%s "
                    "timezone=%s next_run_at=%s",
                    job.id,
                    job.name,
                    job.schedule_type,
                    job.schedule_value,
                    job.timezone,
                    _iso(scheduled.next_run_time),
                )
        logger.info("Scheduler ready: active_jobs=%d", restored)

    async def stop(self) -> None:
        if self.scheduler.running:
            logger.info("Stopping scheduler")
            self.scheduler.shutdown(wait=False)

    async def create_job(
        self,
        *,
        name: str,
        queue: str,
        schedule_type: str,
        schedule_value: str,
        timezone_name: str,
        query: str | None,
        max_issues: int,
    ) -> dict[str, Any]:
        _build_trigger(schedule_type, schedule_value, timezone_name)
        now = _utc_now()
        job = ScheduledReportJob(
            id=uuid.uuid4().hex,
            name=name,
            queue=queue,
            schedule_type=schedule_type,
            schedule_value=schedule_value,
            timezone=timezone_name,
            query=query,
            max_issues=max_issues,
            enabled=True,
            deleted=False,
            created_at=now,
            updated_at=now,
        )
        self.store.save_job(job)
        scheduled = self._schedule(job)
        next_run_at = _iso(scheduled.next_run_time)
        self.store.update_next_run(job.id, next_run_at)
        logger.info(
            "Scheduled job created: job_id=%s name=%r queue=%s type=%s value=%s "
            "timezone=%s query=%r max_issues=%d next_run_at=%s",
            job.id,
            job.name,
            job.queue,
            job.schedule_type,
            job.schedule_value,
            job.timezone,
            job.query,
            job.max_issues,
            next_run_at,
        )
        return _job_result(self.store.get_job(job.id))

    async def list_jobs(self) -> list[dict[str, Any]]:
        results = []
        for job in self.store.list_jobs():
            scheduled = self.scheduler.get_job(job.id)
            next_run_at = _iso(scheduled.next_run_time) if scheduled else None
            if next_run_at != job.next_run_at:
                self.store.update_next_run(job.id, next_run_at)
                job = self.store.get_job(job.id)
            results.append(_job_result(job))
        return results

    async def pause_job(self, job_id: str) -> dict[str, Any]:
        try:
            self.scheduler.pause_job(job_id)
        except JobLookupError:
            pass
        result = _job_result(self.store.set_job_state(job_id, False))
        logger.info("Scheduled job paused: job_id=%s", job_id)
        return result

    async def resume_job(self, job_id: str) -> dict[str, Any]:
        job = self.store.get_job(job_id)
        scheduled = self.scheduler.get_job(job_id)
        if scheduled:
            self.scheduler.resume_job(job_id)
        else:
            scheduled = self._schedule(job)
        next_run_at = _iso(scheduled.next_run_time)
        result = _job_result(self.store.set_job_state(job_id, True, next_run_at))
        logger.info("Scheduled job resumed: job_id=%s next_run_at=%s", job_id, next_run_at)
        return result

    async def delete_job(self, job_id: str) -> dict[str, Any]:
        job = self.store.get_job(job_id)
        try:
            self.scheduler.remove_job(job_id)
        except JobLookupError:
            pass
        self.store.delete_job(job_id)
        result = _job_result(job)
        result.update({"enabled": False, "deleted": True, "next_run_at": None})
        logger.info("Scheduled job deleted: job_id=%s", job_id)
        return result

    async def run_job(self, job_id: str) -> dict[str, Any]:
        job = self.store.get_job(job_id)
        run_id = self.store.start_run(job_id)
        started = time.monotonic()
        logger.info(
            "Scheduled report run started: run_id=%s job_id=%s name=%r queue=%s query=%r",
            run_id,
            job_id,
            job.name,
            job.queue,
            job.query,
        )
        try:
            issues = await self.gateway.search_issues(job.queue, job.query, job.max_issues)
            logger.info(
                "Tracker search completed: run_id=%s job_id=%s issues=%d max_issues=%d",
                run_id,
                job_id,
                len(issues),
                job.max_issues,
            )
            report = build_report(job, issues)
            self.store.save_report(report)
            logger.info(
                "Report generated and saved: run_id=%s report_id=%s aggregates=%s body_chars=%d",
                run_id,
                report["id"],
                report["aggregates"],
                len(report["body"]),
            )
            delivery = await self.notifier.send_report(report)
            self.store.update_report_delivery(report["id"], delivery)
            report.update(
                {
                    "delivery_status": delivery.status,
                    "delivered_chats": delivery.delivered_chats,
                    "delivery_error": delivery.error,
                }
            )
            self.store.finish_run(run_id, "completed", report_id=report["id"])
            log = logger.info if delivery.status == "delivered" else logger.warning
            log(
                "Scheduled report run finished: run_id=%s job_id=%s report_id=%s "
                "delivery_status=%s delivered_chats=%d duration_ms=%d error=%s",
                run_id,
                job_id,
                report["id"],
                delivery.status,
                delivery.delivered_chats,
                int((time.monotonic() - started) * 1000),
                delivery.error,
            )
            if job.schedule_type == "once":
                self.store.set_job_state(job_id, False)
            else:
                scheduled = self.scheduler.get_job(job_id)
                self.store.update_next_run(
                    job_id, _iso(scheduled.next_run_time) if scheduled else None
                )
            return report
        except Exception as error:
            self.store.finish_run(run_id, "failed", error=str(error))
            logger.exception(
                "Scheduled report run failed: run_id=%s job_id=%s duration_ms=%d",
                run_id,
                job_id,
                int((time.monotonic() - started) * 1000),
            )
            if isinstance(error, SchedulerError):
                raise
            raise SchedulerError(f"Scheduled report failed: {error}") from error

    async def latest_report(self, job_id: str | None = None) -> dict[str, Any] | None:
        return self.store.latest_report(job_id)

    async def report_history(self, limit: int = 20) -> list[dict[str, Any]]:
        return self.store.report_history(limit)

    def _schedule(self, job: ScheduledReportJob):
        trigger = _build_trigger(job.schedule_type, job.schedule_value, job.timezone)
        scheduled = self.scheduler.add_job(
            self.run_job,
            trigger=trigger,
            args=[job.id],
            id=job.id,
            name=job.name,
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=300,
        )
        logger.debug(
            "APScheduler job registered: job_id=%s trigger=%s next_run_at=%s",
            job.id,
            trigger,
            _iso(scheduled.next_run_time),
        )
        return scheduled


def build_report(job: ScheduledReportJob, issues: list[dict[str, Any]]) -> dict[str, Any]:
    zone = _timezone(job.timezone)
    today = datetime.now(zone).date()
    status_counts: dict[str, int] = {}
    open_count = 0
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
            open_count += 1
        deadline = _issue_deadline(issue)
        if not is_closed and deadline and deadline < today:
            overdue.append(issue)
        priority = issue.get("priority") if isinstance(issue.get("priority"), dict) else {}
        priority_key = str(priority.get("key") or "").lower()
        if priority_key in {"critical", "blocker"}:
            critical.append(issue)
        if not is_closed and not issue.get("assignee"):
            unassigned.append(issue)

    attention: list[str] = []
    seen: set[str] = set()
    for label, group in (
        ("просрочена", overdue),
        ("критический приоритет", critical),
        ("нет исполнителя", unassigned),
    ):
        for issue in group:
            key = str(issue.get("key") or "без ключа")
            if key in seen:
                continue
            seen.add(key)
            attention.append(f"• {key} — {issue.get('summary', 'Без названия')} ({label})")
            if len(attention) >= 10:
                break
        if len(attention) >= 10:
            break

    generated_at = datetime.now(zone).isoformat()
    title = f"Сводка {job.queue} · {datetime.now(zone):%d.%m.%Y %H:%M}"
    lines = [
        title,
        "",
        f"Всего найдено: {len(issues)}",
        f"Открыто: {open_count}",
        f"Просрочено: {len(overdue)}",
        f"Критических: {len(critical)}",
        f"Без исполнителя: {len(unassigned)}",
    ]
    if status_counts:
        lines.extend(
            ["", "По статусам:"]
            + [f"• {name}: {count}" for name, count in sorted(status_counts.items())]
        )
    if attention:
        lines.extend(["", "Требуют внимания:"] + attention)
    elif issues:
        lines.extend(["", "Явных проблем в выбранной выборке не найдено."])
    else:
        lines.extend(["", "По заданному фильтру задач не найдено."])

    return {
        "id": uuid.uuid4().hex,
        "job_id": job.id,
        "title": title,
        "body": "\n".join(lines),
        "aggregates": {
            "total": len(issues),
            "open": open_count,
            "overdue": len(overdue),
            "critical": len(critical),
            "unassigned": len(unassigned),
            "status_counts": status_counts,
        },
        "generated_at": generated_at,
        "delivery_status": "pending",
        "delivered_chats": 0,
        "delivery_error": None,
    }


def _build_trigger(schedule_type: str, value: str, timezone_name: str):
    zone = _timezone(timezone_name)
    if schedule_type == "cron":
        try:
            return CronTrigger.from_crontab(value, timezone=zone)
        except ValueError as error:
            raise SchedulerError(f"Invalid cron expression: {error}") from error
    if schedule_type == "interval":
        try:
            minutes = int(value)
        except ValueError as error:
            raise SchedulerError("Interval schedule value must be minutes.") from error
        if minutes < 1:
            raise SchedulerError("Interval must be at least one minute.")
        return IntervalTrigger(minutes=minutes, timezone=zone)
    if schedule_type == "once":
        try:
            run_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise SchedulerError("run_at must be an ISO 8601 datetime.") from error
        if run_at.tzinfo is None:
            run_at = run_at.replace(tzinfo=zone)
        if run_at <= datetime.now(run_at.tzinfo):
            raise SchedulerError("run_at must be in the future.")
        return DateTrigger(run_date=run_at)
    raise SchedulerError("schedule_type must be once, interval, or cron.")


def _timezone(value: str) -> ZoneInfo:
    try:
        return ZoneInfo(value)
    except ZoneInfoNotFoundError as error:
        raise SchedulerError(f"Unknown IANA timezone: {value}") from error


def _issue_deadline(issue: dict[str, Any]) -> date | None:
    value = issue.get("deadline") or issue.get("dueDate")
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            return None


def _row_to_job(row: sqlite3.Row) -> ScheduledReportJob:
    return ScheduledReportJob(
        id=row["id"],
        name=row["name"],
        queue=row["queue"],
        schedule_type=row["schedule_type"],
        schedule_value=row["schedule_value"],
        timezone=row["timezone"],
        query=row["query"],
        max_issues=row["max_issues"],
        enabled=bool(row["enabled"]),
        deleted=bool(row["deleted"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        next_run_at=row["next_run_at"],
    )


def _row_to_report(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "job_id": row["job_id"],
        "title": row["title"],
        "body": row["body"],
        "aggregates": json.loads(row["aggregates_json"]),
        "generated_at": row["generated_at"],
        "delivery_status": row["delivery_status"],
        "delivered_chats": row["delivered_chats"],
        "delivery_error": row["delivery_error"],
    }


def _job_result(job: ScheduledReportJob) -> dict[str, Any]:
    return asdict(job)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None
