from __future__ import annotations

import hmac
import logging
import os
import sys
from contextlib import asynccontextmanager
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Literal

from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from pydantic import AnyHttpUrl, BaseModel, Field


logger = logging.getLogger(__name__)

if __package__ in {None, ""}:
    # Support `python src/yandex_tracker_mcp/server.py` in addition to package entry points.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from yandex_tracker_mcp.config import Settings
    from yandex_tracker_mcp.pipeline import PipelineError, PipelineRuntime
    from yandex_tracker_mcp.scheduler import SchedulerError, SchedulerRuntime
    from yandex_tracker_mcp.tracker import (
        CreateIssueCommand,
        TrackerError,
        TrackerGateway,
        UpdateIssueCommand,
        create_gateway,
    )
else:
    from .config import Settings
    from .pipeline import PipelineError, PipelineRuntime
    from .scheduler import SchedulerError, SchedulerRuntime
    from .tracker import (
        CreateIssueCommand,
        TrackerError,
        TrackerGateway,
        UpdateIssueCommand,
        create_gateway,
    )


class IssueResult(BaseModel):
    action: Literal["created", "already_exists", "read", "updated", "cancelled"]
    provider: Literal["yandex", "mock"]
    issue_id: str
    key: str
    summary: str
    version: int | None = None
    status_key: str | None = None
    status_display: str | None = None
    url: str | None = None


class TransitionResult(BaseModel):
    id: str
    display: str | None = None
    to_status_key: str | None = None
    to_status_display: str | None = None


class IssueListItem(BaseModel):
    key: str
    summary: str
    status: str | None = None
    priority: str | None = None
    assignee: str | None = None
    deadline: str | None = None
    url: str


class ScheduledJobResult(BaseModel):
    id: str
    name: str
    queue: str
    schedule_type: Literal["once", "interval", "cron"]
    schedule_value: str
    timezone: str
    query: str | None = None
    max_issues: int
    enabled: bool
    deleted: bool
    created_at: str
    updated_at: str
    next_run_at: str | None = None


class ReportResult(BaseModel):
    id: str
    job_id: str
    title: str
    body: str
    aggregates: dict
    generated_at: str
    delivery_status: str
    delivered_chats: int = 0
    delivery_error: str | None = None


class PipelineSearchResult(BaseModel):
    search_id: str
    queue: str
    query: str | None = None
    max_issues: int
    issues_found: int
    filters: dict
    issues: list[dict]
    created_at: str


class PipelineSummaryResult(BaseModel):
    summary_id: str
    search_id: str
    title: str
    focus: str | None = None
    aggregates: dict
    markdown: str
    created_at: str


class PipelineArtifactResult(BaseModel):
    artifact_id: str
    summary_id: str
    filename: str
    file_path: str
    size_bytes: int
    created_at: str
    telegram_status: str
    delivered_chats: int
    delivery_error: str | None = None


class StaticTokenVerifier(TokenVerifier):
    def __init__(self, expected_token: str) -> None:
        self._expected_token = expected_token

    async def verify_token(self, token: str) -> AccessToken | None:
        if not hmac.compare_digest(token, self._expected_token):
            return None
        return AccessToken(
            token=token,
            client_id="yandex-tracker-agent",
            scopes=["tracker:write"],
            expires_at=None,
        )


settings = Settings.from_env()
_scheduler_runtime: SchedulerRuntime | None = None
_pipeline_runtime: PipelineRuntime | None = None


def _build_mcp() -> FastMCP:
    kwargs: dict = {
        "name": "Yandex Tracker MCP",
        "instructions": (
            "Create, read, and update Yandex Tracker issues. Tracker does not support deleting "
            "individual issues; use list_issue_transitions and cancel_issue instead. Confirm all "
            "write operations with the user before calling them."
        ),
        "host": settings.mcp_host,
        "port": settings.mcp_port,
        "streamable_http_path": "/mcp",
        "stateless_http": True,
        "json_response": True,
    }
    if settings.mcp_api_key:
        kwargs["token_verifier"] = StaticTokenVerifier(settings.mcp_api_key)
        kwargs["auth"] = AuthSettings(
            issuer_url=AnyHttpUrl(settings.mcp_public_url),
            resource_server_url=AnyHttpUrl(f"{settings.mcp_public_url}/mcp"),
            required_scopes=["tracker:write"],
        )
    return FastMCP(**kwargs)


mcp = _build_mcp()


def _http_app():
    app = mcp.streamable_http_app()
    session_manager_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def process_lifespan(starlette_app):
        global _pipeline_runtime, _scheduler_runtime
        logger.info("MCP application startup: initializing scheduler and composition pipeline")
        runtime = SchedulerRuntime(settings)
        await runtime.start()
        _scheduler_runtime = runtime
        pipeline_runtime = PipelineRuntime(settings, gateway=runtime.gateway)
        await pipeline_runtime.start()
        _pipeline_runtime = pipeline_runtime
        logger.info("MCP application startup complete")
        try:
            async with session_manager_lifespan(starlette_app):
                yield
        finally:
            logger.info("MCP application shutdown started")
            _pipeline_runtime = None
            _scheduler_runtime = None
            await runtime.stop()
            logger.info("MCP application shutdown complete")

    app.router.lifespan_context = process_lifespan
    return app


@lru_cache(maxsize=1)
def _gateway() -> TrackerGateway:
    return create_gateway(settings)


def _scheduler() -> SchedulerRuntime:
    if _scheduler_runtime is None:
        raise SchedulerError("Scheduler is not running yet.")
    return _scheduler_runtime


def _pipeline() -> PipelineRuntime:
    if _pipeline_runtime is None:
        raise PipelineError("Composition pipeline is not running yet.")
    return _pipeline_runtime


def _required_text(value: str, name: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise TrackerError(f"{name} cannot be blank.")
    return cleaned


@mcp.tool(
    name="create_issue",
    title="Create Yandex Tracker issue",
    description=(
        "Create one Yandex Tracker issue and return its key and URL. This is a write operation "
        "and requires explicit user confirmation."
    ),
)
async def create_issue(
    summary: Annotated[str, Field(min_length=1, max_length=1024, description="Issue title.")],
    confirmed: Annotated[
        bool,
        Field(description="Must be true after the user confirmed the issue creation details."),
    ],
    queue: Annotated[
        str | None,
        Field(description="Queue key, for example TEST. Defaults to YANDEX_DEFAULT_QUEUE."),
    ] = None,
    description: Annotated[
        str | None, Field(max_length=50000, description="Issue description in Yandex Flavored Markdown.")
    ] = None,
    issue_type: Annotated[
        str | None, Field(description="Issue type key, for example task, bug, or epic.")
    ] = None,
    priority: Annotated[
        str | None, Field(description="Priority key, for example normal, minor, or critical.")
    ] = None,
    assignee: Annotated[str | None, Field(description="Assignee login or ID.")] = None,
    parent: Annotated[str | None, Field(description="Parent issue key or ID.")] = None,
    tags: Annotated[list[str] | None, Field(description="Tags to set on the issue.")] = None,
    followers: Annotated[
        list[str] | None, Field(description="Follower logins or IDs.")
    ] = None,
    unique: Annotated[
        str | None,
        Field(
            min_length=8,
            max_length=255,
            description="Organization-wide idempotency value that prevents duplicate issues.",
        ),
    ] = None,
    notify: Annotated[bool, Field(description="Send Tracker notifications.")] = True,
) -> IssueResult:
    if not confirmed:
        raise TrackerError("The issue was not created because confirmed must be true.")
    target_queue = _required_text(queue or settings.default_queue or "", "queue")
    command = CreateIssueCommand(
        summary=_required_text(summary, "summary"),
        queue=target_queue,
        description=description.strip() if description else None,
        issue_type=issue_type.strip() if issue_type else None,
        priority=priority.strip() if priority else None,
        assignee=assignee.strip() if assignee else None,
        parent=parent.strip() if parent else None,
        tags=tuple(tag.strip() for tag in tags or [] if tag.strip()),
        followers=tuple(item.strip() for item in followers or [] if item.strip()),
        unique=unique.strip() if unique else None,
        notify=notify,
    )
    return IssueResult.model_validate(await _gateway().create_issue(command))


@mcp.tool(
    name="get_issue",
    title="Get Yandex Tracker issue",
    description="Read one Yandex Tracker issue by key or ID.",
)
async def get_issue(
    issue_id: Annotated[str, Field(min_length=1, description="Issue key or ID, e.g. TEST-42.")],
) -> IssueResult:
    return IssueResult.model_validate(await _gateway().get_issue(_required_text(issue_id, "issue_id")))


@mcp.tool(
    name="search_issues",
    title="Search Yandex Tracker issues",
    description=(
        "Find issues for Telegram commands or an agent report. Use either a queue or a full "
        "Yandex Tracker query and return a compact list."
    ),
)
async def search_issues(
    queue: Annotated[
        str | None, Field(description="Queue key. Defaults to YANDEX_DEFAULT_QUEUE.")
    ] = None,
    query: Annotated[
        str | None,
        Field(description="Optional full Yandex Tracker query language expression."),
    ] = None,
    max_results: Annotated[
        int, Field(ge=1, le=100, description="Maximum number of issues to return.")
    ] = 20,
) -> list[IssueListItem]:
    target_queue = _required_text(queue or settings.default_queue or "", "queue")
    issues = await _gateway().search_issues(
        target_queue, query.strip() if query else None, max_results
    )
    results = []
    for issue in issues:
        key = str(issue.get("key") or "")
        status = issue.get("status") if isinstance(issue.get("status"), dict) else {}
        priority = issue.get("priority") if isinstance(issue.get("priority"), dict) else {}
        assignee = issue.get("assignee") if isinstance(issue.get("assignee"), dict) else {}
        results.append(
            IssueListItem(
                key=key,
                summary=str(issue.get("summary") or ""),
                status=status.get("display") or status.get("key"),
                priority=priority.get("display") or priority.get("key"),
                assignee=assignee.get("display") or assignee.get("id"),
                deadline=issue.get("deadline") or issue.get("dueDate"),
                url=f"https://tracker.yandex.ru/{key}",
            )
        )
    return results


@mcp.tool(
    name="search_tracker_issues",
    title="Pipeline step 1: search Tracker issues",
    description=(
        "First step of the report composition pipeline. Search Yandex Tracker, persist an exact "
        "snapshot in SQLite, and return search_id. When the user also asks to summarize, save, "
        "or send the result, pass this search_id to summarize_tracker_issues next. For common "
        "requests such as open or critical issues, use open_only/critical_only and leave query "
        "empty. Use query only when the user supplied an exact valid Yandex Tracker query."
    ),
)
async def search_tracker_issues(
    queue: Annotated[
        str | None, Field(description="Queue key. Defaults to YANDEX_DEFAULT_QUEUE.")
    ] = None,
    query: Annotated[
        str | None,
        Field(
            description=(
                "Optional exact Yandex Tracker query copied from the user. Do not generate a query "
                "for open or critical filters; use the structured boolean parameters instead."
            )
        ),
    ] = None,
    open_only: Annotated[
        bool,
        Field(description="Return only non-closed issues when the user asks for open tasks."),
    ] = False,
    critical_only: Annotated[
        bool,
        Field(description="Return only critical or blocker priority issues."),
    ] = False,
    max_issues: Annotated[
        int, Field(ge=1, le=100, description="Maximum issues stored in the search snapshot.")
    ] = 100,
) -> PipelineSearchResult:
    """Persist the source dataset so later tools pass IDs instead of inventing data."""
    result = await _pipeline().search(
        queue=_required_text(queue or settings.default_queue or "", "queue"),
        query=query.strip() if query else None,
        max_issues=max_issues,
        open_only=open_only,
        critical_only=critical_only,
    )
    return PipelineSearchResult.model_validate(result)


@mcp.tool(
    name="summarize_tracker_issues",
    title="Pipeline step 2: summarize Tracker issues",
    description=(
        "Second step of the report composition pipeline. Read the persisted snapshot identified "
        "by search_id, calculate aggregates, and save a Markdown summary. Use only a real search_id "
        "returned by search_tracker_issues. If the user asked to save or send the report, pass the "
        "returned summary_id to save_tracker_report next."
    ),
)
async def summarize_tracker_issues(
    search_id: Annotated[
        str,
        Field(min_length=1, description="Exact search_id returned by search_tracker_issues."),
    ],
    focus: Annotated[
        str | None,
        Field(max_length=1000, description="Optional emphasis requested by the user."),
    ] = None,
    title: Annotated[
        str | None, Field(max_length=200, description="Optional report title.")
    ] = None,
) -> PipelineSummaryResult:
    result = await _pipeline().summarize(
        search_id=_required_text(search_id, "search_id"),
        focus=focus.strip() if focus else None,
        title=title.strip() if title else None,
    )
    return PipelineSummaryResult.model_validate(result)


@mcp.tool(
    name="save_tracker_report",
    title="Pipeline step 3: save and optionally send report",
    description=(
        "Final step of the report composition pipeline. Use only a real summary_id returned by "
        "summarize_tracker_issues. Save the Markdown report to persistent storage and, when the "
        "user explicitly asked to send it to Telegram, set send_to_telegram=true. Return the actual "
        "file path and delivery status; never describe delivery as successful unless status is "
        "delivered and delivered_chats is greater than zero."
    ),
)
async def save_tracker_report(
    summary_id: Annotated[
        str,
        Field(min_length=1, description="Exact summary_id returned by summarize_tracker_issues."),
    ],
    filename: Annotated[
        str | None,
        Field(max_length=120, description="Optional safe base filename; .md is added automatically."),
    ] = None,
    send_to_telegram: Annotated[
        bool,
        Field(description="Send the saved file to active Telegram subscribers when explicitly asked."),
    ] = False,
    format: Annotated[
        Literal["markdown"], Field(description="Output format. Markdown is currently supported.")
    ] = "markdown",
) -> PipelineArtifactResult:
    del format
    result = await _pipeline().save_report(
        summary_id=_required_text(summary_id, "summary_id"),
        filename=filename.strip() if filename else None,
        send_to_telegram=send_to_telegram,
    )
    return PipelineArtifactResult.model_validate(result)


@mcp.tool(
    name="update_issue",
    title="Update Yandex Tracker issue",
    description=(
        "Update fields of one Yandex Tracker issue. Status changes require a transition and are "
        "handled by cancel_issue. This write operation requires explicit confirmation."
    ),
)
async def update_issue(
    issue_id: Annotated[str, Field(min_length=1, description="Issue key or ID.")],
    confirmed: Annotated[
        bool, Field(description="Must be true after the user confirmed the requested changes.")
    ],
    summary: Annotated[str | None, Field(max_length=1024, description="New issue title.")] = None,
    description: Annotated[
        str | None, Field(max_length=50000, description="New issue description.")
    ] = None,
    issue_type: Annotated[str | None, Field(description="New issue type key.")] = None,
    priority: Annotated[str | None, Field(description="New priority key.")] = None,
    assignee: Annotated[str | None, Field(description="New assignee login or ID.")] = None,
    clear_assignee: Annotated[bool, Field(description="Clear the current assignee.")] = False,
    parent: Annotated[str | None, Field(description="New parent issue key or ID.")] = None,
    clear_parent: Annotated[bool, Field(description="Remove the parent issue.")] = False,
    tags: Annotated[list[str] | None, Field(description="Replace all tags with this list.")] = None,
    add_tags: Annotated[list[str] | None, Field(description="Tags to add.")] = None,
    remove_tags: Annotated[list[str] | None, Field(description="Tags to remove.")] = None,
    version: Annotated[
        int | None, Field(ge=1, description="Optional current issue version for conflict protection.")
    ] = None,
) -> IssueResult:
    if not confirmed:
        raise TrackerError("The issue was not updated because confirmed must be true.")
    if clear_assignee and assignee:
        raise TrackerError("Use either assignee or clear_assignee, not both.")
    if clear_parent and parent:
        raise TrackerError("Use either parent or clear_parent, not both.")
    command = UpdateIssueCommand(
        issue_id=_required_text(issue_id, "issue_id"),
        summary=summary.strip() if summary is not None else None,
        description=description.strip() if description is not None else None,
        issue_type=issue_type.strip() if issue_type else None,
        priority=priority.strip() if priority else None,
        assignee=assignee.strip() if assignee else None,
        clear_assignee=clear_assignee,
        parent=parent.strip() if parent else None,
        clear_parent=clear_parent,
        tags=tuple(tag.strip() for tag in tags if tag.strip()) if tags is not None else None,
        add_tags=tuple(tag.strip() for tag in add_tags or [] if tag.strip()),
        remove_tags=tuple(tag.strip() for tag in remove_tags or [] if tag.strip()),
        version=version,
    )
    return IssueResult.model_validate(await _gateway().update_issue(command))


@mcp.tool(
    name="list_issue_transitions",
    title="List Yandex Tracker issue transitions",
    description="List status transitions currently available for an issue before cancelling it.",
)
async def list_issue_transitions(
    issue_id: Annotated[str, Field(min_length=1, description="Issue key or ID.")],
) -> list[TransitionResult]:
    transitions = await _gateway().list_transitions(_required_text(issue_id, "issue_id"))
    return [TransitionResult.model_validate(item) for item in transitions]


@mcp.tool(
    name="cancel_issue",
    title="Cancel Yandex Tracker issue",
    description=(
        "Cancel or close an issue through an explicit workflow transition. Yandex Tracker does "
        "not support deleting individual issues. Call list_issue_transitions first."
    ),
)
async def cancel_issue(
    issue_id: Annotated[str, Field(min_length=1, description="Issue key or ID.")],
    transition_id: Annotated[
        str, Field(min_length=1, description="Exact transition ID returned by list_issue_transitions.")
    ],
    confirmed: Annotated[
        bool, Field(description="Must be true after the user confirmed cancellation or closing.")
    ],
    comment: Annotated[
        str | None, Field(max_length=10000, description="Optional cancellation comment.")
    ] = None,
) -> IssueResult:
    if not confirmed:
        raise TrackerError("The issue was not cancelled because confirmed must be true.")
    result = await _gateway().cancel_issue(
        _required_text(issue_id, "issue_id"),
        _required_text(transition_id, "transition_id"),
        comment.strip() if comment else None,
    )
    return IssueResult.model_validate(result)


@mcp.tool(
    name="schedule_tracker_report",
    title="Schedule periodic Yandex Tracker report",
    description=(
        "Create a persisted once, interval, or cron report job. The scheduler stores jobs, runs, "
        "and aggregate reports in SQLite and sends completed reports to the Telegram bot service."
    ),
)
async def schedule_tracker_report(
    name: Annotated[str, Field(min_length=1, max_length=200, description="Schedule name.")],
    schedule_type: Annotated[
        Literal["once", "interval", "cron"],
        Field(description="Run once, every N minutes, or by five-field cron expression."),
    ],
    confirmed: Annotated[
        bool, Field(description="Must be true after the user confirmed the schedule details.")
    ],
    queue: Annotated[
        str | None, Field(description="Tracker queue key. Defaults to YANDEX_DEFAULT_QUEUE.")
    ] = None,
    run_at: Annotated[
        str | None, Field(description="ISO 8601 datetime required for schedule_type=once.")
    ] = None,
    interval_minutes: Annotated[
        int | None, Field(ge=1, le=525600, description="Minutes for schedule_type=interval.")
    ] = None,
    cron_expression: Annotated[
        str | None,
        Field(description="Five-field cron, e.g. '0 9 * * 1-5', for schedule_type=cron."),
    ] = None,
    timezone: Annotated[
        str | None, Field(description="IANA timezone. Defaults to SCHEDULER_TIMEZONE.")
    ] = None,
    query: Annotated[
        str | None, Field(description="Optional full Tracker query used instead of queue filter.")
    ] = None,
    max_issues: Annotated[
        int, Field(ge=1, le=100, description="Maximum issues aggregated per report.")
    ] = 100,
) -> ScheduledJobResult:
    if not confirmed:
        raise SchedulerError("The schedule was not created because confirmed must be true.")
    values = {
        "once": run_at,
        "interval": str(interval_minutes) if interval_minutes is not None else None,
        "cron": cron_expression,
    }
    schedule_value = values[schedule_type]
    if not schedule_value:
        raise SchedulerError(f"A schedule value is required for schedule_type={schedule_type}.")
    result = await _scheduler().create_job(
        name=_required_text(name, "name"),
        queue=_required_text(queue or settings.default_queue or "", "queue"),
        schedule_type=schedule_type,
        schedule_value=schedule_value.strip(),
        timezone_name=(timezone or settings.scheduler_timezone).strip(),
        query=query.strip() if query else None,
        max_issues=max_issues,
    )
    return ScheduledJobResult.model_validate(result)


@mcp.tool(
    name="list_scheduled_reports",
    title="List scheduled Tracker reports",
    description="List persisted report schedules with their next run time and enabled state.",
)
async def list_scheduled_reports() -> list[ScheduledJobResult]:
    return [ScheduledJobResult.model_validate(job) for job in await _scheduler().list_jobs()]


@mcp.tool(
    name="pause_scheduled_report",
    title="Pause scheduled Tracker report",
    description="Pause a report schedule without deleting its report history.",
)
async def pause_scheduled_report(
    job_id: Annotated[str, Field(min_length=1, description="Scheduled job ID.")],
    confirmed: Annotated[bool, Field(description="Must be true after user confirmation.")],
) -> ScheduledJobResult:
    if not confirmed:
        raise SchedulerError("The schedule was not paused because confirmed must be true.")
    return ScheduledJobResult.model_validate(
        await _scheduler().pause_job(_required_text(job_id, "job_id"))
    )


@mcp.tool(
    name="resume_scheduled_report",
    title="Resume scheduled Tracker report",
    description="Resume a paused report schedule.",
)
async def resume_scheduled_report(
    job_id: Annotated[str, Field(min_length=1, description="Scheduled job ID.")],
    confirmed: Annotated[bool, Field(description="Must be true after user confirmation.")],
) -> ScheduledJobResult:
    if not confirmed:
        raise SchedulerError("The schedule was not resumed because confirmed must be true.")
    return ScheduledJobResult.model_validate(
        await _scheduler().resume_job(_required_text(job_id, "job_id"))
    )


@mcp.tool(
    name="delete_scheduled_report",
    title="Delete scheduled Tracker report",
    description="Delete a schedule while preserving its previously generated reports in SQLite.",
)
async def delete_scheduled_report(
    job_id: Annotated[str, Field(min_length=1, description="Scheduled job ID.")],
    confirmed: Annotated[bool, Field(description="Must be true after user confirmation.")],
) -> ScheduledJobResult:
    if not confirmed:
        raise SchedulerError("The schedule was not deleted because confirmed must be true.")
    return ScheduledJobResult.model_validate(
        await _scheduler().delete_job(_required_text(job_id, "job_id"))
    )


@mcp.tool(
    name="run_scheduled_report_now",
    title="Run Tracker report now",
    description="Run a persisted report job immediately, save it, and deliver it to Telegram.",
)
async def run_scheduled_report_now(
    job_id: Annotated[str, Field(min_length=1, description="Scheduled job ID.")],
    confirmed: Annotated[bool, Field(description="Must be true after user confirmation.")],
) -> ReportResult:
    if not confirmed:
        raise SchedulerError("The report was not run because confirmed must be true.")
    return ReportResult.model_validate(
        await _scheduler().run_job(_required_text(job_id, "job_id"))
    )


@mcp.tool(
    name="get_latest_tracker_report",
    title="Get latest Tracker report",
    description="Return the most recent persisted aggregate report, optionally for one job.",
)
async def get_latest_tracker_report(
    job_id: Annotated[str | None, Field(description="Optional scheduled job ID.")] = None,
) -> ReportResult:
    report = await _scheduler().latest_report(job_id.strip() if job_id else None)
    if report is None:
        raise SchedulerError("No generated reports were found.")
    return ReportResult.model_validate(report)


@mcp.tool(
    name="get_tracker_report_history",
    title="Get Tracker report history",
    description="Return recent reports persisted in SQLite.",
)
async def get_tracker_report_history(
    limit: Annotated[int, Field(ge=1, le=100, description="Maximum reports to return.")] = 20,
) -> list[ReportResult]:
    return [
        ReportResult.model_validate(report)
        for report in await _scheduler().report_history(limit)
    ]


def main() -> None:
    import uvicorn

    settings.validate_for_startup()
    log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("yandex_tracker_mcp").setLevel(log_level)
    logger.info(
        "Starting Yandex Tracker MCP: host=%s port=%d public_url=%s backend=%s "
        "scheduler_timezone=%s bot_service_url=%s",
        settings.mcp_host,
        settings.mcp_port,
        settings.mcp_public_url,
        settings.backend,
        settings.scheduler_timezone,
        settings.bot_service_url or "not configured",
    )
    uvicorn.run(
        _http_app(),
        host=settings.mcp_host,
        port=settings.mcp_port,
        log_level=log_level.lower(),
    )


if __name__ == "__main__":
    main()
