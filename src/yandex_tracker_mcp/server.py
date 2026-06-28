from __future__ import annotations

import hmac
import sys
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Literal

from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from pydantic import AnyHttpUrl, BaseModel, Field

if __package__ in {None, ""}:
    # Support `python src/yandex_tracker_mcp/server.py` in addition to package entry points.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from yandex_tracker_mcp.config import Settings
    from yandex_tracker_mcp.tracker import (
        CreateIssueCommand,
        TrackerError,
        TrackerGateway,
        UpdateIssueCommand,
        create_gateway,
    )
else:
    from .config import Settings
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


@lru_cache(maxsize=1)
def _gateway() -> TrackerGateway:
    return create_gateway(settings)


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


def main() -> None:
    settings.validate_for_startup()
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
