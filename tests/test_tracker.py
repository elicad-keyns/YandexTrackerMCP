from __future__ import annotations

import pytest

from yandex_tracker_mcp.config import Settings
from yandex_tracker_mcp.tracker import (
    CreateIssueCommand,
    MockTrackerGateway,
    TrackerError,
    UpdateIssueCommand,
    create_issue_payload,
    update_issue_payload,
)


def settings(**overrides):
    values = {
        "backend": "yandex",
        "tracker_token": "y0_valid-oauth-token",
        "auth_type": "oauth",
        "org_id": "1234567",
        "org_header": "X-Org-ID",
        "default_queue": "TEST",
        "api_url": "https://api.tracker.yandex.net/v3",
        "mcp_api_key": "mcp-secret",
        "allow_insecure_no_auth": False,
        "mcp_host": "127.0.0.1",
        "mcp_port": 8788,
        "mcp_public_url": "http://localhost:8788",
    }
    values.update(overrides)
    return Settings(**values)


def create_command(**overrides):
    values = {
        "summary": "Day 17 MCP demo",
        "queue": "TEST",
        "description": "Created through MCP",
        "issue_type": "task",
        "priority": "normal",
        "assignee": None,
        "parent": None,
        "tags": ("mcp",),
        "followers": (),
        "unique": "day17-tracker-demo",
        "notify": False,
    }
    values.update(overrides)
    return CreateIssueCommand(**values)


def update_command(**overrides):
    values = {
        "issue_id": "TEST-1",
        "summary": "Updated through MCP",
        "description": None,
        "issue_type": None,
        "priority": None,
        "assignee": None,
        "clear_assignee": False,
        "parent": None,
        "clear_parent": False,
        "tags": None,
        "add_tags": ("updated",),
        "remove_tags": (),
        "version": 1,
    }
    values.update(overrides)
    return UpdateIssueCommand(**values)


def test_create_payload_matches_tracker_api() -> None:
    payload, params = create_issue_payload(create_command())
    assert payload["queue"] == "TEST"
    assert payload["summary"] == "Day 17 MCP demo"
    assert payload["markupType"] == "md"
    assert payload["unique"] == "day17-tracker-demo"
    assert params == {"notify": "false"}


def test_update_payload_uses_tag_operators() -> None:
    payload, params = update_issue_payload(update_command())
    assert payload == {"summary": "Updated through MCP", "tags": {"add": ["updated"]}}
    assert params == {"version": 1}


def test_update_rejects_conflicting_tag_modes() -> None:
    with pytest.raises(TrackerError):
        update_issue_payload(update_command(tags=("one",), add_tags=("two",)))


def test_settings_reject_client_credentials_in_tracker_fields() -> None:
    with pytest.raises(ValueError, match="Client Secret"):
        settings(tracker_token="f" * 32).validate_for_startup()
    with pytest.raises(ValueError, match="numeric organization ID"):
        settings(org_id="a" * 32).validate_for_startup()
    with pytest.raises(ValueError, match="real Tracker queue key"):
        settings(default_queue="YOURQUEUE").validate_for_startup()


@pytest.mark.asyncio
async def test_mock_create_update_read_and_cancel_flow() -> None:
    gateway = MockTrackerGateway()
    created = await gateway.create_issue(create_command())
    duplicate = await gateway.create_issue(create_command())
    updated = await gateway.update_issue(update_command())
    transitions = await gateway.list_transitions("TEST-1")
    cancelled = await gateway.cancel_issue("TEST-1", "cancel", "No longer needed")
    read = await gateway.get_issue("TEST-1")

    assert created["action"] == "created"
    assert duplicate["action"] == "already_exists"
    assert updated["summary"] == "Updated through MCP"
    assert transitions[0]["id"] == "cancel"
    assert cancelled["status_key"] == "cancelled"
    assert read["version"] == 3
