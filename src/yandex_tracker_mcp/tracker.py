from __future__ import annotations

import copy
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import quote

import httpx

from .config import Settings


logger = logging.getLogger(__name__)


class TrackerError(RuntimeError):
    """A safe error that may be returned to an MCP client."""


@dataclass(frozen=True, slots=True)
class CreateIssueCommand:
    summary: str
    queue: str
    description: str | None
    issue_type: str | None
    priority: str | None
    assignee: str | None
    parent: str | None
    tags: tuple[str, ...]
    followers: tuple[str, ...]
    unique: str | None
    notify: bool


@dataclass(frozen=True, slots=True)
class UpdateIssueCommand:
    issue_id: str
    summary: str | None
    description: str | None
    issue_type: str | None
    priority: str | None
    assignee: str | None
    clear_assignee: bool
    parent: str | None
    clear_parent: bool
    tags: tuple[str, ...] | None
    add_tags: tuple[str, ...]
    remove_tags: tuple[str, ...]
    version: int | None


class TrackerGateway(Protocol):
    async def create_issue(self, command: CreateIssueCommand) -> dict[str, Any]: ...

    async def get_issue(self, issue_id: str) -> dict[str, Any]: ...

    async def search_issues(
        self, queue: str, query: str | None, max_results: int
    ) -> list[dict[str, Any]]: ...

    async def update_issue(self, command: UpdateIssueCommand) -> dict[str, Any]: ...

    async def list_transitions(self, issue_id: str) -> list[dict[str, Any]]: ...

    async def cancel_issue(
        self, issue_id: str, transition_id: str, comment: str | None
    ) -> dict[str, Any]: ...


def create_issue_payload(command: CreateIssueCommand) -> tuple[dict[str, Any], dict[str, Any]]:
    payload: dict[str, Any] = {"summary": command.summary, "queue": command.queue}
    if command.description is not None:
        payload.update({"description": command.description, "markupType": "md"})
    if command.issue_type:
        payload["type"] = command.issue_type
    if command.priority:
        payload["priority"] = command.priority
    if command.assignee:
        payload["assignee"] = command.assignee
    if command.parent:
        payload["parent"] = command.parent
    if command.tags:
        payload["tags"] = list(command.tags)
    if command.followers:
        payload["followers"] = list(command.followers)
    if command.unique:
        payload["unique"] = command.unique
    return payload, {"notify": str(command.notify).lower()}


def update_issue_payload(command: UpdateIssueCommand) -> tuple[dict[str, Any], dict[str, Any]]:
    if command.tags is not None and (command.add_tags or command.remove_tags):
        raise TrackerError("Use either tags or add_tags/remove_tags, not both.")
    payload: dict[str, Any] = {}
    for key, value in (
        ("summary", command.summary),
        ("description", command.description),
        ("type", command.issue_type),
        ("priority", command.priority),
    ):
        if value is not None:
            payload[key] = value
    if command.clear_assignee:
        payload["assignee"] = None
    elif command.assignee is not None:
        payload["assignee"] = command.assignee
    if command.clear_parent:
        payload["parent"] = None
    elif command.parent is not None:
        payload["parent"] = command.parent
    if command.tags is not None:
        payload["tags"] = list(command.tags)
    elif command.add_tags or command.remove_tags:
        payload["tags"] = {}
        if command.add_tags:
            payload["tags"]["add"] = list(command.add_tags)
        if command.remove_tags:
            payload["tags"]["remove"] = list(command.remove_tags)
    if not payload:
        raise TrackerError("At least one field must be provided for update.")
    params = {"version": command.version} if command.version is not None else {}
    return payload, params


class YandexTrackerGateway:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(timeout=30.0)

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": self._settings.tracker_authorization,
            self._settings.org_header: self._settings.org_id or "",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def create_issue(self, command: CreateIssueCommand) -> dict[str, Any]:
        payload, params = create_issue_payload(command)
        result = await self._request("POST", "/issues/", params=params, json=payload)
        return _issue_result(_object_payload(result), "created", "yandex")

    async def get_issue(self, issue_id: str) -> dict[str, Any]:
        result = await self._request("GET", f"/issues/{_path(issue_id)}")
        return _issue_result(_object_payload(result), "read", "yandex")

    async def search_issues(
        self, queue: str, query: str | None, max_results: int
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any]
        if query:
            payload = {"query": query}
        else:
            payload = {"filter": {"queue": queue}, "order": "-updatedAt"}
        result = await self._request(
            "POST",
            "/issues/_search",
            params={
                "perPage": max_results,
                "fields": (
                    "id,key,summary,status,priority,assignee,deadline,dueDate,"
                    "updatedAt,createdAt,queue,tags"
                ),
            },
            json=payload,
        )
        if not isinstance(result, list):
            raise TrackerError("Yandex Tracker returned an unexpected search response.")
        return [item for item in result if isinstance(item, dict)][:max_results]

    async def update_issue(self, command: UpdateIssueCommand) -> dict[str, Any]:
        payload, params = update_issue_payload(command)
        result = await self._request(
            "PATCH", f"/issues/{_path(command.issue_id)}", params=params, json=payload
        )
        return _issue_result(_object_payload(result), "updated", "yandex")

    async def list_transitions(self, issue_id: str) -> list[dict[str, Any]]:
        result = await self._request("GET", f"/issues/{_path(issue_id)}/transitions")
        if not isinstance(result, list):
            raise TrackerError("Yandex Tracker returned an unexpected transitions response.")
        return [_transition_result(item) for item in result if isinstance(item, dict)]

    async def cancel_issue(
        self, issue_id: str, transition_id: str, comment: str | None
    ) -> dict[str, Any]:
        transitions = await self.list_transitions(issue_id)
        if transition_id not in {item["id"] for item in transitions}:
            available = ", ".join(item["id"] for item in transitions) or "none"
            raise TrackerError(
                f"Transition '{transition_id}' is not available. Available transitions: {available}."
            )
        payload = {"comment": comment} if comment else {}
        await self._request(
            "POST",
            f"/issues/{_path(issue_id)}/transitions/{_path(transition_id)}/_execute",
            json=payload,
        )
        result = await self._request("GET", f"/issues/{_path(issue_id)}")
        return _issue_result(_object_payload(result), "cancelled", "yandex")

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        started = time.monotonic()
        logger.info(
            "Yandex Tracker API request started: method=%s path=%s param_keys=%s body_keys=%s",
            method,
            path,
            sorted((kwargs.get("params") or {}).keys()),
            sorted((kwargs.get("json") or {}).keys()),
        )
        try:
            response = await self._client.request(
                method, f"{self._settings.api_url}{path}", headers=self._headers, **kwargs
            )
        except httpx.HTTPError as error:
            logger.exception(
                "Yandex Tracker API transport failed: method=%s path=%s duration_ms=%d",
                method,
                path,
                int((time.monotonic() - started) * 1000),
            )
            raise TrackerError(f"Yandex Tracker request failed: {error}") from error
        try:
            payload = response.json()
        except ValueError as error:
            logger.error(
                "Yandex Tracker returned invalid JSON: method=%s path=%s http_status=%d "
                "duration_ms=%d response=%r",
                method,
                path,
                response.status_code,
                int((time.monotonic() - started) * 1000),
                response.text[:1000],
            )
            raise TrackerError(
                f"Yandex Tracker returned HTTP {response.status_code} with invalid JSON."
            ) from error
        if response.is_error:
            message = _error_message(payload) or response.reason_phrase
            logger.error(
                "Yandex Tracker API error: method=%s path=%s http_status=%d duration_ms=%d "
                "message=%s",
                method,
                path,
                response.status_code,
                int((time.monotonic() - started) * 1000),
                message,
            )
            raise TrackerError(f"Yandex Tracker API error {response.status_code}: {message}")
        result_count = len(payload) if isinstance(payload, list) else None
        logger.info(
            "Yandex Tracker API request succeeded: method=%s path=%s http_status=%d "
            "duration_ms=%d result_count=%s",
            method,
            path,
            response.status_code,
            int((time.monotonic() - started) * 1000),
            result_count,
        )
        return payload


class MockTrackerGateway:
    def __init__(self, default_queue: str = "TEST") -> None:
        self._default_queue = default_queue
        self._issues: dict[str, dict[str, Any]] = {}
        self._unique: dict[str, str] = {}

    async def create_issue(self, command: CreateIssueCommand) -> dict[str, Any]:
        if command.unique and command.unique in self._unique:
            issue = self._issues[self._unique[command.unique]]
            return _issue_result(issue, "already_exists", "mock")
        number = len(self._issues) + 1
        key = f"{command.queue or self._default_queue}-{number}"
        issue = {
            "id": uuid.uuid5(uuid.NAMESPACE_URL, key).hex,
            "key": key,
            "summary": command.summary,
            "description": command.description,
            "version": 1,
            "status": {"key": "open", "display": "Открыта"},
            "tags": list(command.tags),
            "assignee": command.assignee,
        }
        self._issues[key] = issue
        if command.unique:
            self._unique[command.unique] = key
        return _issue_result(issue, "created", "mock")

    async def get_issue(self, issue_id: str) -> dict[str, Any]:
        return _issue_result(self._find(issue_id), "read", "mock")

    async def search_issues(
        self, queue: str, query: str | None, max_results: int
    ) -> list[dict[str, Any]]:
        del query
        prefix = f"{queue.upper()}-"
        return [
            copy.deepcopy(issue)
            for issue in self._issues.values()
            if issue["key"].upper().startswith(prefix)
        ][:max_results]

    async def update_issue(self, command: UpdateIssueCommand) -> dict[str, Any]:
        payload, _ = update_issue_payload(command)
        issue = self._find(command.issue_id)
        for key, value in payload.items():
            if key == "tags" and isinstance(value, dict):
                tags = set(issue.get("tags", []))
                tags.update(value.get("add", []))
                tags.difference_update(value.get("remove", []))
                issue["tags"] = sorted(tags)
            else:
                issue[key] = value
        issue["version"] += 1
        return _issue_result(issue, "updated", "mock")

    async def list_transitions(self, issue_id: str) -> list[dict[str, Any]]:
        self._find(issue_id)
        return [
            {
                "id": "cancel",
                "display": "Отменить",
                "to_status_key": "cancelled",
                "to_status_display": "Отменена",
            }
        ]

    async def cancel_issue(
        self, issue_id: str, transition_id: str, comment: str | None
    ) -> dict[str, Any]:
        if transition_id != "cancel":
            raise TrackerError("Mock transition must be 'cancel'.")
        issue = self._find(issue_id)
        issue["status"] = {"key": "cancelled", "display": "Отменена"}
        issue["version"] += 1
        if comment:
            issue["cancel_comment"] = comment
        return _issue_result(issue, "cancelled", "mock")

    def _find(self, issue_id: str) -> dict[str, Any]:
        if issue_id in self._issues:
            return self._issues[issue_id]
        for issue in self._issues.values():
            if issue["id"] == issue_id:
                return issue
        raise TrackerError(f"Issue '{issue_id}' was not found.")


def create_gateway(settings: Settings) -> TrackerGateway:
    if settings.backend == "mock":
        return MockTrackerGateway(settings.default_queue or "TEST")
    return YandexTrackerGateway(settings)


def _path(value: str) -> str:
    return quote(value.strip(), safe="")


def _object_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload[0]
    raise TrackerError("Yandex Tracker returned an unexpected issue response.")


def _error_message(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    return payload.get("errorMessages", [None])[0] or payload.get("message") or payload.get("error")


def _issue_result(issue: dict[str, Any], action: str, provider: str) -> dict[str, Any]:
    source = copy.deepcopy(issue)
    status = source.get("status") if isinstance(source.get("status"), dict) else {}
    key = str(source.get("key") or "")
    return {
        "action": action,
        "provider": provider,
        "issue_id": str(source.get("id") or ""),
        "key": key,
        "summary": str(source.get("summary") or ""),
        "version": source.get("version"),
        "status_key": status.get("key"),
        "status_display": status.get("display"),
        "url": f"https://tracker.yandex.ru/{key}" if key else None,
    }


def _transition_result(transition: dict[str, Any]) -> dict[str, Any]:
    target = transition.get("to") if isinstance(transition.get("to"), dict) else {}
    return {
        "id": str(transition.get("id") or ""),
        "display": transition.get("display"),
        "to_status_key": target.get("key"),
        "to_status_display": target.get("display"),
    }
