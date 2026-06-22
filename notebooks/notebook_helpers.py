"""Small helpers shared by Reflexio notebooks.

Keep ReflexioClient calls in notebooks so examples show the public API shape.
This module is for polling, model conversion, and fixed demo payloads only.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any

from reflexio import InteractionData


def wait_for(
    label: str,
    check: Callable[[], Any],
    timeout_seconds: int = 180,
    interval_seconds: float = 3.0,
) -> Any:
    """Poll until ``check`` returns a truthy value.

    Transient API errors are kept as the last value and retried. This makes
    notebook execution less brittle while the local backend is running
    extraction workers.
    """
    deadline = time.monotonic() + timeout_seconds
    last_value: Any = None
    while time.monotonic() < deadline:
        try:
            last_value = check()
        except Exception as exc:  # noqa: BLE001 - notebooks should retry transient API failures
            last_value = exc
        if last_value and not isinstance(last_value, Exception):
            return last_value
        time.sleep(interval_seconds)
    raise TimeoutError(f"Timed out waiting for {label}. Last value: {last_value!r}")


def as_dict(model: Any) -> dict[str, Any]:
    """Convert a Pydantic model or dict to a JSON-friendly dict."""
    if hasattr(model, "model_dump"):
        return model.model_dump(mode="json")
    if isinstance(model, dict):
        return model
    raise TypeError(f"Expected a dict or Pydantic model, got {type(model).__name__}")


def resumable_playbook_interactions(run_marker: str) -> list[InteractionData]:
    """Return the fixed interaction that should trigger an ask_human playbook run."""
    return [
        InteractionData(
            role="user",
            content=(
                f"{run_marker}. A customer is disputing a billing charge. "
                "The agent should create a durable support playbook for future "
                "billing disputes, but the organization-wide escalation path is "
                "not present in this transcript. Do not guess it."
            ),
        ),
        InteractionData(
            role="assistant",
            content=(
                "I should not invent the escalation path. I need a model "
                "developer to provide the canonical billing-dispute procedure "
                "before Reflexio stores the durable playbook."
            ),
        ),
    ]


def question_mentions_marker(question: dict[str, Any], run_marker: str) -> bool:
    """Return True when a pending-tool-call payload belongs to this notebook run."""
    haystack = json.dumps(
        {
            "question_text": question.get("question_text"),
            "args": question.get("args"),
            "tags": question.get("tags"),
            "result": question.get("result"),
        },
        default=str,
    )
    return run_marker in haystack


def playbook_text(playbook: Any) -> str:
    """Concatenate text fields that may contain notebook marker/answer text."""
    return "\n".join(
        str(getattr(playbook, field, "") or "")
        for field in ("trigger", "content", "rationale", "source")
    )


def dedupe_playbooks(playbooks: list[Any]) -> list[Any]:
    """Deduplicate playbook objects from exact-list and search calls."""
    seen: set[tuple[Any, str]] = set()
    unique = []
    for playbook in playbooks:
        key = (getattr(playbook, "user_playbook_id", None), playbook_text(playbook))
        if key not in seen:
            seen.add(key)
            unique.append(playbook)
    return unique
