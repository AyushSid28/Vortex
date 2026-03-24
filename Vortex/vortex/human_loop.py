# Human-in-the-loop (pause/approve/resume)

from __future__ import annotations
import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Any


class ApprovalStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


@dataclass
class ApprovalRequest:
    run_id: str
    agent_name: str
    output: dict[str, Any]
    status: ApprovalStatus = ApprovalStatus.PENDING
    feedback: str | None = None


class HumanLoop:
    """Manages human-in-loop approval gates.

    When an agent with requires_approval=True completes, the engine
    creates an ApprovalRequest and awaits approve() or reject() from
    the user (API, CLI). Uses asyncio.Event so the workflow suspends
    without blocking.
    """

    def __init__(self) -> None:
        self._requests: dict[str, ApprovalRequest] = {}
        self._events: dict[str, asyncio.Event] = {}

    def _key(self, run_id: str, agent_name: str) -> str:
        return f"{run_id}:{agent_name}"

    async def wait_for_approval(
        self, run_id: str, agent_name: str, output: dict[str, Any]
    ) -> ApprovalRequest:
        key = self._key(run_id, agent_name)
        request = ApprovalRequest(run_id=run_id, agent_name=agent_name, output=output)
        self._requests[key] = request
        self._events[key] = asyncio.Event()
        await self._events[key].wait()
        return self._requests[key]

    def approve(self, run_id: str, agent_name: str, feedback: str | None = None) -> None:
        key = self._key(run_id, agent_name)
        req = self._requests[key]
        req.status = ApprovalStatus.APPROVED
        req.feedback = feedback
        self._events[key].set()

    def reject(self, run_id: str, agent_name: str, feedback: str | None = None) -> None:
        key = self._key(run_id, agent_name)
        req = self._requests[key]
        req.status = ApprovalStatus.REJECTED
        req.feedback = feedback
        self._events[key].set()

    def get_pending(self) -> list[ApprovalRequest]:
        return [r for r in self._requests.values() if r.status == ApprovalStatus.PENDING]

    def get_request(self, run_id: str, agent_name: str) -> ApprovalRequest | None:
        return self._requests.get(self._key(run_id, agent_name))
