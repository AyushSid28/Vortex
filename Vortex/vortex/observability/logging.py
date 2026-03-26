# Structured JSON logging

from __future__ import annotations
import json
import sys
import time
from typing import Any, TextIO


class StructuredLogger:
    """JSON logger with run_id correlation and agent event tracking.

    Stores records in _records list for testability.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream or sys.stderr
        self._records: list[dict[str, Any]] = []

    def _emit(self, event: str, **fields: Any) -> None:
        record = {"event": event, "ts": time.time(), **fields}
        self._records.append(record)
        self._stream.write(json.dumps(record) + "\n")

    def workflow_start(self, run_id: str, workflow: str) -> None:
        self._emit("workflow_start", run_id=run_id, workflow=workflow)

    def workflow_complete(self, run_id: str, workflow: str, duration_ms: float) -> None:
        self._emit("workflow_complete", run_id=run_id, workflow=workflow, duration_ms=round(duration_ms, 2))

    def workflow_failed(self, run_id: str, workflow: str, error: str, duration_ms: float) -> None:
        self._emit("workflow_failed", run_id=run_id, workflow=workflow, error=error, duration_ms=round(duration_ms, 2))

    def agent_start(self, run_id: str, agent: str) -> None:
        self._emit("agent_start", run_id=run_id, agent=agent)

    def agent_complete(self, run_id: str, agent: str, duration_ms: float, retry_count: int = 0) -> None:
        self._emit("agent_complete", run_id=run_id, agent=agent, duration_ms=round(duration_ms, 2), retry_count=retry_count)

    def agent_failed(self, run_id: str, agent: str, error: str, duration_ms: float, retry_count: int = 0) -> None:
        self._emit("agent_failed", run_id=run_id, agent=agent, error=error, duration_ms=round(duration_ms, 2), retry_count=retry_count)
