# Prometheus metrics

from __future__ import annotations
from prometheus_client import Counter, Histogram, Gauge


class MetricsCollector:
    """Prometheus metrics for workflow and agent monitoring."""

    def __init__(self, prefix: str = "vortex") -> None:
        self.workflow_runs = Counter(
            f"{prefix}_workflow_runs_total",
            "Total workflow runs",
            ["workflow", "status"],
        )
        self.workflow_duration = Histogram(
            f"{prefix}_workflow_duration_seconds",
            "Workflow execution duration",
            ["workflow"],
        )
        self.agent_runs = Counter(
            f"{prefix}_agent_runs_total",
            "Total agent executions",
            ["agent", "status"],
        )
        self.agent_duration = Histogram(
            f"{prefix}_agent_duration_seconds",
            "Agent execution duration",
            ["agent"],
        )
        self.active_workflows = Gauge(
            f"{prefix}_active_workflows",
            "Number of currently running workflows",
        )
        self.retries = Counter(
            f"{prefix}_agent_retries_total",
            "Total agent retry attempts",
            ["agent"],
        )

    def on_workflow_start(self) -> None:
        self.active_workflows.inc()

    def on_workflow_complete(self, workflow: str, duration_s: float) -> None:
        self.active_workflows.dec()
        self.workflow_runs.labels(workflow=workflow, status="completed").inc()
        self.workflow_duration.labels(workflow=workflow).observe(duration_s)

    def on_workflow_failed(self, workflow: str, duration_s: float) -> None:
        self.active_workflows.dec()
        self.workflow_runs.labels(workflow=workflow, status="failed").inc()
        self.workflow_duration.labels(workflow=workflow).observe(duration_s)

    def on_agent_complete(self, agent: str, duration_s: float, retry_count: int = 0) -> None:
        self.agent_runs.labels(agent=agent, status="completed").inc()
        self.agent_duration.labels(agent=agent).observe(duration_s)
        if retry_count > 0:
            self.retries.labels(agent=agent).inc(retry_count)

    def on_agent_failed(self, agent: str, duration_s: float, retry_count: int = 0) -> None:
        self.agent_runs.labels(agent=agent, status="failed").inc()
        self.agent_duration.labels(agent=agent).observe(duration_s)
        if retry_count > 0:
            self.retries.labels(agent=agent).inc(retry_count)
