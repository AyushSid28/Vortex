import asyncio
import io
import json
import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from vortex.agent import Agent, AgentStatus
from vortex.workflow import Workflow
from vortex.engine import WorkflowEngine
from vortex.observability.logging import StructuredLogger
from vortex.observability.metrics import MetricsCollector
from vortex.observability.tracing import WorkflowTracer


# ── Agents ───────────────────────────────────────────────────

class AddAgent(Agent):
    name = "add"
    async def execute(self, state):
        return {"value": state.get("value", 0) + 10}


class MultiplyAgent(Agent):
    name = "multiply"
    async def execute(self, state):
        return {"value": state["value"] * 2}


class FailAgent(Agent):
    name = "fail"
    max_retries = 1
    backoff_base = 0.01
    async def execute(self, state):
        raise RuntimeError("boom")


# ── Structured Logging tests ────────────────────────────────

@pytest.mark.asyncio
async def test_logger_records_workflow_lifecycle():
    buf = io.StringIO()
    logger = StructuredLogger(stream=buf)
    wf = Workflow(name="log-test", agents=[AddAgent()], edges=[])
    engine = WorkflowEngine(logger=logger)

    await engine.run(wf, {"value": 5})

    events = [r["event"] for r in logger._records]
    assert "workflow_start" in events
    assert "agent_start" in events
    assert "agent_complete" in events
    assert "workflow_complete" in events


@pytest.mark.asyncio
async def test_logger_records_failure():
    buf = io.StringIO()
    logger = StructuredLogger(stream=buf)
    wf = Workflow(name="fail-log", agents=[FailAgent()], edges=[])
    engine = WorkflowEngine(logger=logger)

    with pytest.raises(RuntimeError):
        await engine.run(wf, {})

    events = [r["event"] for r in logger._records]
    assert "workflow_start" in events
    assert "agent_failed" in events
    assert "workflow_failed" in events


@pytest.mark.asyncio
async def test_logger_includes_run_id():
    logger = StructuredLogger(stream=io.StringIO())
    wf = Workflow(name="rid-test", agents=[AddAgent()], edges=[])
    engine = WorkflowEngine(logger=logger)

    await engine.run(wf, {"value": 1})

    run_ids = {r.get("run_id") for r in logger._records}
    run_ids.discard(None)
    assert len(run_ids) == 1


@pytest.mark.asyncio
async def test_logger_outputs_valid_json():
    buf = io.StringIO()
    logger = StructuredLogger(stream=buf)
    wf = Workflow(name="json-test", agents=[AddAgent()], edges=[])
    engine = WorkflowEngine(logger=logger)

    await engine.run(wf, {"value": 1})

    for line in buf.getvalue().strip().split("\n"):
        parsed = json.loads(line)
        assert "event" in parsed
        assert "ts" in parsed


# ── Prometheus Metrics tests ────────────────────────────────

@pytest.mark.asyncio
async def test_metrics_counts_successful_run():
    metrics = MetricsCollector(prefix="test_success")
    wf = Workflow(
        name="m-test",
        agents=[AddAgent(), MultiplyAgent()],
        edges=[("add", "multiply")],
    )
    engine = WorkflowEngine(metrics=metrics)

    await engine.run(wf, {"value": 5})

    assert metrics.workflow_runs.labels(workflow="m-test", status="completed")._value.get() == 1
    assert metrics.agent_runs.labels(agent="add", status="completed")._value.get() == 1
    assert metrics.agent_runs.labels(agent="multiply", status="completed")._value.get() == 1
    assert metrics.active_workflows._value.get() == 0


@pytest.mark.asyncio
async def test_metrics_counts_failed_run():
    metrics = MetricsCollector(prefix="test_fail")
    wf = Workflow(name="m-fail", agents=[FailAgent()], edges=[])
    engine = WorkflowEngine(metrics=metrics)

    with pytest.raises(RuntimeError):
        await engine.run(wf, {})

    assert metrics.workflow_runs.labels(workflow="m-fail", status="failed")._value.get() == 1
    assert metrics.agent_runs.labels(agent="fail", status="failed")._value.get() == 1
    assert metrics.active_workflows._value.get() == 0


@pytest.mark.asyncio
async def test_metrics_tracks_retries():
    metrics = MetricsCollector(prefix="test_retry")
    wf = Workflow(name="m-retry", agents=[FailAgent()], edges=[])
    engine = WorkflowEngine(metrics=metrics)

    with pytest.raises(RuntimeError):
        await engine.run(wf, {})

    assert metrics.retries.labels(agent="fail")._value.get() >= 1


# ── OpenTelemetry Tracing tests ─────────────────────────────

@pytest.mark.asyncio
async def test_tracer_creates_workflow_and_agent_spans():
    exporter = InMemorySpanExporter()
    tracer = WorkflowTracer(exporter=exporter, service_name="test-vortex")
    wf = Workflow(
        name="t-test",
        agents=[AddAgent(), MultiplyAgent()],
        edges=[("add", "multiply")],
    )
    engine = WorkflowEngine(tracer=tracer)

    await engine.run(wf, {"value": 5})
    tracer.shutdown()

    spans = exporter.get_finished_spans()
    span_names = [s.name for s in spans]

    assert "workflow:t-test" in span_names
    assert "agent:add" in span_names
    assert "agent:multiply" in span_names


@pytest.mark.asyncio
async def test_tracer_marks_failed_span():
    exporter = InMemorySpanExporter()
    tracer = WorkflowTracer(exporter=exporter, service_name="test-fail-trace")
    wf = Workflow(name="t-fail", agents=[FailAgent()], edges=[])
    engine = WorkflowEngine(tracer=tracer)

    with pytest.raises(RuntimeError):
        await engine.run(wf, {})
    tracer.shutdown()

    spans = exporter.get_finished_spans()
    wf_span = next(s for s in spans if s.name == "workflow:t-fail")
    assert wf_span.attributes["workflow.status"] == "failed"


# ── Regression ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_engine_works_without_observability():
    wf = Workflow(
        name="plain",
        agents=[AddAgent(), MultiplyAgent()],
        edges=[("add", "multiply")],
    )
    engine = WorkflowEngine()
    result = await engine.run(wf, {"value": 5})
    assert result["value"] == 30
