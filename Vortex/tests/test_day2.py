import asyncio
import pytest
from vortex.agent import Agent, AgentStatus
from vortex.workflow import Workflow
from vortex.engine import WorkflowEngine
from vortex.state import StateManager, RunStatus


# ── Test agents ──────────────────────────────────────────────

class FlakyAgent(Agent):
    """Fails N times, then succeeds."""
    name = "flaky"
    max_retries = 3
    backoff_base = 0.01
    backoff_max = 0.05

    def __init__(self, fail_count: int = 2):
        self._fail_count = fail_count
        self._calls = 0

    async def execute(self, state):
        self._calls += 1
        if self._calls <= self._fail_count:
            raise RuntimeError(f"Flaky failure #{self._calls}")
        return {"recovered": True, "attempts": self._calls}


class AlwaysFailAgent(Agent):
    name = "always_fail"
    max_retries = 2
    backoff_base = 0.01
    backoff_max = 0.05

    async def execute(self, state):
        raise RuntimeError("permanent failure")


class SlowAgent(Agent):
    name = "slow"
    timeout = 0.1

    async def execute(self, state):
        await asyncio.sleep(5)
        return {"done": True}


class FastAgent(Agent):
    name = "fast"
    timeout = 5.0

    async def execute(self, state):
        return {"speed": "fast"}


class AddAgent(Agent):
    name = "add"
    async def execute(self, state):
        return {"value": state.get("value", 0) + 10}


class MultiplyAgent(Agent):
    name = "multiply"
    async def execute(self, state):
        return {"value": state["value"] * 2}


class SelectiveRetryAgent(Agent):
    """Only retries on ValueError, not on TypeError."""
    name = "selective"
    max_retries = 3
    backoff_base = 0.01
    retry_on = (ValueError,)

    def __init__(self):
        self._calls = 0

    async def execute(self, state):
        self._calls += 1
        if self._calls == 1:
            raise TypeError("not retriable")
        return {"ok": True}


# ── Retry tests ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_retry_recovers():
    flaky = FlakyAgent(fail_count=2)
    wf = Workflow(name="retry-test", agents=[flaky], edges=[])
    engine = WorkflowEngine()
    result = await engine.run(wf, {})
    assert result["recovered"] is True
    assert result["attempts"] == 3


@pytest.mark.asyncio
async def test_retry_exhausted_raises():
    agent = AlwaysFailAgent()
    wf = Workflow(name="fail-test", agents=[agent], edges=[])
    engine = WorkflowEngine()
    with pytest.raises(RuntimeError, match="failed after 2 retries"):
        await engine.run(wf, {})


@pytest.mark.asyncio
async def test_no_retry_when_max_retries_zero():
    """Agent with max_retries=0 should fail immediately without retrying."""
    class OneShotFail(Agent):
        name = "oneshot"
        max_retries = 0

        async def execute(self, state):
            raise RuntimeError("instant fail")

    wf = Workflow(name="no-retry", agents=[OneShotFail()], edges=[])
    engine = WorkflowEngine()
    with pytest.raises(RuntimeError, match="failed after 0 retries"):
        await engine.run(wf, {})


@pytest.mark.asyncio
async def test_selective_retry_skips_non_matching():
    agent = SelectiveRetryAgent()
    wf = Workflow(name="selective", agents=[agent], edges=[])
    engine = WorkflowEngine()
    with pytest.raises(TypeError):
        await engine.run(wf, {})
    assert agent._calls == 1


# ── Timeout tests ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_timeout_kills_slow_agent():
    wf = Workflow(name="timeout-test", agents=[SlowAgent()], edges=[])
    engine = WorkflowEngine()
    with pytest.raises(RuntimeError, match="timed out"):
        await engine.run(wf, {})


@pytest.mark.asyncio
async def test_fast_agent_no_timeout():
    wf = Workflow(name="fast-test", agents=[FastAgent()], edges=[])
    engine = WorkflowEngine()
    result = await engine.run(wf, {})
    assert result["speed"] == "fast"


# ── State tracking tests ────────────────────────────────────

@pytest.mark.asyncio
async def test_state_tracks_successful_run():
    sm = StateManager()
    wf = Workflow(
        name="tracked",
        agents=[AddAgent(), MultiplyAgent()],
        edges=[("add", "multiply")],
    )
    engine = WorkflowEngine(state_manager=sm)
    result = await engine.run(wf, {"value": 5})

    assert result["value"] == 30

    runs = list(sm._runs.values())
    assert len(runs) == 1
    run = runs[0]
    assert run.status == RunStatus.COMPLETED
    assert run.agents["add"].status == AgentStatus.COMPLETED
    assert run.agents["multiply"].status == AgentStatus.COMPLETED
    assert run.finished_at is not None


@pytest.mark.asyncio
async def test_state_tracks_failed_run():
    sm = StateManager()
    wf = Workflow(name="fail-tracked", agents=[AlwaysFailAgent()], edges=[])
    engine = WorkflowEngine(state_manager=sm)

    with pytest.raises(RuntimeError):
        await engine.run(wf, {})

    runs = list(sm._runs.values())
    run = runs[0]
    assert run.status == RunStatus.FAILED
    assert run.agents["always_fail"].status == AgentStatus.FAILED
    assert run.error is not None


@pytest.mark.asyncio
async def test_state_tracks_retry_count():
    sm = StateManager()
    flaky = FlakyAgent(fail_count=2)
    wf = Workflow(name="retry-tracked", agents=[flaky], edges=[])
    engine = WorkflowEngine(state_manager=sm)
    await engine.run(wf, {})

    run = list(sm._runs.values())[0]
    assert run.agents["flaky"].retry_count == 2
    assert run.status == RunStatus.COMPLETED


@pytest.mark.asyncio
async def test_run_state_stores_input():
    sm = StateManager()
    wf = Workflow(name="input-check", agents=[FastAgent()], edges=[])
    engine = WorkflowEngine(state_manager=sm)
    await engine.run(wf, {"key": "value"})

    run = list(sm._runs.values())[0]
    assert run.input_data == {"key": "value"}


# ── Phase 1 regression ──────────────────────────────────────

@pytest.mark.asyncio
async def test_sequential_still_works():
    wf = Workflow(
        name="seq",
        agents=[AddAgent(), MultiplyAgent()],
        edges=[("add", "multiply")],
    )
    engine = WorkflowEngine()
    result = await engine.run(wf, {"value": 5})
    assert result["value"] == 30