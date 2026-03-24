import asyncio
import pytest
from vortex.agent import Agent, AgentStatus
from vortex.workflow import Workflow
from vortex.engine import WorkflowEngine
from vortex.state import StateManager, RunStatus
from vortex.checkpoint import CheckpointStore
from vortex.human_loop import HumanLoop, ApprovalStatus


# ── Agents ───────────────────────────────────────────────────

class AgentA(Agent):
    name = "a"
    async def execute(self, state):
        state.setdefault("log", []).append("a")
        return {"a_done": True, "log": state["log"]}


class AgentB(Agent):
    name = "b"
    async def execute(self, state):
        state.setdefault("log", []).append("b")
        return {"b_done": True, "log": state["log"]}


class FailingB(Agent):
    name = "b"
    async def execute(self, state):
        raise RuntimeError("b crashed")


class AgentC(Agent):
    name = "c"
    async def execute(self, state):
        state.setdefault("log", []).append("c")
        return {"c_done": True, "log": state["log"]}


class ReviewAgent(Agent):
    name = "review"
    requires_approval = True
    async def execute(self, state):
        return {"report": "generated report"}


class AddAgent(Agent):
    name = "add"
    async def execute(self, state):
        return {"value": state.get("value", 0) + 10}


class MultiplyAgent(Agent):
    name = "multiply"
    requires_approval = True
    async def execute(self, state):
        return {"value": state["value"] * 2}


# ── Checkpoint tests ────────────────────────────────────────

@pytest.mark.asyncio
async def test_checkpoint_saved_on_partial_success():
    """A succeeds, B crashes → checkpoint has A completed."""
    cs = CheckpointStore()
    wf = Workflow(
        name="cp",
        agents=[AgentA(), FailingB(), AgentC()],
        edges=[("a", "b"), ("b", "c")],
    )
    engine = WorkflowEngine(checkpoint_store=cs)

    with pytest.raises(RuntimeError, match="b crashed"):
        await engine.run(wf, {"input": 1})

    run_id = list(engine.state_manager._runs.keys())[0]
    cp = cs.load(run_id)
    assert cp is not None
    assert "a" in cp.completed_agents
    assert "b" not in cp.completed_agents
    assert cp.shared_state["a_done"] is True
    assert cp.last_completed_level == 0


@pytest.mark.asyncio
async def test_resume_skips_completed_agents():
    """Resume from checkpoint → A is skipped, fixed B and C run."""
    cs = CheckpointStore()

    wf_broken = Workflow(
        name="cp",
        agents=[AgentA(), FailingB(), AgentC()],
        edges=[("a", "b"), ("b", "c")],
    )
    engine1 = WorkflowEngine(checkpoint_store=cs)

    with pytest.raises(RuntimeError):
        await engine1.run(wf_broken, {"input": 1})

    failed_run_id = list(engine1.state_manager._runs.keys())[0]

    wf_fixed = Workflow(
        name="cp",
        agents=[AgentA(), AgentB(), AgentC()],
        edges=[("a", "b"), ("b", "c")],
    )
    engine2 = WorkflowEngine(checkpoint_store=cs)
    result = await engine2.run(wf_fixed, {"input": 1}, resume_run_id=failed_run_id)

    assert result["b_done"] is True
    assert result["c_done"] is True


@pytest.mark.asyncio
async def test_checkpoint_deleted_on_success():
    """Successful run cleans up its checkpoint."""
    cs = CheckpointStore()
    wf = Workflow(name="cp", agents=[AgentA(), AgentB()], edges=[("a", "b")])
    engine = WorkflowEngine(checkpoint_store=cs)
    await engine.run(wf, {})

    run_id = list(engine.state_manager._runs.keys())[0]
    assert cs.load(run_id) is None


@pytest.mark.asyncio
async def test_no_checkpoint_when_store_not_provided():
    """Engine works fine without a checkpoint store."""
    wf = Workflow(name="no-cp", agents=[AgentA()], edges=[])
    engine = WorkflowEngine()
    result = await engine.run(wf, {})
    assert result["a_done"] is True


# ── Human-in-the-Loop tests ─────────────────────────────────

@pytest.mark.asyncio
async def test_hitl_approve_continues():
    hl = HumanLoop()
    wf = Workflow(name="hitl", agents=[ReviewAgent()], edges=[])
    engine = WorkflowEngine(human_loop=hl)

    async def auto_approve():
        while not hl.get_pending():
            await asyncio.sleep(0.01)
        req = hl.get_pending()[0]
        hl.approve(req.run_id, "review", feedback="looks good")

    task = asyncio.create_task(auto_approve())
    result = await engine.run(wf, {})
    await task
    assert result["report"] == "generated report"


@pytest.mark.asyncio
async def test_hitl_reject_fails():
    hl = HumanLoop()
    wf = Workflow(name="hitl", agents=[ReviewAgent()], edges=[])
    engine = WorkflowEngine(human_loop=hl)

    async def auto_reject():
        while not hl.get_pending():
            await asyncio.sleep(0.01)
        req = hl.get_pending()[0]
        hl.reject(req.run_id, "review", feedback="needs revision")

    task = asyncio.create_task(auto_reject())
    with pytest.raises(RuntimeError, match="rejected by human"):
        await engine.run(wf, {})
    await task


@pytest.mark.asyncio
async def test_hitl_mid_pipeline():
    """Add → Multiply(needs approval) → approve → result correct."""
    hl = HumanLoop()
    wf = Workflow(
        name="mid-hitl",
        agents=[AddAgent(), MultiplyAgent()],
        edges=[("add", "multiply")],
    )
    engine = WorkflowEngine(human_loop=hl)

    async def auto_approve():
        while not hl.get_pending():
            await asyncio.sleep(0.01)
        req = hl.get_pending()[0]
        hl.approve(req.run_id, "multiply")

    task = asyncio.create_task(auto_approve())
    result = await engine.run(wf, {"value": 5})
    await task
    assert result["value"] == 30


@pytest.mark.asyncio
async def test_hitl_not_triggered_without_flag():
    """Agent without requires_approval runs straight through."""
    hl = HumanLoop()
    wf = Workflow(name="no-hitl", agents=[AddAgent()], edges=[])
    engine = WorkflowEngine(human_loop=hl)
    result = await engine.run(wf, {"value": 5})
    assert result["value"] == 15
    assert len(hl.get_pending()) == 0


# ── Phase 1+2 regression ────────────────────────────────────

@pytest.mark.asyncio
async def test_sequential_regression():
    wf = Workflow(
        name="seq",
        agents=[AddAgent(), MultiplyAgent()],
        edges=[("add", "multiply")],
    )
    hl = HumanLoop()
    engine = WorkflowEngine(human_loop=hl)

    async def auto_approve():
        while not hl.get_pending():
            await asyncio.sleep(0.01)
        req = hl.get_pending()[0]
        hl.approve(req.run_id, "multiply")

    task = asyncio.create_task(auto_approve())
    result = await engine.run(wf, {"value": 5})
    await task
    assert result["value"] == 30
