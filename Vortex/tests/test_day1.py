import asyncio
import pytest
from vortex.agent import Agent, AgentStatus
from vortex.workflow import Workflow, ConditionalEdge
from vortex.engine import WorkflowEngine


class AddAgent(Agent):
    name = "add"
    async def execute(self, state):
        return {"value": state.get("value", 0) + 10}


class MultiplyAgent(Agent):
    name = "multiply"
    async def execute(self, state):
        return {"value": state["value"] * 2}


class UpperAgent(Agent):
    name = "upper"
    async def execute(self, state):
        return {"text": state.get("text", "").upper()}


class LowerAgent(Agent):
    name = "lower"
    async def execute(self, state):
        return {"text": state.get("text", "").lower()}


# --- Sequential ---

@pytest.mark.asyncio
async def test_sequential():
    wf = Workflow(
        name="seq",
        agents=[AddAgent(), MultiplyAgent()],
        edges=[("add", "multiply")],
    )
    engine = WorkflowEngine()
    result = await engine.run(wf, {"value": 5})
    assert result["value"] == 30  # (5+10)*2


# --- Parallel ---

@pytest.mark.asyncio
async def test_parallel():
    wf = Workflow(
        name="par",
        agents=[AddAgent(), UpperAgent()],
        edges=[],
    )
    engine = WorkflowEngine()
    result = await engine.run(wf, {"value": 1, "text": "hello"})
    assert result["value"] == 11
    assert result["text"] == "HELLO"


# --- Conditional ---

@pytest.mark.asyncio
async def test_conditional_true_branch():
    wf = Workflow(
        name="cond",
        agents=[AddAgent(), UpperAgent(), LowerAgent()],
        edges=[("add", "upper"), ("add", "lower")],
        conditional_edges=[
            ConditionalEdge(
                source="add",
                true_target="upper",
                false_target="lower",
                condition=lambda state: state["value"] > 10,
            )
        ],
    )
    engine = WorkflowEngine()
    result = await engine.run(wf, {"value": 5, "text": "Hello"})
    assert result["text"] == "HELLO"  # 5+10=15 > 10, so upper runs


@pytest.mark.asyncio
async def test_conditional_false_branch():
    wf = Workflow(
        name="cond",
        agents=[AddAgent(), UpperAgent(), LowerAgent()],
        edges=[("add", "upper"), ("add", "lower")],
        conditional_edges=[
            ConditionalEdge(
                source="add",
                true_target="upper",
                false_target="lower",
                condition=lambda state: state["value"] > 20,
            )
        ],
    )
    engine = WorkflowEngine()
    result = await engine.run(wf, {"value": 5, "text": "Hello"})
    assert result["text"] == "hello"  # 5+10=15 < 20, so lower runs


def test_cycle_detection():
    with pytest.raises(ValueError, match="cycle"):
        Workflow(
            name="bad",
            agents=[AddAgent(), MultiplyAgent()],
            edges=[("add", "multiply"), ("multiply", "add")],
        )


def test_unknown_agent_in_edge():
    with pytest.raises(ValueError, match="Unknown agent"):
        Workflow(
            name="bad",
            agents=[AddAgent()],
            edges=[("add", "ghost")],
        )