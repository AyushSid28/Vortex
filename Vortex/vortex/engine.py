# Execution runtime (DAG executor)

from __future__ import annotations
import asyncio
import time
from typing import Any

from vortex.workflow import Workflow
from vortex.agent import Agent, AgentResult, AgentStatus
from vortex.retry import retry_with_backoff
from vortex.state import StateManager, RunStatus
from vortex.checkpoint import Checkpoint, CheckpointStore
from vortex.human_loop import HumanLoop, ApprovalStatus
from vortex.observability.metrics import MetricsCollector
from vortex.observability.tracing import WorkflowTracer
from vortex.observability.logging import StructuredLogger

class WorkflowEngine:

    def __init__(
        self,
        state_manager: StateManager | None = None,
        checkpoint_store: CheckpointStore | None = None,
        human_loop: HumanLoop | None = None,
        logger:StructuredLogger | None=None,
        metrics:MetricsCollector | None=None,
        tracer:WorkflowTracer | None=None,
    ):
        self.state_manager = state_manager or StateManager()
        self.checkpoint_store = checkpoint_store
        self.human_loop = human_loop
        self.logger= logger
        self.metrics=metrics
        self.tracer=tracer

    async def run(
        self,
        workflow: Workflow,
        input_data: dict[str, Any],
        resume_run_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute a workflow. Pass resume_run_id to restart from last checkpoint."""

        start_level = 0
        completed: set[str] = set()
        wf_start=time.perf_counter()

        if resume_run_id and self.checkpoint_store:
            cp = self.checkpoint_store.load(resume_run_id)
            if cp is None:
                raise ValueError(f"No checkpoint found for run {resume_run_id!r}")
            state = dict(cp.shared_state)
            start_level = cp.last_completed_level + 1
            completed = set(cp.completed_agents)
        else:
            state = dict(input_data)

        run = self.state_manager.create_run(
            workflow.name, list(workflow.agents.keys()), input_data
        )
        if self.logger:
            self.logger.workflow_start(run.run_id,workflow.name)
        
        if self.metrics:
            self.metrics.on_workflow_start()

        if self.tracer:
            self.tracer.start_workflow_span(run.run_id,workflow.name)


        for name in completed:
            self.state_manager.mark_agent_completed(run.run_id, name, {})

        checkpoint = Checkpoint(
            run_id=run.run_id,
            workflow_name=workflow.name,
            shared_state=dict(state),
            completed_agents=list(completed),
        ) if self.checkpoint_store else None

        levels = workflow.topological_order()

        try:
            for level_idx, level in enumerate(levels):
                if level_idx < start_level:
                    continue

                runnable = self._resolve_level(level, workflow, state)
                runnable = [n for n in runnable if n not in completed]

                if not runnable:
                    continue

                tasks = [
                    self._execute_agent(workflow.agents[name], state, run.run_id)
                    for name in runnable
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for name, result in zip(runnable, results):
                    if isinstance(result, Exception):
                        self.state_manager.mark_agent_failed(run.run_id, name, str(result))
                        self.state_manager.fail_run(run.run_id, str(result))
                        raise result

                    if result.status == AgentStatus.FAILED:
                        if self.logger:
                            self.logger.agent_failed(run.run_id,name,result.error or "",result.duration_ms,result.retry_count)
                        if self.metrics:
                            self.metrics.on_agent_failed(name,result.duration_ms/1000,result.retry_count)
                        self.state_manager.mark_agent_failed(
                            run.run_id, name, result.error or "agent failed", result.retry_count
                        )
                        self.state_manager.fail_run(run.run_id, result.error or "agent failed")
                        raise RuntimeError(
                            f"Agent {name!r} failed after {result.retry_count} retries: "
                            f"{result.error}"
                        )

                    if self.human_loop and getattr(workflow.agents[name], "requires_approval", False):
                        approval = await self.human_loop.wait_for_approval(
                            run.run_id, name, result.output
                        )
                        if approval.status == ApprovalStatus.REJECTED:
                            self.state_manager.mark_agent_failed(
                                run.run_id, name, f"Rejected: {approval.feedback}"
                            )
                            self.state_manager.fail_run(
                                run.run_id, f"Agent {name!r} rejected: {approval.feedback}"
                            )
                            raise RuntimeError(
                                f"Agent {name!r} rejected by human: {approval.feedback}"
                            )

                    if self.logger:
                        self.logger.agent_complete(run.run_id,name)
                    if self.metrics:
                        self.metrics.on_agent_complete(name,result.duration_ms/1000,result.retry_count)

                    self.state_manager.mark_agent_completed(
                        run.run_id, name, result.output, result.retry_count
                    )
                    state.update(result.output)
                    completed.add(name)

                if self.checkpoint_store and checkpoint:
                    checkpoint.completed_agents = list(completed)
                    checkpoint.shared_state = dict(state)
                    checkpoint.last_completed_level = level_idx
                    self.checkpoint_store.save(checkpoint)

            wf_duration=(time.perf_counter()- wf.start)*1000


            self.state_manager.complete_run(run.run_id)

            if self.logger:
                self.logger.workflow_complete(run.run_id,workflow.name,wf_duration)
            if self.metrics:
                self.metrics.on_workflow_complete(workflow.name,wf_duration/1000)
            if self.tracer:
                self.tracer.end_workflow_span(run.run_id,"completed")
            if self.checkpoint_store:
                self.checkpoint_store.delete(run.run_id)

        except Exception as e:
            wf_duration=(time.perf_counter()-wf_start)*1000
            
            if self.state_manager.get_run(run.run_id).status != RunStatus.FAILED:
                self.state_manager.fail_run(run.run_id, str(e))
            if self.logger:
                self.logger.workflow_failure(run.run_id,workflow.name,str(e),wf_duration)
            if self.metrics:
                self.metrics.on_workflow_failed(workflow.name,wf_duration/1000)
            if self.tracer:
                self.tracer.end_workflow_span(run.run_id,"failed")
            raise

        return state

    async def _execute_agent(
        self, agent: Agent, state: dict[str, Any], run_id: str
    ) -> AgentResult:
        self.state_manager.mark_agent_running(run_id, agent.name)

        if self.logger:
            self.logger.agent_start(run_id,agent.name)
        
        agent_span=None
        if self.tracer:
            agent_span=self.tracer.start_agent_span(run_id,agent.name)
        start = time.perf_counter()

        try:
            coro = retry_with_backoff(agent, state)
            result = await asyncio.wait_for(coro, timeout=agent.timeout)
        except asyncio.TimeoutError:
            duration = (time.perf_counter() - start) * 1000
            return AgentResult(
                output={},
                status=AgentStatus.FAILED,
                error=f"Agent {agent.name!r} timed out after {agent.timeout}s",
                duration_ms=duration,
            )

        result.duration_ms = (time.perf_counter() - start) * 1000
        return result

    def _resolve_level(
        self, level: list[str], workflow: Workflow, state: dict[str, Any]
    ) -> list[str]:
        """Filter level based on conditional edges — skip agents whose condition wasn't met."""
        runnable = []
        for name in level:
            ce = workflow.get_conditional_edge_for_target(name)
            if ce is None:
                runnable.append(name)
                continue
            if ce.condition(state):
                if ce.true_target == name:
                    runnable.append(name)
            else:
                if ce.false_target == name:
                    runnable.append(name)
        return runnable
