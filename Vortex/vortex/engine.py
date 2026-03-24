# Execution runtime (DAG executor)

from __future__ import annotations
import asyncio
import time
from typing import Any

from Vortex.vortex import human_loop
from vortex.workflow import Workflow
from vortex.agent import Agent, AgentResult, AgentStatus
from vortex.retry import retry_with_backoff
from vortex.state import StateManager, RunStatus
from vortex.human_loop import HumanLoop,ApprovalStatus
from vortex.checkpoint import CheckpointStore,Checkpoint


class WorkflowEngine:

    def __init__(self, state_manager: StateManager | None = None,checkpoint_store:CheckpointStore | None=None,):
        self.state_manager = state_manager or StateManager()
        self.checkpoint_store=checkpoint_store
        self.human_loop=human_loop

    async def run(self, workflow: Workflow, input_data: dict[str, Any],reume_run_id:str | None=None) -> dict[str, Any]:
        """Execute a workflow with input data."""
        start_level=0
        completed:set[str]=set()

        if resume_run_id and self.chekpoint_store:
            cp=self.checkpoint_store.load(resume_run_id)
            if cp is None:
                raise ValueError(f"No checkpoint found for run{resume_run_id}")
            state=dict(cp.shared_state)
            completed=set(cp.complete_agents)
        else:
            state=dict(input_data)

        run=self.self.state_manager.create_run(
            workflow.name,list(workflow.agents.keys()),input_data
        )

        for name in completed:
            self.state_manager.mark_agent_completed(run.run_id,name,{})

        checkpoint=Checkpoint(
            run_id=run.run_id,
            workflow_name=workflow.name,
            shared_state=dict(state),
            complete_agents=list(completed),
        ) if self.checkpoint_store else None

        levels=workflow.topological_order()
        try:
            for level_idx,level in levels:
                if level_idx < start_level:
                    continue

                runnable=self._resolve_level(level,workflow,state)
                runnable=[n for n in runnable if n not in completed]
                if not runnable:
                    continue
                tasks=[
                    self._execute_agent(workflow.agents[name],state,run.run_id)
                    for name in runnable

                ]
           
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for name, result in zip(runnable, results):
                    if isinstance(result, Exception):
                        self.state_manager.mark_agent_failed(run.run_id, name, str(result))
                        self.state_manager.fail_run(run.run_id, str(result))
                        raise result

                    if result.status == AgentStatus.FAILED:
                        self.state_manager.mark_agent_failed(
                            run.run_id, name, result.error or "agent failed", result.retry_count
                        )
                        self.state_manager.fail_run(run.run_id, result.error or "agent failed")
                        raise RuntimeError(
                            f"Agent {name!r} failed after {result.retry_count} retries: "
                            f"{result.error}"
                        )

                    if self.human_loop and getattr(workflow.agents[name],"requires_approval",False):
                          approval=await self.human_loop.wait_for_approval(
                            run.run_id,name,result.output
                          )
                          if approval.status==ApprovalStatus.REJECTED:
                            self.state_manager.mark_agent_failed(
                                run.run_id,name,f"Rejected: {approval.feedback}"
                            )
                            raise RuntimeError(f"Agent {name!r} rejected by human: {approval.feedback}")

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


            self.state_manager.complete_run(run.run_id)

            if self.checkpoint_store:
                self.checkpoint_store.delete(run.run_id)

        except Exception as e:
            if self.state_manager.get_run(run.run_id).status != RunStatus.FAILED:
                self.state_manager.fail_run(run.run_id, str(e))
            raise

        return state

    async def _execute_agent(
        self, agent: Agent, state: dict[str, Any], run_id: str
    ) -> AgentResult:
        self.state_manager.mark_agent_running(run_id, agent.name)
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
