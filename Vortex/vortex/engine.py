  # Execution runtime (DAG executor)

from __future__ import annotations
import asyncio
import time
from typing import Any

from vortex.workflow import Workflow
from vortex.agent import Agent,AgentResult,AgentStatus
from vortex.retry import retry_with_backoff
from vortex.state import StateManager,RunState



class WorkflowEngine:

  def __init__(self,state_manager:StateManager| None=None):
    self.state_manager=state_manager or StateManager()


  async def run(self,workflow:Workflow,input_data:dict[str,Any])->dict[str,Any]:
    """Execute a workflow with input data"""
    run=self.state.create_run(
      workflow.name,list(workflow.agents.keys()),input_data
    )
    state=dict(input_data)
   
    levels=workflow.topological_order()

    try:
     for level in levels:
      runnable=self._resolve_level(level,workflow,state)
      tasks=[
        self._execute_agent(workflow.agents[name],state)
        for name in runnable
      ]
      results=await asyncio.gather(*tasks,return_exceptions=True)

      for name,result in zip(runnable,results):
        if isinstance(result,Exception):
          self.state.mark_agent_failed(run.run_id,name,str(result))
          self.state.fail_run(run.run_id,str(result))
          raise result




        if result.status==AgentStatus.FAILED:
          self.state.fail_run(run.run_id,result.error or "agent failed")
          raise RuntimeError(
            f"Agent {name!r} failed after {result.retry_count} retries:"
            f"{result.error}"
          )

        self.state.mark_agent_completed(
          run.run_id,name,result.output,result.retry_count
        )
        state.update(result.output)


      self.state.complete_run(run.run_id)
          
    except Exception as e:
      if self.state.get_run(run.run_id).status!="FAILED":
        self.state.fail_run(run.run_id,str(e))

      raise
    return state



  async def _execute_agent(self,agent:Agent,state:dict[str,Any],run_id:str)->AgentResult:
    self.state.mark_agent_running(run_id,agent.name)
    start=time.perf_counter()
    try:
      coro=retry_with_backoff(agent,state)
      result=await asyncio.wait_for(coro,timeout=agent.timeout)
      
    except asyncio.TimeoutError:
      duration=(time.perf_counter()-start)*1000
      return AgentResult(
        output={},status=AgentStatus.FAILED,error=f"Agent {agent.name!r} timed out after {agent.timeout}s",duration_ms=duration
      )
    result.duration_ms=(time.perf_counter()-start)*1000
    return result


  def _resolve_level(
      self,level:list[str],workflow:Workflow,state:dict[str,Any]
    )->list[str]:
     """Filter level based on conditional edges-skip agents whose condition wasn't met"""


     runnable=[]
     for name in level:
      ce = workflow.get_conditional_edge_for_target(name)
      if ce is None:
        runnable.append(name)
        continue
      if ce.condition(state):
        if ce.true_target==name:
          runnable.append(name)
      else:
        if ce.false_target==name:
          runnable.append(name)

     return runnable