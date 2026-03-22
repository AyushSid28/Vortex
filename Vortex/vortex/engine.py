  # Execution runtime (DAG executor)

from __future__ import annotations
import asyncio
import time
from typing import Any

from vortex.workflow import Workflow
from vortex.agent import Agent,AgentResult,AgentStatus



class WorkflowEngine:
  async def run(self,workflow:Workflow,input_data:dict[str,Any])->dict[str,Any]:
    """Execute a workflow with input data"""
    state=dict(input_data)
    agent_results:dict[str,AgentResult]={}
    levels=workflow.topological_order()


    for level in levels:
      runnable=self._resolve_level(level,workflow,state)
      tasks=[
        self._execute_agent(workflow.agents[name],state)
        for name in runnable
      ]
      results=await asyncio.gather(*tasks,return_exceptions=True)

      for name,result in zip(runnable,results):
        if isinstance(result,Exception):
          agent_results[name]=AgentResult(
            output={},status=AgentStatus.FAILED,error=str(result)

          )
          raise result
        agent_results[name]=result
        state.update(result.output)

    return state 



  async def _execute_agent(self,agent:Agent,state:dict[str,Any])->AgentResult:
    start=time.perf_counter()
    try:
      output=await agent.execute(state)
      duration=(time.perf_counter()-start)*1000
      return AgentResult(
        output=output,status=AgentStatus.COMPLETED,duration_ms=duration
      )
    except Exception as e:
      duration=(time.perf_counter()-start)*1000
      return AgentResult(
        output={},status=AgentStatus.FAILED,error=str(e),duration_ms=duration
      )


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