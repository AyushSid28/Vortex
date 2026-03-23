 # Retry logic with exponential backoff

from __future__ import annotations
import asyncio
import random
from typing import Any

from vortex.agent import Agent,AgentResult,AgentStatus

async def retry_with_backoff(
    agent:Agent,
    state:dict[str,Any],
)->AgentResult:
   """Execute an agent with retry and exponential backoff
   
    Uses the agent's own config:max_retries,backoff_base,backoff_max and retry_on to decide retry behavior.
   """
   last_error:Exception |None=None

   for attempt in range(agent.max_retries+1):
     try:
        output=await agent.execute(state)
        return AgentResult(
            output=output,
            status=AgentStatus.COMPLETED,
            retry_count=attempt,
        )
     except agent.retry_on as exc:
            last_error=exc
            if attempt< agent.max_retries:
                delay=_calc_delay(attempt,agent.backoff_base,agent.backoff_max)
                await asyncio.sleep(delay)


   return AgentResult(
      output={},
      status=AgentStatus.FAILED,
      error=str(last_error),
      retry_count=agent.max_retries,
   )

def _calc_delay(attempt:int,base:float,cap:float)->float:
    """Exponential backoff with jitter:min(base* 2^attempt,cap)+ jitter"""
    delay=min(base*(2**attempt),cap)
    jitter=random.uniform(0,delay*0.1)
    return delay+jitter

