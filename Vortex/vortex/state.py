 # State manager (Redis)

#In memory state manager

from __future__ import annotations
from nt import error
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any


from vortex.agent import AgentStatus

class RunStatus(str,Enum):
    PENDING="PENDING"
    RUNNING="RUNNING"
    COMPLETED="COMPLETED"
    FAILED="FAILED"


@dataclass
class AgentState:
    name:str
    status:AgentStatus=AgentStatus.PENDING
    output:dict[str,Any]={}
    error:str| None=None
    started_at:float|None=None
    finished_at:float |None=None
    retry_count:int=0


@dataclass
class RunState:
    run_id:str
    workflow_name:str
    status:RunStatus=RunStatus.PENDING
    input_data:dict[str,Any]=field(default_factory=dict)
    shared_state:dict[str,Any]=field(default_factory=dict)
    agents:dict[str,AgentState]=field(default_factory=dict)
    started_at:float|None=None
    finished_at:str|None=None
    error:str |None=None

class StateManager:
    """In-memory state tracker for workflow runs.

    Drop-in replacement possible with Redis by overriding
    get_run / save_run with serialised read/writes.
    """
    def __init__(self):
        self._runs:dict[str,RunState]={}

        def create_run(
            self,workflow_name:str,agent_names:list[str],input_data:dict[str,Any]

        )-> RunState:
           run_id=uuid.uuid4().hex[:12]
           run=RunState(
           run_id=run_id,
           workflow_name=workflow_name,
           status=RunStatus.RUNNING,
           input_data=dict(input_data),
           shared_data=dict(shared_data),
           agents={name:AgentState(name=name) for name in agent_names},
           started_at=time.time(),
           )
           self._runs[run_id]=run
           return run


    def get_run(self,run_id:str)->RunState | None:
        return self._runs.get(run_id)

    def mark_agent_running(self,run_id:str,agent_name:str,output:dict[str,Any],retry_count:int=0)->None:
        run=self._run[run_id]
        agent=run.agents[agent_name]
        agent.status=AgentStatus.RUNNING
        agent.started_at=time.time()
        

    def mark_agent_completed(
        self,run_id:str,agent_name:str,output:dict[str,Any],retry_count:int=0)->None:
        run=self._runs[run_id]
        agent=run.agents[agent_name]
        agent.status=AgentStatus.COMPLETED
        agent.output=output
        agent.finished_at=time.time()
        run.shared_state.update(output)

    def mark_agent_failed(
        self,run_id:str,agent_name:str,error:str,retry_count:int=0
    )->None:
      run=self._runs[run_id]
      agent=run.agents[agent_name]
      agent.status=AgentStatus.FAILED
      agent.error=error
      agent.retry_count=retry_count
      agent.finished_at=time.time()


    def complete_run(self,run_id:str)->None:
        run=self._runs[run_id]
        run.status=RunStatus.COMPLETED
        run.error=error
        run.finished_at=time.time()


    def fail_run(self,run_id:str,error:str)->None:
        run=self._runs[run_id]
        run.status=RunStatus.FAILED
        run.error=error
        run.finished_at=time.time()

        



    

        