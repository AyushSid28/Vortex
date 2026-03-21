 # CRUD for workflow definitions

from abc import ABC,abstractmethod
from enum import Enum
from typing import Any
from dataclasses import dataclass


class AgentStatus(str,Enum):
    PENDING="PENDING"
    RUNNING="RUNNING"
    COMPLETED="COMPLETED"
    FAILED="FAILED"
    WAITING="WAITING"


@dataclass
class AgentResult:
    output:dict[str,Any]
    status:AgentStatus
    error:str | None=None
    duration_ms:float=0.0
    retry_count: int=0



class Agent(ABC):
    name:str="unnamed_agent"
    description: str=""
    version:str="0.1.0"
    timeout:float=60.0
    max_retries:int=0
    backoff_base:float=1.0
    backoff_max:float=30.0
    retry_on:tuple[type[Exception],...]=(Exception)


    def __init_subclass__(cls,**kwargs):
        super().__init_subclass__(**kwargs)
        if cls.name=="unnamed_agent" and cls.__name__ !="Agent":
            cls.name=cls.__name__

    @abstractmethod
    async def execute(self,state:dict[str,Any])->dict[str,Any]:
        """Run the agent logic.Receives shared state,returns output to merge back"""
        ...

    def __repr__(self)->str:
        return f"<{self.__class__.__name__} name={self.name!r} v{self.version}>"