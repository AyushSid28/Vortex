from __future__ import annotations
from datetime import datetime
from enum import Enum
from re import S
from typing import Any
from pydantic import BaseModel,Field

class RunStatus(str,Enum):
    PENDING="PENDING"
    RUNNING="RUNNING"
    COMPLETED="COMPLETED"
    FAILED="FAILED"
    CANCELLED="CANCELLED"
    WAITING_APPROVAL="WAITING_APPROVAL"



class AgentStatusEnum(str,Enum):
    PENDING="PENDING"
    RUNNING="RUNNING"
    COMPLETED="COMPLETED"
    FAILED="FAILED"
    WAITING="WAITING"


#Workflow Schemas
class WorkflowEdge(BaseModel):
    source:str
    target:str


class WorkflowCreate(BaseModel):
    name:str
    description:str=""
    agent_names:list[str]
    edges:list[WorkflowEdge]=Field(default_factory=list)



class WorkflowResponse(BaseModel):
    id:str
    name:str
    description:str
    agent_names:list[str]
    edges:list[WorkflowEdge]
    created_at:datetime