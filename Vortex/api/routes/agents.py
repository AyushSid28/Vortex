 # Agent registry endpoints

from __future__ import annotations
from fastapi import APIRouter,HTTPException
from api.models.schemas import AgentInfo
from api.routes.runs import _agent_registry


router=APIRouter(prefix="/agents",tags=["agents"])

@router.get("",response_model=list[AgentInfo])
async def list_agents():
    return [
        AgentInfo(
            name=a.name,
            description=a.description,
            version=a.version,
            timeout=a.timeout,
            max_retries=a.max_retries,
            requires_approval=a.requires_approval,
        )
        for a in _agent_registry.values()
    ]

@router.get("/{agent_name}",response_model=AgentInfo)
async def get_agent(agent_name:str):
    agent=_agent_registry.get(agent_name)
    if not agent:
        raise HTTPException(404,f"Agent {agent_name} not registered")


    return AgentInfo(
        name=agent.name,
        description=agent.description,
        version=agent.version,
        timeout=agent.timeout,
        max_retries=agent.max_retries,
        requires_approval=agent.requires_approval,
    )