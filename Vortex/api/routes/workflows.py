 # CRUD for workflow definitions

from __future__ import annotations
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException

from api.models.schemas import WorkflowCreate, WorkflowResponse, WorkflowEdge

router = APIRouter(prefix="/workflows", tags=["workflows"])

_workflows: dict[str, dict] = {}
_counter = 0


def _next_id() -> str:
    global _counter
    _counter += 1
    return f"wf-{_counter:04d}"


@router.post("", response_model=WorkflowResponse, status_code=201)
async def create_workflow(body: WorkflowCreate):
    for existing in _workflows.values():
        if existing["name"] == body.name:
            raise HTTPException(409, f"Workflow {body.name!r} already exists")

    wf_id = _next_id()
    record = {
        "id": wf_id,
        "name": body.name,
        "description": body.description,
        "agent_names": body.agent_names,
        "edges": [e.model_dump() for e in body.edges],
        "created_at": datetime.now(timezone.utc),
    }
    _workflows[wf_id] = record
    return WorkflowResponse(**record)


@router.get("", response_model=list[WorkflowResponse])
async def list_workflows():
    return [WorkflowResponse(**w) for w in _workflows.values()]


@router.get("/{workflow_id}", response_model=WorkflowResponse)
async def get_workflow(workflow_id: str):
    wf = _workflows.get(workflow_id)
    if not wf:
        raise HTTPException(404, f"Workflow {workflow_id!r} not found")
    return WorkflowResponse(**wf)


@router.delete("/{workflow_id}", status_code=204)
async def delete_workflow(workflow_id: str):
    if workflow_id not in _workflows:
        raise HTTPException(404, f"Workflow {workflow_id!r} not found")
    del _workflows[workflow_id]


def get_workflow_record(workflow_id: str) -> dict | None:
    return _workflows.get(workflow_id)