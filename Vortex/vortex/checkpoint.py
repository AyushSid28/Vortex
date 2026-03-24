 # Checkpoint and resume on failure

from __future__ import annotations
import copy
from dataclasses import dataclass,field
from typing import Any

@dataclass
class Checkpoint:
    run_id:str
    workflow_name:str
    complete_agents:list[str]=field(default_factory=list)
    agent_outputs:dict[str,Any]=field(default_factory=dict)
    shared_state:dict[str,Any]=field(default_factory=dict)
    last_completed_level:int=-1


class CheckpointStore:
    """In-memory checkpoint storage"""
    def __init__(self)->None:
        self._checkpoints:dict[str,Checkpoint]={}

    def save(self,checkpoint:Checkpoint)->None:
        self._checkpoints[checkpoint.run_id]=copy.deepcopy(checkpoint)

    def load(self,run_id:str)->Checkpoint | None:
        cp=self._checkpoints.get(run_id)
        return copy.deepcopy(cp) if cp else None

    def delete(self,run_id:str)->None:
        self._checkpoints.pop(run_id,None)