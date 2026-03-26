  # Structured JSON logging

from __future__ import annotations
import json 
import sys
from typing import Any,TextIO
import time

class StructuredLogger:
  """JSON logger with run_id correlation and agent event tracking
  
  Stores structured logs in JSON format with run_id correlation and agent event tracking.
  Store records in _records list for testability
  """
 
  def __init__(self,stream:TextIO| None=None)->None:
      """->None: function return type which means it doesn't return anything"""
      self._stream=stream or sys.stderr
      self._records:list[dict[str,Any]]=[]


  def _emit(self,event:str,**fields:Any)->None:
    record= {"event":event,"ts":time.time(),**fields}
    self._records.append(record)
    self._stream.write(json.dumps(record)+"\n")



  def workflow_start(self,run_id:str,workflow:str)->None:
    self.emit("Workflow_start",run_id=run_id,workflow=workflow)

  def workflow_end(self,run_id:str,workflow:str,duration_ms:float,duration:float)->None:
    self._emit("workflow_complete",run_id=run_id,workflow=workflow,duration_ms=round(duration_ms,2))


  def workflow_failure(self,run_id:str,workflow:str,error:str,duration_ms:float)->None:
    self._emit("Workflow_failed",run_id=run_id,workflow=workflow,error=error,duration_ms=round(duration_ms,2))

  def agent_start(self,run_id:str,agent:str)->None:
    self._emit("agent_start ",run_id=run_id,agent=agent)


  def agent_complete(self,run_id:str,agent:str)->None:
    self._emit("agent_complete",run_id=run_id,agent=agent)

  def agent_failure(self,run_id:str,agent:str,error:str,duration_ms:float,retry_count:int=0)->None:
    self._emit("agent_failed",run_id=run_id,agent=agent,error=error,duration_ms=round(duration_ms,2),retry_count=retry_count)



