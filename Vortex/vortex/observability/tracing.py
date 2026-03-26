  # OpenTelemetry spans

from __future__ import annotations 
from typing import Any
from opentelemtry import trace
from opentelmetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor,SpanExporter


class WorkflowTracer:
  """OpenTelemetry tracer parent span per workflow,child span per agent"""

  def __init__(self,exporter:SpanExporter | None=None,server_name:str="vortex")->None:
    self._provider=TracerProvider()
    if exporter:
      self._provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(self._provider)
    self._tracer=trace.get_tracer(service_name)
    self._workflow_spans:dict[str,trace.Span]={}

  def start_workflow_span(self,run_id:str,workflow:str)->None:
    span=self._tracer.start_span(f"Workflow:{workflow} ")
    self._tracer=trace.get_tracer(service_name)
    self._workflow_spans:dict[str,trace.span]={}

  def start_workflow_span(self,run_id:str,agent:str)->trace.Span:
    span=self._workflow_spans.get(run_id)
    ctx=trace.set_span_in_context(parent) if parent else None
    span.set_attribute("run_id",run_id)
    span.set_attribute("agent.name",agent)
    return span    

  def end_agent_span(self,span:trace.Span,status:str,error:str|None=None,retry_count:int=0)->None:
     span.set_attributes("agent.status",status)
     span.set_attributes("agent.error",error)
     if error:
      span.set_attribute("error.message",error)
      span.set_status(trace.Status(trace.StatusCode.ERROR,error))
     span.end()

  
  def end_workflow_span(self,run_id:str,status:str | None=None)->None:
    span=self_workflow_spans.pop(run_id,None)
    if span is None:
      return
    span.set_attribute("workflow.status", status)
    if error:
     span.set_attribute("error.message",error)
     span.set_status(trace.Status(trace.StatusCode.ERROR,error))
    span.end()

  def shutdown(self)->None:
    self._provider.shutdown()
