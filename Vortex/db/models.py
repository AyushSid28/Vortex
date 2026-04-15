from __future__ import annotations
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Column, String, JSON, Float, Integer, DateTime, ForeignKey, Text, Enum as SAEnum
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


def gen_id() -> str:
    return uuid.uuid4().hex[:12]


class WorkflowModel(Base):
    __tablename__ = "workflows"

    id = Column(String(12), primary_key=True, default=gen_id)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text, default="")
    definition = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    runs = relationship("WorkflowRunModel", back_populates="workflow")


class WorkflowRunModel(Base):
    __tablename__ = "workflow_runs"

    id = Column(String(12), primary_key=True, default=gen_id)
    workflow_id = Column(String(12), ForeignKey("workflows.id"), nullable=False)
    status = Column(String(20), default="PENDING")
    input_data = Column(JSON, default=dict)
    output_data = Column(JSON, default=dict)
    error = Column(Text, nullable=True)
    started_at = Column(Float, nullable=True)
    completed_at = Column(Float, nullable=True)

    workflow = relationship("WorkflowModel", back_populates="runs")
    agent_runs = relationship("AgentRunModel", back_populates="run")


class AgentRunModel(Base):
    __tablename__ = "agent_runs"

   

    run = relationship("WorkflowRunModel", back_populates="agent_runs")