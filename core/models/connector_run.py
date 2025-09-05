import datetime
import uuid

from sqlalchemy import JSON, Boolean, DateTime, Enum, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from core.database import Base
from core.models.enums import RunStatus, RunType


class ConnectorRun(Base):
    __tablename__ = "connector_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    data_source_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("data_sources.id", ondelete="CASCADE"), nullable=False)
    run_type: Mapped[RunType] = mapped_column(Enum(RunType), nullable=False)
    trigger: Mapped[str] = mapped_column(String, nullable=False)
    params: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    started_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[RunStatus] = mapped_column(Enum(RunStatus), nullable=False, default=RunStatus.QUEUED)
    error_message: Mapped[str | None] = mapped_column(String)
    metrics: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    created_by: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))

    # Relationships
    data_source = relationship("DataSource", back_populates="connector_runs")

    __table_args__ = (UniqueConstraint("id", name="uq_connector_runs_id"),)


class ConnectorSchedule(Base):
    __tablename__ = "connector_schedules"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    data_source_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("data_sources.id", ondelete="CASCADE"), nullable=False)
    run_type: Mapped[RunType] = mapped_column(Enum(RunType), nullable=False)
    schedule_cron: Mapped[str] = mapped_column(String, nullable=False)
    params: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    # Relationships
    data_source = relationship("DataSource", back_populates="connector_schedules")

    __table_args__ = (
        UniqueConstraint("tenant_id", "data_source_id", "run_type", "schedule_cron", name="uq_conn_sched_tenant_ds_type_cron"),
    )
