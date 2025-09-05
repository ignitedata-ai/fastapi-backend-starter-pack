import datetime
import uuid

from sqlalchemy import JSON, DateTime, Enum, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from core.database import Base
from core.models.enums import LineageOp


class LineageEvent(Base):
    __tablename__ = "lineage_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    data_source_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("data_sources.id", ondelete="SET NULL"))
    producer: Mapped[str] = mapped_column(String, nullable=False)
    event_time: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    event_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("connector_runs.id", ondelete="SET NULL"))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class LineageEdge(Base):
    __tablename__ = "lineage_edges"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    src_asset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False, index=True)
    dst_asset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False, index=True)
    operation: Mapped[LineageOp] = mapped_column(Enum(LineageOp), nullable=False)
    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("connector_runs.id", ondelete="SET NULL"))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "src_asset_id",
            "dst_asset_id",
            "operation",
            name="uq_lineage_edges_tenant_id_src_asset_id_dst_asset_id_operation",
        ),
    )
