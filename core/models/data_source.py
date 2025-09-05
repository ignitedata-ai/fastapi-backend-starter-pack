import datetime
import uuid

from sqlalchemy import JSON, DateTime, Enum, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from core.database import Base
from core.models.enums import ConnectionStatus, ConnectorKind


class DataSource(Base):
    __tablename__ = "data_sources"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    slug: Mapped[str] = mapped_column(String, nullable=False)
    connector_key: Mapped[str] = mapped_column(String, nullable=False)
    connector_version: Mapped[str] = mapped_column(String, nullable=False)
    kind: Mapped[ConnectorKind] = mapped_column(Enum(ConnectorKind), nullable=False)
    config_json: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    secret_uri: Mapped[str | None] = mapped_column(String)
    network_profile: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    tags: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=[])
    connection_status: Mapped[ConnectionStatus] = mapped_column(
        Enum(ConnectionStatus), nullable=False, default=ConnectionStatus.ACTIVE
    )
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    last_health_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    created_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    updated_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    # Relationships
    assets = relationship("Asset", back_populates="data_source", cascade="all, delete-orphan")
    connector_runs = relationship("ConnectorRun", back_populates="data_source", cascade="all, delete-orphan")
    connector_schedules = relationship("ConnectorSchedule", back_populates="data_source", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint("tenant_id", "slug", name="uq_data_sources_tenant_id_slug"),)
