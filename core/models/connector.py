import datetime
import uuid

from sqlalchemy import JSON, Boolean, DateTime, Enum, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from core.database import Base
from core.models.enums import ConnectorKind


class ConnectorDefinition(Base):
    __tablename__ = "connector_definitions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    key: Mapped[str] = mapped_column(String, nullable=False)
    version: Mapped[str] = mapped_column(String, nullable=False)
    kind: Mapped[ConnectorKind] = mapped_column(Enum(ConnectorKind), nullable=False)
    display_name: Mapped[str] = mapped_column(String, nullable=False)
    capabilities: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    connection_schema: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    secret_schema: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    image_ref: Mapped[str | None] = mapped_column(String)
    docs_url: Mapped[str | None] = mapped_column(String)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (UniqueConstraint("key", "version", name="uq_connector_definitions_key_version"),)
