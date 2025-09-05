import datetime
import uuid

from sqlalchemy import JSON, DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from core.database import Base


class AssetMetadata(Base):
    __tablename__ = "asset_metadata"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    asset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False)
    namespace: Mapped[str] = mapped_column(String, nullable=False, index=True)
    key: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[dict] = mapped_column(JSON, nullable=False)
    source: Mapped[str | None] = mapped_column(String)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    asset = relationship("Asset", back_populates="metadata_entries")

    __table_args__ = (
        UniqueConstraint("tenant_id", "asset_id", "namespace", "key", name="uq_asset_metadata_tenant_id_asset_id_namespace_key"),
    )
