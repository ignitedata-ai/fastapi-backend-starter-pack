import datetime
import uuid

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from core.database import Base


class AssetProfile(Base):
    __tablename__ = "asset_profiles"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    asset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False)
    computed_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    profile_kind: Mapped[str] = mapped_column(String, nullable=False)
    summary: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    histograms: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("connector_runs.id", ondelete="SET NULL"))

    # Relationships
    asset = relationship("Asset", back_populates="profiles")


class FieldProfile(Base):
    __tablename__ = "field_profiles"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    field_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("asset_fields.id", ondelete="CASCADE"), nullable=False)
    computed_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    stats: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("connector_runs.id", ondelete="SET NULL"))

    # Relationships
    field = relationship("AssetField", back_populates="profiles")


class AssetSampleSet(Base):
    __tablename__ = "asset_sample_sets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    asset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False)
    sample_kind: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("connector_runs.id", ondelete="SET NULL"))
    item_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Relationships
    asset = relationship("Asset", back_populates="sample_sets")
    items = relationship("AssetSampleItem", back_populates="sample_set", cascade="all, delete-orphan")


class AssetSampleItem(Base):
    __tablename__ = "asset_sample_items"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sample_set_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("asset_sample_sets.id", ondelete="CASCADE"), nullable=False)
    idx: Mapped[int] = mapped_column(Integer, nullable=False)
    payload_json: Mapped[dict | None] = mapped_column(JSON)
    payload_text: Mapped[str | None] = mapped_column(String)
    payload_url: Mapped[str | None] = mapped_column(String)

    # Relationships
    sample_set = relationship("AssetSampleSet", back_populates="items")

    __table_args__ = (UniqueConstraint("sample_set_id", "idx", name="uq_asset_sample_items_sample_set_id_idx"),)
