import datetime
import uuid

from sqlalchemy import JSON, Boolean, DateTime, Enum, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from core.database import Base
from core.models.enums import AssetType


class Asset(Base):
    __tablename__ = "assets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    data_source_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("data_sources.id", ondelete="CASCADE"), nullable=False)
    type: Mapped[AssetType] = mapped_column(Enum(AssetType), nullable=False, index=True)
    qualified_name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    display_name: Mapped[str | None] = mapped_column(String)
    parent_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"))
    native_identity: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    properties: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    data_source = relationship("DataSource", back_populates="assets")
    parent = relationship("Asset", remote_side=[id], backref="children")
    fields = relationship("AssetField", back_populates="asset", cascade="all, delete-orphan")
    profiles = relationship("AssetProfile", back_populates="asset", cascade="all, delete-orphan")
    sample_sets = relationship("AssetSampleSet", back_populates="asset", cascade="all, delete-orphan")
    metadata_entries = relationship("AssetMetadata", back_populates="asset", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "data_source_id", "qualified_name", name="uq_assets_tenant_id_data_source_id_qualified_name"
        ),
    )


class AssetField(Base):
    __tablename__ = "asset_fields"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    asset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    ordinal_position: Mapped[int | None] = mapped_column(Integer)
    data_type: Mapped[str | None] = mapped_column(String)
    is_nullable: Mapped[bool | None] = mapped_column(Boolean)
    default_expression: Mapped[str | None] = mapped_column(String)
    comment: Mapped[str | None] = mapped_column(String)
    properties: Mapped[dict] = mapped_column(JSON, nullable=False, default={})
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    asset = relationship("Asset", back_populates="fields")
    profiles = relationship("FieldProfile", back_populates="field", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint("tenant_id", "asset_id", "name", name="uq_asset_fields_tenant_id_asset_id_name"),)
