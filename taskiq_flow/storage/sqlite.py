"""
Adaptateur de stockage SQLite avec support SQLAlchemy.

Implémentation de BaseStorageAdapter utilisant SQLAlchemy
pour la persistance dans SQLite ou toute base de données
supportée par SQLAlchemy.

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import asyncio
import fnmatch
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from sqlalchemy import DateTime, Integer, String, Text, create_engine, delete, select
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from taskiq_flow.storage.base import BaseStorageAdapter

logger = logging.getLogger(__name__)


class _Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""


class KVStoreModel(_Base):
    """SQLAlchemy model for generic key-value storage."""

    __tablename__ = "kv_store"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc)
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    metadata_field: Mapped[str | None] = mapped_column(Text, nullable=True)


class SQLiteStorageAdapter(BaseStorageAdapter):
    """
    Adaptateur de stockage basé sur SQLAlchemy/SQLite.

    Supporte SQLite, PostgreSQL, MySQL et toute base de données
    compatible SQLAlchemy. Utilise une table clé-valeur générique
    pour stocker les données de pipeline et de cache.

    Attributes:
        db_url: URL de connexion à la base de données
        async_mode: Mode asynchrone activé

    """

    def __init__(
        self,
        db_url: str = "sqlite+aiosqlite:///taskiq_flow.db",
        async_mode: bool = True,
    ) -> None:
        self.db_url = db_url
        self.async_mode = async_mode

        if async_mode:
            self._async_engine = create_async_engine(db_url, echo=False)
            self._async_session_factory = async_sessionmaker(
                self._async_engine, expire_on_commit=False
            )
        else:
            # Convert to synchronous URL if needed
            sync_url = db_url
            if "+aiosqlite" in sync_url:
                sync_url = sync_url.replace("+aiosqlite", "")
            elif "+asyncpg" in sync_url:
                sync_url = sync_url.replace("+asyncpg", "+psycopg2")
            self._sync_engine = create_engine(sync_url, echo=False)
            self._sync_session_factory = sessionmaker(
                bind=self._sync_engine, expire_on_commit=False
            )

        # Create tables
        self._init_tables()

    def _init_tables(self) -> None:
        """Initialize database tables."""
        if self.async_mode:
            asyncio.run(self._create_tables_async())
        else:
            _Base.metadata.create_all(self._sync_engine)

    async def _create_tables_async(self) -> None:
        """Create tables asynchronously using sync engine for DDL."""
        sync_url = self.db_url
        if self.async_mode:
            if "+aiosqlite" in sync_url:
                sync_url = sync_url.replace("+aiosqlite", "")
            elif "+asyncpg" in sync_url:
                sync_url = sync_url.replace("+asyncpg", "+psycopg2")
        sync_engine = create_engine(sync_url, echo=False)
        _Base.metadata.create_all(sync_engine)
        sync_engine.dispose()

    async def get(self, key: str) -> Any | None:
        """Récupère une valeur par clé."""
        try:
            if self.async_mode:
                async with self._async_session_factory() as session:
                    result = await session.execute(
                        select(KVStoreModel).where(KVStoreModel.key == key)
                    )
                    row = result.scalar_one_or_none()
                    if row is None:
                        return None
                    if row.expires_at is not None and row.expires_at < datetime.now(
                        timezone.utc
                    ).replace(tzinfo=None):
                        await session.delete(row)
                        await session.commit()
                        return None
                    return json.loads(row.value)
            else:
                with self._sync_session_factory() as session:
                    row = session.query(KVStoreModel).filter_by(key=key).first()
                    if row is None:
                        return None
                    if row.expires_at is not None and row.expires_at < datetime.now(
                        timezone.utc
                    ).replace(tzinfo=None):
                        session.delete(row)
                        session.commit()
                        return None
                    return json.loads(row.value)
        except Exception as e:
            logger.error("SQLAlchemy get failed for key %s: %s", key, e)
            raise

    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: int | None = None,
    ) -> None:
        """Stocke une valeur avec une clé et un TTL optionnel."""
        try:
            serialized: str = json.dumps(value, default=str)
            expires_at: datetime | None = None
            if ttl_seconds is not None and ttl_seconds > 0:
                # Store UTC time as naive datetime for SQLite
                expires_at = datetime.now(timezone.utc).replace(
                    tzinfo=None
                ) + timedelta(seconds=ttl_seconds)

            entry = KVStoreModel(
                key=key,
                value=serialized,
                expires_at=expires_at,
                metadata_field=json.dumps({"ttl": ttl_seconds}),
            )

            if self.async_mode:
                async with self._async_session_factory() as session:
                    result = await session.execute(
                        select(KVStoreModel).where(KVStoreModel.key == key)
                    )
                    existing = result.scalar_one_or_none()
                    if existing:
                        existing.value = serialized
                        existing.expires_at = expires_at
                        existing.metadata_field = json.dumps({"ttl": ttl_seconds})
                    else:
                        session.add(entry)
                    await session.commit()
            else:
                with self._sync_session_factory() as session:
                    existing = session.query(KVStoreModel).filter_by(key=key).first()
                    if existing:
                        existing.value = serialized
                        existing.expires_at = expires_at
                        existing.metadata_field = json.dumps({"ttl": ttl_seconds})
                    else:
                        session.add(entry)
                    session.commit()
        except Exception as e:
            logger.error("SQLAlchemy set failed for key %s: %s", key, e)
            raise

    async def delete(self, key: str) -> bool:
        """Supprime une entrée par clé."""
        try:
            if self.async_mode:
                async with self._async_session_factory() as session:
                    result = await session.execute(
                        select(KVStoreModel).where(KVStoreModel.key == key)
                    )
                    row = result.scalar_one_or_none()
                    if row is None:
                        return False
                    await session.delete(row)
                    await session.commit()
                    return True
            with self._sync_session_factory() as session:
                row = session.query(KVStoreModel).filter_by(key=key).first()
                if row is None:
                    return False
                session.delete(row)
                session.commit()
                return True
        except Exception as e:
            logger.error("SQLAlchemy delete failed for key %s: %s", key, e)
            raise

    async def exists(self, key: str) -> bool:
        """Vérifie si une clé existe."""
        try:
            if self.async_mode:
                async with self._async_session_factory() as session:
                    result = await session.execute(
                        select(KVStoreModel.key).where(
                            KVStoreModel.key == key,
                            (KVStoreModel.expires_at.is_(None))
                            | (KVStoreModel.expires_at > datetime.now(timezone.utc)),
                        )
                    )
                    return result.scalar() is not None
            with self._sync_session_factory() as session:
                return (
                    session.query(KVStoreModel.key)
                    .filter_by(key=key)
                    .filter(
                        (KVStoreModel.expires_at.is_(None))
                        | (KVStoreModel.expires_at > datetime.now(timezone.utc))
                    )
                    .first()
                    is not None
                )
        except Exception as e:
            logger.error("SQLAlchemy exists failed for key %s: %s", key, e)
            raise

    async def keys(self, pattern: str = "*") -> list[str]:
        """Liste les clés correspondant à un motif."""
        try:
            if self.async_mode:
                async with self._async_session_factory() as session:
                    result = await session.execute(
                        select(KVStoreModel.key).where(
                            KVStoreModel.expires_at.is_(None)
                            | (KVStoreModel.expires_at > datetime.now(timezone.utc))
                        )
                    )
                    all_keys = [r[0] for r in result.all()]
            else:
                with self._sync_session_factory() as session:
                    all_keys = [
                        r[0]
                        for r in session.query(KVStoreModel.key).filter(
                            (KVStoreModel.expires_at.is_(None))
                            | (
                                KVStoreModel.expires_at
                                > datetime.now(timezone.utc).replace(tzinfo=None)
                            )
                        )
                    ]

            if pattern == "*":
                return all_keys
            return [k for k in all_keys if fnmatch.fnmatch(k, pattern)]
        except Exception as e:
            logger.error("SQLAlchemy keys failed for pattern %s: %s", pattern, e)
            raise

    async def cleanup(self, ttl_seconds: int = 3600) -> int:
        """Nettoie les entrées expirées."""
        try:
            # Remove all expired entries (ttl_seconds ignored for
            # compatibility with MemoryStorageAdapter)
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            count: int = 0

            if self.async_mode:
                async with self._async_session_factory() as session:
                    result = await session.execute(
                        delete(KVStoreModel).where(
                            (KVStoreModel.expires_at.isnot(None))
                            & (KVStoreModel.expires_at < now)
                        )
                    )
                    await session.commit()
                    count = cast(CursorResult[Any], result).rowcount
            else:
                with self._sync_session_factory() as session:
                    count = (
                        session.query(KVStoreModel)
                        .filter(
                            KVStoreModel.expires_at.isnot(None),
                            KVStoreModel.expires_at < now,
                        )
                        .delete()
                    )
                    session.commit()
            return count
        except Exception as e:
            logger.error("SQLAlchemy cleanup failed: %s", e)
            raise
