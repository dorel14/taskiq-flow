"""
Job persistence for scheduled pipelines.

This module provides SQLAlchemy-based job persistence supporting
multiple database backends: SQLite, PostgreSQL, MySQL.
It also supports the new BaseStorageAdapter pattern for unified
storage access.

Author: SoniqueBay Team
Version: 1.2.0
"""

import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel, Field

try:
    from sqlalchemy import (
        Boolean,
        Column,
        DateTime,
        Float,
        Integer,
        String,
        Text,
        create_engine,
        delete,
        select,
    )
    from sqlalchemy.engine import CursorResult
    from sqlalchemy.ext.asyncio import (
        async_sessionmaker,
        create_async_engine,
    )
    from sqlalchemy.orm import DeclarativeBase, sessionmaker

    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    # Sentinel values for missing SQLAlchemy symbols
    Boolean = None  # type: ignore
    Column = None  # type: ignore
    DateTime = None  # type: ignore
    Float = None  # type: ignore
    Integer = None  # type: ignore
    String = None  # type: ignore
    Text = None  # type: ignore
    create_engine = None  # type: ignore
    delete = None  # type: ignore
    select = None  # type: ignore
    CursorResult = None  # type: ignore
    async_sessionmaker = None  # type: ignore
    create_async_engine = None  # type: ignore
    DeclarativeBase = None  # type: ignore
    sessionmaker = None  # type: ignore

from taskiq_flow.storage.base import BaseStorageAdapter

if TYPE_CHECKING:
    pass


class SchedulerJob(BaseModel):
    """Pydantic model for scheduler job."""

    id: str
    pipeline_id: str
    label: str
    cron: str | None = None
    interval_seconds: int | None = None
    timezone: str = "UTC"
    enabled: bool = True
    last_run: datetime | None = None
    next_run: datetime | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PipelineExecution(BaseModel):
    """Pydantic model for pipeline execution."""

    job_id: str
    pipeline_id: str
    status: str
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    duration_seconds: float | None = None
    result: str | None = None
    error: str | None = None


if SQLALCHEMY_AVAILABLE:

    class Base(DeclarativeBase):
        """Base class for SQLAlchemy models."""

    class SchedulerJobModel(Base):
        """SQLAlchemy model for scheduled jobs."""

        __tablename__ = "scheduler_jobs"

        id = Column(String, primary_key=True)
        pipeline_id = Column(String, nullable=False)
        label = Column(String, nullable=False)
        cron = Column(String, nullable=True)
        interval_seconds = Column(Integer, nullable=True)
        timezone = Column(String, default="UTC")
        enabled = Column(Boolean, default=True)
        last_run = Column(DateTime, nullable=True)
        next_run = Column(DateTime, nullable=True)
        created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
        updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    class PipelineExecutionModel(Base):
        """SQLAlchemy model for pipeline execution history."""

        __tablename__ = "pipeline_executions"

        id = Column(Integer, primary_key=True, autoincrement=True)
        job_id = Column(String, nullable=False)
        pipeline_id = Column(String, nullable=False)
        status = Column(String, nullable=False)
        started_at = Column(DateTime, nullable=False)
        completed_at = Column(DateTime, nullable=True)
        duration_seconds = Column(Float, nullable=True)
        result = Column(Text, nullable=True)
        error = Column(Text, nullable=True)

    class PersistenceAdapter(BaseStorageAdapter):
        """
        Adaptateur de persistance SQLAlchemy conformé à BaseStorageAdapter.

        Bridge entre le nouveau système de stockage unifié et la persistance
        SQLAlchemy existante pour l'ordonnancement.

        Attributes:
            db_url: URL de la base de données
            async_mode: Mode asynchrone activé

        """

        def __init__(
            self,
            db_url: str = "sqlite:///jobs.db",
            async_mode: bool = True,
        ) -> None:
            self.db_url = db_url
            self.is_async = async_mode
            if async_mode:
                self._async_engine = create_async_engine(db_url, echo=False)
                self._async_session_factory = async_sessionmaker(
                    self._async_engine, expire_on_commit=False
                )
            else:
                sync_engine = create_engine(db_url, echo=False)
                self._sync_engine = sync_engine
                self._sync_session_factory = sessionmaker(
                    bind=sync_engine, expire_on_commit=False
                )

            if async_mode:
                asyncio.run(self._create_tables_async(db_url))
            else:
                Base.metadata.create_all(self._sync_engine)

        async def _create_tables_async(self, db_url: str) -> None:
            """Create tables for async engine."""
            sync_url = db_url.replace("asyncpg", "psycopg2").replace(
                "aiosqlite", "sqlite"
            )
            sync_engine = create_engine(sync_url, echo=False)
            Base.metadata.create_all(sync_engine)
            sync_engine.dispose()

        async def get(self, key: str) -> Any | None:
            """Get job data by key."""
            if self.is_async:
                async with self._async_session_factory() as session:
                    result = await session.execute(
                        select(SchedulerJobModel).where(SchedulerJobModel.id == key)
                    )
                    row = result.scalar_one_or_none()
                    if row is None:
                        return None
                    return self._model_to_dict(row)
            else:
                with self._sync_session_factory() as session:
                    row = session.query(SchedulerJobModel).filter_by(id=key).first()
                    if row is None:
                        return None
                    return self._model_to_dict_sync(row)

        async def set(
            self,
            key: str,
            value: Any,
            ttl_seconds: int | None = None,
        ) -> None:
            """Persist job data by key."""
            if (
                isinstance(value, dict)
                and value.get("model_type") == "SchedulerJobModel"
            ):
                job = SchedulerJobModel(
                    id=value["id"],
                    pipeline_id=value["pipeline_id"],
                    label=value["label"],
                    cron=value.get("cron"),
                    interval_seconds=value.get("interval_seconds"),
                    timezone=value.get("timezone", "UTC"),
                    enabled=value.get("enabled", True),
                    last_run=value.get("last_run"),
                    next_run=value.get("next_run"),
                    created_at=value.get("created_at", datetime.now(timezone.utc)),
                    updated_at=value.get("updated_at", datetime.now(timezone.utc)),
                )
                if self.is_async:
                    async with self._async_session_factory() as session:
                        session.add(job)
                        await session.commit()
                else:
                    with self._sync_session_factory() as session:
                        session.add(job)
                        session.commit()

        async def delete(self, key: str) -> bool:
            """Delete job by key."""
            if self.is_async:
                async with self._async_session_factory() as session:
                    result = await session.execute(
                        select(SchedulerJobModel).where(SchedulerJobModel.id == key)
                    )
                    row = result.scalar_one_or_none()
                    if row is None:
                        return False
                    await session.delete(row)
                    await session.commit()
                    return True
            else:
                with self._sync_session_factory() as session:
                    row = session.query(SchedulerJobModel).filter_by(id=key).first()
                    if row is None:
                        return False
                    session.delete(row)
                    session.commit()
                    return True

        async def exists(self, key: str) -> bool:
            """Check if job exists by key."""
            if self.is_async:
                async with self._async_session_factory() as session:
                    result = await session.execute(
                        select(SchedulerJobModel.id).where(SchedulerJobModel.id == key)
                    )
                    return result.scalar() is not None
            else:
                with self._sync_session_factory() as session:
                    return (
                        session.query(SchedulerJobModel.id).filter_by(id=key).first()
                        is not None
                    )

        async def keys(self, pattern: str = "*") -> list[str]:
            """List job keys matching pattern."""
            if self.is_async:
                async with self._async_session_factory() as session:
                    result = await session.execute(
                        select(SchedulerJobModel.id).where(SchedulerJobModel.enabled)
                    )
                    return [row[0] for row in result.all()]
            else:
                with self._sync_session_factory() as session:
                    return [
                        row[0]
                        for row in session.query(SchedulerJobModel.id)
                        .filter_by(enabled=True)
                        .all()
                    ]

        async def cleanup(self, ttl_seconds: int = 3600) -> int:
            """Clean up old job entries."""
            cutoff = datetime.now(timezone.utc).timestamp() - ttl_seconds
            if self.is_async:
                async with self._async_session_factory() as session:
                    result = await session.execute(
                        delete(SchedulerJobModel).where(
                            (SchedulerJobModel.created_at)
                            < datetime.fromtimestamp(cutoff, tz=timezone.utc)
                        )
                    )
                    await session.commit()
                    return cast(CursorResult[Any], result).rowcount
            else:
                cutoff_dt = datetime.fromtimestamp(cutoff, tz=timezone.utc)
                with self._sync_session_factory() as session:
                    count: int = (
                        session.query(SchedulerJobModel)
                        .filter(SchedulerJobModel.created_at < cutoff_dt)
                        .delete()
                    )
                    session.commit()
                    return count

        def _model_to_dict(self, model: SchedulerJobModel) -> dict[str, Any]:
            """Convert SQLAlchemy model to dict."""
            return {
                "id": model.id,
                "pipeline_id": model.pipeline_id,
                "label": model.label,
                "cron": model.cron,
                "interval_seconds": model.interval_seconds,
                "timezone": model.timezone,
                "enabled": model.enabled,
                "last_run": (model.last_run.isoformat() if model.last_run else None),
                "next_run": (model.next_run.isoformat() if model.next_run else None),
                "created_at": model.created_at.isoformat(),
                "updated_at": model.updated_at.isoformat(),
                "model_type": "SchedulerJobModel",
            }

        def _model_to_dict_sync(self, model: SchedulerJobModel) -> dict[str, Any]:
            """Convert SQLAlchemy model to dict (sync)."""
            return {
                "id": model.id,
                "pipeline_id": model.pipeline_id,
                "label": model.label,
                "cron": model.cron,
                "interval_seconds": model.interval_seconds,
                "timezone": model.timezone,
                "enabled": model.enabled,
                "last_run": (model.last_run.isoformat() if model.last_run else None),
                "next_run": (model.next_run.isoformat() if model.next_run else None),
                "created_at": model.created_at.isoformat(),
                "updated_at": model.updated_at.isoformat(),
                "model_type": "SchedulerJobModel",
            }
else:
    Base = None  # type: ignore
    SchedulerJobModel = None  # type: ignore
    PipelineExecutionModel = None  # type: ignore
    PersistenceAdapter = None  # type: ignore


if SQLALCHEMY_AVAILABLE:

    class JobPersistenceManager:
        """
        Pluggable storage backend for scheduled jobs.

        Supports multiple database backends via SQLAlchemy:
        - SQLite (default): sqlite:///jobs.db or sqlite+aiosqlite:///jobs.db
        - PostgreSQL: postgresql+asyncpg://user:pass@host/db # pragma: allowlist secret
        - MySQL: mysql+aiomysql://user:pass@host/db // pragma: allowlist secret
        - Any SQLAlchemy-supported database

        Also provides a PersistenceAdapter conforming to BaseStorageAdapter
        for unified storage access via the new middleware pattern.
        """

        def __init__(
            self,
            db_url: str = "sqlite:///jobs.db",
            async_mode: bool = True,
        ) -> None:
            """
            Initialize persistence manager with configurable database backend.

            Args:
                db_url: SQLAlchemy database URL. Defaults to SQLite.
                async_mode: Use async SQLAlchemy engine (recommended for production)

            Examples:
                        - "sqlite:///jobs.db" (sync)
                        - "sqlite+aiosqlite:///jobs.db" (async)
                        # pragma: allowlist nextline secret
                        - "postgresql+asyncpg://user:pass@localhost:5432/"
                        "taskiq_flow" (async)
                        # pragma: allowlist nextline secret
                        - "mysql+aiomysql://user:pass@localhost:3306/"
                        "taskiq_flow" (async)

            """
            self.db_url = db_url
            self.is_async = async_mode

            if async_mode:
                self._async_engine: Any = create_async_engine(db_url, echo=False)
                self._async_session_factory = async_sessionmaker(
                    self._async_engine,
                    expire_on_commit=False,
                )
            else:
                sync_engine = create_engine(db_url, echo=False)
                self._sync_engine = sync_engine
                self._sync_session_factory = sessionmaker(
                    bind=sync_engine,
                    expire_on_commit=False,
                )

            if async_mode:
                asyncio.run(self._create_tables_async(db_url))
            else:
                Base.metadata.create_all(self._sync_engine)

        async def _create_tables_async(self, db_url: str) -> None:
            """Create tables for async engine."""
            sync_url = db_url.replace("asyncpg", "psycopg2").replace(
                "aiosqlite", "sqlite"
            )
            sync_engine = create_engine(sync_url, echo=False)
            Base.metadata.create_all(sync_engine)
            sync_engine.dispose()

        async def save_job(self, job: SchedulerJob) -> None:
            """Persist job configuration to database."""
            if self.is_async:
                async with self._async_session_factory() as session:
                    db_job = SchedulerJobModel(
                        id=job.id,
                        pipeline_id=job.pipeline_id,
                        label=job.label,
                        cron=job.cron,
                        interval_seconds=job.interval_seconds,
                        timezone=job.timezone,
                        enabled=job.enabled,
                        last_run=job.last_run,
                        next_run=job.next_run,
                        created_at=job.created_at,
                        updated_at=job.updated_at,
                    )
                    session.add(db_job)
                    await session.commit()
            else:
                with self._sync_session_factory() as session:
                    db_job = SchedulerJobModel(
                        id=job.id,
                        pipeline_id=job.pipeline_id,
                        label=job.label,
                        cron=job.cron,
                        interval_seconds=job.interval_seconds,
                        timezone=job.timezone,
                        enabled=job.enabled,
                        last_run=job.last_run,
                        next_run=job.next_run,
                        created_at=job.created_at,
                        updated_at=job.updated_at,
                    )
                    session.add(db_job)
                    session.commit()

        async def load_jobs(self) -> list[SchedulerJob]:
            """Load all persisted jobs from database on startup."""
            if self.is_async:
                async with self._async_session_factory() as session:
                    result = await session.execute(
                        select(SchedulerJobModel).where(SchedulerJobModel.enabled),
                    )
                    db_jobs = result.scalars().all()
            else:
                with self._sync_session_factory() as session:
                    db_jobs = (
                        session.query(SchedulerJobModel)
                        .filter(SchedulerJobModel.enabled)
                        .all()
                    )
            return [
                SchedulerJob(
                    id=str(j.id),
                    pipeline_id=str(j.pipeline_id),
                    label=str(j.label),
                    cron=str(j.cron) if j.cron is not None else None,
                    interval_seconds=(
                        int(j.interval_seconds)
                        if j.interval_seconds is not None
                        else None
                    ),
                    timezone=str(j.timezone),
                    enabled=bool(j.enabled),
                    last_run=(
                        datetime.fromisoformat(str(j.last_run))
                        if j.last_run is not None
                        else None
                    ),
                    next_run=(
                        datetime.fromisoformat(str(j.next_run))
                        if j.next_run is not None
                        else None
                    ),
                    created_at=datetime.fromisoformat(str(j.created_at)),
                    updated_at=datetime.fromisoformat(str(j.updated_at)),
                )
                for j in db_jobs
            ]

        async def save_execution_history(
            self,
            job_id: str,
            execution: PipelineExecution,
        ) -> None:
            """Save execution results and history for audit trail."""
            if self.is_async:
                async with self._async_session_factory() as session:
                    db_exec = PipelineExecutionModel(
                        job_id=job_id,
                        pipeline_id=execution.pipeline_id,
                        status=execution.status,
                        started_at=execution.started_at,
                        completed_at=execution.completed_at,
                        duration_seconds=execution.duration_seconds,
                        result=execution.result,
                        error=execution.error,
                    )
                    session.add(db_exec)
                    await session.commit()
            else:
                with self._sync_session_factory() as session:
                    db_exec = PipelineExecutionModel(
                        job_id=job_id,
                        pipeline_id=execution.pipeline_id,
                        status=execution.status,
                        started_at=execution.started_at,
                        completed_at=execution.completed_at,
                        duration_seconds=execution.duration_seconds,
                        result=execution.result,
                        error=execution.error,
                    )
                    session.add(db_exec)
                    session.commit()

        async def get_execution_history(
            self,
            job_id: str,
            limit: int = 100,
        ) -> list[PipelineExecution]:
            """Retrieve execution history for a specific job."""
            if self.is_async:
                async with self._async_session_factory() as session:
                    result = await session.execute(
                        select(PipelineExecutionModel)
                        .where(PipelineExecutionModel.job_id == job_id)
                        .order_by(PipelineExecutionModel.started_at.desc())
                        .limit(limit),
                    )
                    db_executions = result.scalars().all()
            else:
                with self._sync_session_factory() as session:
                    db_executions = (
                        session.query(PipelineExecutionModel)
                        .filter(PipelineExecutionModel.job_id == job_id)
                        .order_by(PipelineExecutionModel.started_at.desc())
                        .limit(limit)
                        .all()
                    )
            return [
                PipelineExecution(
                    job_id=str(e.job_id),
                    pipeline_id=str(e.pipeline_id),
                    status=str(e.status),
                    started_at=datetime.fromisoformat(str(e.started_at)),
                    completed_at=(
                        datetime.fromisoformat(str(e.completed_at))
                        if e.completed_at is not None
                        else None
                    ),
                    duration_seconds=(
                        float(e.duration_seconds)
                        if e.duration_seconds is not None
                        else None
                    ),
                    result=str(e.result) if e.result is not None else None,
                    error=str(e.error) if e.error is not None else None,
                )
                for e in db_executions
            ]

        def as_persistence_adapter(self) -> PersistenceAdapter:
            """
            Return a BaseStorageAdapter wrapper for unified storage access.

            This allows the new StorageMiddleware and CacheMiddleware to
            use the existing SQLAlchemy backend through the standard interface.
            """
            return PersistenceAdapter(db_url=self.db_url, async_mode=self.is_async)

        @classmethod
        def get_connection_url(cls, db_type: str, **kwargs: Any) -> str:
            """
            Helper to generate SQLAlchemy connection URLs.

            Args:
                db_type: Database type (sqlite, postgresql, mysql)
                **kwargs: Connection parameters (host, port, user, password,
                    database)

            Returns:
                SQLAlchemy connection URL

            Examples:
                >>> JobPersistenceManager.get_connection_url(
                ...     "postgresql",
                ...     host="localhost",
                ...     port=5432,
                ...     user="taskiq",
                ...     password="password", # pragma: allowlist secret
                ...     database="taskiq_flow",
                ... )
                # pragma: allowlist nextline secret
                'postgresql+asyncpg://user:password@localhost:5432/taskiq_flow'

            """
            drivers = {
                "sqlite": "sqlite+aiosqlite",
                "postgresql": "postgresql+asyncpg",
                "mysql": "mysql+aiomysql",
            }

            if db_type == "sqlite":
                path = kwargs.get("path", "jobs.db")
                return f"{drivers[db_type]}:///{path}"

            driver = drivers.get(db_type, "sqlite+aiosqlite")
            user = kwargs.get("user", "")
            password = kwargs.get("password", "")
            host = kwargs.get("host", "localhost")
            port = kwargs.get("port", "")
            database = kwargs.get("database", "taskiq_flow")

            auth = f"{user}:{password}@" if user else ""
            port_str = f":{port}" if port else ""

            return f"{driver}://{auth}{host}{port_str}/{database}"
else:

    class JobPersistenceManager:  # type: ignore
        """Placeholder when SQLAlchemy is not available."""

        def __init__(
            self, db_url: str = "sqlite:///jobs.db", async_mode: bool = True
        ) -> None:
            raise ImportError(
                "SQLAlchemy is required for JobPersistenceManager. "
                "Install with: pip install taskiq-flow[scheduler]"
            )
