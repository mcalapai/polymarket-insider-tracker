"""Alembic migration environment configuration.

This project uses SQLAlchemy's async engine (asyncpg) for PostgreSQL.
"""

from __future__ import annotations

import asyncio
import os
from logging.config import fileConfig

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from polymarket_insider_tracker.storage.models import Base

# this is the Alembic Config object
config = context.config

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Load .env so `alembic upgrade head` works without manually exporting vars.
# This mirrors the application's Settings(env_file=".env") behavior.
load_dotenv(override=False)

# Target metadata for 'autogenerate' support
target_metadata = Base.metadata


def _get_database_url() -> str | None:
    # Prefer Alembic's conventional override, but also support the app's DATABASE_URL.
    url = os.environ.get("SQLALCHEMY_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if url:
        url = os.path.expandvars(url)
    if url and url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


database_url = _get_database_url()
if database_url:
    config.set_main_option("sqlalchemy.url", database_url)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection: object) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def _run_migrations_online_async() -> None:
    """Run migrations in 'online' mode using an async engine."""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(_do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    asyncio.run(_run_migrations_online_async())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
