"""
Database Migration Framework for TextileERP.
Auto-detects missing tables/columns and runs migrations from scratch on any fresh database.
Tracks applied migrations in a `_migrations` table.
"""
import os
import importlib
import pkgutil
import logging
import psycopg2
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def get_db_connection():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL not set in environment")
    return psycopg2.connect(db_url)


def ensure_migrations_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _migrations (
            id SERIAL PRIMARY KEY,
            version VARCHAR(10) NOT NULL UNIQUE,
            description TEXT,
            applied_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)


def get_applied_migrations(cursor):
    cursor.execute("SELECT version FROM _migrations ORDER BY version;")
    return {row[0] for row in cursor.fetchall()}


def discover_migrations():
    """Discover all migration modules in the migrations package, sorted by version."""
    migrations = []
    package_dir = os.path.dirname(__file__)
    for _, name, _ in pkgutil.iter_modules([package_dir]):
        if name.startswith("v"):
            mod = importlib.import_module(f"migrations.{name}")
            migrations.append({
                "module_name": name,
                "version": getattr(mod, "VERSION"),
                "description": getattr(mod, "DESCRIPTION"),
                "up": getattr(mod, "up"),
            })
    migrations.sort(key=lambda m: m["version"])
    return migrations


def run_migrations():
    """Run all pending migrations. Safe to call on every startup."""
    try:
        conn = get_db_connection()
        conn.autocommit = False
        cursor = conn.cursor()

        ensure_migrations_table(cursor)
        conn.commit()

        applied = get_applied_migrations(cursor)
        all_migrations = discover_migrations()
        pending = [m for m in all_migrations if m["version"] not in applied]

        if not pending:
            logger.info("Database is up to date. No migrations to run.")
            cursor.close()
            conn.close()
            return True

        for migration in pending:
            version = migration["version"]
            desc = migration["description"]
            logger.info(f"Running migration {version}: {desc}")
            try:
                migration["up"](cursor)
                cursor.execute(
                    "INSERT INTO _migrations (version, description) VALUES (%s, %s);",
                    (version, desc),
                )
                conn.commit()
                logger.info(f"Migration {version} applied successfully.")
            except Exception as e:
                conn.rollback()
                logger.error(f"Migration {version} FAILED: {e}")
                cursor.close()
                conn.close()
                return False

        cursor.close()
        conn.close()
        logger.info(f"All {len(pending)} migration(s) applied successfully.")
        return True

    except Exception as e:
        logger.error(f"Migration framework error: {e}")
        return False
