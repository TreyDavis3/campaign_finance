"""Robust idempotent migration script to ensure required columns and indexes exist.

This script checks information_schema/pg_indexes and only applies ALTER/CREATE INDEX
when necessary, logging the exact changes applied.
"""
import logging
from db_schema import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _column_exists(cur, table_name: str, column_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_name = %s AND column_name = %s",
        (table_name, column_name),
    )
    return cur.fetchone() is not None


def _index_exists(cur, index_name: str) -> bool:
    cur.execute("SELECT 1 FROM pg_indexes WHERE indexname = %s", (index_name,))
    return cur.fetchone() is not None


def run_migrations():
    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                # contributors.contributor_hash
                if not _column_exists(cur, 'contributors', 'contributor_hash'):
                    logger.info("Adding column contributors.contributor_hash")
                    cur.execute("ALTER TABLE contributors ADD COLUMN contributor_hash VARCHAR(64);")
                else:
                    logger.info("Column contributors.contributor_hash already exists")

                # contributions.contribution_hash
                if not _column_exists(cur, 'contributions', 'contribution_hash'):
                    logger.info("Adding column contributions.contribution_hash")
                    cur.execute("ALTER TABLE contributions ADD COLUMN contribution_hash VARCHAR(64);")
                else:
                    logger.info("Column contributions.contribution_hash already exists")

                # indexes
                if not _index_exists(cur, 'contributors_contributor_hash_idx'):
                    logger.info("Creating index contributors_contributor_hash_idx")
                    cur.execute("CREATE UNIQUE INDEX contributors_contributor_hash_idx ON contributors(contributor_hash);")
                else:
                    logger.info("Index contributors_contributor_hash_idx already exists")

                if not _index_exists(cur, 'contributions_contribution_hash_idx'):
                    logger.info("Creating index contributions_contribution_hash_idx")
                    cur.execute("CREATE UNIQUE INDEX contributions_contribution_hash_idx ON contributions(contribution_hash);")
                else:
                    logger.info("Index contributions_contribution_hash_idx already exists")

        logger.info("Migrations applied successfully")
    except Exception:
        logger.exception("Migration failed")
        raise
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    run_migrations()
