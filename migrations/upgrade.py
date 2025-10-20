"""Simple idempotent migration script to add contributor_hash and contribution_hash columns and indexes if they don't exist."""
import logging
from db_schema import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ALTER_STATEMENTS = [
    """
    ALTER TABLE contributors
    ADD COLUMN IF NOT EXISTS contributor_hash VARCHAR(64);
    """,
    """
    ALTER TABLE contributions
    ADD COLUMN IF NOT EXISTS contribution_hash VARCHAR(64);
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS contributors_contributor_hash_idx ON contributors(contributor_hash);
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS contributions_contribution_hash_idx ON contributions(contribution_hash);
    """,
]


def run_migrations():
    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                for stmt in ALTER_STATEMENTS:
                    logger.info("Applying migration: %s", stmt.strip().splitlines()[0])
                    cur.execute(stmt)
        logger.info("Migrations applied successfully")
    except Exception as e:
        logger.exception("Migration failed: %s", e)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    run_migrations()
