import os
import logging
import sys
import hashlib
import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_values

# Support running as a package (pytest) or as a script
try:
    from src.fec_api import get_candidates, get_committees, get_contributions, create_fec_session
    from src.db_schema import get_db_connection
except ImportError:
    from fec_api import get_candidates, get_committees, get_contributions, create_fec_session
    from db_schema import get_db_connection


import concurrent.futures

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurable worker/chunk sizes (can be overridden via environment variables)
DEFAULT_CHUNK_SIZE_INSERT = int(os.getenv("ETL_CHUNK_SIZE_INSERT", "1000"))
DEFAULT_CHUNK_SIZE_UPSERT = int(os.getenv("ETL_CHUNK_SIZE_UPSERT", "500"))


def _normalize_str(s: str) -> str:
    if s is None:
        return ""
    return " ".join(str(s).lower().strip().split())


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def transform_candidates_to_df(candidates_data: dict) -> pd.DataFrame:
    """Transforms the raw candidate data into a pandas DataFrame."""
    transformed_data = []
    for candidate in candidates_data.get("results", []):
        transformed_data.append({
            'candidate_id': candidate.get('candidate_id'),
            'name': candidate.get('name'),
            'party': candidate.get('party'),
            'state': candidate.get('state'),
            'office': candidate.get('office'),
            'election_year': candidate.get('election_years', [None])[0]
        })
    return pd.DataFrame(transformed_data)

def transform_committees_to_df(committees_data: dict) -> pd.DataFrame:
    """Transforms the raw committee data into a pandas DataFrame."""
    transformed_data = []
    for committee in committees_data.get("results", []):
        transformed_data.append({
            'committee_id': committee.get('committee_id'),
            'name': committee.get('name'),
            'city': committee.get('city'),
            'state': committee.get('state'),
            'treasurer_name': committee.get('treasurer_name'),
            'committee_type': committee.get('committee_type')
        })
    return pd.DataFrame(transformed_data)

def transform_contributions_to_df(contributions_data: dict) -> pd.DataFrame:
    """Transforms the raw contribution data into a pandas DataFrame."""
    transformed_data = []
    for contribution in contributions_data.get("results", []):
        transformed_data.append({
            'committee_id': contribution.get('committee_id'),
            'contributor_name': contribution.get('contributor_name'),
            'contributor_city': contribution.get('contributor_city'),
            'contributor_state': contribution.get('contributor_state'),
            'contributor_zip_code': contribution.get('contributor_zip'),
            'contribution_date': contribution.get('contribution_receipt_date'),
            'contribution_amount': contribution.get('contribution_receipt_amount'),
            'contributor_occupation': contribution.get('contributor_occupation'),
            'contributor_employer': contribution.get('contributor_employer')
        })
    return pd.DataFrame(transformed_data)

def load_df_to_db(conn, df: pd.DataFrame, table_name: str, primary_key: str):
    """Loads a pandas DataFrame into a PostgreSQL table using a provided connection."""
    if df.empty:
        logger.info("DataFrame for table %s is empty. Nothing to load.", table_name)
        return

    # Insert in chunks to avoid huge payloads
    tuples = [tuple(x) for x in df.to_numpy()]

    with conn.cursor() as cur:
        table = sql.Identifier(table_name)
        columns = [sql.Identifier(col) for col in df.columns]
        cols_sql = sql.SQL(', ').join(columns)

        if table_name == 'contributions':
            query = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT (contribution_hash) DO NOTHING").format(
                table,
                cols_sql,
            )
            for i in range(0, len(tuples), DEFAULT_CHUNK_SIZE_INSERT):
                execute_values(cur, query, tuples[i:i+chunk_size])
        else:
            # Use primary key upsert for other tables
            primary_key_identifier = sql.Identifier(primary_key)
            update_cols = [col for col in df.columns if col != primary_key]

            update_clause = sql.SQL(', ').join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                for col in update_cols
            )

            query = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {}").format(
                table,
                cols_sql,
                primary_key_identifier,
                update_clause,
            )
            for i in range(0, len(tuples), DEFAULT_CHUNK_SIZE_UPSERT):
                execute_values(cur, query, tuples[i:i+chunk_size])

        conn.commit()
        logger.info("%s records loaded into %s.", len(df), table_name)

def _get_contributor_hash(row: pd.Series) -> str:
    """Generates a SHA256 hash for a contributor row."""
    parts = [
        _normalize_str(row.get('contributor_name')),
        _normalize_str(row.get('contributor_city')),
        _normalize_str(row.get('contributor_state')),
        _normalize_str(row.get('contributor_zip_code')),
        _normalize_str(row.get('contributor_occupation')),
        _normalize_str(row.get('contributor_employer'))
    ]
    return _sha256_hex('|'.join(parts))


def _get_contribution_hash(row: pd.Series) -> str:
    """Generates a SHA256 hash for a contribution row."""
    parts = [
        str(row.get('committee_id') or ''),
        str(row.get('contribution_date') or ''),
        str(row.get('contribution_amount') or ''),
        str(row.get('contributor_hash') or '')
    ]
    return _sha256_hex('|'.join(parts))


def process_and_load_contributors(conn, contributions_df: pd.DataFrame) -> dict:
    """
    Extracts unique contributors, loads them to the DB, and returns a hash-to-ID map.
    Uses `ON CONFLICT DO UPDATE ... RETURNING contributor_id` for efficiency.
    """
    if contributions_df.empty:
        return {}

    logger.info("Processing and loading %d contribution records for contributors.", len(contributions_df))
    contributors_df = contributions_df[[
        'contributor_hash', 'contributor_name', 'contributor_city', 'contributor_state',
        'contributor_zip_code', 'contributor_occupation', 'contributor_employer'
    ]].drop_duplicates('contributor_hash').rename(columns={
        'contributor_name': 'name',
        'contributor_city': 'city',
        'contributor_state': 'state',
        'contributor_zip_code': 'zip_code',
        'contributor_occupation': 'occupation',
        'contributor_employer': 'employer'
    })

    tuples = [tuple(x) for x in contributors_df.to_numpy()]
    if not tuples:
        return {}

    with conn.cursor() as cur:
        cols = list(contributors_df.columns)
        cols_sql = sql.SQL(', ').join(map(sql.Identifier, cols))
        
        # Use ON CONFLICT to update existing records and RETURNING to get IDs back
        # This avoids a second round-trip to the database.
        query_str = f"""
            INSERT INTO contributors ({", ".join(cols)})
            VALUES %s
            ON CONFLICT (contributor_hash) DO UPDATE
            SET name = EXCLUDED.name,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                zip_code = EXCLUDED.zip_code,
                occupation = EXCLUDED.occupation,
                employer = EXCLUDED.employer
            RETURNING contributor_id, contributor_hash;
        """
        
        # execute_values returns the results from RETURNING
        results = execute_values(cur, query_str, tuples, fetch=True, page_size=DEFAULT_CHUNK_SIZE_UPSERT)
        conn.commit()

        contributor_id_map = {r[1]: r[0] for r in results}
        logger.info("Upserted %d contributors and retrieved their IDs.", len(contributor_id_map))
        return contributor_id_map


def run_etl(api_key: str = None, cycle: int = 2024, office: str = "P"):
    """Main ETL process."""
    API_KEY = api_key or os.getenv("FEC_API_KEY")
    if not API_KEY:
        logger.error("FEC_API_KEY is not set. Please set it in your .env file or pass it as an argument.")
        sys.exit(1)

    fec_session = create_fec_session()
    db_conn = None
    try:
        db_conn = get_db_connection()

        # 1. Fetch Candidates and Committees for the cycle
        logger.info(f"Fetching candidates and committees for cycle {cycle}, office {office}...")
        candidates_data = get_candidates(fec_session, API_KEY, cycle=cycle, office=office)
        candidates_df = transform_candidates_to_df(candidates_data)
        if not candidates_df.empty:
            load_df_to_db(db_conn, candidates_df, 'candidates', 'candidate_id')

        # Fetch all committees associated with the candidates
        candidate_committee_ids = set()
        for cand_id in candidates_df['candidate_id'].unique():
            # This part can be further optimized if there's a bulk endpoint
            committee_data = get_committees(fec_session, API_KEY, candidate_id=cand_id, cycle=cycle)
            for committee in committee_data.get("results", []):
                candidate_committee_ids.add(committee['committee_id'])
        
        logger.info(f"Found {len(candidate_committee_ids)} unique committees linked to candidates.")
        if candidate_committee_ids:
            committees_df = transform_committees_to_df(get_committees(fec_session, API_KEY, committee_id=list(candidate_committee_ids)))
            if not committees_df.empty:
                load_df_to_db(db_conn, committees_df, 'committees', 'committee_id')

        # 2. Fetch all contributions for the cycle
        logger.info(f"Fetching all contributions for cycle {cycle}...")
        contributions_data = get_contributions(fec_session, API_KEY, cycle=cycle, per_page=100)
        all_contributions_df = transform_contributions_to_df(contributions_data)

        if all_contributions_df.empty:
            logger.info("No new contributions found for this cycle. ETL finished.")
            return

        # 3. Process contributors
        all_contributions_df['contributor_hash'] = all_contributions_df.apply(_get_contributor_hash, axis=1)
        contributor_id_map = process_and_load_contributors(db_conn, all_contributions_df)

        # 4. Finalize and load contributions
        all_contributions_df['contributor_id'] = all_contributions_df['contributor_hash'].map(contributor_id_map)
        all_contributions_df['contribution_hash'] = all_contributions_df.apply(_get_contribution_hash, axis=1)

        # Drop rows where contributor_id could not be mapped (should be rare)
        all_contributions_df.dropna(subset=['contributor_id'], inplace=True)
        all_contributions_df['contributor_id'] = all_contributions_df['contributor_id'].astype(int)

        # Deduplicate contributions by contribution_hash before loading
        all_contributions_df.drop_duplicates('contribution_hash', inplace=True)

        logger.info("Loading final contributions to the database...")
        final_contribs_df = all_contributions_df[['committee_id', 'contributor_id', 'contribution_date', 'contribution_amount', 'contribution_hash']].copy()
        load_df_to_db(db_conn, final_contribs_df, 'contributions', 'contribution_id')

    except Exception:
        logger.exception("An error occurred during the ETL process")
        raise
    finally:
        if db_conn:
            db_conn.close()
            logger.info("Database connection closed.")
        fec_session.close()


if __name__ == "__main__":
    run_etl()
