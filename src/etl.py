import os
import logging
import hashlib
import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_values
from fec_api import get_candidates, get_committees, get_contributions, create_fec_session
from db_schema import get_db_connection  # Import the centralized connection function
import concurrent.futures

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _normalize_str(s: str) -> str:
    if s is None:
        return ""
    return " ".join(str(s).lower().strip().split())


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def transform_candidates_to_df(candidates_data):
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

def transform_committees_to_df(committees_data):
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

def transform_contributions_to_df(contributions_data):
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

def load_df_to_db(conn, df, table_name, primary_key):
    """Loads a pandas DataFrame into a PostgreSQL table using a provided connection."""
    if df.empty:
        logger.info("DataFrame for table %s is empty. Nothing to load.", table_name)
        return
        return

    # Insert in chunks to avoid huge payloads
    tuples = [tuple(x) for x in df.to_numpy()]
    chunk_size = 1000

    with conn.cursor() as cur:
        table = sql.Identifier(table_name)
        columns = [sql.Identifier(col) for col in df.columns]
        cols_sql = sql.SQL(', ').join(columns)

        if table_name == 'contributions':
            query = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT (contribution_hash) DO NOTHING").format(
                table,
                cols_sql,
            )
            for i in range(0, len(tuples), chunk_size):
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
            for i in range(0, len(tuples), chunk_size):
                execute_values(cur, query, tuples[i:i+chunk_size])

        conn.commit()
        logger.info("%s records loaded into %s.", len(df), table_name)

def fetch_contributions_for_candidate(session, api_key, candidate_name):
    """Fetches and transforms contributions for a single candidate."""
    logger.debug("Fetching contributions for %s", candidate_name)
    contributions_data = get_contributions(session, api_key, contributor_name=candidate_name)
    return transform_contributions_to_df(contributions_data)

def fetch_committee_details(session, api_key, committee_id):
    """Fetches and transforms details for a single committee."""
    logger.debug("Fetching committee %s", committee_id)
    committee_data = get_committees(session, api_key, committee_id=committee_id)
    return transform_committees_to_df(committee_data)

if __name__ == "__main__":
    fec_session = create_fec_session()
    db_conn = None
    try:
        db_conn = get_db_connection()
        API_KEY = os.getenv("FEC_API_KEY")

        # 1. Fetch and load candidates
        logger.info("Fetching and loading candidates...")
        candidates_data = get_candidates(fec_session, API_KEY, cycle=2024, office="P")
        candidates_df = transform_candidates_to_df(candidates_data)
        if not candidates_df.empty:
            load_df_to_db(db_conn, candidates_df, 'candidates', 'candidate_id')

        # 2. Fetch contributions in parallel and gather unique committee IDs
        logger.info("Fetching contributions in parallel and gathering committee IDs...")
        contrib_dfs = []
        all_committee_ids = set()
        max_workers = min(8, max(2, (len(candidates_df) // 10) or 2))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_candidate = {executor.submit(fetch_contributions_for_candidate, fec_session, API_KEY, name): name for name in candidates_df['name']}
            for future in concurrent.futures.as_completed(future_to_candidate):
                name = future_to_candidate[future]
                try:
                    contributions_df = future.result()
                except Exception as e:
                    logger.exception("Failed to fetch contributions for %s: %s", name, e)
                    continue
                if not contributions_df.empty:
                    contrib_dfs.append(contributions_df)
                    for committee_id in contributions_df['committee_id'].dropna().unique():
                        all_committee_ids.add(committee_id)

        all_contributions_df = pd.concat(contrib_dfs, ignore_index=True) if contrib_dfs else pd.DataFrame()
        logger.info("Found %d unique committees.", len(all_committee_ids))

        # Normalize contributor fields and compute contributor_hash
        if not all_contributions_df.empty:
            all_contributions_df['contributor_name_norm'] = all_contributions_df['contributor_name'].apply(_normalize_str)
            all_contributions_df['contributor_city_norm'] = all_contributions_df['contributor_city'].apply(_normalize_str)
            all_contributions_df['contributor_state_norm'] = all_contributions_df['contributor_state'].apply(_normalize_str)
            all_contributions_df['contributor_zip_code_norm'] = all_contributions_df['contributor_zip_code'].apply(_normalize_str)
            all_contributions_df['contributor_occupation_norm'] = all_contributions_df['contributor_occupation'].apply(_normalize_str)
            all_contributions_df['contributor_employer_norm'] = all_contributions_df['contributor_employer'].apply(_normalize_str)

            def make_contributor_hash(row):
                parts = [row['contributor_name_norm'], row['contributor_city_norm'], row['contributor_state_norm'], row['contributor_zip_code_norm'], row['contributor_occupation_norm'], row['contributor_employer_norm']]
                return _sha256_hex('|'.join(parts))

            all_contributions_df['contributor_hash'] = all_contributions_df.apply(make_contributor_hash, axis=1)

            # Upsert contributors and obtain contributor_id mapping
            contributors_df = all_contributions_df[['contributor_hash', 'contributor_name', 'contributor_city', 'contributor_state', 'contributor_zip_code', 'contributor_occupation', 'contributor_employer']].drop_duplicates('contributor_hash').rename(columns={
                'contributor_name': 'name',
                'contributor_city': 'city',
                'contributor_state': 'state',
                'contributor_zip_code': 'zip_code',
                'contributor_occupation': 'occupation',
                'contributor_employer': 'employer'
            })

            # Insert contributors using ON CONFLICT DO NOTHING, then select ids
            with db_conn.cursor() as cur:
                insert_cols = ['name', 'city', 'state', 'zip_code', 'occupation', 'employer', 'contributor_hash']
                columns = [sql.Identifier(c) for c in insert_cols]
                cols_sql = sql.SQL(', ').join(columns)
                values_sql = sql.SQL(', ').join(sql.Placeholder() * len(insert_cols))
                insert_query = sql.SQL('INSERT INTO contributors ({}) VALUES ({}) ON CONFLICT (contributor_hash) DO NOTHING').format(cols_sql, values_sql)
                tuples = [tuple(row[col] if col in row else None for col in insert_cols) for _, row in contributors_df.iterrows()]
                chunk_size = 500
                for i in range(0, len(tuples), chunk_size):
                    execute_values(cur, insert_query.as_string(db_conn), tuples[i:i+chunk_size], template=None, page_size=chunk_size)
                db_conn.commit()

                # Build mapping from contributor_hash to contributor_id
                cur.execute("SELECT contributor_id, contributor_hash FROM contributors WHERE contributor_hash = ANY(%s)", (list(contributors_df['contributor_hash']),))
                rows = cur.fetchall()
                contributor_id_map = {r[1]: r[0] for r in rows}

            # Attach contributor_id to contributions and compute contribution_hash
            all_contributions_df['contributor_id'] = all_contributions_df['contributor_hash'].map(contributor_id_map)

            def make_contribution_hash(row):
                parts = [str(row.get('committee_id') or ''), str(row.get('contribution_date') or ''), str(row.get('contribution_amount') or ''), str(row.get('contributor_hash') or '')]
                return _sha256_hex('|'.join(parts))

            all_contributions_df['contribution_hash'] = all_contributions_df.apply(make_contribution_hash, axis=1)

            # Deduplicate contributions by contribution_hash
            all_contributions_df = all_contributions_df.drop_duplicates('contribution_hash')

        # 3. Fetch and load unique committees in parallel
        logger.info("Fetching and loading unique committees in parallel...")
        all_committees_df = pd.DataFrame()
        if all_committee_ids:
            committee_dfs = []
            max_workers = min(8, max(2, (len(all_committee_ids) // 10) or 2))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_committee = {executor.submit(fetch_committee_details, fec_session, API_KEY, cid): cid for cid in all_committee_ids}
                for future in concurrent.futures.as_completed(future_to_committee):
                    cid = future_to_committee[future]
                    try:
                        committee_df = future.result()
                    except Exception as e:
                        logger.exception("Failed to fetch committee %s: %s", cid, e)
                        continue
                    if not committee_df.empty:
                        committee_dfs.append(committee_df)

            all_committees_df = pd.concat(committee_dfs, ignore_index=True) if committee_dfs else pd.DataFrame()
            if not all_committees_df.empty:
                load_df_to_db(db_conn, all_committees_df, 'committees', 'committee_id')

        # 4. Load contributions
        logger.info("Loading contributions...")
        if not all_contributions_df.empty:
            # Prepare final contributions DataFrame columns to match table
            final_contribs = all_contributions_df[['committee_id', 'contributor_id', 'contribution_date', 'contribution_amount', 'contribution_hash']].copy()
            load_df_to_db(db_conn, final_contribs, 'contributions', 'contribution_id')

    except Exception as e:
        logger.exception("An error occurred during the ETL process: %s", e)
    finally:
        if db_conn:
            db_conn.close()
            logger.info("Database connection closed.")
        fec_session.close()

