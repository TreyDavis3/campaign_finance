import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from fec_api import get_candidates, get_committees, get_contributions, create_fec_session
from dotenv import load_dotenv
import os
import concurrent.futures

load_dotenv()

# Database connection parameters
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
API_KEY = os.getenv("FEC_API_KEY")

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

def load_df_to_db(df, table_name, primary_key):
    """Loads a pandas DataFrame into a PostgreSQL table using psycopg2.extras.execute_values for speed."""
    if df.empty:
        print(f"DataFrame for table {table_name} is empty. Nothing to load.")
        return

    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        with conn.cursor() as cur:
            table = sql.Identifier(table_name)
            columns = [sql.Identifier(col) for col in df.columns]
            cols_sql = sql.SQL(', ').join(columns)
            
            # Convert DataFrame to a list of tuples
            tuples = [tuple(x) for x in df.to_numpy()]

            if table_name == 'contributions':
                query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                    table,
                    cols_sql
                )
                execute_values(cur, query, tuples)
            else:
                # For tables with a primary key, use ON CONFLICT DO UPDATE
                primary_key_identifier = sql.Identifier(primary_key)
                update_cols = [col for col in df.columns if col != primary_key]
                
                # Create the SET part of the query
                update_clause = sql.SQL(', ').join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                    for col in update_cols
                )

                query = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {}").format(
                    table,
                    cols_sql,
                    primary_key_identifier,
                    update_clause
                )
                execute_values(cur, query, tuples)

            conn.commit()
            print(f"{len(df)} records loaded into {table_name}.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error loading data into {table_name}: {error}")
    finally:
        if conn is not None:
            conn.close()

def fetch_contributions_for_candidate(session, candidate_name):
    """Fetches and transforms contributions for a single candidate."""
    print(f"Fetching contributions for {candidate_name}...")
    contributions_data = get_contributions(session, API_KEY, contributor_name=candidate_name)
    return transform_contributions_to_df(contributions_data)

def fetch_committee_details(session, committee_id):
    """Fetches and transforms details for a single committee."""
    print(f"Fetching committee {committee_id}...")
    committee_data = get_committees(session, API_KEY, committee_id=committee_id)
    return transform_committees_to_df(committee_data)

if __name__ == "__main__":
    fec_session = create_fec_session()
    try:
        # 1. Fetch and load candidates
        print("Fetching and loading candidates...")
        candidates_data = get_candidates(fec_session, API_KEY, cycle=2024, office="P")
        candidates_df = transform_candidates_to_df(candidates_data)
        load_df_to_db(candidates_df, 'candidates', 'candidate_id')

        # 2. Fetch contributions in parallel and gather unique committee IDs
        print("\nFetching contributions in parallel and gathering committee IDs...")
        all_contributions_df = pd.DataFrame()
        all_committee_ids = set()
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_candidate = {executor.submit(fetch_contributions_for_candidate, fec_session, name): name for name in candidates_df['name']}
            for future in concurrent.futures.as_completed(future_to_candidate):
                contributions_df = future.result()
                if not contributions_df.empty:
                    all_contributions_df = pd.concat([all_contributions_df, contributions_df], ignore_index=True)
                    for committee_id in contributions_df['committee_id']:
                        if committee_id:
                            all_committee_ids.add(committee_id)

        print(f"Found {len(all_committee_ids)} unique committees.")

        # 3. Fetch and load unique committees in parallel
        print("\nFetching and loading unique committees in parallel...")
        all_committees_df = pd.DataFrame()
        if all_committee_ids:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_committee = {executor.submit(fetch_committee_details, fec_session, cid): cid for cid in all_committee_ids}
                for future in concurrent.futures.as_completed(future_to_committee):
                    committee_df = future.result()
                    all_committees_df = pd.concat([all_committees_df, committee_df], ignore_index=True)
            
            load_df_to_db(all_committees_df, 'committees', 'committee_id')

        # 4. Load contributions
        print("\nLoading contributions...")
        load_df_to_db(all_contributions_df, 'contributions', 'contribution_id')

    finally:
        fec_session.close()
