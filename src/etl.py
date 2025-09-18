
import pandas as pd
import psycopg2
from fec_api import get_candidates, get_committees, get_contributions
from dotenv import load_dotenv
import os

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
    """Loads a pandas DataFrame into a PostgreSQL table."""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        cur = conn.cursor()
        for index, row in df.iterrows():
            columns = ', '.join(row.index)
            placeholders = ', '.join(['%s'] * len(row))
            update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in row.index if col != primary_key])
            # Skip update for contributions table as it has a serial primary key
            if table_name == 'contributions':
                update_clause = ''
            else:
                update_clause = f"ON CONFLICT ({primary_key}) DO UPDATE SET {update_clause}"

            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) {update_clause}"
            cur.execute(query, tuple(row.where(pd.notnull(row), None)))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    # 1. Fetch and load candidates
    print("Fetching and loading candidates...")
    candidates_data = get_candidates(API_KEY, cycle=2024, office="P")
    candidates_df = transform_candidates_to_df(candidates_data)
    load_df_to_db(candidates_df, 'candidates', 'candidate_id')
    print("Candidates loaded successfully.")

    # 2. Fetch contributions and gather unique committee IDs
    print("\nFetching contributions and gathering committee IDs...")
    all_contributions_df = pd.DataFrame()
    all_committee_ids = set()

    for index, candidate in candidates_df.iterrows():
        print(f"Fetching contributions for {candidate['name']}...")
        contributions_data = get_contributions(API_KEY, contributor_name=candidate['name'])
        contributions_df = transform_contributions_to_df(contributions_data)
        if not contributions_df.empty:
            all_contributions_df = pd.concat([all_contributions_df, contributions_df], ignore_index=True)
            for committee_id in contributions_df['committee_id']:
                if committee_id:
                    all_committee_ids.add(committee_id)

    print(f"Found {len(all_committee_ids)} unique committees.")

    # 3. Fetch and load unique committees
    print("\nFetching and loading unique committees...")
    all_committees_df = pd.DataFrame()
    if all_committee_ids:
        for committee_id in all_committee_ids:
            print(f"Fetching committee {committee_id}...")
            committee_data = get_committees(API_KEY, committee_id=committee_id)
            committee_df = transform_committees_to_df(committee_data)
            all_committees_df = pd.concat([all_committees_df, committee_df], ignore_index=True)
        
        load_df_to_db(all_committees_df, 'committees', 'committee_id')
        print("Committees loaded successfully.")
    else:
        print("No committees to load.")

    # 4. Load contributions
    print("\nLoading contributions...")
    if not all_contributions_df.empty:
        load_df_to_db(all_contributions_df, 'contributions', 'contribution_id')
        print("Contributions loaded successfully.")
    else:
        print("No contributions to load.")
