
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
import time
import logging

load_dotenv()

API_KEY = os.getenv("FEC_API_KEY")
BASE_URL = "https://api.open.fec.gov/v1"

def create_fec_session():
    """
    Creates a requests.Session with automatic retries.
    """
    session = requests.Session()
    retry = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def _fetch_all_pages(session, url, params):
    """
    Fetches all pages of results from a given FEC API endpoint.
    """
    all_results = []
    params['page'] = 1
    params['per_page'] = 100

    logging.info(f"Fetching from {url} with initial params: { {k: v for k, v in params.items() if k != 'api_key'} }")
    while True:
        try:
            response = session.get(url, params=params)
            response.raise_for_status()  # Raise an exception for bad status codes
            data = response.json()
            
            all_results.extend(data.get("results", []))
            
            pagination = data.get("pagination", {})
            last_indexes = pagination.get("last_indexes")
            
            if not last_indexes or pagination.get("page") >= pagination.get("pages"):
                break
                
            params['last_index'] = last_indexes.get('last_index')
            # The FEC API documentation specifies that for Schedule A, both last_index and last_contribution_receipt_date might be needed.
            if 'last_contribution_receipt_date' in last_indexes:
                params['last_contribution_receipt_date'] = last_indexes.get('last_contribution_receipt_date')

            # The FEC API asks to not hit it more than once a second
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            logging.error(f"Error during API request: {e}")
            raise e
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            raise e

    return {"results": all_results}


def get_candidates(session, api_key, **kwargs):
    """
    Fetches a list of candidates from the FEC API, handling pagination.
    """
    if not api_key:
        raise ValueError("FEC_API_KEY is not set. Please set it in your .env file.")

    params = {"api_key": api_key, **kwargs}
    url = f"{BASE_URL}/candidates/"
    return _fetch_all_pages(session, url, params)

def get_committees(session, api_key, **kwargs):
    """
    Fetches a list of committees from the FEC API, handling pagination.
    """
    if not api_key:
        raise ValueError("FEC_API_KEY is not set. Please set it in your .env file.")

    params = {"api_key": api_key, **kwargs}
    url = f"{BASE_URL}/committees/"
    return _fetch_all_pages(session, url, params)

def get_contributions(session, api_key, **kwargs):
    """
    Fetches a list of contributions from the FEC API, handling pagination.
    """
    if not api_key:
        raise ValueError("FEC_API_KEY is not set. Please set it in your .env file.")

    params = {"api_key": api_key, **kwargs}
    url = f"{BASE_URL}/schedules/schedule_a/"
    return _fetch_all_pages(session, url, params)

if __name__ == "__main__":
    if not API_KEY:
        raise ValueError("FEC_API_KEY is not set. Please set it in your .env file.")

    fec_session = create_fec_session()

    try:
        # Fetch candidates
        print("--- Fetching all candidates for 2024 cycle (office P) ---")
        candidates_data = get_candidates(fec_session, API_KEY, cycle=2024, office="P")
        print(f"Found {len(candidates_data.get('results', []))} total candidates.")
        print("--- First 5 Candidates ---")
        for candidate in candidates_data.get("results", [])[:5]:
            print(f"Name: {candidate.get('name')}, Party: {candidate.get('party_full')}")

        # Fetch committees
        print("\n--- Fetching all committees for 2024 cycle (type P) ---")
        committees_data = get_committees(fec_session, API_KEY, cycle=2024, committee_type="P")
        print(f"Found {len(committees_data.get('results', []))} total committees.")
        print("--- First 5 Committees ---")
        for committee in committees_data.get("results", [])[:5]:
            print(f"Name: {committee.get('name')}, Type: {committee.get('committee_type_full')}")

        # Fetch contributions
        print("\n--- Fetching all contributions for a specific contributor ---")
        contributions_data = get_contributions(fec_session, API_KEY, contributor_name="BIDEN, JOSEPH R JR", per_page=100)
        print(f"Found {len(contributions_data.get('results', []))} total contributions.")
        print("--- First 5 Contributions ---")
        for contribution in contributions_data.get("results", [])[:5]:
            print(f"Contributor: {contribution.get('contributor_name')}, Amount: {contribution.get('contribution_amount')}")

    except (ValueError, requests.exceptions.RequestException) as e:
        print(f"Error: {e}")
    finally:
        fec_session.close()
