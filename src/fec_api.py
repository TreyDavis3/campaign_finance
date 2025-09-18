
import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FEC_API_KEY")
BASE_URL = "https://api.open.fec.gov/v1"

def get_candidates(api_key, **kwargs):
    """
    Fetches a list of candidates from the FEC API.
    """
    if not api_key:
        raise ValueError("FEC_API_KEY is not set. Please set it in your .env file.")

    params = {"api_key": api_key, **kwargs}
    response = requests.get(f"{BASE_URL}/candidates/", params=params)
    response.raise_for_status()  # Raise an exception for bad status codes
    return response.json()

def get_committees(api_key, **kwargs):
    """
    Fetches a list of committees from the FEC API.
    """
    if not api_key:
        raise ValueError("FEC_API_KEY is not set. Please set it in your .env file.")

    params = {"api_key": api_key, **kwargs}
    response = requests.get(f"{BASE_URL}/committees/", params=params)
    response.raise_for_status()
    return response.json()

def get_contributions(api_key, **kwargs):
    """
    Fetches a list of contributions from the FEC API.
    """
    if not api_key:
        raise ValueError("FEC_API_KEY is not set. Please set it in your .env file.")

    params = {"api_key": api_key, **kwargs}
    response = requests.get(f"{BASE_URL}/schedules/schedule_a/", params=params)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    # Example usage:
    try:
        # Fetch candidates
        candidates_data = get_candidates(API_KEY, cycle=2024, office="P")
        print("--- Candidates ---")
        for candidate in candidates_data.get("results", [])[:5]:
            print(f"Name: {candidate.get('name')}, Party: {candidate.get('party_full')}")

        # Fetch committees
        committees_data = get_committees(API_KEY, cycle=2024, committee_type="P")
        print("\n--- Committees ---")
        for committee in committees_data.get("results", [])[:5]:
            print(f"Name: {committee.get('name')}, Type: {committee.get('committee_type_full')}")

        # Fetch contributions
        contributions_data = get_contributions(API_KEY, contributor_name="BIDEN, JOSEPH R JR")
        print("\n--- Contributions ---")
        for contribution in contributions_data.get("results", [])[:5]:
            print(f"Contributor: {contribution.get('contributor_name')}, Amount: {contribution.get('contribution_amount')}")

    except (ValueError, requests.exceptions.RequestException) as e:
        print(f"Error: {e}")
