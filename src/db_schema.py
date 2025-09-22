
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Database connection parameters
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def create_tables():
    """ Create tables in the PostgreSQL database """
    commands = (
        """
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            party VARCHAR(255),
            state VARCHAR(2),
            office VARCHAR(255),
            election_year INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS committees (
            committee_id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            city VARCHAR(255),
            state VARCHAR(2),
            treasurer_name VARCHAR(255),
            committee_type VARCHAR(255)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS contributors (
            contributor_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            city VARCHAR(255),
            state VARCHAR(2),
            zip_code VARCHAR(255),
            occupation VARCHAR(255),
            employer VARCHAR(255),
            UNIQUE(name, city, state, zip_code, occupation, employer)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS contributions (
            contribution_id SERIAL PRIMARY KEY,
            committee_id VARCHAR(255) REFERENCES committees(committee_id),
            contributor_id INTEGER REFERENCES contributors(contributor_id),
            contribution_date DATE,
            contribution_amount NUMERIC(12, 2)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS candidate_committees (
            candidate_id VARCHAR(255) REFERENCES candidates(candidate_id),
            committee_id VARCHAR(255) REFERENCES committees(committee_id),
            PRIMARY KEY (candidate_id, committee_id)
        )
        """
    )
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        with conn:
            with conn.cursor() as cur:
                # create table one by one
                for command in commands:
                    cur.execute(command)
        print("Tables created successfully (if they did not exist).")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

if __name__ == '__main__':
    create_tables()
