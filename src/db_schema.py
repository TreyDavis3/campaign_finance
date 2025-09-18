
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
        CREATE TABLE candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            party VARCHAR(255),
            state VARCHAR(2),
            office VARCHAR(255),
            election_year INTEGER
        )
        """,
        """
        CREATE TABLE committees (
            committee_id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            city VARCHAR(255),
            state VARCHAR(2),
            treasurer_name VARCHAR(255),
            committee_type VARCHAR(255)
        )
        """,
        """
        CREATE TABLE contributions (
            contribution_id SERIAL PRIMARY KEY,
            committee_id VARCHAR(255) REFERENCES committees(committee_id),
            contributor_name VARCHAR(255),
            contributor_city VARCHAR(255),
            contributor_state VARCHAR(2),
            contributor_zip_code VARCHAR(255),
            contribution_date DATE,
            contribution_amount NUMERIC(12, 2),
            contributor_occupation VARCHAR(255),
            contributor_employer VARCHAR(255)
        )
        """
    )
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    create_tables()
