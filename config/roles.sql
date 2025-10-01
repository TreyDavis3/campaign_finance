-- This script creates three database roles with the minimum necessary privileges.
-- Replace 'your_secure_password_here' with a strong, unique password for each role.
-- Replace 'campaign_finance' with your actual database name if it is different.

-- Create the roles
CREATE ROLE schema_creator WITH LOGIN PASSWORD 'your_secure_password_here';
CREATE ROLE etl_user WITH LOGIN PASSWORD 'your_secure_password_here';
CREATE ROLE dashboard_user WITH LOGIN PASSWORD 'your_secure_password_here';

-- Grant connect access to the database
GRANT CONNECT ON DATABASE campaign_finance TO schema_creator;
GRANT CONNECT ON DATABASE campaign_finance TO etl_user;
GRANT CONNECT ON DATABASE campaign_finance TO dashboard_user;

-- Grant permissions to the schema_creator
-- This role needs to be able to create tables in the public schema
GRANT CREATE ON SCHEMA public TO schema_creator;

-- Grant permissions to the etl_user
-- This role needs to be able to read and write to the tables
GRANT USAGE ON SCHEMA public TO etl_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO etl_user;
-- We also need to grant future permissions for any new tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO etl_user;
-- The etl_user also needs to use the sequences for the serial primary keys
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO etl_user;


-- Grant permissions to the dashboard_user
-- This role only needs to read from the tables
GRANT USAGE ON SCHEMA public TO dashboard_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dashboard_user;
-- We also need to grant future permissions for any new tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dashboard_user;
