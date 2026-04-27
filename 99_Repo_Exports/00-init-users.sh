#!/bin/bash
set -e

# Initialize PostgreSQL users safely using environment variables instead of hardcoded passwords

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
-- Create or update the 'trading' user with password
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'trading') THEN
        CREATE USER trading WITH PASSWORD '${TRADING_PASSWORD}';
    ELSE
        EXECUTE format('ALTER USER trading WITH PASSWORD %L', '${TRADING_PASSWORD}');
    END IF;
END $$;

-- Create or update the 'scanner' user with password
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'scanner') THEN
        CREATE USER scanner WITH PASSWORD '${SCANNER_PASSWORD}';
    ELSE
        EXECUTE format('ALTER USER scanner WITH PASSWORD %L', '${SCANNER_PASSWORD}');
    END IF;
END $$;

-- Create or update the 'trade_user' with project-standard password
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'trade_user') THEN
        CREATE USER trade_user WITH PASSWORD '${TRADE_USER_PASSWORD}';
    ELSE
        EXECUTE format('ALTER USER trade_user WITH PASSWORD %L', '${TRADE_USER_PASSWORD}');
    END IF;
END $$;
EOSQL

