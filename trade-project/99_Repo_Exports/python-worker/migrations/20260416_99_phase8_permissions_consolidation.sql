-- Phase 8 Permissions Consolidation
-- Grants ALL privileges to the 'trading' user on all Phase 8 tables, views and sequences.
-- This catch-up migration resolves 'permission denied' errors after Phase 8 deployment.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trading') THEN
        -- 1. Catch up on existing objects in public schema
        GRANT ALL ON SCHEMA public TO trading;
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trading;
        GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trading;
        GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO trading;

        -- 1.1. Explicitly grant permissions on TimescaleDB internal chunks to avoid warnings/errors
        GRANT USAGE ON SCHEMA _timescaledb_internal TO trading;
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA _timescaledb_internal TO trading;
        GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA _timescaledb_internal TO trading;
        GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA _timescaledb_internal TO trading;

        -- 2. Set default privileges for ALL future objects created by ANY user in this schema
        -- This ensures that if 'postgres' or 'admin' creates a table, 'trading' can access it.
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO trading;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO trading;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO trading;

        ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_internal GRANT ALL ON TABLES TO trading;
        ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_internal GRANT ALL ON SEQUENCES TO trading;
        ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_internal GRANT ALL ON FUNCTIONS TO trading;
        
        RAISE NOTICE 'Permissions granted to trading user successfully.';
    ELSE
        RAISE NOTICE 'User trading does not exist, skipping grants.';
    END IF;
END $$;

SELECT 'Phase 8 permissions consolidated' as status;
