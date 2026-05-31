-- Phase 2.14 does not require new Postgres tables
-- It relies strictly on Redis Hashes for policy and Streams for audit.
-- This file exists for deployment compatibility.
SELECT 1;
