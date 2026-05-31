-- Migration 20260523_03: Fix archive_metadata last_error
-- Purpose: Set default value to 'none' to prevent 0% fill rates in analytics

ALTER TABLE archive_metadata ALTER COLUMN last_error SET DEFAULT 'none';

UPDATE archive_metadata SET last_error = 'none' WHERE last_error IS NULL;
