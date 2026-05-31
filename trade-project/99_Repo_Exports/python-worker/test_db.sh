#!/bin/bash

echo "Testing scanner_analytics database..."

# Test connection and show tables
docker exec scanner-postgres psql -U postgres -d scanner_analytics -c "
SELECT 'Connection successful!' as status;
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;"

# Test trades_closed structure
echo ""
echo "Testing trades_closed table structure:"
docker exec scanner-postgres psql -U postgres -d scanner_analytics -c "
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'trades_closed'
ORDER BY ordinal_position
LIMIT 15;"

echo ""
echo "✅ Database test completed!"
