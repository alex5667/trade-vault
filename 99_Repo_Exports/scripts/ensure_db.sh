#!/bin/bash
set -e

POSTGRES_CONTAINER="scanner-postgres"
DB_NAME="scanner_analytics"
CHECK_TABLE="trades_closed"

echo "⏳ Waiting for $POSTGRES_CONTAINER to be ready..."
until docker exec $POSTGRES_CONTAINER pg_isready -U postgres > /dev/null 2>&1; do
  echo -n "."
  sleep 1
done
echo " Postgres is UP!"

# Check if table exists in scanner_analytics
if docker exec -i $POSTGRES_CONTAINER psql -U trading -d $DB_NAME -tAc "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '$CHECK_TABLE');" | grep -q 't'; then
    echo "✅ Database $DB_NAME appears initialized ($CHECK_TABLE found). Skipping restore."
else
    echo "⚠️  Database $DB_NAME seems empty or missing $CHECK_TABLE."
    echo "🚀 Triggering auto-restore..."
    ./restore_db.sh
fi
