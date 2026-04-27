#!/usr/bin/env bash
export AUTO_EXECUTE=1
cd /home/alex/front/trade/scanner_infra

echo
echo "=============== D5: ML Config Missing ==============="
bash drills/d5_ml_config_missing.sh

echo
echo "=============== D6: Postgres Down ==============="
bash drills/d6_postgres_down.sh

echo
echo "=============== D2: Go Worker Crash ==============="
bash drills/d2_go_worker_crash.sh

echo
echo "=============== D4: Stale Order Book ==============="
bash drills/d4_stale_order_book.sh

echo
echo "=============== Fixing user stream gate (Gap 1) ==============="
sed -i 's/EXEC_BOOTSTRAP_REQUIRE_USER_STREAM_READY: "0"/EXEC_BOOTSTRAP_REQUIRE_USER_STREAM_READY: "1"/g' compose-config.yaml
sed -i 's/EXEC_BOOTSTRAP_REQUIRE_USER_STREAM_READY=0/EXEC_BOOTSTRAP_REQUIRE_USER_STREAM_READY=1/g' .env
docker compose -f compose-config.yaml up -d
echo "Waiting 30s for compose to apply..."
sleep 30

echo
echo "=============== D3: User Stream Disconnect ==============="
bash drills/d3_user_stream_disconnect.sh

echo
echo "=============== D1: Redis Restart ==============="
bash drills/d1_redis_worker_restart.sh

echo
echo "=============== ALL DRILLS COMPLETED ==============="
