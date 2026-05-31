#!/bin/bash

echo "🕐 Запуск планировщика калибровки (сессионное расписание)"
echo "   Азия: 02:10 | Европа: 10:10 | США: 18:10 (Europe/Kyiv)"
echo ""

run_calibration() {
  echo ""
  echo "🎯 Запуск калибровки уверенности сигналов: $(date)"
  cd /app && python scripts/run_local_calibration.py
  echo "✅ Калибровка завершена: $(date)"

  echo ""
  echo "🏆 Запуск менеджера промоушена: $(date)"
  export PYTHONPATH=$PYTHONPATH:/app/python-worker
  
  # Прямая генерация кандидата в confidence_calibration.json через Redis (V2)
  echo ">>> Генерация кандидата калибровки (Redis dataset builder)..."
  TIMESTAMP=$(date +%s)
  python3 -m ml_analysis.tools.build_edge_stack_dataset_from_redis \
    --redis_url "${REDIS_URL:-redis://redis-worker-1:6379/0}" \
    --out_jsonl /app/calibration/dataset.jsonl \
    --out_quarantine_jsonl /app/calibration/quarantine.jsonl \
    --out_report_json /app/calibration/dataset_report.json \
    --signals_count 200000 --closes_count 200000 \
    --since_ms $(( (TIMESTAMP - 7 * 86400) * 1000 )) \
    --until_ms $(( TIMESTAMP * 1000 )) \
    --y_min_r 0.10

  python3 -m ml_analysis.tools.train_confidence_calibrator_v2 \
    --in_jsonl /app/calibration/dataset.jsonl \
    --out_bundle /app/calibration/confidence_calibration.json \
    --key confidence_v1 \
    --method auto \
    --min_rows 400 \
    --hierarchical 1

  export CONF_CAL_CANDIDATE_BUNDLE_PATH=/app/calibration/confidence_calibration.json
  export CONF_CAL_CHAMPION_BUNDLE_PATH=/app/calibration/confidence_calibration_champion.json
  export CONF_CAL_PROOF_STATE_PATH=/app/calibration/conf_cal_proof_state.json
  export CONF_CAL_PROMOTION_STATUS_PATH=/app/calibration/conf_cal_promo_status.json
  python3 -m ml_analysis.tools.conf_cal_promotion_manager_v1 --variant v1
  echo "✅ Промоушен завершен: $(date)"
}

echo "⏳ Ожидание запуска системы..."
sleep 600

echo "🚀 Запуск первой калибровки сразу"
run_calibration

echo "🔄 Запуск планировщика по сессиям..."
while true; do
  CURRENT_HOUR=$(date -u +%-H)
  CURRENT_MIN=$(date -u +%-M)
  KYIV_HOUR=$(( (CURRENT_HOUR + 2) % 24 ))
  KYIV_MIN=$CURRENT_MIN

  if [ "$KYIV_HOUR" = "2" ] && [ "$KYIV_MIN" = "10" ]; then
    echo "🌏 Запуск калибровки по Азиатской сессии"
    run_calibration
  elif [ "$KYIV_HOUR" = "10" ] && [ "$KYIV_MIN" = "10" ]; then
    echo "🇪🇺 Запуск калибровки по Европейской сессии"
    run_calibration
  elif [ "$KYIV_HOUR" = "18" ] && [ "$KYIV_MIN" = "10" ]; then
    echo "🇺🇸 Запуск калибровки по Американской сессии"
    run_calibration
  fi

  sleep 60
done
