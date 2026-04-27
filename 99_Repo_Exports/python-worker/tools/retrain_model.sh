#!/bin/bash
set -e

# Configuration
REDIS_URL=${REDIS_URL:-"redis://redis-worker-1:6379/0"}
OUT_DIR="/tmp/ml_retrain_$(date +%Y%m%d_%H%M%S)"
DATASET_PARQUET="${OUT_DIR}/dataset.parquet"
MODEL_DIR="${OUT_DIR}/model"
INPUTS_NDJSON="${OUT_DIR}/inputs.ndjson"
TB_NDJSON="${OUT_DIR}/tb.ndjson"

echo "🆕 Starting ML Retraining Pipeline..."
mkdir -p "${MODEL_DIR}"

# 1. Export Data
echo "📥 Exporting signals:of:inputs..."
python3 python-worker/tools/export_stream_payload_ndjson_v1.py \
    --stream "signals:of:inputs" \
    --payload-field "payload" \
    --since-hours 168 \
    --out "${INPUTS_NDJSON}" \
    --redis-url "${REDIS_URL}"

echo "📥 Exporting labels:tb..."
python3 python-worker/tools/export_stream_payload_ndjson_v1.py \
    --stream "labels:tb" \
    --payload-field "payload" \
    --since-hours 168 \
    --out "${TB_NDJSON}" \
    --redis-url "${REDIS_URL}"

# 2. Build Dataset
echo "🏗️ Building Parquet dataset..."
python3 python-worker/tools/build_dataset_from_inputs_tb_labels_v3_mh.py \
    --inputs "${INPUTS_NDJSON}" \
    --tb "${TB_NDJSON}" \
    --out "${DATASET_PARQUET}"

# 3. Train Model
echo "🧠 Training Model..."
python3 python-worker/tools/train_ml_confirm_tb_util_mh_v1.py \
    --dataset "${DATASET_PARQUET}" \
    --out-dir "${MODEL_DIR}"

echo "✅ Retraining complete. Model saved in ${MODEL_DIR}"
echo "🚀 Next step: Update Redis configuration with new model path."
