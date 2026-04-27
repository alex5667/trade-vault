#!/bin/bash
# Обертка для обучения и обновления ML модели до формата UtilMHModelV1
# Использует существующий скрипт ml_train_report_tb_v10_4.py
# 
# Примечание: Автоматический таймер ml-train-tb-v10-4-timer уже настроен
# и запускается ежедневно в 03:10-03:20. Этот скрипт нужен для:
# - Немедленного обучения (не дожидаясь таймера)
# - Автоматического копирования модели в production location
# - Создания backup перед заменой
#
# Использование: ./update_ml_model.sh [since-hours] [models-root]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Параметры по умолчанию
SINCE_HOURS=${1:-72}
MODELS_ROOT=${2:-/var/lib/trade/ml_models}
HORIZONS=${TB_HORIZONS_MS:-60000,180000,300000}
REDIS_URL=${REDIS_URL:-redis://redis-worker-1:6379/0}

echo "=== ML Model Update Script ==="
echo "Since hours: $SINCE_HOURS"
echo "Models root: $MODELS_ROOT"
echo "Horizons: $HORIZONS"
echo "Redis URL: $REDIS_URL"
echo ""

# Проверка наличия Python и необходимых модулей
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: python3 not found"
    exit 1
fi

# Проверка наличия скрипта обучения
TRAIN_SCRIPT="python-worker/tools/ml_train_report_tb_v10_4.py"
if [ ! -f "$TRAIN_SCRIPT" ]; then
    echo "❌ Error: Training script not found: $TRAIN_SCRIPT"
    exit 1
fi

echo "📊 Step 1: Training model with ml_train_report_tb_v10_4.py"
echo ""

# Запуск обучения модели
cd python-worker
python3 -m tools.ml_train_report_tb_v10_4 \
    --since-hours "$SINCE_HOURS" \
    --models-root "$MODELS_ROOT" \
    --horizons "$HORIZONS" \
    || {
    echo "❌ Error: Model training failed"
    exit 1
}

cd ..

echo ""
echo "✅ Model training completed"
echo ""
echo "📋 Step 2: Checking model format"
echo ""

# Найти последнюю обученную модель
LATEST_MODEL=$(find "$MODELS_ROOT" -name "model.joblib" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-)

if [ -z "$LATEST_MODEL" ]; then
    echo "❌ Error: No trained model found in $MODELS_ROOT"
    exit 1
fi

echo "Found model: $LATEST_MODEL"

# Проверка формата модели
python3 <<EOF
import sys
sys.path.insert(0, 'python-worker')
import joblib

model_path = "$LATEST_MODEL"
try:
    model = joblib.load(model_path)
    print(f"Model type: {type(model).__name__}")
    print(f"Has predict_util: {hasattr(model, 'predict_util')}")
    print(f"Has predict_unc: {hasattr(model, 'predict_unc')}")
    
    if hasattr(model, 'predict_util') and hasattr(model, 'predict_unc'):
        print("✅ Model format is correct (UtilMHModelV1)")
    else:
        print("❌ Model format is incorrect")
        sys.exit(1)
except Exception as e:
    print(f"❌ Error loading model: {e}")
    sys.exit(1)
EOF

if [ $? -ne 0 ]; then
    echo "❌ Model format validation failed"
    exit 1
fi

echo ""
echo "📋 Step 3: Updating model in production location"
echo ""

# Копирование модели в production location
PROD_MODEL_PATH="/var/lib/trade/of_reports/models/model.joblib"
PROD_MODEL_DIR=$(dirname "$PROD_MODEL_PATH")

# Создание директории если не существует
if [ ! -d "$PROD_MODEL_DIR" ]; then
    echo "Creating directory: $PROD_MODEL_DIR"
    sudo mkdir -p "$PROD_MODEL_DIR" || {
        echo "⚠️  Warning: Cannot create directory. You may need to run with sudo or copy manually."
    }
fi

# Резервная копия старой модели
if [ -f "$PROD_MODEL_PATH" ]; then
    BACKUP_PATH="${PROD_MODEL_PATH}.backup.$(date +%Y%m%d_%H%M%S)"
    echo "Creating backup: $BACKUP_PATH"
    sudo cp "$PROD_MODEL_PATH" "$BACKUP_PATH" || {
        echo "⚠️  Warning: Cannot create backup. You may need to run with sudo."
    }
fi

# Копирование новой модели
echo "Copying model to: $PROD_MODEL_PATH"
sudo cp "$LATEST_MODEL" "$PROD_MODEL_PATH" || {
    echo "⚠️  Warning: Cannot copy model. You may need to run with sudo or copy manually:"
    echo "   sudo cp $LATEST_MODEL $PROD_MODEL_PATH"
}

echo ""
echo "✅ Model update completed"
echo ""
echo "📋 Step 4: Model location: $PROD_MODEL_PATH"
echo "📋 Step 5: Please check Redis configuration and restart worker"
echo ""

