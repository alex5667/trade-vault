#!/bin/bash
set -eo pipefail

echo "================================================="
echo "    Starting Nightly Models Sequential Run v2    "
echo "    $(date -u '+%Y-%m-%d %H:%M:%S UTC')          "
echo "================================================="

INTERVAL=${INTERVAL:-60}

run_step() {
  local step_num=$1
  local container=$2
  local cmd=$3
  local desc=$4
  echo ""
  echo "==========================================="
  echo "  Step ${step_num}: ${desc}"
  echo "  Container: ${container}"
  echo "  Time: $(date -u '+%H:%M:%S UTC')"
  echo "==========================================="
  set +e
  docker exec "$container" sh -c "$cmd" 2>&1
  local exit_code=$?
  set -e
  if [ $exit_code -ne 0 ]; then
    echo "⚠️ WARN: Step ${step_num} returned exit code $exit_code"
  else
    echo "✅ Step ${step_num} OK"
  fi
  echo "Sleeping ${INTERVAL}s before next step..."
  sleep "$INTERVAL"
}

# ═══════════════════════════════════════════════════════
# Phase 0: Build prerequisite datasets
# ═══════════════════════════════════════════════════════
run_step 0 "scanner-confirm-train-v7-builder-timer" \
  'python3 -u -m ml_analysis.tools.build_confirm_train_v7_from_redis --out_decisions "$CONFIRM_V7_OUT_DECISIONS" --out_outcomes "$CONFIRM_V7_OUT_OUTCOMES"' \
  "Build confirm_train_v7 dataset (prerequisite for meta models)"

# ═══════════════════════════════════════════════════════
# Phase 1: Edge Stack training (GPU-heavy, sequential)
# ═══════════════════════════════════════════════════════
run_step 1 "scanner-ml-nightly-edge-stack-train-bundle" \
  'python3 -m ml_analysis.tools.nightly_edge_stack_v1_train_bundle' \
  "Edge Stack v10_of nightly training"

run_step 2 "scanner-ml-nightly-edge-stack-train-bundle-v13" \
  'python3 -m ml_analysis.tools.nightly_edge_stack_v1_train_bundle' \
  "Edge Stack v13_of nightly training"

# ═══════════════════════════════════════════════════════
# Phase 2: Feature selection
# ═══════════════════════════════════════════════════════
run_step 3 "scanner-ml-nightly-feature-selection-bundle" \
  'python3 -m ml_analysis.tools.nightly_feature_selection_loop_bundle_v1' \
  "Nightly feature selection loop"

# ═══════════════════════════════════════════════════════
# Phase 3: Meta model LR training (depends on v7 dataset)
# ═══════════════════════════════════════════════════════
run_step 4 "scanner-train-meta-model-lr-v1-timer" \
  'python3 -m ml_analysis.tools.train_meta_model_lr_v1 --parquet "$META_TRAIN_NDJSON" --schema meta_feat_v8 --y-col y_util_pos_60000 --out "$META_MODEL_OUT_DIR/meta_model_lr_v8.json"' \
  "Meta Model LR v1 training (schema v8)"

run_step 5 "scanner-train-meta-model-lr-v9-timer" \
  'python3 -m ml_analysis.tools.train_meta_model_lr_v1 --parquet "$META_TRAIN_NDJSON" --schema meta_feat_v9 --y-col y_util_pos_60000 --out "$META_MODEL_OUT_DIR/meta_model_lr_v9.json"' \
  "Meta Model LR v9 training"

# ═══════════════════════════════════════════════════════
# Phase 4: Confidence calibration
# ═══════════════════════════════════════════════════════
run_step 6 "scanner-auto-train-conf-calibration-v2-timer" \
  'python3 -m ml_analysis.tools.nightly_confidence_calibrator_bundle_v2 --lookback_days ${CONF_CAL_V2_DAYS:-60} --method ${CONF_CAL_V2_METHOD:-platt} --out_dir /app/calibration --champion_name confidence_calibration_v2.json' \
  "Confidence Calibrator V2"

# ═══════════════════════════════════════════════════════
# Phase 5: Conf score weight tuning
# ═══════════════════════════════════════════════════════
run_step 7 "scanner-conf-score-tuning-nightly-timer" \
  'python3 -u orderflow_services/nightly_conf_score_weight_tuning_bundle_v1.py' \
  "Confidence Score Weight Tuning"

# ═══════════════════════════════════════════════════════
# Phase 6: Meta skew guard + A/B evaluators (depend on v7 dataset)
# ═══════════════════════════════════════════════════════
run_step 8 "scanner-meta-skew-guard-nightly-timer" \
  'python3 -u tools/nightly_meta_skew_guard_v7.py --train-ndjson "$META_TRAIN_NDJSON" --out-dir "$META_SKEW_OUT_DIR" --freeze-json "$META_FREEZE_FILE"' \
  "Meta Skew Guard v7"

run_step 9 "scanner-meta-ab-winner-evaluator-v1" \
  'python3 -u tools/meta_ab_winner_evaluator_v1.py --dataset "$META_TRAIN_NDJSON" --model-champion "$META_MODEL_CHAMPION_PATH" --model-challenger "$META_MODEL_CHALLENGER_PATH" --p-min "$META_P_MIN" --min-delta-exp-r "$META_MIN_DELTA_EXP_R" --tail-slack "$META_TAIL_SLACK" --ramp-step "$META_RAMP_STEP" --share-max "$META_SHARE_MAX" --apply' \
  "Meta A/B Winner Evaluator v1"

run_step 10 "scanner-meta-ab-winner-evaluator-v2" \
  'python3 -u -m tools.meta_ab_winner_evaluator_v2 --in-ndjson "$META_TRAIN_NDJSON" --champion-model "$META_MODEL_CHAMPION_PATH" --challenger-model "$META_MODEL_CHALLENGER_PATH" --p-min "$META_P_MIN" --min-n "$META_AB_MIN_ELIGIBLE" --min-delta-exp-r "$META_AB_MIN_DELTA_EXPR" --tail-r "$META_AB_TAIL_R" --tail-slack "$META_AB_TAIL_SLACK" --bootstrap "$META_AB_BOOTSTRAP" --boot-n "$META_AB_BOOT_N" --strata "$META_AB_STRATA" --strata-topk "$META_AB_STRATA_TOPK" --ramp-step "$META_AB_RAMP_STEP" --max-share "$META_AB_MAX_SHARE" --current-share "$META_AB_CURRENT_SHARE" --apply "$META_AB_APPLY"' \
  "Meta A/B Winner Evaluator v2 (CI + strata)"

# ═══════════════════════════════════════════════════════
# Phase 7: ML All Models Report (summary of all models)
# ═══════════════════════════════════════════════════════
run_step 11 "scanner-ml-nightly-edge-stack-train-bundle" \
  'python3 -u -m tools.ml_all_models_report --meta-dir /var/lib/trade/models --cal-dir /app/calibration --send-telegram 1' \
  "ML All Models Report (comprehensive Telegram summary)"

echo ""
echo "================================================="
echo "    All Nightly Models Execution Finished        "
echo "    $(date -u '+%Y-%m-%d %H:%M:%S UTC')          "
echo "================================================="
