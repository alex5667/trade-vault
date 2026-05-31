#!/bin/bash
set -e

# Apply to python-worker (ignoring tick_flow_full which is for reference)
cd /home/alex/front/trade/scanner_infra/python-worker
patch -p1 --no-backup-if-mismatch -f < ../mega_patch_C1_prom_metrics_bookseq_dq_v3.git.diff || true

# Move test if created in wrong place
if [ -d "orderflow_services/tests" ]; then
    mkdir -p tests/orderflow_services
    mv orderflow_services/tests/test_metrics_bookseq_dq_p112_v1.py tests/orderflow_services/ || true
fi

# Apply to reference
cd /home/alex/front/trade/scanner_infra/reference
patch -p1 --no-backup-if-mismatch -f < ../mega_patch_C1_prom_metrics_bookseq_dq_v3.git.diff || true

