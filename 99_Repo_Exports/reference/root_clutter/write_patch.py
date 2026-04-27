patch = r"""diff --git a/monitoring/alertmanager/telegram_webhook/app.py b/monitoring/alertmanager/telegram_webhook/app.py
index caa3c7c..59e5d0f 100644
--- a/monitoring/alertmanager/telegram_webhook/app.py
+++ b/monitoring/alertmanager/telegram_webhook/app.py
@@ -19,6 +19,12 @@ log = logging.getLogger("alertmanager-telegram-webhook")
 BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
 DEFAULT_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
 DEFAULT_THREAD_ID = os.getenv("TELEGRAM_MESSAGE_THREAD_ID", "")
+
+# Public base URLs (so links in Telegram work from phone/remote)
+RUNBOOKS_BASE_URL = (os.getenv("RUNBOOKS_BASE_URL") or "").strip()
+GRAFANA_BASE_URL = (os.getenv("GRAFANA_BASE_URL") or "").strip()
+ALERTMANAGER_BASE_URL = (os.getenv("ALERTMANAGER_BASE_URL") or "").strip()
 
 # Anti-spam controls
 DEDUP_TTL_S = int(os.getenv("ALERT_DEDUPE_TTL_S", "180"))
 RATE_LIMIT_PER_MIN = int(os.getenv("ALERT_RATE_LIMIT_PER_MIN", "30"))
@@ -104,6 +110,20 @@ def _select_chat(common_labels: Dict[str, Any]) -> Tuple[str, str]:
     chat_id = str((cfg or {}).get("chat_id") or DEFAULT_CHAT_ID)
     thread_id = str((cfg or {}).get("thread_id") or DEFAULT_THREAD_ID)
     return chat_id, thread_id
 
+def _join_url(base: str, path: str) -> str:
+    base = (base or "").strip()
+    path = (path or "").strip()
+    if not base or not path:
+        return ""
+    if path.startswith("http://") or path.startswith("https://"):
+        return path
+    if not path.startswith("/"):
+        path = "/" + path
+    return base.rstrip("/") + path
+
 
 def _fmt_alert_line(a: Dict[str, Any]) -> str:
     labels = a.get("labels") or {}
     ann = a.get("annotations") or {}
@@ -172,16 +211,35 @@ def _build_message(payload: Dict[str, Any]) -> str:
         lines.append(rb_short)
 
     # include runbook/dashboard links when present
-    link_candidates = [
-        common_annotations.get("runbook_url"),
-        common_annotations.get("dashboard"),
-        common_annotations.get("grafana"),
-    ]
-    link_candidates = [x for x in link_candidates if x]
-    if link_candidates:
-        lines.append("Links:")
-        for l in link_candidates[:3]:
-            lines.append(f"- {l}")
+    links: List[str] = []
+    # Prefer explicit URLs, otherwise construct from *_path using public base URLs.
+    runbook_url = str(common_annotations.get("runbook_url") or "").strip()
+    runbook_path = str(common_annotations.get("runbook_path") or "").strip()
+    dash_url = str(common_annotations.get("dashboard") or common_annotations.get("grafana") or "").strip()
+    dash_path = str(common_annotations.get("dashboard_path") or "").strip()
+
+    if runbook_url:
+        links.append(runbook_url)
+    elif runbook_path and RUNBOOKS_BASE_URL:
+        u = _join_url(RUNBOOKS_BASE_URL, runbook_path)
+        if u:
+            links.append(u)
+
+    if dash_url:
+        links.append(dash_url)
+    elif dash_path and GRAFANA_BASE_URL:
+        u = _join_url(GRAFANA_BASE_URL, dash_path)
+        if u:
+            links.append(u)
+
+    if ALERTMANAGER_BASE_URL:
+        links.append(_join_url(ALERTMANAGER_BASE_URL, "/#/silences/new"))
+
+    if links:
+        lines.append("Links:")
+        for l in links[:4]:
+            lines.append(f"- {l}")
 
     # timestamp
     lines.append(f"ts={int(time.time())}")
diff --git a/ok_rate_logic/docker-compose-crypto-orderflow.yml b/ok_rate_logic/docker-compose-crypto-orderflow.yml
index 9f1d3a2..ad0aa56 100644
--- a/ok_rate_logic/docker-compose-crypto-orderflow.yml
+++ b/ok_rate_logic/docker-compose-crypto-orderflow.yml
@@ -1057,6 +1057,11 @@ services:
       - TELEGRAM_MESSAGE_THREAD_ID=${TELEGRAM_MESSAGE_THREAD_ID:-}
       - WEBHOOK_PORT=8081
       - LOG_LEVEL=${ALERT_WEBHOOK_LOG_LEVEL:-INFO}
+      # Public URLs for clickable links in Telegram (set to your domain/https)
+      - RUNBOOKS_BASE_URL=${RUNBOOKS_BASE_URL:-http://127.0.0.1:8082}
+      - GRAFANA_BASE_URL=${GRAFANA_BASE_URL:-http://127.0.0.1:3000}
+      - ALERTMANAGER_BASE_URL=${ALERTMANAGER_BASE_URL:-http://127.0.0.1:9093}
       - PYTHONUNBUFFERED=1
     ports:
       - "${ALERT_WEBHOOK_PORT_HOST:-8081}:8081"
diff --git a/ok_rate_logic/prometheus_alerts_edge_stack_train_p59.yml b/ok_rate_logic/prometheus_alerts_edge_stack_train_p59.yml
index 1ed6d4f..7b8fb2c 100644
--- a/ok_rate_logic/prometheus_alerts_edge_stack_train_p59.yml
+++ b/ok_rate_logic/prometheus_alerts_edge_stack_train_p59.yml
@@ -16,6 +16,7 @@ groups:
         annotations:
           summary: "edge_stack_v1 training failed"
           description: "Last edge_stack_v1 nightly bundle status is not ok. Check tools.nightly_edge_stack_v1_train_bundle logs and metrics:edge_stack_train:last in Redis."
+          runbook_path: "/edge_stack_train_p59.md"
           runbook: |
             1) Check timers worker logs for nightly bundle failures (edge_stack_v1).
             2) Inspect Redis hash metrics:edge_stack_train:last (success/reason/feature_cols_hash/schema_hash).
@@ -23,7 +24,7 @@ groups:
             3) Verify Prometheus targets: edge-stack-train-exporter-p59 is UP.
             4) If feature hash mismatch: rebuild dataset with pinned --feature_schema_ver and retrain.
-          dashboard: "Grafana: http://127.0.0.1:3000/d/edge_stack_overview/edge-stack-overview?orgId=1"
+          dashboard_path: "/d/edge_stack_overview/edge-stack-overview?orgId=1"
 
       - alert: EdgeStackTrainStale
         expr: (time() * 1000 - edge_stack_train_last_updated_ts_ms) > 36 * 3600 * 1000
@@ -39,6 +40,7 @@ groups:
         annotations:
           summary: "edge_stack_v1 training stale (>36h)"
           description: "No successful edge_stack_v1 training metrics update in >36h. Check timers worker and archiver retention."
+          runbook_path: "/edge_stack_train_p59.md"
           runbook: |
             1) Confirm timers schedule ran (systemd/docker timers).
             2) Check Redis connectivity + stream retention (dataset window needs enough samples).
@@ -46,7 +48,7 @@ groups:
             3) Ensure exporters are UP and Prometheus scraping works.
-          dashboard: "Grafana: http://127.0.0.1:3000/d/edge_stack_overview/edge-stack-overview?orgId=1"
+          dashboard_path: "/d/edge_stack_overview/edge-stack-overview?orgId=1"
 
       - alert: EdgeStackTrainQualityDegraded
         expr: (edge_stack_train_last_oof_meta_brier > 0.30) or (edge_stack_train_last_oof_meta_ece > 0.08)
@@ -56,6 +58,7 @@ groups:
         annotations:
           summary: "edge_stack_v1 OOF quality degraded"
           description: "OOF meta brier/ece exceed thresholds. Investigate dataset drift, label join, and DQ/Drift gating regimes."
+          runbook_path: "/edge_stack_train_p59.md"
           runbook: |
             1) Compare candidate vs champion in P60 shadow eval (brier/ece).
             2) Check joined/pos_rate; if too low/high, tune window/filters.
@@ -63,7 +66,7 @@ groups:
             3) Inspect drift: distributions for ofi_z/spread/mp_mid_bps.
-          dashboard: "Grafana: http://127.0.0.1:3000/d/edge_stack_overview/edge-stack-overview?orgId=1"
+          dashboard_path: "/d/edge_stack_overview/edge-stack-overview?orgId=1"
diff --git a/ok_rate_logic/prometheus_alerts_edge_stack_shadow_p60.yml b/ok_rate_logic/prometheus_alerts_edge_stack_shadow_p60.yml
index bd6d79a..d0b70bd 100644
--- a/ok_rate_logic/prometheus_alerts_edge_stack_shadow_p60.yml
+++ b/ok_rate_logic/prometheus_alerts_edge_stack_shadow_p60.yml
@@ -12,6 +12,7 @@ groups:
     annotations:
       summary: "Edge Stack Shadow Eval failed"
       description: "The nightly shadow evaluation for edge_stack_v1 failed. Check logs."
+      runbook_path: "/edge_stack_shadow_p60.md"
       runbook: |
         1) Check tools.edge_stack_shadow_eval_bundle_v1 logs.
         2) Verify shadow_status.json exists and is recent.
@@ -19,7 +20,7 @@ groups:
         3) Verify exporters are UP (P60: edge-stack-shadow-exporter-p60).
-      dashboard: "Grafana: http://127.0.0.1:3000/d/edge_stack_overview/edge-stack-overview?orgId=1"
+      dashboard_path: "/d/edge_stack_overview/edge-stack-overview?orgId=1"
 
   - alert: EdgeStackShadowEvalStale
     expr: (time() * 1000 - edge_stack_shadow_last_updated_ts_ms) > 93600000
@@ -29,6 +30,7 @@ groups:
     annotations:
       summary: "Edge Stack Shadow Eval stale"
       description: "Shadow eval has not updated in >26 hours. Timer might be broken."
+      runbook_path: "/edge_stack_shadow_p60.md"
       runbook: |
         1) Validate timers schedule & that shadow bundle runs daily.
         2) Check report path mount: OF_REPORTS_DIR -> /var/lib/trade/of_reports.
@@ -36,7 +38,7 @@ groups:
-      dashboard: "Grafana: http://127.0.0.1:3000/d/edge_stack_overview/edge-stack-overview?orgId=1"
+      dashboard_path: "/d/edge_stack_overview/edge-stack-overview?orgId=1"
 
   - alert: EdgeStackChampionQualityDegraded
     expr: edge_stack_shadow_champion_brier > 0.25
@@ -44,6 +46,7 @@ groups:
     annotations:
       summary: "Edge Stack Champion Brier high"
       description: "Champion model Brier score > 0.25 on shadow dataset."
+      runbook_path: "/edge_stack_shadow_p60.md"
       runbook: |
         1) Compare candidate vs champion; if candidate better and stable, allow guarded promote.
         2) Investigate data drift/label join quality.
@@ -51,7 +54,7 @@ groups:
-      dashboard: "Grafana: http://127.0.0.1:3000/d/edge_stack_overview/edge-stack-overview?orgId=1"
+      dashboard_path: "/d/edge_stack_overview/edge-stack-overview?orgId=1"
diff --git a/monitoring/runbooks/edge_stack_train_p59.md b/monitoring/runbooks/edge_stack_train_p59.md
index f789cc6..c8b4e6d 100644
--- a/monitoring/runbooks/edge_stack_train_p59.md
+++ b/monitoring/runbooks/edge_stack_train_p59.md
@@ -1,6 +1,20 @@
 # Runbook: Edge Stack Train (P59)
 
+## Owner
+- team: **trade**
+- component: **edge_stack**
+- contact: (add your Telegram/Slack channel here)
+
+## One-command rollback (safe)
+- Disable promotions (candidate-only):
+  - set `EDGE_STACK_AUTO_PROMOTE=0`
+  - restart timers worker / nightly
+
+## How to silence
+- Open Alertmanager → Silences → New, match:
+  - `team="trade"`, `component="edge_stack"`, (optional) `alertname="EdgeStackTrainFailed"`
+
 ## Symptoms
 - Alert: `EdgeStackTrainFailed`
 - Alert: `EdgeStackTrainStale`
@@ -35,6 +49,10 @@ Fix:
 - Compare candidate vs champion via P60 shadow.
 - If candidate is consistently better, enable guarded promote (P60).
 
 ## Escalation
 - If failures persist for 2+ nights: freeze auto-promote and pin to last known good champion.
+
+## Links
+- Grafana dashboard: `/d/edge_stack_overview/edge-stack-overview?orgId=1`
diff --git a/monitoring/runbooks/edge_stack_shadow_p60.md b/monitoring/runbooks/edge_stack_shadow_p60.md
index 9eac7d4..a0a4e26 100644
--- a/monitoring/runbooks/edge_stack_shadow_p60.md
+++ b/monitoring/runbooks/edge_stack_shadow_p60.md
@@ -1,6 +1,18 @@
 # Runbook: Edge Stack Shadow Eval (P60)
 
+## Owner
+- team: **trade**
+- component: **edge_stack**
+- contact: (add your Telegram/Slack channel here)
+
+## How to silence
+- Alertmanager → Silences → New, match:
+  - `team="trade"`, `component="edge_stack"`, (optional) `alertname="EdgeStackShadowEvalFailed"`
+
 ## Symptoms
 - Alert: `EdgeStackShadowEvalFailed`
 - Alert: `EdgeStackShadowEvalStale`
@@ -39,3 +51,7 @@ Run:
 `python -m tools.edge_stack_shadow_eval_bundle_v1 --window_hours 24 --auto_promote_guarded 1`
+
+## Links
+- Grafana dashboard: `/d/edge_stack_overview/edge-stack-overview?orgId=1`
diff --git a/monitoring/README_alertmanager_telegram.md b/monitoring/README_alertmanager_telegram.md
index d1faeab..e726d3c 100644
--- a/monitoring/README_alertmanager_telegram.md
+++ b/monitoring/README_alertmanager_telegram.md
@@ -32,6 +32,12 @@ export TELEGRAM_ROUTING_JSON='{
 }'
+Public URLs for phone-friendly links:
+export RUNBOOKS_BASE_URL="https://YOUR_DOMAIN/runbooks"
+export GRAFANA_BASE_URL="https://YOUR_DOMAIN/grafana"
+export ALERTMANAGER_BASE_URL="https://YOUR_DOMAIN/alertmanager"
+
diff --git a/scripts/send_test_alert_to_alertmanager.sh b/scripts/send_test_alert_to_alertmanager.sh
index 254433a..9b6ff7d 100755
--- a/scripts/send_test_alert_to_alertmanager.sh
+++ b/scripts/send_test_alert_to_alertmanager.sh
@@ -30,9 +30,11 @@ payload=$(cat <<JSON
    },
    "annotations": {
      "summary": "Manual test alert injected via Alertmanager API",
-      "description": "If you see this in Telegram, Alertmanager->webhook->Telegram is working."
+      "description": "If you see this in Telegram, Alertmanager->webhook->Telegram is working.",
+      "runbook_path": "/edge_stack_train_p59.md",
+      "dashboard_path": "/d/edge_stack_overview/edge-stack-overview?orgId=1"
    },
    "startsAt": "${NOW}"
  }
]
JSON
)
"""
patch = patch.replace('a/ok_rate_logic/docker-compose-crypto-orderflow.yml', 'a/docker-compose-crypto-orderflow.yml')
patch = patch.replace('b/ok_rate_logic/docker-compose-crypto-orderflow.yml', 'b/docker-compose-crypto-orderflow.yml')
patch = patch.replace('a/ok_rate_logic/prometheus_alerts_edge_stack_train_p59.yml', 'a/orderflow_services/prometheus_alerts_edge_stack_train_p59.yml')
patch = patch.replace('b/ok_rate_logic/prometheus_alerts_edge_stack_train_p59.yml', 'b/orderflow_services/prometheus_alerts_edge_stack_train_p59.yml')
patch = patch.replace('a/ok_rate_logic/prometheus_alerts_edge_stack_shadow_p60.yml', 'a/orderflow_services/prometheus_alerts_edge_stack_shadow_p60.yml')
patch = patch.replace('b/ok_rate_logic/prometheus_alerts_edge_stack_shadow_p60.yml', 'b/orderflow_services/prometheus_alerts_edge_stack_shadow_p60.yml')
open('runbookpatch.diff', 'w').write(patch)
