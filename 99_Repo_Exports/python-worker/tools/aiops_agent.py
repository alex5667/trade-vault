import html as _html
import json
import math
import os
import socket
import sys
import urllib.parse
import urllib.request

# Попытка загрузить переменные окружения для cron на minik
for env_path in ['/opt/trade-agent/compose/.env', '/home/alex/front/trade/scanner_infra/.env']:
    try:
        with open(env_path) as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    k, v = line.strip().split('=', 1)
                    os.environ[k] = v.strip('"\'')
    except Exception:
        pass

VERSION = "15.7.0"

# ── Настройки ─────────────────────────────────────────────────────────────────
# Prometheus авто-обнаружение: сначала проверяет env var, затем сканирует известные порты
_PROMETHEUS_CANDIDATES = [
    "http://prometheus:9090",    # internal docker dns
    "http://192.168.0.121:9090", # external node
    "http://127.0.0.1:9090",    # minik-prometheus (основной)
    "http://127.0.0.1:19090",   # scanner-prometheus (резервный)
    "http://127.0.0.1:9091",    # agent-prometheus (резервный)
]

GEMINI_KEY      = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL    = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
NVIDIA_KEY      = os.environ.get("NVIDIA_API_KEY", "")
NVIDIA_MODEL    = os.environ.get("NVIDIA_MODEL", "qwen/qwen3.5-397b-a17b")
OLLAMA_URL_ENV      = os.environ.get("OLLAMA_URL", "")
GO_GATEWAY_URL_ENV  = os.environ.get("GO_GATEWAY_URL", "")
PROMETHEUS_ADDR_OVERRIDE = os.environ.get("PROMETHEUS_ADDR_OVERRIDE", None)
MODEL_NAME      = os.environ.get("AIOPS_MODEL", "deepseek-r1:14b")
TIMEOUT         = int(os.environ.get("AIOPS_TIMEOUT", "600"))

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

# AIOps Agent v15.7 — 35 метрик по 11 доменам + Go Gateway liveness + Prometheus авто-обнаружение
QUERIES: dict[str, str] = {
    # 1. Feature Drift & Distribution Shift ─────────────────────────────────
    "psi_drift":         'psi_max_24h',
    "feature_drift_z":   'feature_drift_max_z_24h',
    "dq_flag_rate":      'dq_flag_rate',
    "decision_n_24h":    'sum(signal_quality_n_24h) or vector(0)',

    # 2. Observability Health ────────────────────────────────────────────────
    "prom_bundle_ok":    'max(of_prom_rules_bundle_last_ok)',
    "prom_bundle_errs":  'max(of_prom_rules_bundle_last_error_n)',
    "prom_rules_miss":   'max(rules_files_missing)',
    "exec_slo_stale":    'max(exec_health_slo_stale_instances_total)',

    # 3. Replay Archiver ─────────────────────────────────────────────────────
    "replay_age":        '(time()*1000 - replay_inputs_archiver_last_run_ts_ms) / 1000',

    # 4. Policy Calibration Suggester ────────────────────────────────────────
    "calib_stale":       'policy_calibration_suggest_staleness_sec',
    "calib_action_warn": 'policy_calibration_suggest_warn_action_code',

    # 5. Market Flow & Adverse Selection ─────────────────────────────────────
    "adverse_rd_bad":    'max(trade_adverse_rd_bad_share)',
    "cancel_trade":      'max(trade_cancel_to_trade_bid)',
    "taker_imb_z":       'abs(max(trade_taker_flow_imb_z))',
    "book_churn":        'avg_over_time(max(trade_book_churn_hi)[10m:30s])',

    # 6. TCA & Policy Effectiveness ──────────────────────────────────────────
    "tca_age":           'tca_nightly_report_last_age_seconds',
    "tca_breaches":      'sum(tca_nightly_report_breach_groups{metric="is_p95"}) or vector(0)',

    # 7. Regime & Decision Health ────────────────────────────────────────────
    "decision_lag":      'max(meta_cov_ops_last_decision_age_s) * 1000',
    "regime_unknown":    'decision_regime_share_24h{regime="unknown"}',
    "policy_block":      'decision_policy_mode_share_24h{mode="block"}',

    # 8. Signal Quality & Calibration ────────────────────────────────────────
    "sig_expectancy":    'signal_quality_expectancy_r_24h_by_regime{regime="ok"}',
    "sig_ece":           'signal_quality_ece_24h_by_regime{regime="warn"}',
    "dq_level2":         'avg_over_time((dq_level == bool 2)[1h:15s])',
    "slippage_age":      'of_slippage_calib_last_ok_age_sec',

    # 9. Microstructure & Alpha ──────────────────────────────────────────────
    "micro_div":         'abs(trade_micro_mid_div_bps)',
    "obi_z":             'abs(trade_dw_obi_z)',
    "fill_prob":         'trade_fill_prob',
    "exec_cost":         'histogram_quantile(0.95, sum(rate(trade_exec_pen_bucket[10m])) by (le))',
    "spread_p95":        'histogram_quantile(0.95, sum(rate(trade_spread_bps_bucket[10m])) by (le))',
    "edge_neg_share":    'max(of_enforce_promoter_bucket_edge_neg_share) or vector(0)',

    # 10. Safety & Registry ──────────────────────────────────────────────────
    "reg_success":       'feature_registry_contract_last_success',
    "reg_age":          'feature_registry_contract_last_age_seconds',
    "liqmap_age":        'max(liqmap_snapshot_age_ms)',
    "circuit_trips":     'sum(of_inputs_v3_circuit_cfg_disabled) or vector(0)',
    "ts_missing":        'of_gate_timescale_policies_missing',
    "deriv_ctx_age":     'max(deriv_ctx_exporter_snapshot_age_ms)',
    "freezer_block":     'max(of_enforce_freezer_block_active) or vector(0)',

    # 11. Core Pipeline & Research ───────────────────────────────────────────
    "of_gate_ok":        '(sum(rate(of_gate_ok_hard_total[5m])) / sum(rate(of_gate_eligible_total[5m]))) * 100',
    "cont_ctx_veto":     'sum(increase(strong_gate_veto_total{reason=~".*continuation_gate.*"}[1h])) or vector(0)',
    "strong_gate_insuf_of": 'sum(increase(strong_gate_veto_total{reason="insufficient_of"}[1h])) or vector(0)',
    "g10_adverse_veto":  'sum(increase(g10_adverse_veto_total[1h])) or vector(0)',
    "outcome_entry":     'sum(increase(of_session_outcome_total{outcome="entry"}[1h])) or vector(0)',
    "e2e_lag":           'max(tick_ingest_e2e_delay_ms)',
    "venue_gap":         'max(tick_gap_p95_ms{venue="bybit"})',
    "pbo":               'max(strategy_research_stats_pbo)',
    "rguard_block":      'max(strategy_research_guard_blocker_active)',
    "rguard_age":        'max(strategy_research_guard_report_age_seconds)',
    "rguard_mode":       'max(strategy_research_guard_report_only)',
    "rguard_pbo":        'max(strategy_research_guard_pbo)',
    "redis_rss":         'max(process_resident_memory_bytes{job="redis-workers"}) / 1024 / 1024 / 1024',
    "redis_pel":         'max(tick_gate_group_pending) or vector(0)',
    "db_errors":         'sum(increase(ofc_ctx_writer_db_fail_total[1h])) or vector(0)',
    "news_budget":       '(news_budget_usd_used / clamp_min(news_budget_usd_limit, 0.01)) * 100',

    # 12. Experimental & Subsystems (News Agent / P4.1 / Autoguard) ──────────
    "news_pipe_lag":     'max(news_stream_lag_ms)',
    "news_reco_lag":     'max(trade_news_reco_reader_lag_ms)',
    "news_parse_err":    'sum(increase(trade_news_reco_reader_parse_errors_total[1h])) or vector(0)',
    "tick_stream_lag":   'max(tick_gate_stream_lag_ms)',
    "slo_contract_stale":'sum(latency_contract_slo_stale_total) or vector(0)',
    "slo_deploy_lint":   'max(latency_contract_deploy_lint_gate_active)',
    "exec_autoguard":    'max(exec_health_slo_autoguard_freeze_active)',
    "of_gate_dlq":       'max(of_gate_dlq_len) or vector(0)',
    "liqmap_drops":      'sum(increase(liqmap_evt_drop_total[1h])) or vector(0)',
    "tm_loop_lag":       'time() - avg(trade_monitor_loop_age_seconds)',
    "tm_instances":      'count(trade_monitor_loop_age_seconds) or vector(0)',

    # 13. High-Frequency Microstructure & SRE Tier-1 ─────────────────────────
    "redis_buf_drops":   'sum(increase(redis_client_output_buffer_limit_disconnections_total[1h])) or vector(0)',
    "orphan_orders":     'sum(increase(exec_gate_confirmations_orphan_total[1h])) or vector(0)',
    "redis_clients":     'max(redis_connected_clients) or vector(0)',
    "redis_cmd_sec":     'sum(rate(redis_commands_processed_total[5m])) or vector(0)',
    "worker_lag_p99":    'max(histogram_quantile(0.99, sum(rate(worker_lag_ms_hist_bucket[5m])) by (le))) or vector(0)',
    "redis_entry_lag_p99": 'max(histogram_quantile(0.99, sum(rate(redis_entry_lag_ms_hist_bucket[5m])) by (le))) or vector(0)',
    "market_inactivity_p99": 'max(histogram_quantile(0.99, sum(rate(market_inactivity_lag_ms_hist_bucket[5m])) by (le))) or vector(0)',
    "processing_p99":    'max(histogram_quantile(0.99, sum(rate(processing_time_us_bucket[5m])) by (le))) / 1000 or vector(0)',
    "signal_emit_p99":   'max(histogram_quantile(0.99, sum(rate(signal_emit_latency_us_bucket[5m])) by (le))) / 1000 or vector(0)',
    # 14. Virtual & Open Positions ───────────────────────────────────────────
    # P-FIX: sum() double-counts across shards that recover same positions.
    # max by (symbol) picks the authoritative shard per symbol, then sum across symbols.
    "open_pos":          'sum(max by (symbol) (open_positions_count)) or vector(0)',
    "virtual_pos":       'sum(max by (symbol) (virtual_positions_count)) or vector(0)',

    # 15. ATR Governance & Ops ───────────────────────────────────────────────
    "dr_safe_mode":      'sum(atr_restore_safe_mode) or vector(0)',
    "pr_hold_stale":     'sum(atr_promotion_hold_total{status="active"}) or vector(0)',
    "pr_rollback":       'sum(increase(atr_promotion_rollback_review_total[1h])) or vector(0)',
    # 16. OF Engine Latency Breakdown (Remediation G15) ──────────────────────
    "ofc_avg_ms_total":  'sum(rate(ofconfirm_build_stages_us_sum[10m])) / sum(rate(ofconfirm_build_stages_us_count[10m])) / 1000',
    "ofc_ms_init":       'avg(sum(rate(ofconfirm_build_stages_us_sum{stage="init"}[10m])) / sum(rate(ofconfirm_build_stages_us_count{stage="init"}[10m]))) / 1000',
    "ofc_ms_evidence":   'avg(sum(rate(ofconfirm_build_stages_us_sum{stage="evidence"}[10m])) / sum(rate(ofconfirm_build_stages_us_count{stage="evidence"}[10m]))) / 1000',
    "ofc_ms_scoring":    'avg(sum(rate(ofconfirm_build_stages_us_sum{stage="scoring"}[10m])) / sum(rate(ofconfirm_build_stages_us_count{stage="scoring"}[10m]))) / 1000',
    "ofc_ms_gates":       'avg(sum(rate(ofconfirm_build_stages_us_sum{stage="gates"}[10m])) / sum(rate(ofconfirm_build_stages_us_count{stage="gates"}[10m]))) / 1000',
    "ofc_ms_ml":         'avg(sum(rate(ofconfirm_build_stages_us_sum{stage=~"ml_confirm|meta_model"}[10m])) / sum(rate(ofconfirm_build_stages_us_count{stage=~"ml_confirm|meta_model"}[10m]))) / 1000',
    "ofc_ms_io":         'avg(sum(rate(ofconfirm_build_stages_us_sum{stage="capture_export"}[10m])) / sum(rate(ofconfirm_build_stages_us_count{stage="capture_export"}[10m]))) / 1000',
    # 17. Phase 2 Validation (Gates) ─────────────────────────────────────────
    "phase2_veto_adverse": 'sum(rate(of_session_outcome_total{outcome=~"veto_adverse.*"}[5m])) or vector(0)',
    "phase2_veto_low_conf_share": 'sum(rate(of_session_outcome_total{outcome="veto_low_conf"}[1h])) / clamp_min(sum(rate(of_session_outcome_total[1h])), 1)',
    "phase2_strong_gate_stressed": 'sum(rate(strong_gate_veto_total{mode="ENFORCE", reason="stressed_liq"}[5m])) or vector(0)',

    # 18. CoinGecko API Health ───────────────────────────────────────────────
    "cg_requests":       'sum(rate(coingecko_requests_total[5m])) or vector(0)',
    "cg_429":            'sum(rate(coingecko_429_total[5m])) or vector(0)',
    "cg_skipped":        'sum(rate(coingecko_scheduler_skipped_total[5m])) or vector(0)',
}


def _tcp_ping(host: str, port: int, timeout: float = 3.0) -> bool:
    """TCP-пинг: проверяет доступность порта без HTTP-запроса."""
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True
    except OSError:
        return False


def _discover_prometheus() -> tuple[str, str]:
    """
    Авто-обнаружение доступного инстанса Prometheus.
    Возвращает (PROMETHEUS_URL, PROMETHEUS_BASE) для первого живого кандидата,
    или фолбэк на первого кандидата если никто не отвечает.
    """
    env_url = os.environ.get("PROMETHEUS_URL", "")
    if env_url:
        base = env_url.replace("/api/v1/query", "")
        return env_url, base

    if PROMETHEUS_ADDR_OVERRIDE:
        p_c = [PROMETHEUS_ADDR_OVERRIDE]
    else:
        p_c = _PROMETHEUS_CANDIDATES

    for base in p_c:
        import urllib.parse as _up
        parsed = _up.urlparse(base)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 9090
        if _tcp_ping(host, port, timeout=2.0):
            try:
                resp = urllib.request.urlopen(f"{base}/-/healthy", timeout=3)
                if resp.status == 200:
                    print(f"  🔍 Prometheus обнаружен: {base}")
                    return f"{base}/api/v1/query", base
            except Exception:
                # TCP открыт, но HTTP health не ответил — всё равно используем
                print(f"  🔍 Prometheus TCP открыт (HTTP health не ответил): {base}")
                return f"{base}/api/v1/query", base

    # Фолбэк на первого кандидата
    fallback = _PROMETHEUS_CANDIDATES[0]
    print(f"  ⚠️  Ни один Prometheus не доступен, используем фолбэк: {fallback}")
    return f"{fallback}/api/v1/query", fallback
PROMETHEUS_URL, PROMETHEUS_BASE = _discover_prometheus()


def _discover_go_gateway() -> str:
    if GO_GATEWAY_URL_ENV: return GO_GATEWAY_URL_ENV
    import urllib.parse as _up
    for c in ["http://scanner-go-gateway:8090", "http://127.0.0.1:8090", "http://192.168.0.121:8090"]:
        try:
            if _tcp_ping(_up.urlparse(c).hostname, _up.urlparse(c).port, timeout=1.0):
                print(f"  🔍 Go Gateway обнаружен: {c}")
                return c
        except Exception: pass
    return "http://scanner-go-gateway:8090"

def _discover_ollama() -> str:
    if OLLAMA_URL_ENV: return OLLAMA_URL_ENV
    import urllib.parse as _up
    for c in ["http://ollama:11434/api/generate", "http://127.0.0.1:11434/api/generate", "http://192.168.0.121:11434/api/generate"]:
        try:
            if _tcp_ping(_up.urlparse(c).hostname, _up.urlparse(c).port, timeout=1.0):
                print(f"  🔍 Ollama обнаружен: {c}")
                return c
        except Exception: pass
    return "http://ollama:11434/api/generate"

GO_GATEWAY_URL = _discover_go_gateway()
OLLAMA_URL = _discover_ollama()
def check_prometheus_healthy() -> bool:
    """Проверяет доступность Prometheus через socket + HTTP."""
    import urllib.parse as _up
    parsed = _up.urlparse(PROMETHEUS_BASE)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 19090
    if not _tcp_ping(host, port, timeout=3.0):
        return False
    try:
        resp = urllib.request.urlopen(f"{PROMETHEUS_BASE}/-/healthy", timeout=5)
        return resp.status == 200
    except Exception:
        return False


def check_ollama_healthy() -> bool:
    """Проверяет доступность Ollama через TCP-пинг."""
    import urllib.parse as _up
    parsed = _up.urlparse(OLLAMA_URL)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 11434
    return _tcp_ping(host, port, timeout=3.0)


def check_go_gateway_healthy() -> bool:
    """Проверяет Go Gateway через TCP-пинг + HTTP /healthz."""
    import urllib.parse as _up
    parsed = _up.urlparse(GO_GATEWAY_URL)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 8090
    if not _tcp_ping(host, port, timeout=3.0):
        return False
    try:
        resp = urllib.request.urlopen(f"{GO_GATEWAY_URL}/healthz", timeout=5)
        return resp.status in (200, 204)
    except Exception:
        # /healthz endpoint may not exist — TCP open is sufficient
        return True


def get_metric(query: str) -> float | None:
    """
    Запрашивает одно значение из Prometheus.
    Возвращает:
      float  — если метрика есть и имеет значение
      None   — если метрика не экспортируется (empty result set)
    При ошибке соединения возвращает None с предупреждением.
    """
    try:
        url = f"{PROMETHEUS_URL}?query={urllib.parse.quote(query)}"
        req = urllib.request.urlopen(url, timeout=TIMEOUT)
        resp = json.loads(req.read())
        if resp.get('status') == 'success':
            data = resp['data']['result']
            if data:
                v = float(data[0]['value'][1])
                return v if not math.isnan(v) else 0.0
            # result пустой — метрика не экспортируется
            return None
        return None
    except Exception as e:
        print(f"  ⚠️  get_metric error ({query[:60]}): {e}", file=sys.stderr)
        return None


def fmt(v: float | None, fmt_str: str = ".2f", fallback: str = "N/A") -> str:
    """Форматирует Optional[float], возвращая fallback если None."""
    if v is None:
        return fallback
    return format(v, fmt_str)



def run_cycle() -> None:
    # ── Запуск ────────────────────────────────────────────────────────────────────
    print(f"🔍 AIOps Agent v{VERSION} — Drift, DQ, Relay & Observability + Gateway...")
    print(f"   Prometheus: {PROMETHEUS_BASE}")

    # ── 0. Go Gateway liveness check ──────────────────────────────────────────────
    go_gateway_ok = check_go_gateway_healthy()
    if not go_gateway_ok:
        msg = (
            f"🔴 КРИТИЧНО: Go Gateway НЕДОСТУПЕН! Торговля остановлена.\n"
            f"   URL: {GO_GATEWAY_URL}\n"
            f"   Проверьте: docker ps | grep go-gateway\n"
            f"   docker logs scanner-go-gateway --tail 50"
        )
        print(msg)
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                tg_data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg}).encode()
                tg_req = urllib.request.Request(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    data=tg_data, headers={"Content-Type": "application/json"}
                )
                urllib.request.urlopen(tg_req, timeout=10)
            except Exception as te:
                print(f"❌ Telegram Error: {te}")
        # Continue to Prometheus checks (don't sys.exit — gather full picture)
    else:
        print(f"  ✅ Go Gateway доступен: {GO_GATEWAY_URL}")

    # ── 1. Prometheus health check ─────────────────────────────────────────────────
    prom_healthy = check_prometheus_healthy()
    if not prom_healthy:
        msg = (
            f"🔴 КРИТИЧНО: Prometheus недоступен!\n"
            f"   URL: {PROMETHEUS_BASE}\n"
            f"   Проверьте: docker ps | grep prometheus\n"
            f"   Ожидается health endpoint: {PROMETHEUS_BASE}/-/healthy"
        )
        print(msg)
        # Отправим в Telegram если настроен
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                tg_data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg}).encode()
                tg_req = urllib.request.Request(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    data=tg_data, headers={"Content-Type": "application/json"}
                )
                urllib.request.urlopen(tg_req, timeout=10)
                print("✅ Алерт отправлен в Telegram.")
            except Exception as te:
                print(f"❌ Telegram Error: {te}")
        sys.exit(1)

    print(f"  ✅ Prometheus доступен: {PROMETHEUS_BASE}")

    m: dict[str, float | None] = {k: get_metric(v) for k, v in QUERIES.items()}

    # Подсчёт покрытия метрик
    total = len(m)
    available = sum(1 for v in m.values() if v is not None)
    empty = total - available
    print(f"  📊 Метрики: {available}/{total} доступны ({empty} не экспортируются)")

    # ── Alert Logic ───────────────────────────────────────────────────────────────
    alerts: list[str] = []

    # Go Gateway (повторяем в alerts если недоступен, чтобы включить в Telegram-рапорт)
    if not go_gateway_ok:
        alerts.append(f"🔴 КРИТИЧНО: Go Gateway НЕДОСТУПЕН ({GO_GATEWAY_URL})! Инжест данных остановлен.")

    # Drift & Observability
    if m['psi_drift'] is not None and m['psi_drift'] > 0.25:
        alerts.append(f"🔴 КРИТИЧНО: Сильный PSI-дрейф признаков ({m['psi_drift']:.3f} > 0.25). Модель деградирует!")
    elif m['psi_drift'] is not None and m['psi_drift'] > 0.10:
        alerts.append(f"🟠 ВНИМАНИЕ: Умеренный PSI-дрейф ({m['psi_drift']:.3f}).")
    if m['feature_drift_z'] is not None and m['feature_drift_z'] > 5:
        alerts.append(f"🟠 ВНИМАНИЕ: Экстремальный дрейф признаков (Z={m['feature_drift_z']:.1f} > 5).")
    if m['dq_flag_rate'] is not None and m['dq_flag_rate'] > 0.05:
        alerts.append(f"🟠 ВНИМАНИЕ: Высокий DQ Flag Rate ({m['dq_flag_rate']*100:.1f}% > 5%).")

    # Observability integrity — алертим ТОЛЬКО если метрика РЕАЛЬНО = 0, не если не экспортируется
    if m['prom_bundle_ok'] is not None and m['prom_bundle_ok'] == 0:
        alerts.append("🔴 КРИТИЧНО: Prometheus Rules Bundle невалиден — есть 'тихие' сбои в мониторинге!")
    if m['prom_bundle_errs'] is not None and m['prom_bundle_errs'] > 0:
        alerts.append(f"🟠 ВНИМАНИЕ: {m['prom_bundle_errs']:.0f} ошибок в Prometheus Rules Bundle.")
    if m['prom_rules_miss'] is not None and m['prom_rules_miss'] > 0:
        alerts.append(f"🔴 КРИТИЧНО: {m['prom_rules_miss']:.0f} файлов с метриками (Prom Rules) отвалились из рантайма!")
    if m['exec_slo_stale'] is not None and m['exec_slo_stale'] > 0:
        alerts.append(f"🔴 КРИТИЧНО: Зависшие инстансы Exec Health SLO ({m['exec_slo_stale']:.0f}).")

    # Replay Archiver
    if m['replay_age'] is not None and m['replay_age'] > 900:
        alerts.append(f"🟠 ВНИМАНИЕ: Replay Archiver не обновлялся {m['replay_age']/60:.0f}мин (>15).")

    # Policy Calibration
    if m['calib_stale'] is not None and m['calib_stale'] > 7200:
        alerts.append(f"🟠 ВНИМАНИЕ: Policy Calibration Suggester устарел ({m['calib_stale']/3600:.1f}ч).")
    if m['calib_action_warn'] is not None and m['calib_action_warn'] != 0:
        action = "TIGHTEN" if m['calib_action_warn'] > 0 else "LOOSEN"
        alerts.append(f"🟠 ВНИМАНИЕ: P74 рекомендует {action} WARN-порог.")

    # Market Flow
    if m['adverse_rd_bad'] is not None and m['adverse_rd_bad'] > 0.60:
        alerts.append(f"🔴 КРИТИЧНО: Adverse Drift Bad Share {m['adverse_rd_bad']*100:.1f}%.")
    if m['edge_neg_share'] is not None and m['edge_neg_share'] > 0.10:
        alerts.append(f"🔴 КРИТИЧНО: Доля трейдов с негативным Edge > 10% ({m['edge_neg_share']*100:.1f}%).")
    if m['cancel_trade'] is not None:
        if m['cancel_trade'] > 8:
            alerts.append(f"🔴 КРИТИЧНО: Cancel-to-Trade экстремален ({m['cancel_trade']:.1f}x).")
        elif m['cancel_trade'] > 4:
            alerts.append(f"🟠 ВНИМАНИЕ: Cancel-to-Trade высок ({m['cancel_trade']:.1f}x).")

    # TCA & Regime
    if m['tca_age'] is not None and m['tca_age'] > 129600:
        alerts.append(f"🟠 ВНИМАНИЕ: TCA-отчет устарел ({m['tca_age']/3600:.0f}ч).")
    if m['decision_lag'] is not None and m['decision_lag'] > 360_000:
        alerts.append(f"🔴 КРИТИЧНО: Поток решений завис ({m['decision_lag']/1000:.0f}с).")
    if m['sig_expectancy'] is not None and m['sig_expectancy'] < 0:
        alerts.append(f"🔴 КРИТИЧНО: Ожидаемость OK-сигнала отрицательная ({m['sig_expectancy']:.3f}).")
    if m['slippage_age'] is not None and m['slippage_age'] > 172_800:
        alerts.append("🔴 КРИТИЧНО: Калибровка проскальзывания > 48ч.")

    # Strategy R-Guard (G14)
    if m['rguard_age'] is not None and m['rguard_age'] > 129600:
        alerts.append(f"🔴 КРИТИЧНО: Отчет R-Guard (G14) устарел ({m['rguard_age']/3600:.0f}ч > 36ч).")
    if m['rguard_block'] is not None and m['rguard_block'] > 0:
        mode_str = "(REPORT-ONLY)" if (m['rguard_mode'] == 1) else "(ENFORCE)"
        alerts.append(f"🔴 КРИТИЧНО: Strategy R-Guard заблокировал метрики {mode_str}!")
    if m['rguard_pbo'] is not None and m['rguard_pbo'] > 0.05:
        alerts.append(f"🟠 ВНИМАНИЕ: Высокая вероятность переобучения PBO ({m['rguard_pbo']:.3f}).")

    # Safety & Pipeline
    # Guard against cold-start false-positives: only alert if reg_success==0
    # AND the metrics record is old enough to be reliable (>= 300s since last update).
    # reg_age < 300 means the exporter just started and hasn't had time to populate.
    _reg_age_ok = m.get('reg_age') is None or m.get('reg_age', 9999) >= 300
    if m['reg_success'] is not None and m['reg_success'] == 0 and _reg_age_ok:
        alerts.append("🔴 КРИТИЧНО: Feature Registry Contract Mismatch!")
    if m['ts_missing'] is not None and m['ts_missing'] > 0:
        alerts.append(f"🔴 КРИТИЧНО: Отсутствуют политики TimescaleDB ({m['ts_missing']:.0f})!")
    if m['circuit_trips'] is not None and m['circuit_trips'] > 0:
        alerts.append(f"🔴 КРИТИЧНО: Срабатывание Circuit Breakers ({m['circuit_trips']:.0f})!")
    if m['deriv_ctx_age'] is not None and m['deriv_ctx_age'] > 45_000:
        alerts.append(f"🔴 КРИТИЧНО: Отсутствует Derivatives Context ({m['deriv_ctx_age']/1000:.0f}с), угроза fail-open!")
    if m['liqmap_age'] is not None and m['liqmap_age'] > 300_000:
        alerts.append(f"🟠 ВНИМАНИЕ: Карта ликвидаций устарела ({m['liqmap_age']/1000:.0f}с).")
    if m['tm_loop_lag'] is not None and m['tm_loop_lag'] > 60:
        alerts.append(f"🔴 КРИТИЧНО: TradeMonitor завис (lag {m['tm_loop_lag']:.0f}s) — обновление жизненного цикла сделок заблокировано!")
    if m['freezer_block'] is not None and m['freezer_block'] > 0:
        alerts.append("🔴 КРИТИЧНО: SLO Freezer заблокировал применение Execution бакетов!")
    if m['of_gate_ok'] is not None and m['of_gate_ok'] < 50:
        alerts.append(f"🟠 ВНИМАНИЕ: Крайне низкий OF Gate Success Rate ({m['of_gate_ok']:.1f}%).")
    if m['exec_cost'] is not None and m['exec_cost'] > 5.0:
        alerts.append(f"🟠 ВНИМАНИЕ: Высокая цена исполнения P95 ({m['exec_cost']:.1f}bps).")
    if m['regime_unknown'] is not None and m['regime_unknown'] > 0.15:
        alerts.append(f"🟠 ВНИМАНИЕ: Высокая доля неизвестных режимов ({m['regime_unknown']*100:.1f}%).")
    if m['venue_gap'] is not None and m['venue_gap'] > 2000:
        alerts.append(f"🔴 КРИТИЧНО: Venue lag {m['venue_gap']:.0f}мс (Bybit).")

    # Trade Monitor & Virtual Positions
    if m['tm_loop_lag'] is not None and m['tm_loop_lag'] > 60:
        alerts.append(f"🔴 КРИТИЧНО: TradeMonitor завис (средний lag {m['tm_loop_lag']:.0f}s).")
    if m['tm_instances'] is not None and m['tm_instances'] < 2:
        alerts.append(f"🔴 КРИТИЧНО: Деградация кластера TradeMonitor (инстансов: {m['tm_instances']:.0f} < 2).")
    # Virtual Positions & Open Positions
    if m['open_pos'] is not None and m['open_pos'] > 250:
        real_pos = (m['open_pos'] or 0) - (m['virtual_pos'] or 0)
        if real_pos > 150:
            alerts.append(f"🟠 ВНИМАНИЕ: Аномально много РЕАЛЬНЫХ открытых позиций ({real_pos:.0f} > 150, total={m['open_pos']:.0f}).")
        else:
            alerts.append(f"⚪ ИНФО: Высокий total open_pos ({m['open_pos']:.0f}), но реальных только {real_pos:.0f} (virtual={(m['virtual_pos'] or 0):.0f}).")
    if m['virtual_pos'] is not None and m['virtual_pos'] > 0:
        alerts.append(f"⚪ ИНФО: Активно {m['virtual_pos']:.0f} виртуальных позиций (Paper Trading).")

    # Redis Saturation & Worker Lag
    if m['redis_clients'] is not None and m['redis_clients'] > 5000:
        alerts.append(f"🔴 КРИТИЧНО: Экстремальное количество коннектов к Redis ({m['redis_clients']:.0f} > 5000). Пул исчерпан!")
    elif m['redis_clients'] is not None and m['redis_clients'] > 1500:
        alerts.append(f"🟠 ВНИМАНИЕ: Высокое количество коннектов к Redis ({m['redis_clients']:.0f} > 1500).")
    # Worker Lag P99: включает Binance RTT 80-130ms. Baseline = 120-200ms.
    # ВНИМАНИЕ > 250ms = выше network baseline. КРИТИЧНО > 800ms = деградация event loop.
    # НО: если market_inactivity_p99 объясняет большую часть лага → это нормально (тихий рынок).
    wlag = m.get('worker_lag_p99')
    m_inact = m.get('market_inactivity_p99')
    r_entry = m.get('redis_entry_lag_p99')
    proc = m.get('processing_p99')
    if wlag is not None and wlag > 800:
        # Проверяем: объясняет ли market_inactivity большую часть лага?
        if m_inact is not None and m_inact > 0 and (m_inact / wlag) > 0.6:
            # Большая часть лага — рыночная тишина (tick_gap). Event Loop в норме.
            breakdown = (
                f"market_inactivity={m_inact:.0f}ms"
                + (f", redis_entry={r_entry:.0f}ms" if r_entry else "")
                + (f", processing={proc:.1f}ms" if proc else "")
            )
            alerts.append(
                f"⚪ ИНФО: Worker Lag P99 {wlag:.0f}ms — объясняется рыночной тишиной "
                f"(tick_gap). Event Loop в норме. Breakdown: {breakdown}."
            )
        else:
            alerts.append(f"🔴 КРИТИЧНО: Worker Lag P99 > 800ms ({wlag:.0f}ms). Критическая деградация event loop или Redis backlog!")
    elif wlag is not None and wlag > 250:
        alerts.append(f"🟠 ВНИМАНИЕ: Worker Lag P99 > 250ms ({wlag:.0f}ms). Проверьте сеть Binance и Redis backlog.")
    if m.get('redis_entry_lag_p99') is not None and m['redis_entry_lag_p99'] > 150:
        alerts.append(f"🔴 КРИТИЧНО: Redis Entry Lag P99 > 150ms ({m['redis_entry_lag_p99']:.0f}ms). Event Loop Python заблокирован или Redis Pool исчерпан!")
    elif m.get('redis_entry_lag_p99') is not None and m['redis_entry_lag_p99'] > 50:
        alerts.append(f"🟠 ВНИМАНИЕ: Redis Entry Lag P99 > 50ms ({m['redis_entry_lag_p99']:.0f}ms). Возможна перегрузка пула Redis (батчинг).")
    # Signal Emit P99: чистый Redis XADD round-trip. SLO < 8ms (номинал).
    # При насыщении пула (redis_clients > 1500) наблюдается 15-30ms — это деградация, не катастрофа.
    # КРИТИЧНО > 50ms = пул исчерпан, XADD ждёт слота. ВНИМАНИЕ > 15ms = ранний индикатор.
    if m['signal_emit_p99'] is not None and m['signal_emit_p99'] > 50:
        alerts.append(f"🔴 КРИТИЧНО: Signal Emit P99 > 50ms ({m['signal_emit_p99']:.1f}ms). Redis пул исчерпан, XADD блокируется!")
    elif m['signal_emit_p99'] is not None and m['signal_emit_p99'] > 15:
        alerts.append(f"🟠 ВНИМАНИЕ: Signal Emit P99 > 15ms ({m['signal_emit_p99']:.1f}ms). Redis-пул под давлением (норма < 8ms).")

    # 16. OF Engine Bottlenecks
    if m['ofc_avg_ms_total'] is not None and m['ofc_avg_ms_total'] > 40:
        # Find hottest stage
        stages = {
            "evidence": m.get('ofc_ms_evidence'),
            "ml_confirm": m.get('ofc_ms_ml'),
            "gates": m.get('ofc_ms_gates'),
            "io_export": m.get('ofc_ms_io'),
            "scoring": m.get('ofc_ms_scoring'),
            "init": m.get('ofc_ms_init')
        }
        # Filter out None and sort
        valid_stages = {k: v for k, v in stages.items() if v is not None}
        if valid_stages:
            hottest = max(valid_stages, key=valid_stages.get)
            h_ms = valid_stages[hottest]
            severity = "🔴 КРИТИЧНО" if m['ofc_avg_ms_total'] > 100 else "🟠 ВНИМАНИЕ"
            alerts.append(f"{severity}: OF Engine Bottleneck detected! Total: {m['ofc_avg_ms_total']:.1f}ms (Stage '{hottest}': {h_ms:.1f}ms).")

    # 17. Phase 2 Validation (Gates)
    if m.get('phase2_veto_adverse') is not None and m['phase2_veto_adverse'] > 10.0:
        alerts.append(f"🟠 ВНИМАНИЕ: Слишком много блокировок adverse selection (rate: {m['phase2_veto_adverse']:.2f}). Окно 10с может быть слишком коротким.")
    if m.get('phase2_manip_penalty') is not None and m['phase2_manip_penalty'] > 0:
        alerts.append(f"⚪ ИНФО: MANIP Gate активен, макс штраф: {m['phase2_manip_penalty']:.1f} bps.")
    if m.get('phase2_strong_gate_stressed') is not None and m['phase2_strong_gate_stressed'] > 0:
        alerts.append("⚪ ИНФО: Сработал динамический Strong Gate (Stressed Liquidity).")
    if m.get('phase2_drift_tighten') is not None and m['phase2_drift_tighten'] > 0:
        alerts.append("⚪ ИНФО: Защита Feature Drift активна, порог ML конфиденса поднят до 90%.")

    # 18. CoinGecko API Health
    if m.get('cg_429') is not None and m['cg_429'] > 0:
        alerts.append(f"🔴 КРИТИЧНО: CoinGecko API возвращает 429 (Rate Limit Exceeded: {m['cg_429']:.1f}/s)! Риск потери макро-метрик.")
    if m.get('cg_skipped') is not None and m['cg_skipped'] > 5:
        alerts.append(f"🟠 ВНИМАНИЕ: Планировщик CoinGecko пропускает задачи (budget/cooldown skips: {m['cg_skipped']:.1f}/s).")

    # Предупреждение о частично недоступных метриках
    if empty > 0:
        alerts.append(
            f"⚪ ИНФО: {empty}/{total} метрик не экспортируются "
            f"(сервисы ещё не запущены или не существуют в данной конфигурации)."
        )

    alerts_text = "\n".join(alerts) if alerts else "✅ Дрейф, DQ и наблюдаемость в норме."

    # ── Форматирование метрик ─────────────────────────────────────────────────────
    def pct(v: float | None, fallback: str = "N/A") -> str:
        return f"{v*100:.1f}%" if v is not None else fallback

    def sec_to_h(v: float | None, fallback: str = "N/A") -> str:
        return f"{v/3600:.1f}h" if v is not None else fallback

    def ms(v: float | None, fallback: str = "N/A") -> str:
        return f"{v:.0f}ms" if v is not None else fallback


    metrics_text = f"""
    [FEATURE DRIFT & DQ]
    - PSI Drift 24h: {fmt(m['psi_drift'], ".3f")} | Feature Drift Z: {fmt(m['feature_drift_z'], ".1f")}
    - DQ Flag Rate: {pct(m['dq_flag_rate'])} | Decisions 24h: {fmt(m['decision_n_24h'], ".0f")}

    [OBSERVABILITY]
    - Prom Bundle OK: {fmt(m['prom_bundle_ok'], ".0f")} | Bundle Errors: {fmt(m['prom_bundle_errs'], ".0f")}
    - Missing Prom Rules: {fmt(m['prom_rules_miss'], ".0f")} | Exec SLO Stale: {fmt(m['exec_slo_stale'], ".0f")}
    - Replay Archiver Age: {sec_to_h(m['replay_age'])} | Calib Stale: {sec_to_h(m['calib_stale'])}
    - DB Writer Errors 1h: {fmt(m['db_errors'], ".0f")}

    [MARKET FLOW & TCA]
    - Adverse Drift Bad: {pct(m['adverse_rd_bad'])} | Cancel/Trade: {fmt(m['cancel_trade'], ".1f")}x
    - Edge Neg Share: {pct(m['edge_neg_share'])} | Taker Imb Z: {fmt(m['taker_imb_z'], ".2f")}
    - Book Churn: {fmt(m['book_churn'], ".2f")} | TCA Age: {sec_to_h(m['tca_age'])}

    [SIGNAL & REGIME]
    - Decision Lag: {fmt(m['decision_lag'], ".0f") + 'ms' if m['decision_lag'] is not None else 'N/A'} | Policy Block: {pct(m['policy_block'])}
    - Regime Unknown: {pct(m['regime_unknown'])} | Signal Exp(OK): {fmt(m['sig_expectancy'], ".4f")}
    - P74 Warn Action: {fmt(m['calib_action_warn'], ".0f")} | DQ Level-2: {pct(m['dq_level2'])}
    - ContCtx Vetoes 1h: {fmt(m['cont_ctx_veto'], ".0f")}

    [MICROSTRUCTURE & EXECUTION]
    - Micro-Mid Div: {fmt(m['micro_div'], ".2f")}bps | OBI Z: {fmt(m['obi_z'], ".2f")}
    - Fill Prob: {fmt(m['fill_prob'], ".2f")} | Exec Cost P95: {fmt(m['exec_cost'], ".1f")}bps
    - Exec Rejects 1h: {fmt(m.get('exec_rejects'), ".0f")} | Binance Weight: {fmt(m.get('api_weight'), ".0f")}

    [SYSTEM & RESILIENCE]
    - Registry: {'OK' if m['reg_success'] == 1 else ('FAIL' if m['reg_success'] == 0 else 'N/A')} | TS Missing: {fmt(m['ts_missing'], ".0f")}
    - Circuit Trips: {fmt(m['circuit_trips'], ".0f")} | Freezer Block: {fmt(m['freezer_block'], ".0f")}
    - Deriv Ctx Age: {ms(m['deriv_ctx_age'])} | WS Disconnects 1h: {fmt(m.get('ws_disconnects'), ".0f")}
    - OF Gate OK: {fmt(m['of_gate_ok'], ".1f") + '%' if m['of_gate_ok'] is not None else 'N/A'}
    - Strong Gate Veto(Insuff_OF): {fmt(m['strong_gate_insuf_of'], ".0f")} | G10 Adverse: {fmt(m['g10_adverse_veto'], ".0f")} | Entries: {fmt(m['outcome_entry'], ".0f")}
    - TradeMonitor Lag/Inst: {fmt(m['tm_loop_lag'], ".0f")}s / {fmt(m['tm_instances'], ".0f")}
    - Open Pos: {fmt(m['open_pos'], ".0f")} | Virtual Pos: {fmt(m['virtual_pos'], ".0f")}
    - Redis RSS: {fmt(m['redis_rss'], ".1f")}GB | Redis PEL: {fmt(m['redis_pel'], ".0f")}

    [STRATEGY R-GUARD G14]
    - R-Guard Blocker: {fmt(m['rguard_block'], ".0f")} | Report Age: {sec_to_h(m['rguard_age'])}
    - R-Guard Mode(1=Report): {fmt(m['rguard_mode'], ".0f")} | PBO: {fmt(m['rguard_pbo'], ".3f")}

    [EXPERIMENTAL: NEWS & SUBSYSTEMS]
    - News LLM Lag: {ms(m['news_pipe_lag'])} | News Reco Lag: {ms(m['news_reco_lag'])}
    - News Parse Err: {fmt(m['news_parse_err'], ".0f")} | Tick Stream Lag: {ms(m['tick_stream_lag'])}
    - SLO Stale Total: {fmt(m['slo_contract_stale'], ".0f")} | SLO Deploy Lint: {fmt(m['slo_deploy_lint'], ".0f")}
    - Exec AutoGuard: {fmt(m['exec_autoguard'], ".0f")} | DLQ Len: {fmt(m['of_gate_dlq'], ".0f")}
    - Liqmap Drops 1h: {fmt(m['liqmap_drops'], ".0f")}

    [TIER-1 SRE & SYSTEM SATURATION]
    - Clock Drift: {fmt(m.get('clock_drift'), ".4f")}s | Ctx Switches/s: {fmt(m.get('ctx_switches'), ".0f")}
    - TCP Listen Drops 1h: {fmt(m.get('tcp_listen_drops'), ".0f")} | Redis Buf Drops 1h: {fmt(m['redis_buf_drops'], ".0f")}
    - Orphan Orders 1h: {fmt(m['orphan_orders'], ".0f")}
    - Redis Clients: {fmt(m['redis_clients'], ".0f")} | Redis Cmd/s: {fmt(m['redis_cmd_sec'], ".0f")}
    - Worker Lag P99: {fmt(m['worker_lag_p99'], ".0f")}ms | Redis Entry Lag P99: {fmt(m.get('redis_entry_lag_p99'), ".0f")}ms | Signal Emit P99: {fmt(m['signal_emit_p99'], ".1f")}ms

    [ATR GOVERNANCE]
    - DR Safe Mode: {fmt(m['dr_safe_mode'], ".0f")} | PR Hold Active: {fmt(m['pr_hold_stale'], ".0f")} | PR Rollback Reviews 1h: {fmt(m['pr_rollback'], ".0f")}

    [OF ENGINE BOTTLE-NECKS]
    - Avg Total: {fmt(m['ofc_avg_ms_total'], ".1f")}ms | Evidence: {fmt(m['ofc_ms_evidence'], ".1f")}ms | ML: {fmt(m['ofc_ms_ml'], ".1f")}ms
    - Gates: {fmt(m['ofc_ms_gates'], ".1f")}ms | IO/Export: {fmt(m['ofc_ms_io'], ".1f")}ms | Scoring: {fmt(m['ofc_ms_scoring'], ".1f")}ms

    [PHASE 2 VALIDATION]
    - G10 Adverse Veto Rate: {fmt(m.get('phase2_veto_adverse'), ".2f")}
    - MANIP Penalty Max: {fmt(m.get('phase2_manip_penalty'), ".1f")} bps
    - Veto Low Conf Share: {pct(m.get('phase2_veto_low_conf_share'))}
    - Strong Gate Stressed Liq Rate: {fmt(m.get('phase2_strong_gate_stressed'), ".2f")}
    - Feature Drift Tighten Active: {fmt(m.get('phase2_drift_tighten'), ".0f")}

    [COINGECKO API]
    - Requests/s: {fmt(m.get('cg_requests'), ".2f")} | 429 Errors/s: {fmt(m.get('cg_429'), ".2f")} | Skips/s: {fmt(m.get('cg_skipped'), ".2f")}

    - Metrics coverage: {available}/{total} available, {empty} not exported
    """

    print(f"\nАлерты:\n{alerts_text}\n")

    # ── AIOps Анамалии (TimescaleDB) ──────────────────────────────────────────────
    try:
        import os

        from sqlalchemy import create_engine

        # [AUTOGRAVITY CLEANUP]         sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
        from services.aiops_anomaly_extractor import AIOpsAnomalyExtractor
        from services.analytics_db import TRADES_DB_DSN

        _engine = create_engine(TRADES_DB_DSN)
        _extractor = AIOpsAnomalyExtractor(db_engine=_engine, top_n=50, mad_threshold=3.0)
        _anomaly_payload = _extractor.get_llm_payload()
        timescale_anomalies_text = json.dumps(_anomaly_payload, ensure_ascii=False, indent=2)
    except Exception as e:
        timescale_anomalies_text = f"ОШИБКА ИЗВЛЕЧЕНИЯ АНОМАЛИЙ: {e}"
        print(f"❌ AIOpsAnomalyExtractor Error: {e}")

    # ── Ollama LLM Рапорт ─────────────────────────────────────────────────────────
    prompt = f"""
    Ты Principal ML/SRE аналитик торговой системы. 
    Твоя задача — выдать не обзор, а жёсткий диагностический вывод по риску деградации сигнала и риску fail-open.

    Правила ответа:
    - Строго русский язык.
    - Ровно 6 предложений.
    - Без списков, без markdown, без вводных слов, без рекомендаций.
    - Стиль: сухой, инженерный, как запись в инцидент-рапорт.
    - Используй только метрики и алерты из входных данных.
    - Метрики со значением "N/A" полностью игнорируй.
    - Не пересказывай всё подряд: упоминай только то, что влияет на деньги, деградацию сигнала или потерю наблюдаемости.
    - Если риска по вектору нет, пиши это кратко и без воды.
    - Если данных недостаточно, прямо укажи: "не оценено из-за отсутствия метрик".

    Приоритет анализа:
    1) Ingestion fail-open и потеря наблюдаемости (WS disconnects, DB Errors, Decision Lag).
    2) Infrastructure Saturation (Redis PEL, Circuit trips, TCP Drops, Redis Buf Drops, Ctx Switches).
    3) Execution-риск и утечка edge (Exec Rejects, API Weight, Clock Drift).
    4) Drift, ContCtx Vetoes и деградация матожидания.

    Интерпретация:
    - Clock Drift > 0.05s = критический риск таймштампов HFT.
    - TCP Listen Drops 1h > 0 = ядро ОС дропает тики сокетов (тихое ослепление).
    - Redis Buf Drops 1h > 0 = Redis принудительно дисконнектит воркеры из-за переполнения OOM.
    - Redis Clients > 5000 = пул коннектов исчерпан, сервисы ожидают соединения (каскадные таймауты).
    - Worker Lag P99 включает ~80-130ms неконтролируемого Binance RTT. Нормальный baseline = 120-200ms. Только > 250ms = аномалия сети. > 800ms = критическая деградация event loop.
    - Redis Entry Lag P99 = чистое время от Redis XADD до Python read (без Binance RTT). Норма < 50ms при пакетной обработке. > 150ms = Event Loop python_worker заблокирован тяжелой задачей или Redis Pool исчерпан.
    - Signal Emit P99: SLO < 8ms (номинал). При redis_clients > 1500 возможно 15-30ms — это деградация пула, не катастрофа. > 50ms = пул исчерпан, XADD ждёт слота!
    - Orphan Orders 1h = это нормальная штатная асинхронность потока. Риска десинхронизации НЕТ! Пиши, что это норма.
    - Redis PEL > 1000 = зависание консьюмеров или бэкпрешур, предвестник OOM.
    - DB Writer Errors 1h > 0 = слепота TCA анализа, данные не пишутся в Timescale.
    - Exec Rejects 1h > 0 или высокие значения API Weight = риск отказов при исполнении на Binance.
    - ContCtx Vetoes 1h > 1000 = сломана логика continuation-гейта или OBI мёртв.
    - Strong Gate Veto(Insuff_OF) = Если 0, фильтр OFConfirm сломан. Должно быть большим при высокой отбраковке ложных сигналов.
    - G10 Adverse = Успешные срабатывания гейта (veto). Если > 0, это ХОРОШО (отсев токсичных откатов). Если 0, защита отключена.
    - Entries = Объём входов по 'of_session_outcome_total'. Падение объёма при росте Veto = норма для фильтрации качества.
    - PSI Drift > 0.25 = критический drift (если <= 0.25, то норма). Signal Exp < 0 = стратегия теряет деньги на сделках.
    - Exec Cost P95 > 5 bps = риск прямого ухудшения исполнения. Edge Neg Share > 10% = система допускает плохие входы.
    - Decision Lag: до 360000ms это НОРМА (интервал батчей). Если Decision Lag < 360000ms, то писать, что зависания потока решений НЕТ.
    - R-Guard Blocker > 0 = критическая деградация метрик стратегии G14.
    - R-Guard Age > 129600 = слепота системы проверки качества стратегий G14.
    - TradeMonitor Lag > 60s = критическое зависание мониторинга жизненного цикла сделок.
    - TradeMonitor Instances < 2 = критическая потеря избыточности мониторинга.
    - Virtual Pos > 0 = наличие активного бумажного трейдинга (норма).
    - DR Safe Mode > 0 = система в безопасном режиме после аварии.
    - PR Hold Stale > 0 = зависание процесса observation после релиза.
    - PR Rollback > 0 = срабатывание инвариантов - требуются откаты после релиза.
    - ofconfirm_build_stages (все стадии): evidence > 15ms = неоптимальный расчет признаков; ml_confirm > 10ms = перегрузка ML/CPU; gates > 5ms = слишком длинная цепочка гейтов; io_export > 10ms = тормозит экспорт в Redis/Disk.
    - Phase 2 Validation: Adverse Veto > 0 (успех отсева токсики), MANIP Penalty > 0 (успех пенализации манипуляций), Feature Drift Tighten > 0 (безопасная деградация), Stressed Liq > 0 (включение умного Strong Gate).
    - CoinGecko API: 429 Errors > 0 = исчерпание лимитов CoinGecko, риск ослепления макро-метрик.

    Формат ответа строго такой:
    Предложение 1: состояние Ingestion, Observability, DB Errors и лимитов CoinGecko (включая WS Disconnects, TCP Drops, 429 Errors).
    Предложение 2: состояние Infrastructure Saturation (Redis PEL, Circuit Trips, Redis Buf Drops, Ctx Switches).
    Предложение 3: состояние Execution Risk (Orphan Orders, Rejects, Exec Cost, Edge Neg Share, Clock Drift).
    Предложение 4: итоговый вердикт по Signal Drift и ContCtx Vetoes с указанием, где именно система сейчас проливает деньги или где она ослепла.
    Предложение 5: анализ бутылочных горлышек OF Engine (на основе стадий из ofconfirm_build_stages_us) и состояние ATR Governance.
    Предложение 6: анализ результатов Phase 2 Validation (Adverse Selection, MANIP, Low Conf Share, Stressed Liq, Drift Tighten).

    Запрещено:
    - давать рекомендации;
    - писать "в целом система стабильна", если есть критичные алерты;
    - упоминать метрики, которых нет;
    - делать предположения вне входных алертов и метрик.

    АЛЕРТЫ:
    {alerts_text}

    МЕТРИКИ (Prometheus):
    {metrics_text}
    
    СТАТИСТИЧЕСКИЕ АНОМАЛИИ Z-SCORE (TimescaleDB, отфильтровано):
    {timescale_anomalies_text}
    """

    llm_report = ""
    agent_name = ""
    success = False

    # ── LLM Chain (Gemini -> NVIDIA -> Ollama) ────────────────────────────────────
    if GEMINI_KEY:
        try:
            agent_name = "[Gemini]"
            endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
            data = json.dumps({
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2, "maxOutputTokens": 512},
            }).encode()
            req = urllib.request.Request(endpoint, data=data, headers={
                "Content-Type": "application/json",
                "x-goog-api-key": GEMINI_KEY
            })
            resp = urllib.request.urlopen(req, timeout=TIMEOUT)
            result = json.loads(resp.read())
            llm_report = result["candidates"][0]["content"]["parts"][0]["text"]
            success = True
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8', errors='replace')
            print(f"❌ Gemini HTTP Error {e.code}: {body}")
        except Exception as e:
            print(f"❌ Gemini Error: {e}")

    if not success and NVIDIA_KEY:
        try:
            agent_name = "[NVIDIA/DS]"
            data = json.dumps({
                "model": NVIDIA_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.2,
                "max_tokens": 512,
                "stream": False
            }).encode()
            req = urllib.request.Request("https://integrate.api.nvidia.com/v1/chat/completions", data=data, headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {NVIDIA_KEY}"
            })
            resp = urllib.request.urlopen(req, timeout=TIMEOUT)
            result = json.loads(resp.read())
            llm_report = result["choices"][0]["message"]["content"]
            success = True
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8', errors='replace')
            print(f"❌ NVIDIA HTTP Error {e.code}: {body}")
        except Exception as e:
            print(f"❌ NVIDIA Error: {e}")

    if not success:
        if not check_ollama_healthy():
            llm_report = f"⚠️ Ollama недоступен ({OLLAMA_URL}) — LLM-рапорт пропущен."
            print(f"\n{llm_report}")
            agent_name = "[Ollama Down]"
        else:
            try:
                from orderflow_services.providers.ollama_gpu_lock import OllamaGpuLock, OllamaGpuLockTimeout
                _redis_url = os.environ.get("REDIS_URL", "redis://redis-worker-1:6379/0")
                _gpu_lock = OllamaGpuLock(redis_url=_redis_url)
                with _gpu_lock.acquire_sync(owner="aiops_agent", timeout_sec=120):
                    agent_name = f"[{MODEL_NAME}]"
                    data = json.dumps({"model": MODEL_NAME, "prompt": prompt, "stream": False}).encode()
                    req = urllib.request.Request(OLLAMA_URL, data=data, headers={"Content-Type": "application/json"})
                    resp = urllib.request.urlopen(req, timeout=TIMEOUT)
                    result = json.loads(resp.read())
                    llm_report = result.get("response", "Empty response")
                    success = True
            except OllamaGpuLockTimeout as lte:
                llm_report = f"⚠️ GPU-лок таймаут: {lte}"
                agent_name = "[GPU Lock Timeout]"
            except Exception as e:
                # Catch urllib HTTPError separately if we want to print body
                if hasattr(e, 'read'):
                    err_body = e.read().decode('utf-8', errors='replace')
                    llm_report = f"Ollama ошибка HTTP {getattr(e, 'code', '404')}: {err_body}"
                else:
                    llm_report = f"Ollama ошибка: {e}"
                agent_name = "[Ollama Error]"

    if success:
        # Strip <think> tags for reasoning models
        import re
        llm_report = re.sub(r'<think>.*?</think>', '', llm_report, flags=re.DOTALL).strip()
        print("=" * 60)
        print(f"🤖 РАПОРТ AIOPS-АГЕНТА {VERSION} {agent_name} (DRIFT & OBSERVABILITY):")
        print("=" * 60)
        print(llm_report)
    else:
        print(f"\n⚠️ Ни один LLM-провайдер не ответил.\nСводка:\n{metrics_text}")

    # ── Telegram ──────────────────────────────────────────────────────────────────
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        # HTML parse_mode is safe for arbitrary text — no escaping tables needed;
        # use html.escape() on dynamic parts to prevent tag injection / 400 errors.
        def _e(s: str) -> str:
            """Escape a string for Telegram HTML parse_mode."""
            return _html.escape(str(s))

        tg_text = (
            f"🔍 <b>AIOps v{_e(VERSION)} {_e(agent_name)}</b>\n"
            f"📡 Prometheus: {_e(PROMETHEUS_BASE)}\n"
            f"📈 Coverage: {available}/{total} метрик\n\n"
            f"<b>Алерты:</b>\n{_e(alerts_text[:2000])}\n\n"
            f"<b>LLM-рапорт:</b>\n{_e(llm_report[:1500])}"
        )


        try:
            tg_data = json.dumps({
                "chat_id": TELEGRAM_CHAT_ID,
                "text": tg_text,
                "parse_mode": "HTML"
            }).encode()
            tg_req = urllib.request.Request(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                data=tg_data, headers={"Content-Type": "application/json"}
            )
            urllib.request.urlopen(tg_req, timeout=10)
            print("✅ Рапорт отправлен в Telegram.")
        except urllib.error.HTTPError as te:
            body = te.read().decode('utf-8', errors='replace')
            print(f"❌ Telegram Error: HTTP {te.code}: {body[:400]}")
        except Exception as te:
            print(f"❌ Telegram Error: {te}")


if __name__ == "__main__":
    import time
    if "--daemon" in sys.argv:
        print('[AIOps Agent] Starting periodic loop (every 30 min)...')
        time.sleep(10)  # Wait for dependencies to settle
        last_run_min = -1
        while True:
            m = time.localtime().tm_min
            if m in (0, 30) and m != last_run_min:
                print('[AIOps] ════════════════════════════════════════')
                print(f"[AIOps] Cycle start: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
                try:
                    run_cycle()
                except SystemExit:
                    pass  # Sys exit from prometheus check should not crash the loop
                except Exception as e:
                    print(f'[AIOps] Cycle FAILED: {e}')
                print('[AIOps] ════════════════════════════════════════')
                last_run_min = m
                time.sleep(60) # prevent running twice in the same minute
            else:
                time.sleep(15)
    else:
        run_cycle()
