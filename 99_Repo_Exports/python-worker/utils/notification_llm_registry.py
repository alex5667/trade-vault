from __future__ import annotations

"""DeepSeek notification analysis registry for trade project.

Production-oriented prompt + routing layer for LLM analysis of notifications.
Goals:
- deterministic routing by notification type/source
- low hallucination risk via strict system prompt + JSON-only output
- payload sanitization/whitelisting before sending to the model
- stable reason-code enums per notification type

This module is intentionally stdlib-only.
"""

from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence
import json
import re

SECRET_KEY_RE = re.compile(
    r"(api[_-]?key|secret|token|password|passphrase|authorization|cookie)",
    re.IGNORECASE,
)

MAX_STRING_CHARS = 2048
MAX_LIST_ITEMS = 50
MAX_DEPTH = 4


def _redact_and_cap(value: Any, *, depth: int = 0) -> Any:
    if depth > MAX_DEPTH:
        return "[truncated_depth]"

    if isinstance(value, Mapping):
        out = {}
        for k, v in value.items():
            ks = str(k)
            if SECRET_KEY_RE.search(ks):
                out[ks] = "[redacted]"
            else:
                out[ks] = _redact_and_cap(v, depth=depth + 1)
        return out

    if isinstance(value, list):
        return [_redact_and_cap(x, depth=depth + 1) for x in value[:MAX_LIST_ITEMS]]

    if isinstance(value, str):
        return value[:MAX_STRING_CHARS] + ("...[truncated]" if len(value) > MAX_STRING_CHARS else "")

    return value


# ---------------------------------------------------------------------------
# Core contracts
# ---------------------------------------------------------------------------

COMMON_SYSTEM_PROMPT = """Ты — Движок Анализа Уведомлений для проекта trade.

Контекст:
- Стек: Go -> Redis -> Python -> NestJS -> Next.js -> Postgres/Timescale.
- Поля времени в payload используют epoch ms (миллисекунды), если не указано иное.
- Классы уведомлений: trade_event, incident, report, recommendation.
- Твоя задача — строго анализировать входящие уведомления на основе данных из payload.

Жесткие правила:
1) Всегда разделяй:
   - facts (факты): то, что явно присутствует в payload.
   - assumptions (предположения): то, что выведено гипотетически.
   - risks (риски): операционные риски или риск потери капитала, следующие из данных.
2) Никогда не выдумывай отсутствующие значения. Используй null, \"missing\" или упоминай отсутствие в предположениях.
3) Анализ должен быть максимально кратким (максимум 7 предложений суммарно) и полезным для действий. Выдавай только краткие выводы. Избегай повторения всего payload.
4) Приоритет для инцидентов:
   защита капитала > биржевая истина (exchange truth) > идемпотентность > задержки (latency) > удобство.
5) Для времени и качества данных:
   - интерпретируй ts_ms / event_ts_ms / ingest_ts_ms как epoch ms.
   - упоминай устаревание (stale), перекос (skew), разрывы (gap) или дубликаты только при наличии доказательств в payload.
   - отмечай неопределенность, если семантика времени неясна.
6) Для торговых событий (trade events):
   - не называй событие «хорошим» или «плохим» без доказательств.
   - различай нормальное исполнение и подозрительное (suspicious execution).
7) Для ML / AB / калибровки / отчетов:
   - оценивай размер выборки, калибровку, дрифт и неопределенность.
   - не рекомендуй запуск (rollout) без достаточных доказательств.
8) Поле reason_code должно быть в snake_case.
9) Ответ должен быть СТРОГО в формате валидного JSON. Без markdown.
10) Гипотезы о первопричине: максимум 2 и только если они полезны.
11) КРИТИЧНО: ТЫ ДОЛЖЕН ПЕРЕВОДИТЬ ВСЕ ЗНАЧЕНИЯ В МАССИВАХ facts, assumptions, risks, steps_now, steps_later И ПОЛЕ summary_1line НА РУССКИЙ ЯЗЫК. НЕ ОСТАВЛЯЙ ИХ НА АНГЛИЙСКОМ. (reason_code, tags и enum типы оставляй как есть).

Возвращай JSON строго по этой структуре:
{
  \"notification_class\": \"trade_event|incident|report|recommendation\",
  \"reason_code\": \"string\",
  \"severity\": \"info|warning|critical\",
  \"summary_1line\": \"string\",
  \"facts\": [\"...\"],
  \"assumptions\": [\"...\"],
  \"risks\": [\"...\"],
  \"operator_action\": {
    \"needed\": true,
    \"urgency\": \"none|low|medium|high\",
    \"owner\": \"execution|risk|sre|ml|data|unknown\",
    \"steps_now\": [\"...\"],
    \"steps_later\": [\"...\"]
  },
  \"root_cause_hypotheses\": [\"...\"],
  \"confidence\": 0.0,
  \"tags\": [\"...\"],
  \"suppress_key\": \"string|null\"
}
"""


OUTPUT_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "notification_class",
        "reason_code",
        "severity",
        "summary_1line",
        "facts",
        "assumptions",
        "risks",
        "operator_action",
        "root_cause_hypotheses",
        "confidence",
        "tags",
        "suppress_key",
    ],
    "properties": {
        "notification_class": {
            "type": "string",
            "enum": ["trade_event", "incident", "report", "recommendation"],
        },
        "reason_code": {"type": "string"},
        "severity": {"type": "string", "enum": ["info", "warning", "critical"]},
        "summary_1line": {"type": "string"},
        "facts": {"type": "array", "items": {"type": "string"}},
        "assumptions": {"type": "array", "items": {"type": "string"}},
        "risks": {"type": "array", "items": {"type": "string"}},
        "operator_action": {
            "type": "object",
            "additionalProperties": False,
            "required": ["needed", "urgency", "owner", "steps_now", "steps_later"],
            "properties": {
                "needed": {"type": "boolean"},
                "urgency": {"type": "string", "enum": ["none", "low", "medium", "high"]},
                "owner": {
                    "type": "string",
                    "enum": ["execution", "risk", "sre", "ml", "data", "unknown"],
                },
                "steps_now": {"type": "array", "items": {"type": "string"}},
                "steps_later": {"type": "array", "items": {"type": "string"}},
            },
        },
        "root_cause_hypotheses": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "tags": {"type": "array", "items": {"type": "string"}},
        "suppress_key": {"type": ["string", "null"]},
    },
}


@dataclass(frozen=True)
class ModelProfile:
    name: str
    temperature: float
    top_p: float
    max_tokens: int
    repeat_penalty: float
    json_mode: bool = True
    presence_penalty: float = 0.0
    frequency_penalty: float = 0.0
    seed: int = 42


MODEL_PROFILE_REGISTRY: Dict[str, ModelProfile] = {
    "trade_event": ModelProfile(
        name="trade_event",
        temperature=0.15,
        top_p=0.85,
        max_tokens=1500,
        repeat_penalty=1.05,
    ),
    "incident": ModelProfile(
        name="incident",
        temperature=0.10,
        top_p=0.80,
        max_tokens=2000,
        repeat_penalty=1.07,
    ),
    "report": ModelProfile(
        name="report",
        temperature=0.20,
        top_p=0.90,
        max_tokens=3000,
        repeat_penalty=1.03,
    ),
    "recommendation": ModelProfile(
        name="recommendation",
        temperature=0.15,
        top_p=0.85,
        max_tokens=2000,
        repeat_penalty=1.05,
    ),
}


@dataclass(frozen=True)
class PromptSpec:
    notification_type: str
    notification_class: str
    profile: str
    owner: str
    reason_codes: Sequence[str]
    source_services: Sequence[str]
    prompt: str


BASE_WRAPPER_TEMPLATE = """Проанализируй уведомление торгового проекта.

Требования:
- вывод только в JSON
- ПИШИ КРАТКО И СТРОГО НА РУССКОМ ЯЗЫКЕ (КИРИЛЛИЦЕЙ). Это критично.
- ограничься максимум 7 предложениями суммарно, давай только краткие выводы
- раздели факты, предположения, риски
- не копируй весь сырой payload, выделяй только главные выводы и аномалии
- выдели возможность действий (actionability)
- если это рутинное событие без проблем, используй severity=info и operator_action.needed=false
- если это может затронуть капитал, исполнение, откат, заморозку (freeze) или целостность состояния, используй severity как минимум warning

ВНИМАНИЕ: ВЕСЬ ТЕКСТ (summary_1line, массивы facts, assumptions, risks, steps_now, steps_later) ДОЛЖЕН БЫТЬ ПЕРЕВЕДЕН И НАПИСАН СТРОГО НА РУССКОМ ЯЗЫКЕ. ЭТО ЖЕСТКОЕ ТРЕБОВАНИЕ.

notification_type={notification_type}
source_service={source_service}
raw_payload={raw_payload}
"""


PROMPT_REGISTRY: Dict[str, PromptSpec] = {
    "entry_opened": PromptSpec(
        notification_type="entry_opened",
        notification_class="trade_event",
        profile="trade_event",
        owner="execution",
        source_services=("orderflow_strategy.py", "binance_executor.py", "paper_orders_notifier.py"),
        reason_codes=(
            "entry_opened_normal",
            "entry_opened_missing_protection",
            "entry_opened_risk_too_high",
            "entry_opened_payload_incomplete",
            "entry_opened_suspicious_execution",
        ),
        prompt=(
            "Проанализируй уведомление об открытии сделки. Проверь символ/сторону/цену входа/sl/tp/риск%/уверенность. "
            "Определи, является ли это нормальным входом или подозрительным событием с деградацией исполнения. "
            "Отмечай отсутствие SL, подозрительно высокий риск%, неполный payload или несоответствие политике."
        ),
    ),
    "close_stoploss": PromptSpec(
        notification_type="close_stoploss",
        notification_class="trade_event",
        profile="trade_event",
        owner="execution",
        source_services=("trade_monitor.py", "binance_executor.py", "paper_orders_notifier.py"),
        reason_codes=(
            "close_stoploss_normal",
            "close_stoploss_after_trailing",
            "close_stoploss_with_excess_slippage",
            "close_stoploss_suspicious",
            "close_stoploss_payload_incomplete",
        ),
        prompt=(
            "Проанализируй уведомление о закрытии по стоп-лоссу. Различай контролируемый убыток и аномальное исполнение. "
            "Используй данные о трейлинге/безубытке/гэпе/проскальзывании только если они есть в payload. "
            "Действие оператора требуется только при аномальном проскальзывании, повторяющихся аномалиях или отсутствии защиты."
        ),
    ),
    "close_takeprofit": PromptSpec(
        notification_type="close_takeprofit",
        notification_class="trade_event",
        profile="trade_event",
        owner="execution",
        source_services=("trade_monitor.py", "binance_executor.py", "paper_orders_notifier.py"),
        reason_codes=(
            "close_takeprofit_normal",
            "close_takeprofit_partial",
            "close_takeprofit_with_fill_issue",
            "close_takeprofit_payload_incomplete",
        ),
        prompt=(
            "Проанализируй уведомление о закрытии по тейк-профиту. Определи полный или частичный TP, отметь проблемы с качеством исполнения (fill). "
            "Обычно severity=info; используй warning только при аномалиях исполнения."
        ),
    ),
    "close_trailing": PromptSpec(
        notification_type="close_trailing",
        notification_class="trade_event",
        profile="trade_event",
        owner="execution",
        source_services=("trade_monitor.py", "order_trailing_dispatcher.py"),
        reason_codes=(
            "close_trailing_profit_lock",
            "close_trailing_near_breakeven",
            "close_trailing_excess_giveback",
            "close_trailing_suspicious",
        ),
        prompt=(
            "Проанализируй уведомление о закрытии по трейлинг-стопу. Различай фиксацию прибыли и выход около безубытка. "
            "Отмечай задержку активации трейлинга или избыточный возврат прибыли (giveback), если payload это подтверждает."
        ),
    ),
    "dust_cleanup": PromptSpec(
        notification_type="dust_cleanup",
        notification_class="trade_event",
        profile="trade_event",
        owner="execution",
        source_services=("binance_dust_cleanup_admin_notifier.py",),
        reason_codes=(
            "dust_cleanup_routine",
            "dust_cleanup_repeated_residuals",
            "dust_cleanup_stuck_cooldown",
            "dust_cleanup_old_denylist",
            "dust_cleanup_requires_operator_ack",
        ),
        prompt=(
            "Проанализируй уведомление о очистке пыли (dust cleanup). Различай рутинную очистку и рекуррентные проблемы. "
            "Ищи повторяющиеся остатки, зависший цикл ожидания, устаревший черный список или необходимость подтверждения (ACK) оператором."
        ),
    ),
    "iceberg_detection": PromptSpec(
        notification_type="iceberg_detection",
        notification_class="trade_event",
        profile="trade_event",
        owner="execution",
        source_services=("binance_iceberg_detector.py",),
        reason_codes=(
            "iceberg_detected_context_only",
            "iceberg_detected_actionable_buy",
            "iceberg_detected_actionable_sell",
            "iceberg_detected_low_quality",
        ),
        prompt=(
            "Проанализируй уведомление об обнаружении айсберга. Сосредоточься на level_kind, level_price, refresh_count, visible_qty, duration_sec, atr_used. "
            "Обычно это контекстная информация/наблюдаемость, а не инцидент, требующий вмешательства."
        ),
    ),
    "freeze_alarm": PromptSpec(
        notification_type="freeze_alarm",
        notification_class="incident",
        profile="incident",
        owner="risk",
        source_services=("entry_policy_regression_service.py", "meta_guardrails_v1.py"),
        reason_codes=(
            "risk_freeze_triggered",
            "risk_freeze_possible_false_positive",
            "risk_freeze_due_to_data_quality",
            "risk_freeze_due_to_execution_drift",
            "risk_freeze_due_to_model_drift",
        ),
        prompt=(
            "Проанализируй сигнал заморозки (Freeze Alarm). Реши, выглядит ли заморозка как обоснованное защитное действие или как ложное срабатывание. "
            "Приоритетные гипотезы: статистическая деградация, качество данных, дрифт исполнения и дрифт модели. Severity должна быть critical."
        ),
    ),
    "unfreeze_ramp": PromptSpec(
        notification_type="unfreeze_ramp",
        notification_class="recommendation",
        profile="recommendation",
        owner="risk",
        source_services=("entry_policy_regression_service.py", "meta_guardrails_v1.py"),
        reason_codes=(
            "unfreeze_recommended",
            "unfreeze_recommended_limited_share",
            "unfreeze_insufficient_evidence",
            "unfreeze_blocked_by_risk",
        ),
        prompt=(
            "Проанализируй уведомление о разморозке/наращивании (ramp-up). Проверь достаточность доказательств для разморозки и определи, безопаснее ли использовать canary/ограниченную долю, чем полное включение. "
            "Включи рекомендацию по развертыванию в сводку: hold, shadow_only, canary или partial_enable."
        ),
    ),
    "rollback_alert": PromptSpec(
        notification_type="rollback_alert",
        notification_class="incident",
        profile="incident",
        owner="risk",
        source_services=("entry_policy_rollback_guard_v2.py",),
        reason_codes=(
            "rollback_suggestion",
            "rollback_enforced",
            "rollback_catastrophic",
            "rollback_insufficient_post_apply_sample",
        ),
        prompt=(
            "Проанализируй уведомление об откате (rollback). Сосредоточься на from_arm/to_arm, post_n, post_mean_r, post_lcb_r, baseline и на том, является ли это предложением или принудительным откатом. "
            "Используй critical для принудительного или катастрофического отката."
        ),
    ),
    "active_symbol_guard_incident": PromptSpec(
        notification_type="active_symbol_guard_incident",
        notification_class="incident",
        profile="incident",
        owner="execution",
        source_services=("active_symbol_guard_incident_notifier.py", "active_symbol_guard_incident_policy.py"),
        reason_codes=(
            "active_symbol_guard_stuck",
            "active_symbol_guard_writer_race",
            "active_symbol_guard_exchange_conflict",
            "active_symbol_guard_pending_release",
            "active_symbol_guard_stale_tombstone",
        ),
        prompt=(
            "Проанализируй инцидент Active Symbol Guard. Используй классификацию, критичность, fingerprint, sid, решение, детали hold/ack/renew, если они есть. "
            "Приоритет: биржевая истина, застрявший активный символ, tombstones и риски расхождения состояния/race condition."
        ),
    ),
    "latency_violation": PromptSpec(
        notification_type="latency_violation",
        notification_class="incident",
        profile="incident",
        owner="sre",
        source_services=("trade_monitor.py", "notify_receiver.py", "error_monitor.py"),
        reason_codes=(
            "latency_budget_violation_spike",
            "latency_budget_violation_sustained",
            "latency_budget_violation_trading_impact",
            "latency_budget_violation_unknown_stage",
        ),
        prompt=(
            "Проанализируй нарушение бюджета задержки (latency). Различай изолированный всплеск и устойчивое нарушение, выяви вероятное узкое место, если payload это позволяет."
        ),
    ),
    "orphans_collected": PromptSpec(
        notification_type="orphans_collected",
        notification_class="incident",
        profile="incident",
        owner="execution",
        source_services=("trade_monitor.py", "binance_executor.py"),
        reason_codes=(
            "orphan_cleanup_routine",
            "orphan_cleanup_executed",
            "orphan_force_close_executed",
            "orphan_state_divergence_risk",
            "orphan_cleanup_dry_run",
        ),
        prompt=(
            "Проанализируй уведомление о сборе «сирот» (orphans). Проверь наличие устаревшего локального состояния, расхождения между биржей и локальной БД, режим (dry-run vs реальное закрытие) и риски идемпотентности."
        ),
    ),
    "redis_lag_pressure": PromptSpec(
        notification_type="redis_lag_pressure",
        notification_class="incident",
        profile="incident",
        owner="sre",
        source_services=("trade_monitor.py", "notify_receiver.py"),
        reason_codes=(
            "redis_lag_pressure_transient",
            "redis_lag_pressure_sustained",
            "redis_lag_pressure_consumer_stuck",
            "redis_lag_pressure_trading_impact",
        ),
        prompt=(
            "Проанализируй уведомление о лаге/давлении в Redis. Используй данные о лаге stream/group/consumer и тренд бэклога. "
            "Сосредоточься на устаревших решениях, задержках ордеров и здоровье потребителей."
        ),
    ),
    "nightly_model_report": PromptSpec(
        notification_type="nightly_model_report",
        notification_class="report",
        profile="report",
        owner="ml",
        source_services=("baseline_promoter_worker.py", "ml_promo_callbacks_worker_tb_v10_4.py", "baseline_promoter_worker.py"),
        reason_codes=(
            "nightly_model_report_stable",
            "nightly_model_report_improved",
            "nightly_model_report_degraded",
            "nightly_model_report_calibration_issue",
            "nightly_model_report_insufficient_sample",
        ),
        prompt=(
            "Проанализируй ночной отчет по модели. Оцени precision, logloss, ECE, сравнение baseline/champion/challenger, размер выборки и риски калибровки. "
            "Заверши сводку одним из: keep, shadow, canary, promote, reject."
        ),
    ),
    "ab_winner_suggester": PromptSpec(
        notification_type="ab_winner_suggester",
        notification_class="recommendation",
        profile="recommendation",
        owner="ml",
        source_services=("ab_winner_suggester_service_v3.py",),
        reason_codes=(
            "ab_winner_recommend_apply",
            "ab_winner_recommend_canary",
            "ab_winner_no_clear_winner",
            "ab_winner_fallback_baseline",
            "ab_winner_insufficient_sample",
        ),
        prompt=(
            "Проанализируй предложение победителя AB-теста. Оцени winner_arm, преимущество LCB перед baseline, консистентность по режимам/сценариям и достаточность выборки."
        ),
    ),
    "calibration_sync": PromptSpec(
        notification_type="calibration_sync",
        notification_class="report",
        profile="report",
        owner="ml",
        source_services=("adverse_gate_calibrator_service.py", "strong_gate_calibrator_service.py"),
        reason_codes=(
            "calibration_sync_normal",
            "calibration_sync_regime_shift",
            "calibration_sync_too_aggressive",
            "calibration_sync_requires_shadow",
            "calibration_sync_possible_instability",
        ),
        prompt=(
            "Проанализируй уведомление о синхронизации калибровки. Определи, выглядят ли изменения порогов как нормальная адаптация к режиму или как потенциальная нестабильность. "
            "Обсуждай риски pass-rate / false-positive / false-negative, только если payload это поддерживает."
        ),
    ),
    "of_gate_sre": PromptSpec(
        notification_type="of_gate_sre",
        notification_class="report",
        profile="report",
        owner="sre",
        source_services=("of_gate_sre_monitor.py",),
        reason_codes=(
            "of_gate_sre_normal",
            "of_gate_sre_low_data",
            "of_gate_sre_high_latency",
            "of_gate_sre_high_soft_rate",
            "of_gate_sre_data_quality_issue",
            "of_gate_sre_drift_issue",
        ),
        prompt=(
            "Проанализируй агрегированный SRE отчет шлюза OrderFlow (OF_GATE_SRE). Данные содержат "
            "информацию об объеме обработанных данных, задержках (lat_p99_us, ml_lat_p99_us), качестве данных "
            "(ok_rate, soft_rate) и сработавших алертах (ALERTS).\n"
            "Определи, есть ли признаки деградации по времени или качеству данных. Если все показатели в норме, "
            "severity должен быть info, и операционное вмешательство не нужно. Если есть ошибки (no_data, latency, "
            "low ok_rate), используй warning/critical. "
            "Рекомендуется отслеживать Metrics / Grafana по SMT-отказам для альткоинов. "
            "При необходимости для шардов 2 и 3 можно будет запустить отдельный, специализированный бандл (например, SMT_COH_BUNDLE=alts_memes)."
        ),
    ),
    "auto_apply_skip": PromptSpec(
        notification_type="auto_apply_skip",
        notification_class="incident",
        profile="incident",
        owner="sre",
        source_services=("auto_apply_job_entrypoint_hardguard_v1.py",),
        reason_codes=(
            "auto_apply_skip_frozen",
            "auto_apply_skip_block_active",
            "auto_apply_skip_system_error",
        ),
        prompt=(
            "Проанализируй уведомление о пропуске авто-применения (Auto-Apply SKIPPED / hardguard).\n"
            "ОБЯЗАТЕЛЬНО извлеки из payload:\n"
            "- block_key: какой именно блокировщик сработал (например tick_gate, prom_rules_loaded_probe)\n"
            "- reason: числовой код (1 = блок активен)\n"
            "- cmd: какая команда была заблокирована\n\n"
            "Объясни КОНКРЕТНО что означает этот block_key для оператора:\n"
            "- tick_gate: входящие тики не проходят проверку качества\n"
            "- prom_rules_loaded_probe: Prometheus правила не загружены, мониторинг неполный\n"
            "- meta_freeze: символ/группа заморожена из-за просадки\n\n"
            "Severity=info если это штатная защита. Severity=warning если блокировка длится давно или причина неочевидна."
        ),
    ),
    "confidence_drift": PromptSpec(
        notification_type="confidence_drift",
        notification_class="report",
        profile="report",
        owner="ml",
        source_services=("run_confidence_drift_monitor.sh",),
        reason_codes=(
            "confidence_drift_high_z",
            "confidence_drift_routine",
            "confidence_drift_requires_investigation",
            "confidence_drift_possible_data_quality",
            "confidence_drift_regime_shift",
        ),
        prompt=(
            "Проанализируй алерт о дрейфе уверенности модели (Confidence Drift). "
            "Каждая строка содержит symbol/group и Z-score отклонения. "
            "Z>10 — экстремальный дрейф, требует расследования. Z=4..10 — значимый, нужен мониторинг. "
            "Установи severity=warning при Z>10 хотя бы по одному символу, иначе info. "
            "Гипотезы: изменение режима рынка, проблема качества данных, распределение признаков сместилось."
        ),
    ),
    "loss_report": PromptSpec(
        notification_type="loss_report",
        notification_class="report",
        profile="report",
        owner="risk",
        source_services=("top_loss_reasons_v1.py", "nightly_loss_report.py"),
        reason_codes=(
            "loss_report_cost_dominates",
            "loss_report_toxic_tags",
            "loss_report_toxic_sources",
            "loss_report_normal",
            "loss_report_requires_investigation",
        ),
        prompt=(
            "Проанализируй отчёт о топ-причинах убытков (Top Loss Reasons / Loss Report).\n"
            "ОБЯЗАТЕЛЬНО проанализируй:\n"
            "1. TOP LOSS BUCKETS: какой тип убытков доминирует (COST_DOMINATES = комиссии > прибыли, "
            "ADVERSE_MOVE = движение рынка против позиции, POOR_ENTRY = плохая точка входа).\n"
            "2. TOP TOXIC TAGS: какие entry_tag дают наихудший winrate и PnL. "
            "weak_progress = слабое развитие после входа, absorption = поглощение ордербука.\n"
            "3. TOP TOXIC SOURCES: какие источники сигналов (CryptoOrderFlow и т.д.) по каким символам убыточны.\n\n"
            "Дай КОНКРЕТНЫЕ рекомендации: какие символы/теги стоит приостановить или поместить в shadow, "
            "какие пороги confidence стоит пересмотреть.\n"
            "Severity=warning если есть явно токсичные комбинации, info если потери в пределах нормы."
        ),
    ),
    "dlq_replay": PromptSpec(
        notification_type="dlq_replay",
        notification_class="report",
        profile="report",
        owner="sre",
        source_services=("dlq_auto_replay.py",),
        reason_codes=(
            "dlq_replay_all_clear",
            "dlq_replay_partial_success",
            "dlq_replay_still_failing",
            "dlq_replay_empty",
        ),
        prompt=(
            "Проанализируй отчёт DLQ auto-replay (Dead Letter Queue — очередь ошибочных записей).\n"
            "ОБЯЗАТЕЛЬНО извлеки:\n"
            "- streams: количество DLQ потоков, seen/eligible/replayed/deleted — объём переповтора\n"
            "- allow_fixes: какие автоматические исправления разрешены\n"
            "- still_bad: записи которые не удалось исправить\n"
            "- by_stream: какие именно потоки затронуты\n\n"
            "Если seen=0 и eligible=0, это нормальная ситуация (DLQ пуст) — severity=info.\n"
            "Если still_bad>0, severity=warning — есть записи которые не поддаются автоматическому исправлению.\n"
            "Если replay_write_failed>0, severity=critical — проблема с записью исправленных сообщений."
        ),
    ),
    "ml_confirm_sre": PromptSpec(
        notification_type="ml_confirm_sre",
        notification_class="report",
        profile="report",
        owner="ml",
        source_services=("ml_confirm_sre_monitor.py",),
        reason_codes=(
            "ml_confirm_sre_healthy",
            "ml_confirm_sre_low_allow_rate",
            "ml_confirm_sre_stale_stream",
            "ml_confirm_sre_high_latency",
            "ml_confirm_sre_edge_zero",
            "ml_confirm_sre_label_stale",
        ),
        prompt=(
            "Проанализируй составной SRE отчёт по ML-подтверждению (ML_CONFIRM SRE ALERT).\n"
            "Отчёт содержит три секции:\n\n"
            "1. ML_CONFIRM: mode (SHADOW/ENFORCE), n (число проверок), allow_rate, p50 (медиана вероятности), "
            "lat_p99_ms (задержка), stream_stale_ms (устаревание потока), p_edge_zero_rate.\n"
            "2. TB_LABELER: input_lag_ms (задержка входных данных), label_stale_ms (устаревание меток), "
            "pending (ожидающие обработки), tb_alerts (конкретные алерты).\n"
            "3. CFG_SUGGESTIONS: meta_freeze scopes, pending/approved/applied рекомендации.\n\n"
            "КЛЮЧЕВЫЕ пороги:\n"
            "- stream_stale_ms > 60000 — поток устарел, данные не поступают\n"
            "- input_lag_ms > 300000 — критическая задержка входных меток\n"
            "- label_stale_ms > 360000 — метки устарели > 6 минут\n"
            "- allow_rate=0 при mode=ENFORCE — ML блокирует ВСЕ сигналы\n\n"
            "Severity=warning при любом tb_alert. Severity=critical при allow_rate=0 в ENFORCE."
        ),
    ),
    "meta_freeze_status": PromptSpec(
        notification_type="meta_freeze_status",
        notification_class="report",
        profile="report",
        owner="risk",
        source_services=("meta_guardrails_v1.py", "nightly_meta_enforce_ramp_or_freeze_bundle.py"),
        reason_codes=(
            "meta_freeze_active",
            "meta_freeze_cleared",
            "meta_freeze_ramp_in_progress",
            "meta_freeze_pending_review",
        ),
        prompt=(
            "Проанализируй статус мета-заморозки (CFG_SUGGESTIONS meta_freeze).\n"
            "Извлеки: scopes (ALL / конкретные символы), pending/approved/applied рекомендации, "
            "oldest_pending_min (сколько минут ожидает самая старая рекомендация).\n\n"
            "Объясни оператору:\n"
            "- Если scopes=['ALL'] — заморожены ВСЕ символы, это критично для торговли.\n"
            "- Если pending>0 и applied=0 — есть рекомендации, но они не применены (требует внимания).\n"
            "- Если pending=0 и applied>0 — система нормально применяет изменения.\n\n"
            "Severity=warning если freeze активен. Severity=info если всё разморожено."
        ),
    ),
    "unknown_notification": PromptSpec(
        notification_type="unknown_notification",
        notification_class="incident",
        profile="incident",
        owner="unknown",
        source_services=(),
        reason_codes=(
            "unknown_notification_type",
            "unknown_notification_unroutable",
            "unknown_notification_payload_incomplete",
        ),
        prompt=(
            "Проанализируй неизвестное уведомление. Не классифицируй его как торговое событие без доказательств. "
            "Определи, каких полей не хватает для маршрутизации, и предложи безопасное действие: quarantine или manual_review."
        ),
    ),
}


# ---------------------------------------------------------------------------
# Routing and payload sanitization
# ---------------------------------------------------------------------------

SOURCE_TO_NOTIFICATION_TYPE: Dict[str, str] = {
    "services/binance_dust_cleanup_admin_notifier.py": "dust_cleanup",
    "binance_dust_cleanup_admin_notifier.py": "dust_cleanup",
    "services/binance_iceberg_detector.py": "iceberg_detection",
    "binance_iceberg_detector.py": "iceberg_detection",
    "services/active_symbol_guard_incident_notifier.py": "active_symbol_guard_incident",
    "active_symbol_guard_incident_notifier.py": "active_symbol_guard_incident",
    "services/entry_policy_rollback_guard_v2.py": "rollback_alert",
    "entry_policy_rollback_guard_v2.py": "rollback_alert",
    "services/ab_winner_suggester_service_v3.py": "ab_winner_suggester",
    "ab_winner_suggester_service_v3.py": "ab_winner_suggester",
    "services/baseline_promoter_worker.py": "nightly_model_report",
    "baseline_promoter_worker.py": "nightly_model_report",
    "run_confidence_drift_monitor.sh": "confidence_drift",
}


SUBTYPE_TO_NOTIFICATION_TYPE: Dict[str, str] = {
    "active_symbol_guard_incident": "active_symbol_guard_incident",
    "iceberg": "iceberg_detection",
    "rollback": "rollback_alert",
    "nightly_model_report": "nightly_model_report",
    "of_gate_sre": "of_gate_sre",
    "confidence_drift": "confidence_drift",
}


# ⚠️ ORDER MATTERS: Python dict preserves insertion order.
# More specific (longer) patterns MUST come BEFORE shorter/generic ones.
# E.g. "auto_apply_job_entrypoint" MUST come before "freeze",
# because auto_apply text often contains "ramp_or_freeze_bundle".
TEXT_HEURISTIC_ROUTES: Dict[str, str] = {
    # ── Tier 1: exact multi-word phrases (most specific) ──
    "auto_apply_job_entrypoint_hardguard_v1": "auto_apply_skip",
    "auto_apply_job_entrypoint": "auto_apply_skip",
    "auto-apply skipped": "auto_apply_skip",
    "nightly_meta_enforce_ramp_or_freeze_bundle": "auto_apply_skip",
    "rollback suggestion": "rollback_alert",
    "rollback enforced": "rollback_alert",
    "confidence drift alert": "confidence_drift",
    "confidence drift": "confidence_drift",
    "active symbol guard": "active_symbol_guard_incident",
    "dust cleanup": "dust_cleanup",
    "signalemitp99high": "latency_violation",
    "workerlagp99high": "latency_violation",
    # ── Tier 2: composite SRE reports (before generic tokens) ──
    "ml_confirm sre alert": "ml_confirm_sre",
    "ml_confirm sre": "ml_confirm_sre",
    "tb_labeler": "ml_confirm_sre",
    "cfg_suggestions": "meta_freeze_status",
    "meta_freeze": "meta_freeze_status",
    "of_gate_sre": "of_gate_sre",
    "of-gate dlq auto-replay": "dlq_replay",
    "dlq auto-replay": "dlq_replay",
    "stream:dlq:": "dlq_replay",
    "no_data_total": "of_gate_sre",
    "low_n_total": "of_gate_sre",
    # ── Tier 3: loss/analytics reports ──
    "top loss buckets": "loss_report",
    "top 3 loss reasons": "loss_report",
    "top toxic tags": "loss_report",
    "top toxic sources": "loss_report",
    "cost_dominates": "loss_report",
    "loss reasons": "loss_report",
    # ── Tier 4: trade events ──
    "takeprofit": "close_takeprofit",
    "stoploss": "close_stoploss",
    "trailing": "close_trailing",
    "opened": "entry_opened",
    # ── Tier 5: generic tokens (shortest, last resort) ──
    # ⚠️ "unfreeze" MUST come before "freeze"
    "unfreeze": "unfreeze_ramp",
    "ramp-up": "unfreeze_ramp",
    "freeze alarm": "freeze_alarm",
    "freeze_alarm": "freeze_alarm",
    "freeze triggered": "freeze_alarm",
    "iceberg": "iceberg_detection",
    "orphan": "orphans_collected",
    "redis lag": "redis_lag_pressure",
    "winner": "ab_winner_suggester",
    "calibration": "calibration_sync",
    "ece": "nightly_model_report",
    "logloss": "nightly_model_report",
    "latency": "latency_violation",
    "rollback": "rollback_alert",
    # ⚠️ bare "freeze" is LAST — only matches if nothing above caught it
    "freeze": "freeze_alarm",
}



PAYLOAD_WHITELISTS: Dict[str, Sequence[str]] = {

    "entry_opened": (
        "type", "subtype", "source", "symbol", "sid", "signal_id", "side", "direction",
        "entry_price", "price", "sl_price", "sl", "tp1_price", "tp2_price", "tp_levels",
        "risk_pct", "confidence", "confidence_pct", "ts_ms", "ts", "venue", "kind",
        "source_service", "metadata", "payload", "payload_json",
    ),
    "close_stoploss": (
        "type", "subtype", "source", "symbol", "sid", "signal_id", "side", "direction",
        "entry_price", "exit_price", "fill_price", "sl_price", "current_sl", "close_reason",
        "slippage_bps", "ts_ms", "ts", "be_activated", "trailing_active", "payload", "payload_json",
    ),
    "close_takeprofit": (
        "type", "subtype", "source", "symbol", "sid", "signal_id", "side", "direction",
        "entry_price", "exit_price", "fill_price", "tp1_price", "tp2_price", "close_reason",
        "partial", "qty_closed", "slippage_bps", "ts_ms", "ts", "payload", "payload_json",
    ),
    "close_trailing": (
        "type", "subtype", "source", "symbol", "sid", "signal_id", "side", "direction",
        "entry_price", "exit_price", "fill_price", "prev_sl", "new_sl", "current_sl", "trailing_distance",
        "trailing_active", "be_activated", "close_reason", "giveback_bps", "ts_ms", "ts", "payload", "payload_json",
    ),
    "dust_cleanup": (
        "type", "subtype", "source", "severity", "symbol", "kind", "text", "ts_ms",
        "payload_json", "payload", "ack", "ack_state", "renew", "renew_state", "cooldown_sec",
        "age_sec", "denylist_age_sec", "reason", "status",
    ),
    "iceberg_detection": (
        "type", "subtype", "source", "symbol", "sid", "signal_id", "side", "direction",
        "price", "entry_price", "confidence", "confidence_pct", "ts_ms", "ts", "metadata", "payload", "payload_json",
        "level_kind", "level_price", "refresh_count", "visible_qty", "duration_sec", "atr_used",
        "hidden_qty_est", "book_side", "distance_bps",
    ),
    "freeze_alarm": (
        "type", "subtype", "source", "symbol", "regime", "group", "severity", "decision",
        "text", "reason", "reason_code", "ts_ms", "payload", "payload_json",
    ),
    "unfreeze_ramp": (
        "type", "subtype", "source", "symbol", "regime", "group", "severity", "decision",
        "text", "reason", "reason_code", "ts_ms", "payload", "payload_json", "lcb", "n", "share",
    ),
    "rollback_alert": (
        "type", "subtype", "source", "symbol", "regime", "group", "severity", "decision", "text",
        "from_arm", "to_arm", "post_n", "post_mean_r", "post_lcb_r", "baseline", "reason", "reason_code",
        "sid", "ts_ms", "payload", "payload_json",
    ),
    "active_symbol_guard_incident": (
        "type", "subtype", "source", "symbol", "sid", "severity", "decision", "fingerprint", "classification",
        "text", "ts", "ts_ms", "payload", "payload_json", "hold", "ack", "renew", "status",
    ),
    "latency_violation": (
        "type", "subtype", "source", "severity", "symbol", "sid", "text", "budget_ms", "latency_ms",
        "latency_us", "stage", "stream", "consumer", "ts_ms", "payload", "payload_json",
    ),
    "orphans_collected": (
        "type", "subtype", "source", "severity", "symbol", "sid", "text", "dry_run", "executed",
        "force_closed", "cancelled", "position_id", "order_id", "ts_ms", "payload", "payload_json",
    ),
    "redis_lag_pressure": (
        "type", "subtype", "source", "severity", "text", "stream", "group", "consumer", "lag", "pending",
        "backlog", "growth_rate", "ts_ms", "payload", "payload_json",
    ),
    "nightly_model_report": (
        "type", "subtype", "source", "severity", "text", "model_ver", "champion_ver", "challenger_ver",
        "precision", "logloss", "ece", "n", "sample_size", "class_balance", "regime", "decision", "ts_ms",
        "payload", "payload_json",
    ),
    "ab_winner_suggester": (
        "type", "subtype", "source", "severity", "text", "symbol", "regime", "group", "scenario",
        "winner_arm", "baseline_arm", "lcb", "lcb_edge", "min_n", "n", "alpha", "decision", "sid",
        "ts_ms", "payload", "payload_json",
    ),
    "calibration_sync": (
        "type", "subtype", "source", "severity", "text", "symbol", "regime", "thresholds_before", "thresholds_after",
        "pass_rate_before", "pass_rate_after", "decision", "ts_ms", "payload", "payload_json",
    ),
    "of_gate_sre": (
        "type", "subtype", "source", "severity", "text", "ts_ms", "payload", "payload_json",
    ),
    "auto_apply_skip": (
        "type", "subtype", "source", "level", "title", "text", "ts_ms",
        "block_key", "reason", "cmd", "raw", "value", "rid",
        "payload", "payload_json",
    ),
    "confidence_drift": (
        "type", "subtype", "source", "source_service", "notification_type",
        "text", "message", "ts_ms", "alerts", "payload", "payload_json",
    ),
    "loss_report": (
        "type", "subtype", "source", "severity", "text", "message", "ts_ms",
        "payload", "payload_json",
    ),
    "dlq_replay": (
        "type", "subtype", "source", "severity", "text", "message", "ts_ms",
        "streams", "seen", "eligible", "replayed", "deleted", "still_bad",
        "allow_fixes", "by_stream", "top_err_prefix", "top_fix_tags",
        "payload", "payload_json",
    ),
    "ml_confirm_sre": (
        "type", "subtype", "source", "severity", "text", "message", "ts_ms",
        "mode", "n", "allow_rate", "p50", "lat_p99_ms", "stream_stale_ms",
        "p_edge_zero_rate", "required_missing_rate", "missing_rate", "err_rate",
        "input_lag_ms", "label_stale_ms", "pending", "group_lag_ms",
        "payload", "payload_json",
    ),
    "meta_freeze_status": (
        "type", "subtype", "source", "severity", "text", "message", "ts_ms",
        "scopes", "pending", "approved", "applied", "oldest_pending_min",
        "payload", "payload_json",
    ),
}


def _try_parse_json_doc(raw: Any) -> Any:
    if not isinstance(raw, str):
        return raw
    s = raw.strip()
    if not s:
        return raw
    if s[0] not in "[{":
        return raw
    try:
        return json.loads(s)
    except Exception:
        return raw


def _shallow_normalize(payload: Mapping[str, Any]) -> Dict[str, Any]:
    out = dict(payload)
    for key in ("payload", "payload_json", "metadata"):
        if key in out:
            out[key] = _try_parse_json_doc(out[key])
    # normalize obvious aliases
    if "timestamp" in out and "ts_ms" not in out:
        out["ts_ms"] = out["timestamp"]
    if "ts" in out and "ts_ms" not in out:
        out["ts_ms"] = out["ts"]
    if "entry" in out and "entry_price" not in out:
        out["entry_price"] = out["entry"]
    if "sl" in out and "sl_price" not in out:
        out["sl_price"] = out["sl"]
    if "tp" in out and "tp1_price" not in out:
        out["tp1_price"] = out["tp"]
    return out


def sanitize_payload(notification_type: str, payload: Mapping[str, Any]) -> Dict[str, Any]:
    normalized = _shallow_normalize(payload)
    whitelist = PAYLOAD_WHITELISTS.get(notification_type)
    if not whitelist:
        return _redact_and_cap(normalized)
    out: Dict[str, Any] = {}
    for key in whitelist:
        if key in normalized:
            out[key] = _redact_and_cap(normalized[key])
    # preserve critical identity keys if available
    for key in ("symbol", "sid", "signal_id", "fingerprint", "type", "subtype", "source", "text"):
        if key in normalized and key not in out:
            out[key] = _redact_and_cap(normalized[key])
    return out


def _normalize_route_key(x: Any) -> str:
    return str(x or "").strip().lower()


def _route_by_source(source_service: Optional[str]) -> Optional[str]:
    src = _normalize_route_key(source_service)
    if not src:
        return None

    if src in SOURCE_TO_NOTIFICATION_TYPE:
        return SOURCE_TO_NOTIFICATION_TYPE[src]

    base = src.rsplit("/", 1)[-1]
    if base in SOURCE_TO_NOTIFICATION_TYPE:
        return SOURCE_TO_NOTIFICATION_TYPE[base]

    for known, routed in SOURCE_TO_NOTIFICATION_TYPE.items():
        if src.endswith(known.lower()):
            return routed

    return None


def route_notification(
    *,
    notification_type: Optional[str] = None,
    subtype: Optional[str] = None,
    source_service: Optional[str] = None,
    payload: Optional[Mapping[str, Any]] = None,
) -> str:
    if notification_type and notification_type in PROMPT_REGISTRY:
        return notification_type
    if subtype and subtype in SUBTYPE_TO_NOTIFICATION_TYPE:
        return SUBTYPE_TO_NOTIFICATION_TYPE[subtype]

    source_routed = _route_by_source(source_service)
    if source_routed:
        return source_routed

    payload = payload or {}
    normalized = _shallow_normalize(payload)

    # explicit fields first
    for key in ("notification_type", "kind", "event_type"):
        val = str(normalized.get(key) or "").strip().lower()
        if val in PROMPT_REGISTRY:
            return val

    hay = " ".join(
        str(normalized.get(k) or "")
        for k in ("type", "subtype", "text", "reason", "reason_code", "source", "message", "msg")
    ).lower()
    for token, routed in TEXT_HEURISTIC_ROUTES.items():
        if token in hay:
            return routed

    return "unknown_notification"


def render_user_prompt(notification_type: str, source_service: str, payload: Mapping[str, Any]) -> str:
    spec = PROMPT_REGISTRY[notification_type]
    sanitized = sanitize_payload(notification_type, payload)
    payload_json = json.dumps(sanitized, ensure_ascii=False, separators=(",", ":"), default=str)
    parts = [
        spec.prompt,
        f"Allowed reason_codes={','.join(spec.reason_codes)}",
        BASE_WRAPPER_TEMPLATE.format(
            notification_type=notification_type,
            source_service=source_service,
            raw_payload=payload_json,
        ),
    ]
    return "\n\n".join(parts)


def build_deepseek_request(
    *,
    source_service: str,
    payload: Mapping[str, Any],
    notification_type: Optional[str] = None,
    subtype: Optional[str] = None,
    model: str = "deepseek-14b",
) -> Dict[str, Any]:
    routed = route_notification(
        notification_type=notification_type,
        subtype=subtype,
        source_service=source_service,
        payload=payload,
    )
    spec = PROMPT_REGISTRY[routed]
    profile = MODEL_PROFILE_REGISTRY[spec.profile]
    user_prompt = render_user_prompt(routed, source_service, payload)
    return {
        "model": model,
        "messages": [
            {"role": "system", "content": COMMON_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": profile.temperature,
        "top_p": profile.top_p,
        "max_tokens": profile.max_tokens,
        "response_format": OUTPUT_JSON_SCHEMA,
    }


# ---------------------------------------------------------------------------
# Optional helper for dispatcher/worker integration
# ---------------------------------------------------------------------------


def build_analysis_envelope(
    *,
    source_service: str,
    payload: Mapping[str, Any],
    notification_type: Optional[str] = None,
    subtype: Optional[str] = None,
    model: str = "deepseek-14b",
) -> Dict[str, Any]:
    """Return an apply-ready envelope for a notification-analysis worker.

    Example downstream flow:
      req = build_analysis_envelope(...)
      resp = llm_client.chat.completions.create(**req["llm_request"])
      store req["analysis_key"], req["notification_type"], resp_json
    """
    routed = route_notification(
        notification_type=notification_type,
        subtype=subtype,
        source_service=source_service,
        payload=payload,
    )
    norm = _shallow_normalize(payload)
    symbol = str(norm.get("symbol") or "")
    sid = str(norm.get("sid") or norm.get("signal_id") or norm.get("fingerprint") or "")
    ts_ms = norm.get("ts_ms") or norm.get("ts") or ""
    analysis_key = f"notify_analysis:{routed}:{symbol}:{sid}:{ts_ms}"
    return {
        "analysis_key": analysis_key,
        "notification_type": routed,
        "source_service": source_service,
        "sanitized_payload": sanitize_payload(routed, payload),
        "llm_request": build_deepseek_request(
            source_service=source_service,
            payload=payload,
            notification_type=routed,
            subtype=subtype,
            model=model,
        ),
    }


def validate_reason_code(parsed: Dict[str, Any], notification_type: str) -> None:
    """Ensure reason_code is valid for the given notification type."""
    if notification_type not in PROMPT_REGISTRY:
        return
    spec = PROMPT_REGISTRY[notification_type]
    rc = parsed.get("reason_code")
    if rc not in spec.reason_codes:
        parsed["reason_code"] = spec.reason_codes[0] if spec.reason_codes else "unknown"


def validate_llm_response(parsed: Dict[str, Any], notification_type: str) -> None:
    """Validate LLM response against schema and correct missing fields in-place."""
    if not isinstance(parsed, dict):
        raise ValueError("LLM response must be a JSON object")

    for req in OUTPUT_JSON_SCHEMA["required"]:
        if req not in parsed:
            if req in ("facts", "assumptions", "risks", "root_cause_hypotheses", "tags"):
                parsed[req] = []
            elif req == "summary_1line":
                parsed[req] = "No summary provided."
            elif req == "severity":
                parsed[req] = "info"
            elif req == "confidence":
                parsed[req] = 0.0
            elif req == "operator_action":
                parsed[req] = {"needed": False, "urgency": "none", "owner": "unknown", "steps_now": [], "steps_later": []}
            else:
                parsed[req] = None
    
    validate_reason_code(parsed, notification_type)


__all__ = [
    "COMMON_SYSTEM_PROMPT",
    "OUTPUT_JSON_SCHEMA",
    "ModelProfile",
    "PromptSpec",
    "MODEL_PROFILE_REGISTRY",
    "PROMPT_REGISTRY",
    "SOURCE_TO_NOTIFICATION_TYPE",
    "SUBTYPE_TO_NOTIFICATION_TYPE",
    "PAYLOAD_WHITELISTS",
    "sanitize_payload",
    "route_notification",
    "render_user_prompt",
    "build_deepseek_request",
    "build_analysis_envelope",
    "validate_reason_code",
    "validate_llm_response",
]
