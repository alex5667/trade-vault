from prometheus_client import Counter

atr_promotion_policy_suggest_total = Counter(
    "atr_promotion_policy_suggest_total",
    "Total number of suggestions generated",
    ["reason_code"]
)

atr_promotion_policy_apply_total = Counter(
    "atr_promotion_policy_apply_total",
    "Total number of policies applied",
    ["stop_ttl_mode", "trailing_mode"]
)

atr_promotion_policy_active_total = Counter(
    "atr_promotion_policy_active_total",
    "Total active policies currently resolving",
    ["symbol", "scenario", "regime", "bucket"]
)

atr_policy_resolver_hit_total = Counter(
    "atr_policy_resolver_hit_total",
    "Active policy cache hits and misses",
    ["level"]
)

atr_policy_runtime_apply_total = Counter(
    "atr_policy_runtime_apply_total",
    "Active policy applied at runtime",
    ["layer", "mode"]
)

atr_policy_tg_proposal_publish_total = Counter(
    "atr_policy_tg_proposal_publish_total",
    "Total policy proposals published to Telegram",
    ["status"]
)

atr_policy_tg_callback_total = Counter(
    "atr_policy_tg_callback_total",
    "Total Telegram callbacks received",
    ["action"]
)

atr_policy_tg_callback_denied_total = Counter(
    "atr_policy_tg_callback_denied_total",
    "Total Telegram callbacks denied (auth/allowlist)",
    []
)

atr_policy_tg_callback_duplicate_total = Counter(
    "atr_policy_tg_callback_duplicate_total",
    "Total duplicate Telegram callbacks suppressed",
    []
)

atr_policy_tg_ack_total = Counter(
    "atr_policy_tg_ack_total",
    "Total acks sent back to Telegram",
    ["status"]
)

atr_policy_reconcile_apply_total = Counter(
    "atr_policy_reconcile_apply_total",
    "Total successful ATR policy reconciliations applied",
    []
)

atr_policy_tg_pack_publish_total = Counter(
    "atr_policy_tg_pack_publish_total",
    "Total ops pack publications via Telegram",
    []
)

atr_policy_tg_pack_action_total = Counter(
    "atr_policy_tg_pack_action_total",
    "Total ops pack actions invoked via Telegram",
    ["action"]
)

atr_policy_tg_pack_refresh_total = Counter(
    "atr_policy_tg_pack_refresh_total",
    "Total ops pack refreshes",
    []
)

atr_policy_tg_pack_revoke_total = Counter(
    "atr_policy_tg_pack_revoke_total",
    "Total ops pack revokes via active tab",
    []
)

atr_policy_tg_pack_error_total = Counter(
    "atr_policy_tg_pack_error_total",
    "Total ops pack errors",
    ["action"]
)
