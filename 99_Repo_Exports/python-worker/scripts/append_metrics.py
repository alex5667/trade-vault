with open("services/orderflow/metrics.py", "a") as f:
    f.write("\n")
    f.write("tick_uid_missing_total = _get_or_create_prom_counter('tick_uid_missing_total', 'Tick UID missing', ['symbol'])\n")
    f.write("tick_trade_id_missing_total = _get_or_create_prom_counter('tick_trade_id_missing_total', 'Tick Trade ID missing', ['symbol'])\n")
    f.write("ticks_deduped_total = _get_or_create_prom_counter('ticks_deduped_total', 'Ticks deduped', ['symbol'])\n")
    f.write("redis_pel_claimed_total = _get_or_create_prom_counter('redis_pel_claimed_total', 'Redis PEL messages claimed', ['symbol', 'stream'])\n")
    f.write("ticks_quarantined_total = _get_or_create_prom_counter('ticks_quarantined_total', 'Ticks quarantined', ['symbol', 'reason'])\n")
    f.write("ticks_schema_invalid_total = _get_or_create_prom_counter('ticks_schema_invalid_total', 'Ticks schema invalid', ['symbol', 'field'])\n")
print("Metrics appended.")
