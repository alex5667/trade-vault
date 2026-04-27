# Полная карта показателей Gate — CryptoOrderFlow

Ниже представлена детальная карта всех применяемых gate-ов (в пайплайне `CryptoOrderFlow`), их показателей, пороговых значений (с default-настройками) и поддержки **shadow mode** (режима аудита без жестких блокировок потока).

---

## **Порядок выполнения gate (pipeline)**

1. `DataQualityGate` / `HardDataQualityGate` (Жёсткое вето по качеству данных)
2. `RegimeGate` (Вето по рыночному режиму)
3. `LiquidityGate` + `RegimeSessionGate` (Режимы, сессии и параметры ликвидности)
4. `SignalConsistencyGate` / `ConsistencyGate` (Согласованность признаков сигнала)
5. `EntryPolicyGate` (Микроструктурный шок, поведение спреда и C2T)
6. `EdgeCostGate` (Ожидаемая стоимость исполнения и профит)
7. `SmtLeaderCoherenceGate` / `SmtCoherenceGate` (Следование за поводырем)
8. `CancellationSpikeGate` (L3-lite отказы ликвидности)
9. `StrongOfGate` — `eval_reversal` / `eval_continuation` (Энричмент сильными признаками)
10. `BurstCandidateSelector` / `BurstGate` (Выбор лучшего кандидата в окне)

---

## **1. DataQualityGate (Жёсткое вето по качеству данных)**

**Файлы:** `quality_gates.py`, `pre_publish_gates.py`
**Shadow Mode:** Нет. Это Hard-гейт, блокирующий "плохие" данные. Fail-open применяется при отсутствии метрик.

| **Показатель** | **Порог (ENV)** | **Default** | **reason_code / Детали** |
| --- | --- | --- | --- |
| Флаги качества | `DATA_VETO_FLAGS` | `""` (выкл) | `VETO_DATA_FLAGS` / `VETO_QUALITY_FLAG` (например: stale_l2, l3_missing) |
| Timestamp не epoch ms | `DATA_REQUIRE_EPOCH_TS` | `True` | `VETO_NON_EPOCH_TS` / Отсев ts в секундах или минутах |
| Максимальный лаг события | `DATA_MAX_EVENT_LAG_MS` | **2500 ms** | `VETO_EVENT_LAG` / Отказ сильно опоздавшим событиям |
| Timestamp из будущего | `DATA_MAX_FUTURE_SKEW_MS` | **200 ms** | `VETO_FUTURE_TS` |
| Out-of-order stream | `DATA_OUT_OF_ORDER_TOL_MS`| **1000 ms** | `VETO_OUT_OF_ORDER` / Нарушение монотонности таймстемпов |
| Карантин по времени | `DATA_QUARANTINE_VETO` | `True` | `VETO_TIME_QUARANTINE` |
| ATR устарел | `DATA_ATR_STALE_MAX_MS` | **60 000 ms** | `VETO_ATR_STALE` (только если ATR timestamp присутствует) |
| Строгий парсинг ATR TS | `DATA_STRICT_MISSING_ATR_TS`| `False` | `VETO_MISSING_ATR_TS` / Если True, банит без atr_ts_ms |
| Touch snapshot устарел | `DATA_TOUCH_STALE_VETO` | `False` | `VETO_TOUCH_STALE` / Зависит от списка `DATA_TOUCH_STALE_APPLY_KINDS` |

**ENV Включения:** `DATA_QUALITY_GATE_ENABLED=1`, `DATA_HARD_GATE_ENABLED=1` (HardDataQualityGate)

---

## **2. RegimeGate (Вето по рыночному режиму)**

**Файлы:** `quality_gates.py` 
**Shadow Mode:** Нет. Направлен на прямое фильтрование рыночных фаз.

| **Показатель** | **Запрет (ENV)** | **Default deny** | **reason_code** |
| --- | --- | --- | --- |
| Режим breakout | `REGIME_DENY_BREAKOUT` | `range,squeeze` | `VETO_REGIME` |
| Режим absorption | `REGIME_DENY_ABSORPTION` | `trending_bull,trending_bear,expansion` | `VETO_REGIME` |
| Режим extreme | `REGIME_DENY_EXTREME` | `""` (выкл) | `VETO_REGIME` |
| Режим obi_spike | `REGIME_DENY_OBI_SPIKE` | `""` (выкл) | `VETO_REGIME` |
| Обязательное наличие | `REGIME_REQUIRE_PRESENT` | `False` (fail-open) | `VETO_MISSING_REGIME` |

**Применяется к kinds:** `REGIME_APPLY_KINDS` = `breakout,absorption,extreme,obi_spike`
**ENV Включения:** `REGIME_GATE_ENABLED=1`

---

## **3. LiquidityGate / RegimeSessionGate (Ликвидность и сессии)**

**Файлы:** `quality_gates.py`, `pre_publish_gates.py`
**Shadow Mode:** Нет (работает как жесткий фильтр). Если метрика ликвидности отсутствует, логика `fail-open` (если не включен `STRICT_MISSING_METRICS`).

| **Показатель** | **Порог (ENV)** | **Default** | **reason_code / Детали** |
| --- | --- | --- | --- |
| Максимальный Spread | `LIQ_MAX_SPREAD_BPS` (`RS_SPREAD_MAX_BPS`) | **15.0 bps** | `VETO_SPREAD` / `VETO_RS_SPREAD` |
| Минимальный Depth 5L | `LIQ_MIN_DEPTH_5` (`RS_DEPTH_MIN`) | **0** (выкл) | `VETO_DEPTH` / Сверка min(depth20_bid, depth20_ask) |
| Минимальный Depth 20L| `RS_DEPTH20_MIN` | **0** (выкл) | `VETO_RS_DEPTH20` |
| Burst flip ratio | `LIQ_MAX_BURST_FLIP_RATIO` | **0.80** | `VETO_BURST_FLIP` / Нестабильность стакана |
| Разрешенные Сессии | `QUALITY_ALLOW_SESSIONS__{+KIND}`| — | `VETO_SESSION_NOT_ALLOWED` / Отбраковка по сессии (например us_main) |
| Границы ATR | `QUALITY_DAILY_ATR_BPS_MIN`/`MAX` | `0` / `10_000` | `VETO_DAILY_ATR_BPS_OUT_OF_RANGE` |
| Quantile ATR (14) | `QUALITY_ATR_Q14_MIN`/`MAX` | `-1.0` / `2.0` | `VETO_ATR_Q14_OUT_OF_RANGE` |

> **Drift Tightening:** `RS_DRIFT_TIGHTEN=1` — глубина домножается на `drift_factor^power` (увеличивает требования при плохом рынке).

---

## **4. SignalConsistencyGate (Логическая согласованность признаков)**

**Файлы:** `quality_gates.py`, `pre_publish_gates.py` (ConsistencyGate)
**Shadow Mode:** Нет. Непосредственно бракует псевдосигналы при несовпадении ключевых свойств модели.

| **Сигнал (Kind)** | **Показатель / Условие (ENV)** | **Default** | **reason_code** |
| --- | --- | --- | --- |
| **All** | Строгое требование всех метрик (`CONSISTENCY_STRICT_MISSING_METRICS`) | `False` | `VETO_MISSING_{METRIC}` |
| **Breakout** | Минимальный Z Delta (`CONS_BREAKOUT_MIN_Z`) | **2.0** | `VETO_BREAKOUT_Z_TOO_LOW` |
| | Совпадение OBI (`BREAKOUT_REQUIRE_OBI`) / (`BREAKOUT_REQUIRE_OBI20`) | `True` | `VETO_BREAKOUT_OBI_TOO_WEAK` / `SIGN_MISMATCH` |
| | Microprice shift bps (`BREAKOUT_MIN_MICROPRICE_SHIFT_BPS`) | **0.0** | `VETO_BREAKOUT_MICROSHIFT_TOO_LOW` |
| | Touch Fresh & Tag (`CONS_BREAKOUT_TOUCH_TAG_REQUIRED`) | `depletion` | `VETO_BREAKOUT_TOUCH_TAG_MISMATCH` |
| | Touch Rho & TradedW | `0.10` rho / `0.0`W | `VETO_BREAKOUT_TOUCH_RHO_LOW` |
| **Absorption** | Минимальный Z Delta (`CONS_ABSORPTION_MIN_Z`) | **2.0** | `VETO_ABSORPTION_Z_TOO_LOW` |
| | Наличие Weak Progress (`CONS_ABSORPTION_REQUIRE_WEAK_PROGRESS`) | `True` | `VETO_ABSORPTION_NO_WEAK_PROGRESS` |
| | Touch Tag (`CONS_ABSORPTION_TOUCH_TAG_REQUIRED`) | `refill` | `VETO_ABSORPTION_TOUCH_TAG_MISMATCH` |
| | Touch Rho (`CONS_ABSORPTION_MIN_TOUCH_RHO`) | `0.10` | `VETO_ABSORPTION_TOUCH_RHO_LOW` |
| **Extreme** | Макс Ratio отмен к трейдам L3 (`EXTREME_L3_MAX_CANCEL_TO_TRADE`) | `1e9` | `VETO_EXTREME_CANCEL_TO_TRADE_HIGH` |
| **Obi Spike**| Требовать Sustained OBI (`CONS_OBI_SPIKE_REQUIRE_SUSTAINED`)| `True` | `VETO_OBI_SPIKE_NOT_SUSTAINED` |

---

## **5. EntryPolicyGate (Поведение спреда и C2T)**

**Файлы:** `entry_policy_gate.py`
**Shadow Mode:** **Да.** Поддерживаются профили `GATE_PROFILE` = `default` / `soft` / `strict` / `hard`. При `default`/`soft` — только аннотирует `ctx` для дальнейшего ужесточения (audit-only), не блокирует поток.

| **Показатель** | **Порог (ENV)** | **Default** | **Действие при профиле** |
| --- | --- | --- | --- |
| Режим работы | `GATE_PROFILE` | `default` | Если default/soft -> Не ветирует (`audit_only`) |
| Spread Shock soft | `ENTRY_SPREAD_SHOCK_BPS` | **35 bps** | soft flag, повышает `tighten_k` (1.1 или 1.25) |
| Spread Shock hard | `ENTRY_SPREAD_SHOCK_BPS_HARD` | **60 bps** | Вето в `strict` и `hard` (VETO_SPREAD_SHOCK) |
| Burst flip | `ENTRY_BURST_FLIP_MAX` | **0.85** | soft flag |
| Cancel-2-Trade | `ENTRY_C2T_MAX` | **8.0** | soft flag |
| Feature Drift | `FEATURE_DRIFT_ENABLED` & `FEATURE_DRIFT_Z` | `False` / **6.0** | soft flag / Вето в `hard` |
| BookTrade Stale Soft| `ENTRY_BOOK_STALE_SOFT_MS` | **600 ms**   | soft flag |
| BookTrade Stale Hard| `ENTRY_BOOK_STALE_HARD_MS` | **1200 ms**  | Вето в `hard` |
| Adverse Cross Soft | `ENTRY_ADVERSE_CROSS_SOFT_BPS`| **0.5 bps**  | soft flag |
| Adverse Cross Hard | `ENTRY_ADVERSE_CROSS_HARD_BPS`| **1.5 bps**  | Вето в `hard` |

---

## **6. EdgeCostGate (Анализ ожидаемого заработка vs косты)**

**Файлы:** `edge_cost_gate.py`
**Shadow Mode:** Модуль TCA Execution Health имеет `EXEC_HEALTH_MODE`: `off` / `monitor` (shadow) / `tighten` / `veto` / `auto`. Итоговый Gate жестко ветирует, если ожидаемый профит ниже порога K * Costs.

| **Модель** | **Показатель (ENV)** | **Default** | **Описание / Детали** |
| --- | --- | --- | --- |
| Основная | `EDGE_COST_K` | **4.0** | `move_bps >= K * (fees + slippage)` (`REASON_BELOW_K`) |
| Режимы | `EDGE_EXPECTED_MOVE_MODE` | `tp1` | `tp1`, `rr`, `atr`, `ev`. EV использует метрики вероятности. |
| Комиссии | `EDGE_FEES_BPS_DEFAULT` (`CRYPTO_COMMISSION_RATE` * 2) | **4.0 bps** | Хардкод по умолчанию (либо с env) |
| Slippage | `EDGE_SLIPPAGE_BPS_DEFAULT` | **4.0 bps** | Либо default, либо половина спреда, либо Slippage EMA из Redis. |
| EV Mode | `EDGE_EV_P_MIN` | **0.55** | Минимальная вероятность для EV-mode (включая per-kind пороги). |
| Dynamic K | `EDGE_EV_DYNAMIC_K_ENABLED` | `False` | Домнoжает множитель K в зависимости от ATR волатильности среды. |
| **Exec Health** | `EXEC_HEALTH_MODE` | `off`/`auto`| В `monitor` (shadow) - аннотирует. В `tighten` добавляет add_bps к слиппеджу. В `veto` лочит. |
| Exec IS p95 | `EXEC_MAX_IS_P95_BPS` | **0.0** (выкл) | Превышение Execution Implementation Shortfall |
| Exec Impact | `EXEC_MAX_PERM_IMPACT_P95_BPS` | **0.0** (выкл) | Превышение Permanent Impact (TCA) |

---

## **7. SmtLeaderCoherenceGate (Полеты за поводырем / SMT)**

**Файлы:** `smt_coherence_gate.py`, `pre_publish_gates.py`
**Shadow Mode:** **Да.** Режим переключается через `SMT_LEADER_MODE` (`observe` = shadow mode / `veto`).

| **Показатель** | **Порог (ENV)** | **Default** | **reason_code / Детали** |
| --- | --- | --- | --- |
| Режим работы | `SMT_LEADER_MODE` | `observe` | Если `observe`, просто аннотирует `ctx.smt_*` (`SMT_OBSERVE`) |
| Степень поддержки | `SMT_COH_HI_THRESHOLD` (`RELIABILITY_SMT_COH_THR`) | **0.65** | Признак высокой когерентности SMT корзины (`coh >= threshold`) |
| Условия Вето | Leader Confirm = 1 AND Coh_hi = 1 AND Align = 0 | — | `VETO_SMT_COUNTERTREND` / Контртренд подтвержденному лидеру SMT |
| News Block | Стейт корзины (news_blocked=1) | — | `VETO_SMT_NEWS_GATE` (блокировка новостями в режиме `veto`) |
| SMT V2 Золотой билет| совпадение symbol == pick | — | Пропускает сигнал (обходит вето контртренда: `SMT_GOLDEN_REVERSAL`) |

---

## **8. CancellationSpikeGate (Отказы и импульсы ликвидности)**

**Файлы:** `cancellation_spike_gate.py`
**Shadow Mode:** **Да.** Режим переключается через `OF_CANCEL_SPIKE_MODE` (`monitor` = shadow mode / `veto`).

| **Показатель** | **Порог (ENV)** | **Default** | **Детали** |
| --- | --- | --- | --- |
| Режим работы | `OF_CANCEL_SPIKE_MODE` | `veto` | `monitor` -> `cancel_spike_monitor_ok` (без вето) |
| Ratio Threshold | `OF_CANCEL_SPIKE_RATIO_TH` | **3.0** | Скачок доли отмен в N раз выше baseline. |
| Robust Z-Score | `OF_CANCEL_SPIKE_Z_TH` | **3.5** | Использование robust z-score (median/MAD) для спайков. |
| Min Baseline | `OF_CANCEL_SPIKE_MIN_BASELINE`| **0.0** | Минимальный бейзлайн (EMA) отмен для учета всплеска. |
| Условие Вето 1 | Withdrawal Support | — | Снятие ликвидности на стороне поддержки (`support_pulled`) |
| Условие Вето 2 | Pull w/o aggression | `min_taker_rate: 0.0` | Отмена противоположной стороны БЕЗ агрессии тейкера (защита от spoofing, fake-impulse) |

---

## **9. StrongOfGate (Обогащение сильными свойствами)**

**Файлы:** `strong_of_gate.py`
**Shadow Mode:** Это гейт типа Feature Enrichment. Он не бракует, а добавляет в метрики `gate_bits` (усиливает доверие к уже готовому сигналу).

*   **eval_reversal:** Требуется 2 из 3 условий:
    1.  `(abs(delta_z) >= strong_z_min) AND (weak_progress)`
    2.  `sweep_recent AND reclaim_recent`
    3.  `obi_stable OR iceberg_strict OR fp_edge_absorb OR ofi_leg`
*   **eval_continuation:** Требуется 2 из 3 условий:
    1.  `hidden_ctx_recent AND direction == trend_dir`
    2.  `obi_stable OR iceberg_strict OR ofi_leg OR fp_edge_absorb`
    3.  `cont_ctx_recent` (недавняя контртрендовая абсорбция)

Включается агрегация в `StrongGateDecision(gate_bits)`.

---

## **10. BurstCandidateSelector**

**Файлы:** `burst_gate.py`
Выполняет пре-кулдаун селекцию: в рамках заданного временного окна `window_ms` (по умолчанию 2500 ms) удерживается и в итоге выдаётся (emit) только **один лучший кандидат** с максимальным `score`. Это механизм оптимизации и убора мелкого спама в моменты пиков волатильности (Burst limit window).
