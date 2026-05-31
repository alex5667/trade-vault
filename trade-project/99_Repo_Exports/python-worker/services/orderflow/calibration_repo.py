import json
import logging
from typing import Any

from core.book_rate_calibrator import BookRateCalibrator
from core.dyn_cfg_keys import DynCfgKeys as DK
from services.orderflow.metrics import calib_persist_total, log_silent_error
from core.redis_keys import RedisStreams as RS

logger = logging.getLogger("crypto_orderflow.calibration_repo")

class CalibrationRepository:
    def __init__(self, *, redis_ticks, pm, logger_service=None):
        self.r = redis_ticks
        self.pm = pm
        self.log = logger_service or logger

    async def load_all(self, runtime) -> None:
        """
        Redis-first + PG safety-net. Applies into runtime calibrators.
        """
        sym = str(runtime.symbol)

        # 1. Load EffQuote (effq)
        await self._load_effq(runtime)

        # 2. Load BookRate (bookrate)
        await self._load_bookrate(runtime)

        # 3. Load DeltaNotional (dn) and Tick DeltaNotional (tick_dn)
        await self._load_dn(runtime)
        await self._load_tick_dn(runtime)

        # 4. Load ATR BPS
        await self._load_atr_bps(runtime)

        # 5. Load ATR Sanity
        await self._load_atr_sanity(runtime)

        # 6. Load ATR TF
        await self._load_atr_tf(runtime)

        # 7. Redundant load from PostgreSQL (Safety Net)
        if self.pm:
            try:
                states = await self.pm.load_calibration_states(sym)
                for s in states:
                    kind = s.get("kind")
                    state_json = s.get("state_json")
                    if not state_json:
                        continue

                    if kind == "effq":
                        runtime.eff_calib.load_regime_state(state_json)
                    elif kind == "dn":
                        runtime.dn_calib.load_regime_state(state_json)
                    elif kind == "tick_dn":
                        runtime.tick_dn_calib.load_regime_state(state_json)
                    elif kind == "bookrate":
                        runtime.br_calib.load_regime_state(state_json)
                    elif kind == "atrbps":
                        runtime.atr_bps_calib.load_regime_state(state_json)
                runtime._pg_loaded = True
            except Exception as e:
                self.log.warning(f"⚠️ Failed to load redundant calibration from PG for {sym}: {e}")

    async def _load_effq(self, runtime):
        sym = str(runtime.symbol)
        prefix = str(runtime.config.get("calib_key_prefix", "calib:effq"))
        set_prefix = str(runtime.config.get("calib_regimes_set_prefix", "calib:effq:regimes"))
        regimes_key = f"{set_prefix}:{sym}"
        try:
            regimes = list(await self.r.smembers(regimes_key))
            if "na" not in regimes:
                regimes.append("na")
            for rg in regimes:
                key = f"{prefix}:{sym}:{rg}"
                raw = await self.r.get(key)
                if raw:
                    st = json.loads(raw)
                    if isinstance(st, dict):
                        runtime.eff_calib.load_regime_state(st)
        except Exception as exc:
            log_silent_error(exc, 'calib_load_failure', sym, 'repo:load_effq')

    async def _load_bookrate(self, runtime):
        sym = str(runtime.symbol)
        prefix = str(runtime.config.get("book_calib_key_prefix", "calib:bookrate"))
        set_prefix = str(runtime.config.get("book_calib_regimes_set_prefix", "calib:bookrate:regimes"))
        regimes_key = f"{set_prefix}:{sym}"
        try:
            regimes = list(await self.r.smembers(regimes_key))
            if "na" not in regimes:
                regimes.append("na")
            for rg in regimes:
                key = f"{prefix}:{sym}:{rg}"
                raw = await self.r.get(key)
                if raw:
                    st = BookRateCalibrator.loads(raw)
                    if isinstance(st, dict):
                        runtime.br_calib.load_regime_state(st)
        except Exception as exc:
            log_silent_error(exc, 'calib_load_failure', sym, 'repo:load_bookrate')

    async def _load_dn(self, runtime):
        sym = str(runtime.symbol)
        prefix = str(runtime.config.get("dn_key_prefix", "calib:dn"))
        set_prefix = str(runtime.config.get("dn_regimes_set_prefix", "calib:dn:regimes"))
        regimes_key = f"{set_prefix}:{sym}"
        try:
            regimes = list(await self.r.smembers(regimes_key))
            if "na" not in regimes:
                regimes.append("na")
            for rg in regimes:
                key = f"{prefix}:{sym}:{rg}"
                raw = await self.r.get(key)
                if raw:
                    st = json.loads(raw)
                    if isinstance(st, dict):
                        runtime.dn_calib.load_regime_state(st)
        except Exception as exc:
            log_silent_error(exc, 'calib_load_failure', sym, 'repo:load_dn')

    async def _load_tick_dn(self, runtime):
        sym = str(runtime.symbol)
        prefix = str(runtime.config.get("tick_dn_key_prefix", "calib:tick_dn"))
        set_prefix = str(runtime.config.get("tick_dn_regimes_set_prefix", "calib:tick_dn:regimes"))
        regimes_key = f"{set_prefix}:{sym}"
        try:
            regimes = list(await self.r.smembers(regimes_key))
            if "na" not in regimes:
                regimes.append("na")
            for rg in regimes:
                key = f"{prefix}:{sym}:{rg}"
                raw = await self.r.get(key)
                if raw:
                    st = json.loads(raw)
                    if isinstance(st, dict):
                        runtime.tick_dn_calib.load_regime_state(st)
        except Exception as exc:
            log_silent_error(exc, 'calib_load_failure', sym, 'repo:load_tick_dn')

    async def _load_atr_bps(self, runtime):
        sym = str(runtime.symbol)
        prefix = str(runtime.config.get("atr_bps_calib_key_prefix", "calib:atrbps"))
        set_prefix = str(runtime.config.get("atr_bps_calib_regimes_set_prefix", "calib:atrbps:regimes"))
        regimes_key = f"{set_prefix}:{sym}"
        try:
            regimes = list(await self.r.smembers(regimes_key))
            if "na" not in regimes:
                regimes.append("na")
            for rg in regimes:
                key = f"{prefix}:{sym}:{rg}"
                raw = await self.r.get(key)
                if raw:
                    st = json.loads(raw)
                    if isinstance(st, dict):
                        runtime.atr_bps_calib.load_regime_state(st)
        except Exception as exc:
            log_silent_error(exc, 'calib_load_failure', sym, 'repo:load_atr_bps')

    async def _load_atr_sanity(self, runtime):
        sym = str(runtime.symbol)
        prefix = str(runtime.config.get("atr_sanity_key_prefix", "calib:atrbps:src"))
        set_prefix = str(runtime.config.get("atr_sanity_set_prefix", "calib:atrbps:src:set"))
        set_key = f"{set_prefix}:{sym}"
        try:
            tfs = list(await self.r.smembers(set_key))
            for tfk in tfs:
                key = f"{prefix}:{sym}:{tfk}"
                raw = await self.r.get(key)
                if raw:
                    st = json.loads(raw)
                    if isinstance(st, dict):
                        runtime.atr_sanity.load_state(st)
        except Exception as exc:
            log_silent_error(exc, 'calib_load_failure', sym, 'repo:load_atr_sanity')

    async def _load_atr_tf(self, runtime):
        sym = str(runtime.symbol)

        # 1. Load Choice (Active Selection)
        try:
            prefix = str(runtime.config.get("atr_tf_key_prefix", "calib:atr_tf"))
            sym_upper = str(runtime.symbol).upper()
            key = f"{prefix}:{sym_upper}"
            raw = await self.r.get(key)
            if raw:
                st = json.loads(raw)
                tf = (st.get("tf") or "")
                if tf:
                    runtime.dynamic_cfg[DK.ATR_TF_SELECTED] = tf
                    runtime.dynamic_cfg[DK.ATR_TF_SRC] = (st.get("src") or "na")
                    runtime.dynamic_cfg[DK.ATR_TF_SCORE] = float(st.get("score", 0.0) or 0.0)
                    runtime.dynamic_cfg[DK.ATR_TF_UPDATED_TS_MS] = int(st.get("updated_ts_ms", 0) or 0)
        except Exception as exc:
            log_silent_error(exc, 'calib_load_failure', sym, 'repo:load_atr_tf_choice')

        # 2. Load Calibrator States (History/Regimes)
        if hasattr(runtime, "atr_tf_calib"):
            c_prefix = str(runtime.config.get("atr_tf_calib_key_prefix", "calib:atrtf"))
            c_set_prefix = str(runtime.config.get("atr_tf_calib_regimes_set_prefix", "calib:atrtf:regimes"))
            regimes_key = f"{c_set_prefix}:{sym}"
            try:
                regimes = list(await self.r.smembers(regimes_key))
                if "na" not in regimes:
                    regimes.append("na")
                for rg in regimes:
                    key = f"{c_prefix}:{sym}:{rg}"
                    raw = await self.r.get(key)
                    if raw:
                        st = json.loads(raw)
                        if isinstance(st, dict):
                            runtime.atr_tf_calib.load_regime_state(st)
            except Exception as exc:
                log_silent_error(exc, 'calib_load_failure', sym, 'repo:load_atr_tf_regimes')

    async def save_effq(self, runtime, *, regime: str, ts_ms: int) -> None:
        cfg = runtime.config
        sym = str(runtime.symbol)
        rg = (regime or "na")

        prefix = (cfg.get("calib_key_prefix", "calib:effq"))
        set_prefix = (cfg.get("calib_regimes_set_prefix", "calib:effq:regimes"))
        ttl_sec = int(cfg.get("calib_ttl_sec", 7 * 24 * 3600))
        key = f"{prefix}:{sym}:{rg}"
        regimes_key = f"{set_prefix}:{sym}"

        payload = runtime.eff_calib.dump_regime_state(symbol=sym, regime=rg, updated_ts_ms=int(ts_ms))

        # 1. Redis State
        try:
            await self.r.set(key, json.dumps(payload, ensure_ascii=False), ex=ttl_sec)
            await self.r.sadd(regimes_key, rg)
            await self.r.expire(regimes_key, ttl_sec)
        except Exception as exc:
            log_silent_error(exc, "redis_write_failure", sym, "calib_repo:save_effq:redis")

        # 2. PG Redundancy
        if self.pm:
            try:
                await self.pm.save_calibration_state(sym, rg, "effq", int(ts_ms), payload)
            except Exception as exc:
                log_silent_error(exc, "persist_failure", sym, "calib_repo:save_effq:pg_save")

        # 3. Audit Stream (with Dedup)
        try:
            if bool(int(cfg.get("calib_audit_enable", 1))):
                audit_stream = (cfg.get("calib_audit_stream", RS.CALIB_AUDIT))
                maxlen = int(cfg.get("calib_audit_stream_maxlen", 200000))

                th = runtime.eff_calib.thresholds(
                    regime=rg,
                    default_eff_th=float(cfg.get("abs_lvl_eff_quote_th", 0.0020)),
                    default_min_qd=float(cfg.get("abs_lvl_min_quote_delta", 0.0)),
                )
                ev = runtime.eff_calib.audit_event(
                    symbol=sym,
                    regime=rg,
                    ts_ms=int(ts_ms),
                    eff_quote_th=float(th.eff_quote_th),
                    min_quote_delta=float(th.min_quote_delta),
                    src=str(th.src),
                )

                last_key = f"{prefix}:last_hash:{sym}:{rg}"
                prev = await self.r.get(last_key)
                if (prev or "") != (ev.get("state_hash", "")):
                    await self.r.set(last_key, (ev.get("state_hash", "")), ex=ttl_sec)
                    await self.r.xadd(
                        audit_stream,
                        fields={"payload": json.dumps(ev, ensure_ascii=False)},
                        maxlen=maxlen,
                        approximate=True,
                    )
        except Exception as exc:
            log_silent_error(exc, "persist_failure", sym, "calib_repo:save_effq:audit_stream")

        # Metrics
        if calib_persist_total:
            calib_persist_total.labels(kind="effq", symbol=sym, regime=rg).inc()

    async def save_bookrate(self, runtime, *, regime: str, ts_ms: int) -> None:
        cfg = runtime.config
        sym = str(runtime.symbol)
        rg = (regime or "na")

        prefix = (cfg.get("book_calib_key_prefix", "calib:bookrate"))
        set_prefix = (cfg.get("book_calib_regimes_set_prefix", "calib:bookrate:regimes"))
        ttl_sec = int(cfg.get("book_calib_ttl_sec", 7 * 24 * 3600))
        key = f"{prefix}:{sym}:{rg}"
        regimes_key = f"{set_prefix}:{sym}"

        payload = runtime.br_calib.dump_regime_state(symbol=sym, regime=rg, updated_ts_ms=int(ts_ms))

        try:
            await self.r.set(key, json.dumps(payload, ensure_ascii=False), ex=ttl_sec)
            await self.r.sadd(regimes_key, rg)
            await self.r.expire(regimes_key, ttl_sec)
        except Exception as exc:
            log_silent_error(exc, 'redis_write_failure', sym, 'calib_repo:save_bookrate:redis')

        if self.pm:
            try:
                await self.pm.save_calibration_state(sym, rg, "bookrate", int(ts_ms), payload)
            except Exception as exc:
                log_silent_error(exc, 'persist_failure', sym, 'calib_repo:save_bookrate:pg_save')

        if calib_persist_total:
            calib_persist_total.labels(kind="bookrate", symbol=sym, regime=rg).inc()

    async def save_dn(self, runtime, *, regime: str, ts_ms: int) -> None:
        cfg = runtime.config
        sym = str(runtime.symbol)
        rg = (regime or "na")

        prefix = (cfg.get("calib_dn_prefix", "calib:dn"))
        set_prefix = (cfg.get("calib_dn_set_prefix", "calib:dn:regimes"))
        ttl_sec = int(cfg.get("calib_ttl_sec", 7 * 24 * 3600))
        key = f"{prefix}:{sym}:{rg}"
        regimes_key = f"{set_prefix}:{sym}"

        payload = runtime.dn_calib.dump_regime_state(symbol=sym, regime=rg, updated_ts_ms=int(ts_ms))

        try:
            await self.r.set(key, json.dumps(payload, ensure_ascii=False), ex=ttl_sec)
            await self.r.sadd(regimes_key, rg)
            await self.r.expire(regimes_key, ttl_sec)
        except Exception as exc:
            log_silent_error(exc, 'redis_write_failure', sym, 'calib_repo:save_dn:redis')

        if self.pm:
            try:
                await self.pm.save_calibration_state(sym, rg, "dn", int(ts_ms), payload)
            except Exception as exc:
                log_silent_error(exc, 'persist_failure', sym, 'calib_repo:save_dn:pg_save')

        if calib_persist_total:
            calib_persist_total.labels(kind="dn", symbol=sym, regime=rg).inc()

    async def save_tick_dn(self, runtime, *, regime: str, ts_ms: int) -> None:
        cfg = runtime.config
        sym = str(runtime.symbol)
        rg = (regime or "na")

        prefix = (cfg.get("tick_dn_calib_prefix", "calib:tick_dn"))
        set_prefix = (cfg.get("tick_dn_calib_set_prefix", "calib:tick_dn:regimes"))
        ttl_sec = int(cfg.get("calib_ttl_sec", 7 * 24 * 3600))
        key = f"{prefix}:{sym}:{rg}"
        regimes_key = f"{set_prefix}:{sym}"

        payload = runtime.tick_dn_calib.dump_regime_state(symbol=sym, regime=rg, updated_ts_ms=int(ts_ms))

        try:
            await self.r.set(key, json.dumps(payload, ensure_ascii=False), ex=ttl_sec)
            await self.r.sadd(regimes_key, rg)
            await self.r.expire(regimes_key, ttl_sec)
        except Exception as exc:
            log_silent_error(exc, 'redis_write_failure', sym, 'calib_repo:save_tick_dn:redis')

        if self.pm:
            try:
                await self.pm.save_calibration_state(sym, rg, "tick_dn", int(ts_ms), payload)
            except Exception as exc:
                log_silent_error(exc, 'persist_failure', sym, 'calib_repo:save_tick_dn:pg_save')

        if calib_persist_total:
            calib_persist_total.labels(kind="tick_dn", symbol=sym, regime=rg).inc()

    async def save_atr_bps(self, runtime, *, regime: str, ts_ms: int) -> None:
        cfg = runtime.config
        sym = str(runtime.symbol)
        rg = (regime or "na")

        prefix = (cfg.get("atr_bps_key_prefix", "calib:atrbps"))
        set_prefix = (cfg.get("atr_bps_regimes_set_prefix", "calib:atrbps:regimes"))
        ttl_sec = int(cfg.get("calib_ttl_sec", 7 * 24 * 3600))
        key = f"{prefix}:{sym}:{rg}"
        regimes_key = f"{set_prefix}:{sym}"

        payload = runtime.atr_bps_calib.dump_regime_state(symbol=sym, regime=rg, updated_ts_ms=int(ts_ms))

        try:
            await self.r.set(key, json.dumps(payload, ensure_ascii=False), ex=ttl_sec)
            await self.r.sadd(regimes_key, rg)
            await self.r.expire(regimes_key, ttl_sec)
        except Exception as exc:
            log_silent_error(exc, 'redis_write_failure', sym, 'calib_repo:save_atr_bps:redis')

        if self.pm:
            try:
                await self.pm.save_calibration_state(sym, rg, "atrbps", int(ts_ms), payload)
            except Exception as exc:
                log_silent_error(exc, 'persist_failure', sym, 'calib_repo:save_atr_bps:pg_save')

        if calib_persist_total:
            calib_persist_total.labels(kind="atrbps", symbol=sym, regime=rg).inc()

    async def save_atr_sanity(self, runtime, *, tf_norm: str, ts_ms: int) -> None:
        cfg = runtime.config
        sym = str(runtime.symbol)
        tfk = (tf_norm or "M1").upper()
        prefix = (cfg.get("atr_sanity_key_prefix", "calib:atrbps:src"))
        set_prefix = (cfg.get("atr_sanity_set_prefix", "calib:atrbps:src:set"))
        ttl_sec = int(cfg.get("atr_sanity_ttl_sec", 7 * 24 * 3600))
        key = f"{prefix}:{sym}:{tfk}"
        set_key = f"{set_prefix}:{sym}"

        payload = runtime.atr_sanity.dump_state(symbol=sym, tf_norm=tfk, updated_ts_ms=int(ts_ms))

        try:
            await self.r.set(key, json.dumps(payload, ensure_ascii=False), ex=ttl_sec)
            await self.r.sadd(set_key, tfk)
            await self.r.expire(set_key, ttl_sec)
        except Exception as exc:
            log_silent_error(exc, 'redis_write_failure', sym, 'calib_repo:save_atr_sanity:redis')

    async def save_atr_tf_choice(self, runtime, *, choice_state: dict[str, Any], ts_ms: int) -> None:
        cfg = runtime.config
        sym = str(runtime.symbol).upper()
        prefix = (cfg.get("atr_tf_key_prefix", "calib:atr_tf"))
        ttl_sec = int(cfg.get("atr_tf_ttl_sec", 7 * 24 * 3600))
        key = f"{prefix}:{sym}"
        try:
            await self.r.set(key, json.dumps(choice_state, ensure_ascii=False), ex=ttl_sec)
        except Exception as exc:
            log_silent_error(exc, 'persist_failure', sym, 'calib_repo:save_atr_tf_choice')

    async def save_atr_tf_regime(self, runtime, *, regime: str, ts_ms: int) -> None:
        cfg = runtime.config
        sym = str(runtime.symbol)
        rg = (regime or "na")
        prefix = (cfg.get("calib_atr_tf_key_prefix", "calib:atrtf"))
        set_prefix = (cfg.get("calib_atr_tf_regimes_set_prefix", "calib:atrtf:regimes"))
        ttl_sec = int(cfg.get("calib_ttl_sec", 7 * 24 * 3600))
        key = f"{prefix}:{sym}:{rg}"
        regimes_key = f"{set_prefix}:{sym}"

        if hasattr(runtime, "atr_tf_calib"):
            payload = runtime.atr_tf_calib.dump_regime_state(symbol=sym, regime=rg, updated_ts_ms=int(ts_ms))
            try:
                await self.r.set(key, json.dumps(payload, ensure_ascii=False), ex=ttl_sec)
                await self.r.sadd(regimes_key, rg)
                await self.r.expire(regimes_key, ttl_sec)
            except Exception as exc:
                log_silent_error(exc, 'redis_write_failure', sym, 'calib_repo:save_atr_tf_regime:redis')
