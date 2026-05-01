from __future__ import annotations
"""
SignalPerformanceTracker: Online performance analysis for executed signals.

Tracks TTD, MFE/MAE, and outcome classification for each signal.
Integrates with TimescaleDB for historical analysis and TTD optimization.
"""

from utils.time_utils import get_ny_time_millis

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional, TYPE_CHECKING, Set
from collections import deque, defaultdict
import time

from .context import SignalContext
from .models import Bar1m, Side, ExecutionPlan
from .repository import SignalRepository

if TYPE_CHECKING:
    from .bus import SignalBus

# Импортируем TimeSampler для троттлинга housekeeping
try:
    from common.log_sampling import TimeSampler
except ImportError:
    # Fallback если модуль недоступен
    class TimeSampler:
        def __init__(self, every_ms: int):
            self.every_ms = every_ms
            self._next_ms = 0
        
        def hit(self) -> bool:
            now_ms = get_ny_time_millis()
            if now_ms >= self._next_ms:
                self._next_ms = now_ms + self.every_ms
                return True
            return False


class Outcome(str, Enum):
    TARGET_HIT = "target_hit"
    STOP_HIT = "stop_hit"
    BREAKEVEN = "breakeven"
    EXPIRED_NO_ENTRY = "expired_no_entry"
    EXPIRED_NO_TARGET = "expired_no_target"
    MANUAL_EXIT = "manual_exit"
    UNKNOWN = "unknown"


@dataclass
class SignalPerfState:
    """
    Internal state for tracking one signal.
    """
    signal_id: str
    symbol: str
    setup_type: str
    side: Side

    ts_signal: datetime
    price_at_signal: float
    atr_1m: float

    stop_price: float

    expiry_bars: int          # planned lifetime (never enter after this)
    max_ttd_bars: int         # for measuring TTD

    # execution fields
    ts_entry: Optional[datetime] = None
    entry_price: Optional[float] = None
    ts_exit: Optional[datetime] = None
    exit_price: Optional[float] = None

    # NEW: бар-индексы (если у вас есть понятие bar_idx / candle_idx)
    # Позволяют отслеживать TTL в барах для финализации зависших позиций
    bar_signal: Optional[int] = None
    bar_entry: Optional[int] = None
    bar_exit: Optional[int] = None

    # TTD / MFE / MAE
    ttd_bars: Optional[int] = None
    ttd_seconds: Optional[int] = None
    mfe_R: float = 0.0
    mae_R: float = 0.0

    # counters
    bars_seen: int = 0
    bars_to_entry: Optional[int] = None
    bars_to_exit: Optional[int] = None

    # flags
    expired_without_entry: bool = False
    finalized: bool = False
    outcome: Outcome = Outcome.UNKNOWN
    notes: str = ""

    # NEW: причина финализации (для аудита и отладки)
    finalize_reason: Optional[str] = None

    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SignalPerformance:
    """
    Performance snapshot for TimescaleDB.
    """
    signal_id: str
    symbol: str
    setup_type: str
    side: Side

    ts_signal: datetime
    ts_entry: Optional[datetime]
    ts_exit: Optional[datetime]

    price_at_signal: float
    entry_price: Optional[float]
    exit_price: Optional[float]
    stop_price: Optional[float]

    realized_R: Optional[float]
    mfe_R: Optional[float]
    mae_R: Optional[float]

    ttd_bars: Optional[int]
    ttd_seconds: Optional[int]

    bars_to_entry: Optional[int]
    bars_to_exit: Optional[int]

    outcome: Outcome
    notes: str
    extra: Dict[str, Any]


class SignalPerformanceTracker:
    """
    Online tracker:
    - Register signal + ExecutionPlan
    - Feed 1m bars (on_bar)
    - Feed execution events (on_execution_event)
    - Finalize and store in Timescale when done
    
    NEW: Housekeeping для финализации зависших позиций:
    - Автоматически финализирует позиции, которые "вошли, но не вышли" после max_lifetime_bars_after_entry
    - Защита от "поздних exit событий" через LRU финализированных signal_id
    - Идемпотентная финализация (повторные вызовы безопасны)
    """

    def __init__(
        self, 
        repo: SignalRepository, 
        ttd_target_R: float = 1.0, 
        max_ttd_bars: int = 30, 
        bus: Optional["SignalBus"] = None,
        max_lifetime_bars_after_entry: Optional[int] = None,
        max_lifetime_ms_after_entry: Optional[int] = None,
        housekeeping_every_ms: int = 1000,
    ):
        self.repo = repo
        self.bus = bus
        self.ttd_target_R = ttd_target_R
        self.max_ttd_bars = max_ttd_bars
        self._states: Dict[str, SignalPerfState] = {}
        
        # ----------------------------
        # NEW: Конфиги "вошли, но не вышли"
        # ----------------------------
        # Баровый TTL — основной (на 1m барах).
        # Time-fallback — если по какой-то причине on_bar не зовётся регулярно.
        #
        # По умолчанию:
        #   bars_after_entry = max(3*expiry_bars, 180) задаётся при регистрации сигнала
        #   ms_after_entry   = 0 (выключено)
        #
        # Если хотите включить time-fallback:
        #   PERF_MAX_LIFETIME_MS_AFTER_ENTRY=3600000  (например, 1 час)
        self._default_max_lifetime_bars_after_entry = (
            int(max_lifetime_bars_after_entry)
            if max_lifetime_bars_after_entry is not None
            else int(os.getenv("PERF_MAX_LIFETIME_BARS_AFTER_ENTRY", "180"))
        )
        self._default_max_lifetime_ms_after_entry = (
            int(max_lifetime_ms_after_entry)
            if max_lifetime_ms_after_entry is not None
            else int(os.getenv("PERF_MAX_LIFETIME_MS_AFTER_ENTRY", "3600000"))
        )
        
        # Для обратной совместимости сохраняем публичные атрибуты
        self.max_lifetime_bars_after_entry = self._default_max_lifetime_bars_after_entry
        self.max_lifetime_ms_after_entry = self._default_max_lifetime_ms_after_entry
        
        # NEW: троттлинг housekeeping (не гонять O(N) на каждом баре/событии)
        # КРИТИЧНО: TimeSampler (и fallback) работают в миллисекундах, НЕ секундах!
        self._housekeeping_sampler = TimeSampler(int(housekeeping_every_ms))
        
        # NEW: защита от "поздних exit событий".
        # Делаем O(1) membership через set + ручной LRU-буфер.
        self._finalized_set: Set[str] = set()
        self._finalized_lru: deque[str] = deque()
        self._finalized_lru_max: int = 4096
        
        # NEW: индекс по символу, чтобы on_bar_1m работал быстро
        self._ids_by_symbol: Dict[str, Set[str]] = defaultdict(set)

    # --- Helper methods для работы с datetime ---
    
    @staticmethod
    def _dt_to_naive_utc(dt: datetime) -> datetime:
        """
        Приводим datetime к naive UTC, чтобы безопасно сравнивать с datetime.utcfromtimestamp().
        Если dt уже naive — считаем, что он в UTC (как у вас обычно в пайплайне).
        """
        if dt.tzinfo is None:
            return dt
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    
    @staticmethod
    def _naive_utc_from_ms(ts_ms: int) -> datetime:
        """Конвертирует timestamp в миллисекундах в naive UTC datetime."""
        return datetime.utcfromtimestamp(ts_ms / 1000.0)

    @staticmethod
    def _dt_to_epoch_ms(dt: datetime) -> int:
        """Naive datetime трактуем как UTC (как у вас в трекере)."""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    def _remember_finalized(self, signal_id: str) -> None:
        """Запоминаем финализированный signal_id, чтобы поздние события не трогали state."""
        if not signal_id:
            return
        if signal_id in self._finalized_set:
            return
        self._finalized_set.add(signal_id)
        self._finalized_lru.append(signal_id)
        # ручное ограничение, чтобы set и deque оставались согласованы
        while len(self._finalized_lru) > self._finalized_lru_max:
            old = self._finalized_lru.popleft()
            self._finalized_set.discard(old)

    # --- Public API ---

    def register_signal(
        self,
        ctx: SignalContext,
        plan: ExecutionPlan,
        bar_idx: Optional[int] = None,
    ) -> None:
        """
        Register new signal + plan for tracking.
        ctx needed for atr_1m.
        bar_idx: текущий индекс бара (если доступен) для отслеживания TTL в барах.
        """
        state = SignalPerfState(
            signal_id=plan.signal_id,
            symbol=plan.symbol,
            setup_type=plan.setup_type,
            side=plan.side,
            ts_signal=plan.ts_signal,
            price_at_signal=plan.price_at_signal,
            atr_1m=max(getattr(ctx, "atr_1m", 0.0), 1e-6),
            stop_price=plan.stop_price,
            expiry_bars=plan.expiry_bars,
            max_ttd_bars=self.max_ttd_bars,
            bar_signal=bar_idx,  # NEW: сохраняем индекс бара сигнала
        )
        
        # NEW: запоминаем "сколько можно жить после entry"
        # Можно переопределять на уровне plan/strategy, но базово берём дефолт.
        state.extra["max_lifetime_bars_after_entry"] = self._default_max_lifetime_bars_after_entry
        state.extra["max_lifetime_ms_after_entry"] = self._default_max_lifetime_ms_after_entry
        
        self._states[plan.signal_id] = state
        
        # NEW: добавляем в индекс по символу для быстрого доступа в on_bar_1m
        self._ids_by_symbol[plan.symbol].add(plan.signal_id)

    def on_bar(self, symbol: str, bar: Bar1m, bar_idx: Optional[int] = None) -> None:
        """
        Feed each closed 1m bar by symbol.
        bar_idx: текущий индекс бара (если доступен) для housekeeping по TTL в барах.
        
        NEW: периодически вызывает housekeeping для финализации зависших позиций.
        
        DEPRECATED: используйте on_bar_1m для более надёжной обработки.
        Оставлено для обратной совместимости.
        """
        # Делегируем на on_bar_1m для единообразной обработки
        self.on_bar_1m(symbol, bar)
    
    def on_bar_1m(self, symbol: str, bar: Bar1m) -> None:
        """
        NEW: вызывать на каждом завершённом 1m баре.
        Именно здесь надёжнее всего:
          - увеличивать bars_seen
          - выставлять bars_to_entry / bars_to_exit
          - финализировать протухшие состояния
        
        Это основной метод для обработки баров. Заменяет старый on_bar().
        """
        # Получаем список signal_id для этого символа из индекса
        ids = list(self._ids_by_symbol.get(symbol, set()))
        if not ids:
            return
        
        # Конвертируем timestamp бара в naive UTC datetime
        # Если bar.ts уже datetime, используем его; иначе конвертируем из ms
        if isinstance(bar.ts, datetime):
            bar_dt = self._dt_to_naive_utc(bar.ts)
        else:
            # bar.ts может быть int (миллисекунды)
            bar_dt = self._naive_utc_from_ms(int(bar.ts))
        
        for signal_id in ids:
            st = self._states.get(signal_id)
            if st is None or st.finalized:
                continue
            
            # 1) бар "увидели"
            st.bars_seen += 1
            
            # фиксируем bar_signal / bar_entry (если нужно для аудита)
            if st.bar_signal is None:
                st.bar_signal = 1
            if st.ts_entry is not None and st.bar_entry is None:
                st.bar_entry = st.bars_seen
            
            # 2) если entry уже был, а bars_to_entry ещё не зафиксирован —
            #    фиксируем на первом баре после entry (как у вас задумано)
            if st.ts_entry is not None and st.bars_to_entry is None:
                st.bars_to_entry = st.bars_seen
            
            # 3) Обновляем TTD и MFE/MAE если нужно
            self._update_ttd(st, bar)
            if st.entry_price is not None:
                self._update_mfe_mae(st, bar)
            
            # 4) протухание БЕЗ входа: EXPIRED_NO_ENTRY
            if st.ts_entry is None and st.bars_seen >= st.expiry_bars:
                st.expired_without_entry = True
                st.outcome = Outcome.EXPIRED_NO_ENTRY
                # ts_exit ставим для аналитики "когда стало понятно, что сигнал умер"
                st.ts_exit = st.ts_exit or bar_dt
                st.exit_price = None
                st.bars_to_exit = st.bars_to_exit or st.bars_seen
                notes_msg = f"expired_no_entry bars_seen={st.bars_seen} expiry_bars={st.expiry_bars}"
                st.notes = (st.notes + " | " if st.notes else "") + notes_msg
                self._finalize_and_store(st, reason=notes_msg)
                continue
            
            # 5) КРИТИЧНО: вошли, но не вышли → EXPIRED_NO_TARGET
            if st.ts_entry is not None and st.ts_exit is None:
                # ВАЖНО: ttl_bars<=0 означает "выключено", иначе вы получите мгновенный EXPIRED_NO_TARGET.
                # Также важно различать:
                #  - ключ отсутствует → берём default
                #  - ключ присутствует и равен 0 → это явное отключение для конкретного сигнала
                if "max_lifetime_bars_after_entry" in (st.extra or {}):
                    ttl_bars = int(st.extra.get("max_lifetime_bars_after_entry") or 0)
                else:
                    ttl_bars = int(getattr(self, "_default_max_lifetime_bars_after_entry", 0) or 0)
                
                if ttl_bars > 0:
                    # Расчет held_bars: используем bar_entry (если есть) или bars_to_entry
                    entry_bar = st.bar_entry or st.bars_to_entry
                    held_bars = 0 if not entry_bar else (st.bars_seen - entry_bar)
                    
                    if held_bars >= ttl_bars:
                        st.outcome = Outcome.EXPIRED_NO_TARGET
                        st.ts_exit = st.ts_exit or bar_dt
                        # Для EXPIRED_NO_TARGET полезно mark-to-market по close бара:
                        # так у вас появится realized_R, а не None.
                        try:
                            st.exit_price = float(getattr(bar, "close"))
                        except Exception:
                            st.exit_price = None
                        st.bars_to_exit = st.bars_to_exit or st.bars_seen
                        notes_msg = f"expired_no_target held_bars={held_bars} ttl_bars={ttl_bars}"
                        st.notes = (st.notes + " | " if st.notes else "") + notes_msg
                        self._finalize_and_store(st, reason=notes_msg)
                        continue
            
            # 6) If exit already known and bar passed — finalize
            if st.ts_exit is not None and bar_dt >= self._dt_to_naive_utc(st.ts_exit) and not st.finalized:
                if st.bars_to_exit is None:
                    st.bars_to_exit = st.bars_seen
                self._finalize_and_store(st, reason="normal_exit")

    def on_execution_event(
        self,
        signal_id: str,
        event_type: str,
        ts: datetime,
        price: float,
        bar_idx: Optional[int] = None,
    ) -> None:
        """
        Feed execution events from MT5/ExecutionEngine:

          event_type:
            - "ENTRY_FILLED"
            - "MANUAL_EXIT"
            - "STOP_HIT"
            - "TP_HIT"
            - "BREAKEVEN"

        Payload can include volumes, but for TTD/quality price and time suffice.
        bar_idx: текущий индекс бара (если доступен) для отслеживания TTL.
        
        NEW: игнорирует события для финализированных signal_id (защита от поздних событий).
        """
        # NEW: Поздние события после finalize игнорируем (O(1) через set)
        if signal_id in self._finalized_set:
            # Важно: это защищает от "позднего exit события" после рестарта/лага,
            # которое иначе могло бы пересоздать state и исказить статистику.
            # Можно добавить логирование если нужно:
            # self.logger.warning("Late event ignored for finalized signal_id=%s", signal_id)
            return
        
        state = self._states.get(signal_id)
        if state is None or state.finalized:
            return

        if event_type == "ENTRY_FILLED":
            state.ts_entry = ts
            state.entry_price = price
            if bar_idx is not None:
                state.bar_entry = bar_idx  # NEW: сохраняем индекс бара входа
            # bars_to_entry set on next on_bar
        elif event_type in {"STOP_HIT", "TP_HIT", "BREAKEVEN", "MANUAL_EXIT"}:
            state.ts_exit = ts
            state.exit_price = price
            if bar_idx is not None:
                state.bar_exit = bar_idx  # NEW: сохраняем индекс бара выхода
            state.outcome = {
                "STOP_HIT": Outcome.STOP_HIT,
                "TP_HIT": Outcome.TARGET_HIT,
                "BREAKEVEN": Outcome.BREAKEVEN,
                "MANUAL_EXIT": Outcome.MANUAL_EXIT,
            }[event_type]
            # NEW: финализируем сразу при exit событии
            self._finalize_and_store(state, reason=f"exit_{event_type.lower()}")
            return
        
        # optional time-fallback housekeeping (если баров нет/редко приходят)
        self._housekeep_time_fallback()

    # --- Internal logic ---
    
    def _housekeep_time_fallback(self) -> None:
        """
        Фоллбек по времени (ms TTL после entry), если on_bar_1m не зовётся регулярно.
        Троттлим через TimeSampler.
        """
        ttl_ms_default = int(self._default_max_lifetime_ms_after_entry or 0)
        if ttl_ms_default <= 0:
            return
        if not self._housekeeping_sampler.hit():
            return
        
        now_ms = get_ny_time_millis()
        for st in list(self._states.values()):
            if st is None or st.finalized:
                continue
            if st.ts_entry is None or st.ts_exit is not None:
                continue
            
            try:
                entry_ms = self._dt_to_epoch_ms(st.ts_entry)
            except Exception:
                continue
            
            age_ms = now_ms - entry_ms
            if age_ms < 0:
                continue
            
            ttl_ms = int((st.extra or {}).get("max_lifetime_ms_after_entry") or ttl_ms_default)
            if ttl_ms <= 0:
                continue
            
            if age_ms >= ttl_ms:
                st.outcome = Outcome.EXPIRED_NO_TARGET
                st.ts_exit = st.ts_exit or datetime.now(timezone.utc).replace(tzinfo=None)
                # без цены — ставим entry_price (нулевой realized_R), но явно маркируем причину
                st.exit_price = st.exit_price or st.entry_price
                st.finalize_reason = f"expired_no_target_time age_ms={age_ms} ttl_ms={ttl_ms}"
                st.notes = (st.notes + " | " if st.notes else "") + st.finalize_reason
                self._finalize_and_store(st, reason=st.finalize_reason)


    def _update_ttd(self, state: SignalPerfState, bar: Bar1m) -> None:
        """
        TTD: bars after signal when price first reaches ttd_target_R in our direction.
        For LONG: check high; for SHORT: check low.
        """
        if state.ttd_bars is not None:
            return

        atr = state.atr_1m
        if atr <= 0:
            return

        if state.side == Side.LONG:
            move = (bar.high - state.price_at_signal) / atr
        else:
            move = (state.price_at_signal - bar.low) / atr

        if move >= self.ttd_target_R:
            state.ttd_bars = state.bars_seen
            state.ttd_seconds = state.ttd_bars * 60

        if state.bars_seen >= state.max_ttd_bars and state.ttd_bars is None:
            # Edge never realized before max_ttd_bars
            state.ttd_bars = state.max_ttd_bars
            state.ttd_seconds = state.ttd_bars * 60

    def _update_mfe_mae(self, state: SignalPerfState, bar: Bar1m) -> None:
        """
        MFE/MAE in R from entry point.
        """
        atr = state.atr_1m
        if atr <= 0 or state.entry_price is None:
            return

        if state.side == Side.LONG:
            mfe_abs = bar.high - state.entry_price
            mae_abs = bar.low - state.entry_price
        else:
            mfe_abs = state.entry_price - bar.low
            mae_abs = state.entry_price - bar.high

        mfe_R = mfe_abs / atr
        mae_R = mae_abs / atr

        state.mfe_R = max(state.mfe_R, mfe_R)
        state.mae_R = min(state.mae_R, mae_R)
    


    def _finalize_and_store(self, state: SignalPerfState, reason: Optional[str] = None) -> None:
        """
        Финализация должна быть:
        - идемпотентной (двойной вызов не ломает статистику)
        - удалять state из памяти (исправляет утечки/искажение статистики)
        - не давать "поздним" событиям воскресить state
        
        reason: причина финализации (для аудита и отладки).
        """
        # NEW: идемпотентность - если уже финализирован, ничего не делаем
        if state.finalized:
            return
        
        state.finalized = True
        state.finalize_reason = state.finalize_reason or (reason or "normal_finalization")

        # 1) NEW: снять из индекса по symbol (иначе set растёт вечно)
        try:
            ids = self._ids_by_symbol.get(state.symbol)
            if ids:
                ids.discard(state.signal_id)
                if not ids:
                    self._ids_by_symbol.pop(state.symbol, None)
        except Exception:
            pass

        # 2) NEW: LRU защита от late-events (O(1) через set + deque)
        self._remember_finalized(state.signal_id)

        realized_R: Optional[float] = None
        if state.entry_price is not None and state.exit_price is not None and state.atr_1m > 0:
            diff = (
                state.exit_price - state.entry_price
                if state.side == Side.LONG
                else state.entry_price - state.exit_price
            )
            realized_R = diff / state.atr_1m

        perf = SignalPerformance(
            signal_id=state.signal_id,
            symbol=state.symbol,
            setup_type=state.setup_type,
            side=state.side,
            ts_signal=state.ts_signal,
            ts_entry=state.ts_entry,
            ts_exit=state.ts_exit,
            price_at_signal=state.price_at_signal,
            entry_price=state.entry_price,
            exit_price=state.exit_price,
            stop_price=state.stop_price,
            realized_R=realized_R,
            mfe_R=state.mfe_R,
            mae_R=state.mae_R,
            ttd_bars=state.ttd_bars,
            ttd_seconds=state.ttd_seconds,
            bars_to_entry=state.bars_to_entry,
            bars_to_exit=state.bars_to_exit,
            outcome=state.outcome,
            notes=state.notes,
            extra=state.extra,
        )

        # 2) Store in TimescaleDB
        # NOTE: важно — outcome теперь может быть EXPIRED_NO_TARGET
        self.repo.insert_signal_performance(perf)

        # 3) Publish summary to Redis (optional — wire at service layer via await bus.publish_performance)
        # TODO: call self.bus.publish_performance(perf.to_dict()) from async context when bus is wired


        # 3) NEW: удалить из активных (критично для исправления утечки памяти)
        self._states.pop(state.signal_id, None)
