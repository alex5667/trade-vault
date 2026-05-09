import logging
from typing import Any

# P5: book sanity + stream integrity
from services.orderflow.book_sanity import check_book_sanity
from services.orderflow.components.parsing import OrderFlowParsing
from services.orderflow.configuration import _safe_int
from services.orderflow.metrics import log_silent_error
from services.orderflow.metrics_book_sanity_p5 import book_crossed_total, book_sanity_flags_total
from services.orderflow.metrics_stream_integrity_p5 import emit_integrity_metrics
from services.orderflow.runtime import BookSnapshot, BookState, SymbolRuntime
import contextlib

logger = logging.getLogger("orderflow_book_state_updater")

class BookStateUpdater:
    @staticmethod
    def parse_and_update(
        processor: Any, runtime: SymbolRuntime, raw: dict[str, Any], ingest_ts_ms: int
    ) -> tuple[bool, dict[str, Any] | None, BookSnapshot | None, BookSnapshot | None, int, int]:
        """
        Parses raw payload and updates the atomic BookState on runtime.
        Returns:
            (success, book_raw, snap, prev_snap, book_ts_ms, prev_ts_ms)
        """
        try:
            # 1. Parsing
            book_raw = OrderFlowParsing.parse_book_payload(raw, runtime.symbol)
            if not book_raw:
                return False, None, None, None, 0, 0

            # 2. Build Typed Snapshot
            prev_snap = getattr(runtime, "last_book", None)
            prev_ts_ms = _safe_int(getattr(runtime, "last_book_ts_ms", 0) or 0)
            snap = BookSnapshot.from_raw(book_raw)

            # Basic timestamps
            book_ts_ms = _safe_int(book_raw.get("ts_ms") or book_raw.get("ts") or book_raw.get("timestamp") or 0)

            # -------------------------------------------------------------
            # P5: Stream integrity + schema drift (book stream)
            # -------------------------------------------------------------
            try:
                seq = _safe_int(book_raw.get("u") or book_raw.get("final_id") or 0)
                runtime.book_integrity.update_schema(book_raw.keys())
                if seq > 0 and book_ts_ms > 0:
                    snap_i = runtime.book_integrity.update_seq(seq=seq, ts_ms=int(book_ts_ms))
                    emit_integrity_metrics(symbol=str(runtime.symbol), stream="book", snap=snap_i)
            except Exception:
                pass

            # -------------------------------------------------------------
            # P5: Book sanity (crossed BBO / NaNs / negative qty)
            # -------------------------------------------------------------
            try:
                bs = check_book_sanity(book=snap)
                runtime.book_sanity_ok = int(1 if bs.ok else 0)
                runtime.book_sanity_flags = ",".join(bs.flags)
                if not bs.ok:
                    try:
                        if book_sanity_flags_total is not None:
                            book_sanity_flags_total.labels(symbol=str(runtime.symbol)).inc()
                    except Exception:
                        pass
                if "crossed_bbo" in bs.flags:
                    try:
                        if book_crossed_total is not None:
                            book_crossed_total.labels(symbol=str(runtime.symbol)).inc()
                    except Exception:
                        pass
            except Exception:
                pass

            # Strict DQ: book missing-seq continuity (Binance depthUpdate U/u)
            with contextlib.suppress(Exception):
                processor._update_book_missing_seq(runtime, book_raw)

            # Atomic Snapshot
            try:
                runtime.book_state = BookState(
                    raw=book_raw,
                    snap=snap,
                    prev_snap=prev_snap,
                    ts_ms=_safe_int(book_ts_ms),
                    prev_ts_ms=_safe_int(prev_ts_ms),
                    ingest_ts_ms=_safe_int(ingest_ts_ms),
                )
            except Exception as exc:
                log_silent_error(exc, 'book_state_failure', runtime.symbol, 'BookProcessor:book_state')

            # Backward compatibility
            runtime.last_book_raw = book_raw
            runtime.prev_book = prev_snap
            runtime.last_book = snap

            return True, book_raw, snap, prev_snap, book_ts_ms, prev_ts_ms

        except Exception as exc:
            log_silent_error(exc, 'book_process_failure', runtime.symbol, 'BookStateUpdater:parse_and_update')
            from services.orderflow.metrics import book_parse_errors_total
            with contextlib.suppress(Exception):
                book_parse_errors_total.labels(
                    symbol=str(runtime.symbol),
                    reason=type(exc).__name__,
                ).inc()
            return False, None, None, None, 0, 0
