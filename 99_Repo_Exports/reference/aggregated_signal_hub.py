# aggregated_signal_hub.py
"""
Aggregated Signal Hub - объединяет сигналы из разных источников с DOM-инъекцией.
Эта версия (V1) сохранена для совместимости. Используйте aggregated_signal_hub_v2.py для новых функций.
"""
from __future__ import annotations
import os
import json
import time
from typing import Dict, Any, Optional

try:
    import redis
except ImportError:
    redis = None

from common.log import setup_logger
from smart_cluster_analyzer import SmartClusterAnalyzer
from filtered_signal_writer import FilteredSignalWriter, WriterConfig

log = setup_logger("agg_hub")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
AUDIT_SIGNAL_STREAM = os.getenv("SIGNAL_AUDIT_STREAM", "signals:audit:XAUUSD")

def _get_redis():
    """Получить Redis клиент."""
    if not redis:
        raise RuntimeError("redis-py не установлен")
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)

class AggregatedSignalHub:
    """
    Берём сигналы медицинских каналов:
      - signals:orderflow:<SYMBOL>
      - signals:ta:<SYMBOL>
    Подмешиваем кластер-оценку (по DOM-ключу book:levels:<SYMBOL>) и считаем финальный confidence.
    Порог/кулдаун/риск — в FilteredSignalWriter.
    """
    
    def __init__(self, symbol: str = "XAUUSD"):
        self.symbol = symbol
        self.r = _get_redis()
        self.stream_of = os.getenv("STREAM_ORDERFLOW", f"signals:orderflow:{symbol}")
        self.stream_ta = os.getenv("STREAM_TA", f"signals:ta:{symbol}")
        self.book_key = os.getenv("BOOK_LAST_KEY", f"book:levels:{symbol}")
        self.group = os.getenv("HUB_GROUP", f"hub-{symbol}")
        self.consumer = os.getenv("HUB_CONSUMER", f"hub-{int(time.time())}")
        
        self.cluster = SmartClusterAnalyzer()
        self.writer = FilteredSignalWriter(
            r=self.r,
            cfg=WriterConfig(
                symbol=symbol,
                min_confidence=float(os.getenv("MIN_CONF", "60")),
                cooldown_sec=int(os.getenv("HUB_COOLDOWN", "300"))
            )
        )
        
        # веса для смешивания confidence
        self.w_of = float(os.getenv("W_OF", "0.45"))
        self.w_ta = float(os.getenv("W_TA", "0.35"))
        self.w_cl = float(os.getenv("W_CLUSTER", "0.20"))
        
        # счётчик для "no messages" логов (выводим каждое 10000-е)
        self.no_msg_counters: Dict[str, int] = {}

        # создаём consumer group'ы (могут уже существовать)
        for s in [self.stream_of, self.stream_ta]:
            try:
                self.r.xgroup_create(s, self.group, id="$", mkstream=True)
            except Exception:
                pass

    def _read(self, stream: str, count=20, block_ms=1000):
        """Читать сообщения из stream через consumer group."""
        try:
            msgs = self.r.xreadgroup(
                self.group,
                self.consumer,
                {stream: ">"},
                count=count,
                block=block_ms
            )
            if not msgs:
                # Выводим только каждое 10000-е "no messages"
                if stream not in self.no_msg_counters:
                    self.no_msg_counters[stream] = 0
                self.no_msg_counters[stream] += 1
                if self.no_msg_counters[stream] % 10000 == 0:
                    log.debug("xreadgroup: stream=%s no messages (count=%d)", stream, self.no_msg_counters[stream])
                return []
            # msgs = [(stream, [(id, {k:v,...}), ...])]
            items = msgs[0][1] if msgs and msgs[0][0] == stream else []
            if items:
                log.debug("xreadgroup: stream=%s fetched=%d", stream, len(items))
                # Сбрасываем счётчик при получении сообщений
                self.no_msg_counters[stream] = 0
            return items
        except Exception as e:
            log.debug("Read error: %s", e)
            return []

    def _parse_signal(self, fields: Dict[str, str]) -> Optional[Dict]:
        """Парсить сигнал из полей Redis stream."""
        # допускаем json в поле "data" или плоские поля
        if "data" in fields:
            try:
                return json.loads(fields["data"])
            except Exception:
                return None
        return {
            k: (json.loads(v) if v and v.startswith("{") else v)
            for k, v in fields.items()
        }

    def _dom_cluster(self) -> Optional[Dict]:
        """Получить кластерный анализ из DOM."""
        try:
            s = self.r.get(self.book_key)
            if not s:
                return None
            levels = json.loads(s)
            return self.cluster.analyze_from_dom(levels)
        except Exception as e:
            log.debug("DOM cluster error: %s", e)
            return None

    def _base_conf(self, sig: Dict) -> float:
        """Вычислить базовый confidence из сигнала."""
        # если генератор уже прислал confidence — уважаем
        if "confidence" in sig:
            try:
                return float(sig["confidence"])
            except Exception:
                pass
        
        # иначе — мягкая шкала от сильных условий:
        # для TA: ema/macd/rsi флаги; для OF: z_delta, breakout и т.п.
        c = 50.0
        ctx = sig.get("context") or {}
        if ctx.get("ta_bullish") or ctx.get("ta_bearish"):
            c += 10
        if ctx.get("of_extreme"):
            c += 10
        if ctx.get("pivot_breakout"):
            c += 10
        return min(90.0, c)

    def _blend(self, of: Optional[float], ta: Optional[float], cl: Optional[float]) -> float:
        """Смешать confidence с весами."""
        val = 0.0
        wsum = 0.0
        if of is not None:
            val += of * self.w_of
            wsum += self.w_of
        if ta is not None:
            val += ta * self.w_ta
            wsum += self.w_ta
        if cl is not None:
            val += cl * self.w_cl
            wsum += self.w_cl
        return (val / wsum) if wsum > 0 else 0.0

    def _handle(self, source: str, msg_id: str, fields: Dict[str, str]):
        """Обработать одно сообщение из stream."""
        log.debug("handle: source=%s id=%s raw_fields_keys=%s", source, msg_id, list(fields.keys()))
        sig = self._parse_signal(fields)
        if not sig:
            log.debug("handle: parse failed → xack id=%s", msg_id)
            self.r.xack(source, self.group, msg_id)
            return
        
        if sig.get("symbol") != self.symbol:
            log.debug("handle: skip other symbol=%s (need %s) → xack id=%s", sig.get("symbol"), self.symbol, msg_id)
            self.r.xack(source, self.group, msg_id)
            return

        base = self._base_conf(sig)
        log.debug("handle: base_conf=%.1f", base)
        dom = self._dom_cluster()
        cl_score = (dom or {}).get("cluster_score") if dom and dom.get("available") else None
        if dom:
            log.debug("handle: dom.available=%s cluster_score=%s details=%s", dom.get("available"), cl_score, dom)

        # если присланы частные confidence
        cof = sig.get("confidence_of")
        cta = sig.get("confidence_ta")
        cof = float(cof) if cof is not None else None
        cta = float(cta) if cta is not None else None

        # «впрыскиваем» DOM-кластер
        final_conf = self._blend(cof or base, cta or base, cl_score)
        log.info(
            "processed: src=%s id=%s side=%s cof=%s cta=%s dom=%s base=%.1f final=%.1f",
            "OF" if source == self.stream_of else "TA",
            msg_id,
            (sig.get("side") or "").upper(),
            (f"{cof:.1f}" if cof is not None else "-"),
            (f"{cta:.1f}" if cta is not None else "-"),
            (f"{cl_score:.1f}" if cl_score is not None else "-"),
            base,
            final_conf,
        )

        # Определяем источник сигнала
        signal_source = "AggregatedHub-V2"  # Для объединённых сигналов
        
        enriched = {
            "sid": sig.get("sid") or f"{self.symbol}:{int(time.time())}",
            "symbol": self.symbol,
            "side": sig.get("side", "").upper(),
            "entry": sig.get("entry"),
            "atr": sig.get("atr"),
            "confidence": round(final_conf, 1),
            "source": signal_source,  # ✅ Добавляем source на верхний уровень для TradeMonitor
            "context": {
                "source": "orderflow" if source == self.stream_of else "ta",
                "of": cof,
                "ta": cta,
                "dom": dom
            }
        }
        log.debug("enriched: %s", enriched)
        # дальше — в writer (дедуп, кулдаун, риск-сайзинг, /orders/push)
        res = self.writer.write(enriched)
        # v7: аудит объединённого сигнала
        try:
            audit_env = {
                "RISK_PCT": os.getenv("RISK_PCT", ""),
                "SL_MULT": os.getenv("SL_MULT", ""),
                "TP_MULTS": os.getenv("TP_MULTS", ""),
                "MIN_CONF": os.getenv("MIN_CONF", ""),
                "HUB_COOLDOWN": os.getenv("HUB_COOLDOWN", ""),
            }
            audit = {
                "sid": enriched["sid"],
                "symbol": self.symbol,
                "source": "aggregated",
                "ts": int(time.time()*1000),
                "side": enriched["side"],
                "entry": enriched.get("entry"),
                "atr": enriched.get("atr"),
                "confidence": enriched["confidence"],
                "of_conf": cof,
                "ta_conf": cta,
                "cluster": (dom or {}),
                "env": audit_env,
            }
            self.r.xadd(AUDIT_SIGNAL_STREAM, {"data": json.dumps(audit)}, maxlen=200000, approximate=True)
        except Exception:
            pass
        log.debug("writer_result: %s", res)
        self.r.xack(source, self.group, msg_id)
        log.debug("acked: id=%s", msg_id)

    def run(self):
        """Главный цикл чтения и обработки сигналов."""
        log.info(
            "AggregatedSignalHub started | symbol=%s streams=%s,%s book_key=%s",
            self.symbol,
            self.stream_of,
            self.stream_ta,
            self.book_key
        )
        
        while True:
            try:
                # читаем оба канала по очереди
                for s in [self.stream_of, self.stream_ta]:
                    msgs = self._read(s)
                    for (msg_id, fields) in msgs:
                        self._handle(s, msg_id, fields)
            except KeyboardInterrupt:
                log.info("Stopped by user")
                break
            except Exception as e:
                log.error("Loop error: %s", e)
                time.sleep(1.0)

if __name__ == "__main__":
    symbol = os.getenv("SYMBOL", "XAUUSD")
    AggregatedSignalHub(symbol=symbol).run()
