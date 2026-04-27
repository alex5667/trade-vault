"""
Улучшенный notifier для отправки сигналов в Telegram бот с retry логикой и rate limiting.
"""

import json
import logging
import math
import os
import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
import redis
from redis.exceptions import ConnectionError as RedisConnectionError


# ---------------------------------------------------------------------------
# Функция-помощник (после импортов)
# ---------------------------------------------------------------------------

def _is_preformatted_signal(parsed: Dict[str, Any], raw: Dict[str, Any]) -> bool:
    """
    Возвращает True только для сигналов, где текст ДОЛЖЕН уходить "как есть".

    ВАЖНО: raw_text может присутсвовать и у обычных сигналов,
    но это НЕ означает, что сообщение уже отформатировано для Telegram.
    """
    return bool(raw.get("is_xauusd") or parsed.get("is_xauusd"))


# ---------------------------------------------------------------------------
# Настройки бота и получателей
# ---------------------------------------------------------------------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
# Поддерживаем оба варианта переменной для совместимости
chat_ids = os.getenv("TELEGRAM_CHAT_ID", "") or os.getenv("TELEGRAM_NOTIFY_CHAT_IDS", "")
RECIPIENTS: List[str] = [x.strip() for x in chat_ids.split(",") if x.strip()]

API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage" if BOT_TOKEN else ""
ENABLED = bool(BOT_TOKEN and RECIPIENTS)

# Redis настройки
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

@dataclass
class RateLimiter:
    """Rate limiter для Telegram API."""
    max_requests: int = 20  # Максимум запросов
    window_seconds: int = 60  # За период в секундах
    requests: List[float] = field(default_factory=list)
    
    def can_send(self) -> bool:
        """Проверяет, можно ли отправить сообщение."""
        current_time = time.time()
        
        # Удаляем старые запросы
        self.requests = [req_time for req_time in self.requests 
                        if current_time - req_time < self.window_seconds]
        
        return len(self.requests) < self.max_requests
    
    def record_request(self):
        """Записывает отправленный запрос."""
        self.requests.append(time.time())
    
    def get_wait_time(self) -> int:
        """Возвращает время ожидания в секундах."""
        if not self.requests:
            return 0
        
        oldest_request = min(self.requests)
        wait_time = self.window_seconds - (time.time() - oldest_request)
        return max(0, int(wait_time))

@dataclass
class NotificationStats:
    """Статистика уведомлений."""
    sent: int = 0
    failed: int = 0
    rate_limited: int = 0
    retry_attempts: int = 0
    last_sent: Optional[float] = None
    start_time: float = field(default_factory=time.time)
    
    def get_stats_dict(self) -> dict:
        """Возвращает статистику в виде словаря."""
        uptime = time.time() - self.start_time
        return {
            'sent': self.sent,
            'failed': self.failed,
            'rate_limited': self.rate_limited,
            'retry_attempts': self.retry_attempts,
            'uptime_seconds': int(uptime),
            'success_rate': (self.sent / (self.sent + self.failed) * 100) if (self.sent + self.failed) > 0 else 0,
            'notifications_per_hour': (self.sent / (uptime / 3600)) if uptime > 0 else 0,
            'last_sent': self.last_sent
        }

class ImprovedTelegramNotifier:
    """Улучшенный отправщик уведомлений в Telegram."""
    
    def __init__(self):
        """Инициализирует notifier."""
        # Disable console logging completely, only log to file
        file_handler = logging.FileHandler('improved_notifier.log')
        file_handler.setLevel(logging.INFO)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[file_handler]
        )
        self.logger = logging.getLogger(__name__)
        
        # Статистика (всегда инициализируем, чтобы избежать AttributeError)
        self.stats = NotificationStats()
        
        # Проверяем настройки
        if not ENABLED:
            self.logger.warning("⚠️ Telegram notifier отключен: отсутствует BOT_TOKEN или RECIPIENTS")
            return
        
        self.logger.info(f"✅ Telegram notifier инициализирован для {len(RECIPIENTS)} получателей")
        
        # Redis
        self.redis = None
        self._connect_redis()
        
        # Rate limiting
        self.rate_limiter = RateLimiter()
        
        # HTTP клиент
        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(5.0, read=10.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10)
        )
        
        # Настройки retry
        self.max_retries = 2
        self.retry_delays = [1, 3]  # секунды (DLQ возьмет остальное)
        
        # DLQ (dead-letter queue) stream name
        self.dlq_stream = os.getenv("NOTIFY_DLQ_STREAM", "notify:dlq")
        self.dlq_max_len = int(os.getenv("NOTIFY_DLQ_MAXLEN", "500"))
        
        self.logger.info(f"🚀 Notifier готов к работе")
    
    def _connect_redis(self):
        """Подключение к Redis."""
        try:
            self.redis = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=10)
            self.redis.ping()
            self.logger.info("✅ Подключение к Redis установлено")
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к Redis: {e}")
            self.redis = None
    
    def _ensure_redis_connection(self) -> bool:
        """Проверяет и восстанавливает соединение с Redis."""
        if self.redis is None:
            self._connect_redis()
            return self.redis is not None
        
        try:
            self.redis.ping()
            return True
        except Exception:
            self.logger.warning("🔄 Переподключение к Redis...")
            self._connect_redis()
            return self.redis is not None
    
    def _format_price(self, value) -> str:
        """Форматирует цену, избегая научной нотации."""
        if value in ["-", None, ""]:
            return "-"
        
        try:
            # Конвертируем в float
            price = float(value)
            
            # Определяем количество знаков после запятой
            if price >= 1000:
                # Для больших чисел: 1234.56
                return f"{price:.2f}"
            elif price >= 1:
                # Для обычных чисел: 12.345
                return f"{price:.3f}"
            elif price >= 0.01:
                # Для малых чисел: 0.12345
                return f"{price:.5f}"
            elif price >= 0.0001:
                # Для очень малых чисел: 0.001234
                return f"{price:.6f}"
            else:
                # Для экстремально малых чисел: 0.00005879
                return f"{price:.8f}".rstrip('0').rstrip('.')
        except (ValueError, TypeError):
            return str(value)

    # ---------------------------------------------------------------------
    # config_params rendering (sidecar meta from outbox)
    #
    # Задача:
    #   - config_params НЕ должен раздувать payload в signal stream/outbox stream
    #   - но должен отображаться в Telegram (в конце сообщения) при наличии
    #
    # Источники (по приоритету):
    #   1) parsed["config_params"]                    (подтянут notify_worker.py из Redis meta)
    #   2) parsed["signal_settings"]["config_params"] (если notify_worker вложил в settings)
    #   3) raw["config_params"] / raw["signal_settings"]["config_params"] (fallback)
    # ---------------------------------------------------------------------

    def _safe_json_dumps(self, obj: Any) -> str:
        """Fail-open JSON dumps for debug text blocks."""
        try:
            return json.dumps(obj, ensure_ascii=False, sort_keys=True)
        except Exception:
            return str(obj)

    def _compact_config_params(self, cfg: Any) -> Any:
        """
        Защита от слишком больших словарей config_params.
        Управление через env:
          TG_CONFIG_PARAMS_MAX_KEYS (0 = без ограничения, default 0)
        """
        if not isinstance(cfg, dict):
            return cfg
        try:
            max_keys = int(os.getenv("TG_CONFIG_PARAMS_MAX_KEYS", "0") or 0)
        except Exception:
            max_keys = 0
        if max_keys > 0 and len(cfg) > max_keys:
            keys = sorted(cfg.keys())[:max_keys]  # детерминированно
            return {k: cfg.get(k) for k in keys}
        return cfg

    def _format_config_params_section(self, cfg: Any) -> str:
        """
        Формирует компактный блок для Telegram.
        Управление через env:
          TG_SHOW_CONFIG_PARAMS (default 1)
          TG_CONFIG_PARAMS_MODE = "kv" | "json" (default "kv")
          TG_CONFIG_PARAMS_MAX_LINES (default 12)  (для kv)
        """
        if os.getenv("TG_SHOW_CONFIG_PARAMS", "1").lower() in {"0", "false", "no"}:
            return ""
        if not cfg:
            return ""

        cfg = self._compact_config_params(cfg)
        mode = (os.getenv("TG_CONFIG_PARAMS_MODE", "kv") or "kv").lower()

        if mode == "json":
            # JSON блок (может быть длинным) — оставляем как опцию.
            return "\n\n🧩 **Config Params (json):**\n" + self._safe_json_dumps(cfg)

        # default: kv bullets
        if not isinstance(cfg, dict):
            return "\n\n🧩 **Config Params:**\n" + str(cfg)

        try:
            max_lines = int(os.getenv("TG_CONFIG_PARAMS_MAX_LINES", "12") or 12)
        except Exception:
            max_lines = 12

        lines = ["\n\n🧩 **Config Params:**"]
        keys = sorted(cfg.keys())
        shown = 0
        for k in keys:
            if max_lines > 0 and shown >= max_lines:
                lines.append(f"• ... (+{len(keys) - shown})")
                break
            v = cfg.get(k)
            # чтобы не заспамить вложенными dict/list — печатаем компактно
            if isinstance(v, (dict, list, tuple)):
                v_str = self._safe_json_dumps(v)
            else:
                v_str = str(v)
            # ограничение по длине значения (защита от огромных строк)
            if len(v_str) > 120:
                v_str = v_str[:120] + "…"
            lines.append(f"• {k}: {v_str}")
            shown += 1
        return "\n".join(lines)

    def format_signal_message(self, parsed: Dict[str, Any], raw: Dict[str, Any]) -> str:
        """Форматирует сообщение о сигнале в красивом формате."""
        # -----------------------------------------------------------------------------
        # CRITICAL FIX:
        #   parsed["raw_text"] может присутствовать у ЛЮБЫХ сигналов (как исходный текст/сниппет),
        #   но напрямую отправлять raw_text нужно ТОЛЬКО для специально помеченных preformatted-сообщений,
        #   иначе мы теряем нормальное форматирование (TP/SL/конфиденс/канал/время/настройки).
        #
        # Контракт:
        #   - XAUUSD/MT5 worker ставит raw["is_xauusd"]=True и кладёт готовый текст в parsed["raw_text"]
        #   - все остальные сигналы форматируются ниже.
        # -----------------------------------------------------------------------------
        if _is_preformatted_signal(parsed, raw):
            # prefer parsed.raw_text, fallback to raw.text
            return str(parsed.get("raw_text") or raw.get("text") or "").strip()

        # Notifier только форматирует данные, которые уже прошли валидацию в parser.

        symbol = str(parsed.get("symbol", "") or "").strip()

        # -----------------------------------------------------------------------------
        # CRYPTO SIGNALS: Use old CryptoSignalFormatter format for crypto signals
        # -----------------------------------------------------------------------------
        symbol_upper = symbol.upper()
        is_crypto = symbol_upper.endswith("USDT") or symbol_upper in {"BTCUSD", "ETHUSD"}
        if is_crypto and raw.get("text"):
            # For crypto signals, prefer the pre-formatted text from raw["text"]
            # This preserves the old format: "🚨 🟢 BTCUSDT LONG @ 88690.50, Volume 5.00 USDT..."
            return str(raw.get("text", "")).strip()
        direction = str(parsed.get("direction") or "").strip()
        entry = parsed.get("entry") or "-"
        stop = parsed.get("stop") or "-"
        tp = parsed.get("tp") or []
        leverage = parsed.get("leverage") or "-"
        confidence = parsed.get("confidence") or "-"
        source = parsed.get("source") or "-"
        # ✅ FIX: Используем source в приоритете, потом chat_title, потом username
        # Фильтруем пустые строки и None
        channel = (
            (parsed.get("source") if parsed.get("source") and parsed.get("source") not in ["None", ""] else None) or
            (raw.get("chat_title") if raw.get("chat_title") and raw.get("chat_title") not in ["None", ""] else None) or
            (raw.get("username") if raw.get("username") and raw.get("username") not in ["None", ""] else None) or
            "Unknown Channel"
        )
        
        # NEW: Извлекаем тип ордера и потенциальную прибыль
        order_type = parsed.get("orderType") or "-"
        profit_pct = parsed.get("profitPct") or "-"
        exchange = parsed.get("exchange") or "-"
        
        # 🎯 SENIOR DEV: Extract ORIGINAL message time from Telegram (not current time!)
        # Timestamp хранится в миллисекундах
        timestamp = parsed.get("timestamp") or raw.get("timestamp")
        try:
            if timestamp:
                ts_seconds = int(timestamp) / 1000
                dt = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
                # ✅ FIX: Добавляем дату к времени для полной информации в UTC
                message_time = dt.strftime("%H:%M:%S %d.%m.%Y UTC")  # Оригинальное время из Telegram в UTC
            else:
                message_time = datetime.now(tz=timezone.utc).strftime("%H:%M:%S %d.%m.%Y UTC")  # Fallback
        except Exception as e:
            message_time = datetime.now(tz=timezone.utc).strftime("%H:%M:%S %d.%m.%Y UTC")  # Fallback on error
        
        # Форматируем take profits (избегаем научной нотации)
        if isinstance(tp, list) and tp:
            tp_str = " | ".join(f"{self._format_price(t)}$" for t in tp[:3])  # Максимум 3 TP
            if len(tp) > 3:
                tp_str += f" | ... (+{len(tp) - 3})"
        else:
            tp_str = self._format_price(tp) if tp else "-"
        
        # Определяем эмодзи для направления
        direction_upper = direction.upper() if direction else ""
        direction_emoji = "🟢" if direction_upper in ["LONG", "BUY"] else "🔴"
        
        # Форматируем stop loss (избегаем научной нотации)
        stop_formatted = self._format_price(stop)
        
        # Формируем красивое сообщение
        message = f"""🚨 ТОРГОВЫЙ СИГНАЛ

{direction_emoji} {direction} {symbol}
💰 Вход: {entry}$ ({leverage}x)
🎯 Цели: {tp_str}
🛑 Стоп: {stop_formatted}$
📈 Потенциал: {profit_pct}%
🏢 {exchange} | {order_type}

📺 Канал: {channel}
⏰ {message_time}"""

        # Добавляем настройки сигнала, если они есть
        signal_settings = parsed.get("signal_settings") or raw.get("signal_settings")
        if signal_settings and isinstance(signal_settings, dict):
            message += "\n\n⚙️ **Signal Settings:**\n"

            # Основные thresholds
            if 'breakoutZThreshold' in signal_settings:
                message += f"• Breakout Z: {signal_settings['breakoutZThreshold']}\n"
            if 'absorptionZThreshold' in signal_settings:
                message += f"• Absorption Z: {signal_settings['absorptionZThreshold']}\n"
            if 'extremeZThreshold' in signal_settings:
                message += f"• Extreme Z: {signal_settings['extremeZThreshold']}\n"
            if 'mainZThreshold' in signal_settings:
                message += f"• Main Z: {signal_settings['mainZThreshold']}\n"

            # OBI settings
            if 'obiSustainedMinSamples' in signal_settings:
                message += f"• OBI Min Samples: {signal_settings['obiSustainedMinSamples']}\n"
            if 'obiSustainedMinFraction' in signal_settings:
                message += f"• OBI Min Fraction: {signal_settings['obiSustainedMinFraction']}\n"

            # Delta bucket
            if 'deltaBucketMs' in signal_settings:
                message += f"• Delta Bucket: {signal_settings['deltaBucketMs']}ms\n"

            # Burstiness
            if 'burstRatioMin' in signal_settings:
                message += f"• Burst Ratio Min: {signal_settings['burstRatioMin']}\n"
            if 'fanoMin' in signal_settings:
                message += f"• Fano Min: {signal_settings['fanoMin']}\n"

            # Execution filters
            if 'execFiltersEnabled' in signal_settings:
                message += f"• Exec Filters: {'ON' if signal_settings['execFiltersEnabled'] else 'OFF'}\n"
            if 'etaMaxSec' in signal_settings:
                message += f"• ETA Max: {signal_settings['etaMaxSec']}s\n"

            # Confidence
            if 'minSignalConfidence' in signal_settings:
                message += f"• Min Confidence: {signal_settings['minSignalConfidence']}%\n"

            # TP shifts
            if 'tp1ShiftMult' in signal_settings and signal_settings['tp1ShiftMult'] != 1.0:
                message += f"• TP1 Shift: {signal_settings['tp1ShiftMult']}x\n"

        # -----------------------------------------------------------------------------
        # OUTBOX: signal_settings + meta-sidecar (config_params)
        #
        # signal_settings: то, что приходит в outbox entry (динамические/калибровочные параметры)
        # config_params: то, что python-worker сохранил в meta по signal_id (InstrumentConfig/ENV snapshot)
        # -----------------------------------------------------------------------------
        # Добавляем config_params, если они подтянуты из meta-sidecar.
        # Мы НЕ кладём их в payload, чтобы не раздувать stream entry, но бот может показать их в конце.
        cfg_params = None
        try:
            # два supported варианта:
            # 1) signal_settings["config_params"] (рекомендуемый: всё рядом со settings)
            # 2) parsed["config_params"] (fallback)
            if isinstance(signal_settings, dict) and isinstance(signal_settings.get("config_params"), dict):
                cfg_params = signal_settings.get("config_params")
                self.logger.debug(f"🔧 Found config_params in signal_settings: {list(cfg_params.keys()) if cfg_params else 'None'}")
            elif isinstance(parsed.get("config_params"), dict):
                cfg_params = parsed.get("config_params")
                self.logger.debug(f"🔧 Found config_params in parsed: {list(cfg_params.keys()) if cfg_params else 'None'}")
        except Exception as e:
            cfg_params = None
            self.logger.debug(f"🔧 Error getting config_params: {e}")

        if cfg_params and isinstance(cfg_params, dict):
            self.logger.debug(f"🔧 Rendering config_params block")
            # ограничим длину, чтобы не превращать сообщение в простыню
            # показываем только "самое полезное" + остальное (если влезет)
            preferred_keys = [
                "delta_z_threshold",
                "obi_threshold",
                "min_signal_interval_sec",
                "delta_window_ticks",
                "obi_min_duration",
                "weak_progress_atr",
                "dist_atr_threshold",
                "stop_mode",
                "stop_atr_mult",
                "stop_pct",
                "tp_mode",
                "tp_rr",
                "tp_atr_mults",
            ]
            items: List[tuple[str, Any]] = []
            for k in preferred_keys:
                if k in cfg_params:
                    items.append((k, cfg_params.get(k)))
            # добавим остальные (но не более 6)
            for k, v in cfg_params.items():
                if k in preferred_keys:
                    continue
                items.append((k, v))
                if len(items) >= len(preferred_keys) + 6:
                    break

            message += "\n\n🧩 **Config Params (meta):**\n"
            for k, v in items:
                message += f"• {k}: {_fmt_val(v)}\n"

        return message
    def _split_message(self, text: str, max_length: int = 4000) -> List[str]:
        """
        Splits text into chunks respecting max_length.
        Prioritizes splitting by newlines.
        """
        if len(text) <= max_length:
            return [text]
        
        chunks = []
        current_chunk = []
        current_length = 0
        
        lines = text.split('\n')
        
        for line in lines:
            line_len = len(line) + 1  # +1 for newline
            
            # Scenario 1: Line itself is too long
            if line_len > max_length:
                # Flush current chunk
                if current_chunk:
                    chunks.append('\n'.join(current_chunk))
                    current_chunk = []
                    current_length = 0
                
                # Split huge line by brute force
                # leave some margin for "Part X/Y" suffix
                # safe_len must be at least 1 to avoid infinite loop or ValueError
                margin = 100 if max_length > 200 else int(max_length * 0.2)
                safe_len = max(1, max_length - margin)
                parts = [line[i:i+safe_len] for i in range(0, len(line), safe_len)]
                
                chunks.extend(parts[:-1])
                current_chunk = [parts[-1]]
                current_length = len(parts[-1]) + 1
                
            # Scenario 2: Line fits but overflows current chunk
            elif current_length + line_len > max_length:
                chunks.append('\n'.join(current_chunk))
                current_chunk = [line]
                current_length = line_len
                
            # Scenario 3: Line fits in current chunk
            else:
                current_chunk.append(line)
                current_length += line_len
        
        # Flush last chunk
        if current_chunk:
            chunks.append('\n'.join(current_chunk))
            
        # Add pagination if multiple chunks
        if len(chunks) > 1:
            final_chunks = []
            for i, chunk in enumerate(chunks):
                # Ensure we don't exceed limit with suffix
                suffix = f"\n\n📄 Part {i+1}/{len(chunks)}"
                if len(chunk) + len(suffix) > 4096:
                    # Rare edge case: brute cut to fit suffix
                    cut_idx = 4096 - len(suffix)
                    chunk = chunk[:cut_idx]
                final_chunks.append(chunk + suffix)
            return final_chunks
            
        return chunks

    async def send_notification(self, message: str, max_retries: Optional[int] = None, **kwargs) -> bool:
        """Отправляет уведомление всем получателям (с поддержкой разбиения длинных сообщений)."""
        if not ENABLED:
            return False
        
        # Check if message needs splitting
        # Telegram limit is 4096, we use 4000 to be safe
        chunks = self._split_message(message, max_length=4000)
        
        total_success = True
        
        for i, chunk in enumerate(chunks):
            # Only attach buttons/keyboard to the LAST chunk
            chunk_kwargs = kwargs.copy()
            if i < len(chunks) - 1:
                chunk_kwargs.pop("reply_markup", None)
                chunk_kwargs.pop("buttons", None)
            
            # Send current chunk
            chunk_success = await self._send_chunk(chunk, max_retries, **chunk_kwargs)
            if not chunk_success:
                total_success = False
                self.logger.error(f"❌ Failed to send chunk {i+1}/{len(chunks)}")
            
            # Small delay between chunks to avoid rate limits
            if len(chunks) > 1 and i < len(chunks) - 1:
                await asyncio.sleep(0.5)

        return total_success

    async def _send_chunk(self, message: str, max_retries: Optional[int] = None, **kwargs) -> bool:
        """Internal method to send a single message chunk."""
        max_retries = max_retries or self.max_retries
        
        # Проверяем rate limiting
        if not self.rate_limiter.can_send():
            wait_time = self.rate_limiter.get_wait_time()
            self.stats.rate_limited += 1
            self.logger.warning(f"⏳ Rate limit: ждем {wait_time} секунд")
            await asyncio.sleep(wait_time)
        
        success_count = 0
        
        for recipient in RECIPIENTS:
            if await self._send_to_recipient(recipient, message, max_retries, **kwargs):
                success_count += 1
            else:
                self.stats.failed += 1
        
        if success_count > 0:
            self.stats.sent += 1
            self.stats.last_sent = time.time()
            self.rate_limiter.record_request()
            self.logger.info(f"✅ Уведомление отправлено {success_count}/{len(RECIPIENTS)} получателям")
            return True
        else:
            error_msg = f"❌ Не удалось отправить уведомление ни одному получателю (получателей: {len(RECIPIENTS)})"
            self.logger.error(error_msg)
            print(error_msg)  # Выводим в stdout для docker logs
            # Выводим статистику для диагностики
            stats = self.stats.get_stats_dict()
            print(f"   Статистика: sent={stats['sent']}, failed={stats['failed']}, success_rate={stats['success_rate']:.1f}%")
            # Сохраняем в DLQ для повторной попытки позже
            await self._write_to_dlq(message, "all_recipients_failed", **kwargs)
            return False
    
    async def _send_to_recipient(self, chat_id: str, message: str, max_retries: int, **kwargs) -> bool:
        """Отправляет сообщение конкретному получателю с retry."""
        reply_markup = kwargs.get("reply_markup") or kwargs.get("buttons")
        
        def transform_button(btn: dict) -> dict:
            """Преобразует кнопку: "callback" -> "callback_data" для Telegram API."""
            if not isinstance(btn, dict):
                return btn
            telegram_btn = {"text": btn.get("text", "")}
            # Преобразуем "callback" в "callback_data"
            if "callback" in btn:
                telegram_btn["callback_data"] = btn["callback"]
            elif "callback_data" in btn:
                telegram_btn["callback_data"] = btn["callback_data"]
            # Сохраняем другие поля кнопки (url, etc.)
            for key in btn:
                if key not in ("callback", "text"):
                    telegram_btn[key] = btn[key]
            return telegram_btn
        
        # Если передали buttons (список списков), формируем inline_keyboard
        if isinstance(reply_markup, list) and not isinstance(reply_markup, str):
            # Преобразуем кнопки: "callback" -> "callback_data" для Telegram API
            transformed_buttons = []
            for row in reply_markup:
                transformed_row = [transform_button(btn) for btn in row]
                transformed_buttons.append(transformed_row)
            # Telegram API ждет {"inline_keyboard": [[...]]}
            reply_markup = {"inline_keyboard": transformed_buttons}
        elif isinstance(reply_markup, dict) and "inline_keyboard" in reply_markup:
            # Если уже dict с inline_keyboard, преобразуем кнопки внутри
            transformed_keyboard = []
            for row in reply_markup["inline_keyboard"]:
                transformed_row = [transform_button(btn) for btn in row]
                transformed_keyboard.append(transformed_row)
            reply_markup = {"inline_keyboard": transformed_keyboard}
            
        for attempt in range(max_retries):
            try:
                payload = {
                    'chat_id': chat_id,
                    'text': message,
                    'parse_mode': 'HTML',
                    'disable_web_page_preview': True
                }
                if reply_markup:
                    self.logger.debug(f"🔧 Sending reply_markup: {reply_markup}")
                    # Telegram API ждет JSON-строку для reply_markup при Content-Type: application/json
                    payload['reply_markup'] = json.dumps(reply_markup)
                
                response = await self.http_client.post(API_URL, json=payload)
                
                if response.status_code == 200:
                    self.logger.debug(f"✅ Отправлено получателю {chat_id}")
                    return True
                elif response.status_code == 429:  # Too Many Requests
                    # Telegram rate limiting
                    retry_after = response.headers.get('retry-after', 60)
                    self.logger.warning(f"⏳ Telegram rate limit для {chat_id}, ждем {retry_after}с")
                    await asyncio.sleep(int(retry_after))
                    continue
                else:
                    # Пытаемся извлечь детали ошибки из ответа Telegram API
                    try:
                        error_json = response.json()
                        error_description = error_json.get("description", "Unknown error")
                        error_code = error_json.get("error_code", response.status_code)
                    except:
                        error_description = response.text[:200] if response.text else "No error text"
                        error_code = response.status_code
                    
                    error_msg = f"❌ HTTP {response.status_code} (code {error_code}) для {chat_id}: {error_description}"
                    self.logger.error(error_msg)
                    print(error_msg)  # Выводим в stdout для docker logs
                    # Логируем первые 100 символов сообщения для отладки
                    msg_preview = message[:100] if len(message) > 100 else message
                    self.logger.debug(f"   Сообщение (первые 100 символов): {msg_preview}...")
                    print(f"   Сообщение (первые 100 символов): {msg_preview}...")  # Выводим в stdout

                    # CRITICAL FIX: Treat client errors (4xx) as terminal to prevent infinite retries of bad data
                    if 400 <= response.status_code < 500:
                        print(f"🛑 Permanent client error (HTTP {response.status_code}). Dropping message to prevent block.")
                        return True
                    
            except httpx.TimeoutException as e:
                error_msg = f"⏳ Таймаут для {chat_id} (попытка {attempt + 1}/{max_retries})"
                self.logger.warning(error_msg)
                print(error_msg)  # Выводим в stdout
            except httpx.ConnectError as e:
                error_msg = f"🔌 Ошибка соединения для {chat_id} (попытка {attempt + 1}/{max_retries}): {e}"
                self.logger.warning(error_msg)
                print(error_msg)  # Выводим в stdout
            except Exception as e:
                error_msg = f"❌ Неожиданная ошибка для {chat_id}: {e}"
                self.logger.error(error_msg)
                print(error_msg)  # Выводим в stdout
                import traceback
                traceback.print_exc()
            
            # Ждем перед повторной попыткой
            if attempt < max_retries - 1:
                delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                await asyncio.sleep(delay)
                self.stats.retry_attempts += 1
        
        return False

    # =========================================================================
    # Dead-Letter Queue (DLQ)
    # =========================================================================

    async def _write_to_dlq(self, message: str, error: str, **kwargs) -> None:
        """Записывает неотправленное сообщение в Redis Stream DLQ для повторной попытки."""
        try:
            if not self._ensure_redis_connection():
                print("⚠️ DLQ: Redis недоступен, сообщение потеряно")
                return

            entry = {
                "text": message[:4000],  # ограничиваем размер
                "error": str(error)[:200],
                "failed_at": str(int(time.time())),
                "attempt_count": "1",
            }
            # Сохраняем buttons если были
            buttons = kwargs.get("buttons") or kwargs.get("reply_markup")
            if buttons:
                try:
                    entry["buttons"] = json.dumps(buttons, ensure_ascii=False)
                except Exception:
                    pass

            self.redis.xadd(self.dlq_stream, entry, maxlen=self.dlq_max_len)
            print(f"📥 DLQ: сообщение сохранено ({len(message)} символов, ошибка: {error[:80]})")
        except Exception as e:
            print(f"⚠️ DLQ: ошибка записи: {e}")

    async def retry_dlq(self, max_items: int = 10) -> int:
        """Читает сообщения из DLQ и пытается отправить повторно.

        Returns:
            Количество успешно отправленных сообщений.
        """
        if not self._ensure_redis_connection():
            return 0

        try:
            entries = self.redis.xrange(self.dlq_stream, count=max_items)
        except Exception as e:
            self.logger.warning(f"DLQ retry: ошибка чтения: {e}")
            return 0

        if not entries:
            return 0

        sent = 0
        for msg_id, fields in entries:
            text = fields.get("text", "")
            if not text:
                # Пустое сообщение — удаляем
                self.redis.xdel(self.dlq_stream, msg_id)
                continue

            attempt_count = int(fields.get("attempt_count", "1"))
            failed_at = int(fields.get("failed_at", "0"))
            age_sec = int(time.time()) - failed_at if failed_at else 0

            # Слишком старые сообщения (>1 час) — удаляем
            if age_sec > 3600:
                self.redis.xdel(self.dlq_stream, msg_id)
                print(f"🗑️ DLQ: удалено устаревшее сообщение (возраст {age_sec}с)")
                continue

            # Слишком много попыток — удаляем
            if attempt_count >= 6:
                self.redis.xdel(self.dlq_stream, msg_id)
                print(f"🗑️ DLQ: удалено после {attempt_count} попыток")
                continue

            # Восстанавливаем buttons если были
            buttons_raw = fields.get("buttons")
            send_kwargs = {}
            if buttons_raw:
                try:
                    send_kwargs["buttons"] = json.loads(buttons_raw)
                except Exception:
                    pass

            # Пытаемся отправить (с уменьшенным количеством ретраев)
            success = await self._send_chunk(text, max_retries=2, **send_kwargs)

            if success:
                self.redis.xdel(self.dlq_stream, msg_id)
                sent += 1
                print(f"✅ DLQ: сообщение доставлено (попытка {attempt_count + 1})")
            else:
                # Обновляем счётчик попыток — удаляем старую запись и пишем новую
                new_entry = dict(fields)
                new_entry["attempt_count"] = str(attempt_count + 1)
                new_entry["failed_at"] = str(int(time.time()))
                try:
                    self.redis.xdel(self.dlq_stream, msg_id)
                    self.redis.xadd(self.dlq_stream, new_entry, maxlen=self.dlq_max_len)
                except Exception:
                    pass

        if sent > 0:
            print(f"📤 DLQ retry: доставлено {sent}/{len(entries)}")
        return sent

    async def process_notifications(self):
        """Основной цикл обработки уведомлений."""
        self.logger.info("🔄 Запуск процессора уведомлений")
        
        last_stats_log = 0
        stats_interval = 300  # 5 минут
        
        while True:
            try:
                # Проверяем Redis соединение
                if not self._ensure_redis_connection():
                    await asyncio.sleep(5)
                    continue
                
                # Читаем новые уведомления
                try:
                    streams = {'notify:telegram': '$'}
                    messages = self.redis.xread(streams, count=1, block=1000)
                    
                    for stream, msgs in messages:
                        for msg_id, fields in msgs:
                            await self._process_notification(fields)
                            
                except RedisConnectionError:
                    self.logger.error("❌ Потеряно соединение с Redis")
                    await asyncio.sleep(5)
                    continue
                
                # Логируем статистику
                current_time = time.time()
                if current_time - last_stats_log > stats_interval:
                    stats = self.stats.get_stats_dict()
                    self.logger.info(f"📊 Статистика notifier: {json.dumps(stats)}")
                    
                    # Сохраняем статистику в Redis
                    if self.redis:
                        try:
                            self.redis.hset("notifier:stats", mapping=stats)
                            self.redis.expire("notifier:stats", 3600)
                        except:
                            pass
                    
                    last_stats_log = current_time
                
            except KeyboardInterrupt:
                self.logger.info("🛑 Получен сигнал остановки")
                break
            except Exception as e:
                self.logger.error(f"❌ Неожиданная ошибка в процессоре: {e}")
                await asyncio.sleep(5)
        
        await self.close()
    
    async def _process_notification(self, fields: Dict[str, str]):
        """Обрабатывает одно уведомление."""
        try:
            # Проверяем, что это распарсенный сигнал
            if fields.get('parsed') != 'true':
                return
            
            # Формируем сообщение
            tp_raw = fields.get('tp', '')
            try:
                tp_parsed = json.loads(tp_raw) if tp_raw else []
                if not isinstance(tp_parsed, list):
                    tp_parsed = []
            except (json.JSONDecodeError, TypeError, ValueError):
                tp_parsed = []

            parsed_data = {
                'symbol': fields.get('symbol', ''),
                'direction': fields.get('direction', ''),
                'entry': fields.get('entry', ''),
                'stop': fields.get('stop', ''),
                'tp': tp_parsed,
                'leverage': fields.get('leverage', ''),
                'confidence': fields.get('confidence', ''),
                'source': fields.get('source', ''),
                'orderType': fields.get('orderType', ''),
                'profitPct': fields.get('profitPct', '')
            }
            
            raw_data = {}  # Можно расширить при необходимости
            
            message = self.format_signal_message(parsed_data, raw_data)
            
            # Отправляем уведомление
            await self.send_notification(message)
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обработки уведомления: {e}")
            self.stats.failed += 1
    
    async def close(self):
        """Закрывает соединения."""
        self.logger.info("🛑 Закрытие notifier...")
        
        if self.http_client:
            await self.http_client.aclose()
        
        if self.redis:
            self.redis.close()
        
        # Финальная статистика
        stats = self.stats.get_stats_dict()
        self.logger.info(f"📊 Финальная статистика: {json.dumps(stats, indent=2)}")
        
        self.logger.info("✅ Notifier остановлен")

# Функции для совместимости с существующим кодом
def _fmt_list(vals):
    """Форматирует список значений в строку."""
    return ", ".join(str(v) for v in vals) if (isinstance(vals, list) and vals) else "-"

def build_message(parsed: Dict[str, Any], raw: Dict[str, Any]) -> str:
    """Форматирует сообщение (совместимость)."""
    notifier = ImprovedTelegramNotifier()
    return notifier.format_signal_message(parsed, raw)

async def send_signal_async(parsed: Dict[str, Any], raw: Dict[str, Any] = None) -> bool:
    """Отправляет сигнал асинхронно (совместимость)."""
    if not ENABLED:
        return False
    
    notifier = ImprovedTelegramNotifier()
    message = notifier.format_signal_message(parsed, raw or {})
    result = await notifier.send_notification(message)
    await notifier.close()
    return result

def send_signal_sync(parsed: Dict[str, Any], raw: Dict[str, Any] = None) -> bool:
    """Отправляет сигнал синхронно (совместимость)."""
    return asyncio.run(send_signal_async(parsed, raw))

# Основная функция для запуска notifier
async def main():
    """Главная функция."""
    notifier = ImprovedTelegramNotifier()
    if ENABLED:
        await notifier.process_notifications()
    else:
        logging.info("Notifier отключен")

if __name__ == "__main__":
    asyncio.run(main()) 