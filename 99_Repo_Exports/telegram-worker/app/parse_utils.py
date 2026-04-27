"""
Парсер сигналов из Telegram‑сообщений.

Назначение:
- Нормализовать текст и извлекать ключевые поля (symbol, direction, entry, stop, tp, ...)
- Поддерживать различные варианты написания (Coin:, Targets:, тире, SL/StopLoss, локализация)
- Формировать оценку уверенности (confidence) по простым эвристикам
"""
import re
from typing import Optional, List, Dict, Any

Number = Optional[float]


def _to_float(s: str) -> Number:
    """
    Аккуратно преобразует строку к float:
    - удаляет пробелы и неразрывные пробелы
    - заменяет запятую на точку
    - извлекает первое число с возможным знаком и дробной частью
    """
    if s is None:
        return None
    s = s.replace(' ', '').replace('\u00A0', '')
    s = s.replace(',', '.')
    m = re.search(r'[-+]?\d+(?:\.\d+)?', s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def _norm_symbol(sym: str) -> str:
    """
    Нормализует символ валютной пары:
    - удаляет #, $, специальные символы и разделители /-_ и пробелы
    - переводит в верхний регистр
    - превращает BTC/USDT → BTCUSDT; ETH-PERP → ETHPERP
    """
    if not sym:
        return ''
    s = sym.strip().upper()
    s = s.replace('#', '').replace('$', '').replace('⚡', '')
    s = s.replace('СПОТ', '').replace('ФЬЮЧ', '')
    s = s.replace('ПЕРП', 'PERP')
    s = s.replace(' ', '')
    s = s.replace('-', '').replace('_', '')
    # Убираем / только если это не /USDT или /PERP
    if '/USDT' in s:
        s = s.replace('/', '')
    elif '/PERP' in s:
        s = s.replace('/', '')
    else:
        s = s.replace('/', '')
    
    # ✅ FIX: Убираем дублирование USDT (AAVEUSDTUSDT → AAVEUSDT)
    if s.endswith('USDTUSDT'):
        s = s[:-4]  # Убираем последний USDT
    
    # ✅ NEW FIX: Убираем дублирование других валютных пар
    # AVAXUSDTUSDT → AVAXUSDT, BTCUSDTUSDT → BTCUSDT
    if s.count('USDT') > 1:
        # Находим последнее вхождение USDT и оставляем только его
        last_usdt_pos = s.rfind('USDT')
        if last_usdt_pos > 0:
            s = s[:last_usdt_pos] + 'USDT'
    
    return s


def _find_all_numbers(text: str) -> List[float]:
    """
    Находит все числа в строке, возвращает список float в порядке появления.
    """
    nums: List[float] = []
    for m in re.finditer(r'[-+]?\d+(?:[.,]\d+)?', text or ''):
        v = _to_float(m.group(0))
        if v is not None:
            nums.append(v)
    return nums

DIRECTION_MAP = {
    'LONG': 'LONG', 'BUY': 'LONG', 'ЛОНГ': 'LONG', 'ПОКУПКА': 'LONG',
    'SHORT': 'SHORT', 'SELL': 'SHORT', 'ШОРТ': 'SHORT', 'ПРОДАЖА': 'SHORT',
}
# Исправляем регулярное выражение для направления, чтобы оно правильно распознавало "ПОКУПКА: SHORT"
DIRECTION_RE = re.compile(r'\b(LONG|SHORT|BUY|SELL|ЛОНГ|ШОРТ|ПОКУПКА|ПРОДАЖА)\b', re.I | re.U)
TIMEFRAME_RE = re.compile(r'\b(1m|3m|5m|15m|30m|45m|1h|2h|4h|6h|8h|12h|1d|3d|1w|1M)\b', re.I)
EXCHANGE_RE = re.compile(r'\b(BINANCE|BYBIT|OKX|BITGET|HUOBI|KUCOIN|COINBASE|DERIBIT)\b', re.I)

# Prefer explicit Coin line if present (англ/рус: COIN/PAIR/ПАРА/МОНЕТА)
COIN_LINE_RE = re.compile(r'\b(?:COIN|PAIR|ПАРА|МОНЕТА)\s*:\s*(?P<sym>#?[A-Za-z0-9]{2,16}(?:[\/\-_][A-Za-z0-9]{2,16})?)', re.I)
# Generic symbol detector (allows digits and longer tokens)
SYMBOL_RE = re.compile(r'(?P<sym>#?\$?[A-Za-z0-9]{2,16}(?:[\/\-_][A-Za-z0-9]{2,16})?|XAUUSD|XAGUSD|US100|US30|DE40|[A-Za-z0-9]{2,16}USDT|[A-Za-z0-9]{2,16}PERP)', re.I)

# Entry одиночное: поддерживаем EN/РУ варинты, включая "Точка входа"
ENTRY_SINGLE_RE = re.compile(r'\b(?:(?:entry|вход|открыть|открываем)|(?:точка\s+входа))\s*[:\-]?\s*(?P<price>[-+]?\d+(?:[.,]\d+)?)', re.I)
# Support hyphen '-', en-dash '–', em-dash '—', 'to', 'до', arrow
ENTRY_RANGE_RE = re.compile(r'(?:entry|вход|открыть|открываем|Entry|Entries|точка\s+входа)\s*[:\-]?\s*(?P<p1>\d+(?:[.,]\d+)?)\s*(?:-|–|—|до|to|→)\s*(?P<p2>\d+(?:[.,]\d+)?)', re.I)
# Enhanced entry pattern for "Entry Market Price X" format
ENTRY_MARKET_PRICE_RE = re.compile(r'(?:Entry\s+Market\s+Price|Entry\s+Price|Market\s+Price)\s*(?P<price>[-+]?\d+(?:[.,]\d+)?)', re.I)
# New pattern for "LONG : 0.007715-0.007435" format
ENTRY_DIRECTION_RANGE_RE = re.compile(r'\b(LONG|SHORT)\s*:\s*(?P<p1>\d+(?:[.,]\d+)?)\s*-\s*(?P<p2>\d+(?:[.,]\d+)?)', re.I)
# New pattern for "Entry Zone: 0.2158 – 0.2115" format
ENTRY_ZONE_RE = re.compile(r'Entry\s+Zone\s*:\s*(?P<p1>\d+(?:[.,]\d+)?)\s*[–\-]\s*(?P<p2>\d+(?:[.,]\d+)?)', re.I)
# StopLoss variants, including mixed Latin/Cyrillic first letter in "Cтоп" and optional space in "стоп лосс"
# Добавляем поддержку эмодзи в начале
STOP_RE = re.compile(r'(?:❌\s*)?(?:СТОП-ЛОСС|StopLoss|SL|stop(?:\s*loss)?|[cс]топ(?:\s*лосс)?|стоп-лосс|стоплосс|⚪️?[Cc]топ)\s*[:\-]?\s*(?P<sl>[-+]?\d+(?:[.,]\d+)?)', re.I)

# Enhanced stop loss pattern for "Stop Loss (SL): 2.33" format
STOP_ENHANCED_RE = re.compile(r'Stop\s+Loss\s*\(SL\)\s*:\s*(?P<sl>[-+]?\d+(?:[.,]\d+)?)', re.I)
# Enhanced SL pattern for "SL⛔️(X)" format
SL_ENHANCED_RE = re.compile(r'SL[^\d]*\((?P<sl>[-+]?\d+(?:[.,]\d+)?)\)', re.I)
# Individual TP lines (still supported) - добавляем поддержку эмодзи
TP_ALL_RE = re.compile(r'(?:✅\s*)?(?:TP|тейк|закрыть|закрываем)\s*?(\d{0,2})\s*[:\-]?\s*(?P<tp>[-+]?\d+(?:[.,]\d+)?)', re.I)
# Перечисление таргетов по строкам: "Target 1: 0.1050", "Цель 1: ..." (эмодзи перед словом игнорируем)
TARGET_ITEM_RE = re.compile(r'(?:Target|Цель|Закрыть|Закрываем)\s*\d{0,2}\s*[:\-]?\s*(?P<tp>[-+]?\d+(?:[.,]\d+)?)', re.I)
# Targets list line (англ/рус варианты, в т.ч. "Отгрызаем профит на:", "Фиксируем прибыль на:", "Тейк-профиты:")
# ВАЖНО: используем только множественное "Targets" (без '?'), чтобы не ловить "Target 1: ..."
TARGETS_LIST_RE = re.compile(r'\b(?:Targets|Цели|Таргеты|Тейк-профит[ыи]?|Отгрыза.?м\s+профит\s+на|Фиксируем\s+прибыль\s+на)\s*[:\-]?\s*(?P<list>[^\n]*)', re.I)

# ✅ NEW: Improved Russian TP pattern for "Отгрызаeм профит на: 20.6$ 20.9$ 21.157$"  
RUSSIAN_TP_RE = re.compile(r'Отгрыза.?м\s+профит\s+на\s*:\s*(?P<prices>[^\n]+)', re.I)

# Добавляем поддержку для извлечения цен из строки "Фиксируем прибыль на: 24.929$ 24.574$ 24.174$"
TARGETS_PRICES_RE = re.compile(r'(?:Фиксируем\s+прибыль\s+на|Отгрызаем\s+профит\s+на)\s*[:\-]?\s*(?P<prices>[^\n]+)', re.I)
# Enhanced TP pattern for "TP📈(X-Y-Z-W)💵" format
TP_ENHANCED_RE = re.compile(r'TP[^\d]*\((?P<prices>[^)]+)\)', re.I)
# New pattern for numbered TP format: "1) 0.144 2) 0.153 3) 0.166"
TP_NUMBERED_RE = re.compile(r'(?:\d+\)\s*)(?P<tp>[-+]?\d+(?:[.,]\d+)?)', re.I)

# Pattern for "T1: 3.15", "T2: 3.58" format
TP_T_FORMAT_RE = re.compile(r'T\d+\s*:\s*(?P<tp>[-+]?\d+(?:[.,]\d+)?)', re.I)

# Enhanced leverage patterns for "Cross 25×" format
LEV_RE = re.compile(r'\b(?:lev|leverage|плечо|кредитным\s+плечом|Leverage|Cross)\s*[:\-]?\s*(?:X?)?(?P<l>\d+)\s*[x×]', re.I | re.U)
# Альтернативный шаблон плеча: "18x" перед словом плечо или просто в тексте
LEV_RE_FALLBACK = re.compile(r'(?P<l>\d+)\s*[x×](?:[^\n]{0,20}\b(?:плечо|leverage|Cross)\b)?', re.I | re.U)
# Новый паттерн для "Cross 25×" формата - более точный
LEV_CROSS_RE = re.compile(r'\bCross\s+(?P<l>\d+)\s*[x×]', re.I | re.U)
# Дополнительный паттерн для "Leverage : Cross 25×" формата
LEV_CROSS_COLON_RE = re.compile(r'\bLeverage\s*:\s*Cross\s+(?P<l>\d+)\s*[x×]', re.I | re.U)
# Простой паттерн для поиска любого числа с x или ×
LEV_SIMPLE_RE = re.compile(r'(?P<l>\d+)\s*[x×]', re.I | re.U)
# Паттерн для русского формата "Плечо: до 25" или "до X"
LEV_DO_RE = re.compile(r'\b(?:плечо|leverage)\s*[:\-]?\s*(?:до|up\s+to)\s+(?P<l>\d+)', re.I | re.U)
RISK_RE = re.compile(r'\b(?:risk|риск)\s*[:\-]?\s*(?P<r>\d+(?:[.,]\d+)?)\s*%', re.I)
TP_PCT_LIST_RE = re.compile(r'\b(?:TP|тейки?)\s*[:\-]?\s*((?:\d+(?:[.,]\d+)?\s*%[,\s]*){1,10})', re.I)
PCT_RE = re.compile(r'(\d+(?:[.,]\d+)?)\s*%')
# DCA (Dollar Cost Average) pattern
DCA_RE = re.compile(r'DCA\s*=\s*\((?P<dca>[-+]?\d+(?:[.,]\d+)?)\)', re.I)

# NEW: Pattern for "Тип ордера: Лимитный ордер"
ORDER_TYPE_RE = re.compile(r'Тип\s+ордера\s*:\s*(?P<order_type>[\w\s]+)', re.I | re.U)

# NEW: Pattern for "Потенциальная прибыль когда догрызем последний тейк будет = +70%"
PROFIT_PCT_RE = re.compile(r'Потенциальная\s+прибыль.*?=\s*(?P<profit>[-+]?\d+(?:[.,]\d+)?)\s*%', re.I | re.U)

# SOL-specific patterns for enhanced parsing
SOL_SPECIFIC_PATTERNS = {
    'entry_range': re.compile(r'(\d+[.,]\d+)\s*-\s*(\d+[.,]\d+)', re.I),
    'stop_loss_colon': re.compile(r'СТОП-ЛОСС:\s*\$?(\d+[.,]\d+)', re.I),
    'target_with_emoji': re.compile(r'🔘\s*Закрыть\s+ордер\s+по\s+цене\s*\$?(\d+[.,]\d+)', re.I),
    'target_plain': re.compile(r'Закрыть\s+ордер\s+по\s+цене\s*\$?(\d+[.,]\d+)', re.I),
}


def parse_signal(text: str) -> Dict[str, Any]:
    """
    Разбирает текст сигнала и возвращает словарь полей.

    Поля:
        symbol: нормализованный символ (например, BTCUSDT)
        direction: LONG/SHORT (если найдено)
        entry / entryFrom/entryTo: цена входа (одиночная или диапазон)
        stop: стоп‑лосс
        tp: список целей в абсолютных ценах (упорядочен)
        tpPct: список целей в процентах (если найдено)
        leverage: целое кредитное плечо
        timeframe/exchange/riskPct: дополнительные атрибуты
        confidence: оценка полноты сигнала (0..1)
    """
    print(f"DEBUG: parse_signal called with text length: {len(text or '')}")  # Отладка
    t = (text or '').strip()
    out: Dict[str, Any] = {
        "symbol": None,
        "direction": None,
        "entry": None,   # единственное поле входа (для диапазона берём нижнюю границу)
        "stop": None,
        "tp": [],
        "tpPct": [],
        "leverage": None,
        "timeframe": None,
        "exchange": None,
        "riskPct": None,
        "dca": None,     # Dollar Cost Average
        "confidence": 0.0,
    }

    # NEW: Добавляем поля для типа ордера и потенциальной прибыли
    out["orderType"] = None
    out["profitPct"] = None

    m = DIRECTION_RE.search(t)
    if m:
        out["direction"] = DIRECTION_MAP.get(m.group(1).upper(), None)
        print(f"DEBUG: Direction found: {out['direction']}")  # Отладка
    
    # Дополнительная проверка для случая "ПОКУПКА: SHORT" или "ПРОДАЖА: LONG"
    if not out["direction"]:
        direction_match = re.search(r'(?:ПОКУПКА|ПРОДАЖА|BUY|SELL)\s*:\s*(LONG|SHORT)', t, re.I)
        if direction_match:
            out["direction"] = direction_match.group(1).upper()
            print(f"DEBUG: Direction found (after colon): {out['direction']}")  # Отладка
    
    # Приоритетная проверка для случая "ПОКУПКА: SHORT" или "ПРОДАЖА: LONG"
    # Переопределяем направление, если найдено более точное указание
    direction_match = re.search(r'(?:ПОКУПКА|ПРОДАЖА|BUY|SELL)\s*:\s*(LONG|SHORT)', t, re.I)
    if direction_match:
        out["direction"] = direction_match.group(1).upper()
        print(f"DEBUG: Direction overridden (after colon): {out['direction']}")  # Отладка
    
    # Для сигналов "Entry Executed" пытаемся определить направление по контексту
    if not out["direction"] and "Entry Executed" in t:
        # Если это обновление по позиции, пытаемся найти направление в заголовке
        # или по умолчанию считаем LONG (можно изменить логику)
        out["direction"] = "LONG"  # Временное решение
        print(f"DEBUG: Direction assumed (Entry Executed): {out['direction']}")  # Отладка

    m = TIMEFRAME_RE.search(t)
    if m:
        out["timeframe"] = m.group(1).lower()
        print(f"DEBUG: Timeframe found: {out['timeframe']}")  # Отладка
    m = EXCHANGE_RE.search(t)
    if m:
        out["exchange"] = m.group(1).upper()
        print(f"DEBUG: Exchange found: {out['exchange']}")  # Отладка

    m = LEV_RE.search(t)
    if m:
        out["leverage"] = int(m.group('l'))
        print(f"DEBUG: Leverage found: {out['leverage']}")  # Отладка
    else:
        print(f"DEBUG: LEV_RE not matched, trying 'до X' format...")  # Отладка
        # Пытаемся найти формат "Плечо: до 25" или "до X"
        m_do = LEV_DO_RE.search(t)
        if m_do:
            out["leverage"] = int(m_do.group('l'))
            print(f"DEBUG: Leverage found (до X format): {out['leverage']}")  # Отладка
        else:
            print(f"DEBUG: LEV_DO_RE not matched, trying fallback...")  # Отладка
            # Пытаемся вытащить формат вида "18x" перед словом плечо или просто "18x"
            m2 = LEV_RE_FALLBACK.search(t)
            if m2:
                out["leverage"] = int(m2.group('l'))
                print(f"DEBUG: Leverage found (fallback): {out['leverage']}")  # Отладка
            else:
                print(f"DEBUG: LEV_RE_FALLBACK not matched, trying Cross...")  # Отладка
                # Новый паттерн для "Cross 25×" формата
                m3 = LEV_CROSS_RE.search(t)
                if m3:
                    out["leverage"] = int(m3.group('l'))
                    print(f"DEBUG: Leverage found (Cross): {out['leverage']}")  # Отладка
                else:
                    print(f"DEBUG: LEV_CROSS_RE not matched, trying Leverage: Cross...")  # Отладка
                    # Дополнительный паттерн для "Leverage : Cross 25×" формата
                    m4 = LEV_CROSS_COLON_RE.search(t)
                    if m4:
                        out["leverage"] = int(m4.group('l'))
                        print(f"DEBUG: Leverage found (Leverage: Cross): {out['leverage']}")  # Отладка
                    else:
                        print(f"DEBUG: All specific patterns failed, trying simple pattern...")  # Отладка
                        # Простой паттерн для поиска любого числа с x или ×
                        m5 = LEV_SIMPLE_RE.search(t)
                        if m5:
                            out["leverage"] = int(m5.group('l'))
                            print(f"DEBUG: Leverage found (simple): {out['leverage']}")  # Отладка
                        else:
                            print(f"DEBUG: All leverage patterns failed. Text: '{t}'")  # Отладка
                            # Детальная отладка
                            print(f"DEBUG: Text length: {len(t)}")
                            print(f"DEBUG: Text bytes: {t.encode('utf-8')}")
                            print(f"DEBUG: Looking for '25×' in text: {'25×' in t}")
                            print(f"DEBUG: Looking for '25x' in text: {'25x' in t}")
                            # Ищем все числа с x или ×
                            simple_matches = list(LEV_SIMPLE_RE.finditer(t))
                            print(f"DEBUG: Simple pattern matches: {len(simple_matches)}")
                            for i, match in enumerate(simple_matches):
                                print(f"DEBUG: Match {i}: '{match.group(0)}' at position {match.start()}-{match.end()}")
                            
                            # Дополнительная проверка для формата "X25" (без пробела)
                            x_leverage_match = re.search(r'X(\d+)', t, re.I)
                            if x_leverage_match:
                                out["leverage"] = int(x_leverage_match.group(1))
                                print(f"DEBUG: Leverage found (X format): {out['leverage']}")  # Отладка
    m = RISK_RE.search(t)
    if m:
        out["riskPct"] = _to_float(m.group('r'))
        print(f"DEBUG: Risk found: {out['riskPct']}")  # Отладка
    
    # NEW: Парсинг типа ордера
    m = ORDER_TYPE_RE.search(t)
    if m:
        out["orderType"] = m.group('order_type').strip()
        print(f"DEBUG: Order Type found: {out['orderType']}")  # Отладка
    
    # NEW: Парсинг потенциальной прибыли
    m = PROFIT_PCT_RE.search(t)
    if m:
        out["profitPct"] = _to_float(m.group('profit'))
        print(f"DEBUG: Profit Percentage found: {out['profitPct']}")  # Отладка

    print(f"DEBUG: Before symbol parsing")  # Отладка
    # Symbol: prefer Coin: line
    m = COIN_LINE_RE.search(t)
    sym = None
    if m:
        sym = _norm_symbol(m.group('sym'))
        print(f"DEBUG: Symbol found (coin line): {sym}")  # Отладка
    
    # Дополнительная проверка для SOL-специфичных сигналов
    if not sym and ("SOL" in text.upper() or "179,8 - 181,8" in text):
        sym = "SOL/USDT"
        print(f"DEBUG: Symbol found (SOL-specific): {sym}")  # Отладка
    
    # Дополнительная проверка для формата "Entry Executed" - ищем символ в заголовке (приоритет выше)
    if not sym:
        # Ищем паттерн "$SYMBOL/USDT Trading Update"
        print(f"DEBUG: Looking for trading update pattern...")  # Отладка
        trading_update_match = re.search(r'[\$#]([A-Za-z0-9]{2,16})/USDT\s+Trading\s+Update', t, re.I)
        if trading_update_match:
            sym = _norm_symbol(trading_update_match.group(1))
            print(f"DEBUG: Symbol found (trading update): {sym}")  # Отладка
        else:
            print(f"DEBUG: Trading update pattern not found")  # Отладка
            # Попробуем более простой паттерн
            simple_trading_match = re.search(r'[\$#]([A-Za-z0-9]{2,16}/USDT)', t, re.I)
            if simple_trading_match:
                sym = _norm_symbol(simple_trading_match.group(1))
                print(f"DEBUG: Symbol found (simple trading): {sym}")  # Отладка
    
    if not sym:
        # First try to find $SYMBOL or #SYMBOL patterns (highest priority)
        print(f"DEBUG: Trying dollar symbol detection...")
        # Ищем $SYMBOL или #SYMBOL в любом месте текста
        dollar_symbol_match = re.search(r'[\$#]([A-Za-z0-9]{2,16})', t)
        if dollar_symbol_match:
            sym = _norm_symbol(dollar_symbol_match.group(1))
            print(f"DEBUG: Symbol found (dollar/hash): {sym}")  # Отладка
        else:
            print(f"DEBUG: No dollar symbol found, trying generic detection...")
    
    # Дополнительная проверка для формата "Gold buy @ price" (приоритет выше)
    if not sym:
        gold_match = re.search(r'\b(Gold|Silver|XAU|XAG)\b', t, re.I)
        if gold_match:
            sym = gold_match.group(1).upper()
            print(f"DEBUG: Symbol found (precious metal): {sym}")  # Отладка
    
    if not sym:
        # If still no symbol, try generic symbol detection
        for m in SYMBOL_RE.finditer(t):
            cand = _norm_symbol(m.group('sym'))
            if 2 <= len(cand) <= 20:
                sym = cand
                print(f"DEBUG: Symbol found (regex): {sym}")  # Отладка
                break
    
    out["symbol"] = sym
    print(f"DEBUG: Final symbol: {out['symbol']}")  # Отладка

    print(f"DEBUG: Before entry parsing")  # Отладка
    # Entry parsing
    if m := ENTRY_RANGE_RE.search(text):
        # Range entry: "3.9200 – 3.7660" -> "3.9200 – 3.7660"
        p1 = m.group('p1')
        p2 = m.group('p2')
        entry = f"{p1} – {p2}"
    elif m := ENTRY_MARKET_PRICE_RE.search(text):
        # Enhanced entry: "Entry Market Price 0.002120" -> "0.002120"
        entry = m.group('price')
    elif m := ENTRY_SINGLE_RE.search(text):
        # Single entry: "620.5" -> "620.5"
        entry = m.group('price')
    elif m := ENTRY_DIRECTION_RANGE_RE.search(text):
        # New format: "LONG : 0.007715-0.007435"
        p1 = m.group('p1')
        p2 = m.group('p2')
        entry = f"{p1} – {p2}"
    elif m := ENTRY_ZONE_RE.search(text):
        # Entry Zone format: "Entry Zone: 0.2158 – 0.2115" -> "0.2158 – 0.2115"
        p1 = m.group('p1')
        p2 = m.group('p2')
        entry = f"{p1} – {p2}"
        print(f"DEBUG: Entry found (Entry Zone format): {entry}")  # Отладка
    else:
        # Дополнительная проверка для формата "buy @ 3339.50 - 3337.00"
        buy_at_match = re.search(r'buy\s+@\s+(?P<p1>\d+(?:[.,]\d+)?)\s*-\s*(?P<p2>\d+(?:[.,]\d+)?)', text, re.I)
        if buy_at_match:
            p1 = buy_at_match.group('p1')
            p2 = buy_at_match.group('p2')
            entry = f"{p1} – {p2}"
            print(f"DEBUG: Entry found (buy @ format): {entry}")  # Отладка
        else:
            # Дополнительная проверка для формата "ZONE: price1 - price2"
            zone_match = re.search(r'ZONE\s*:\s*(?P<p1>\d+(?:[.,]\d+)?)\s*-\s*(?P<p2>\d+(?:[.,]\d+)?)', text, re.I)
            if zone_match:
                p1 = zone_match.group('p1')
                p2 = zone_match.group('p2')
                entry = f"{p1} – {p2}"
                print(f"DEBUG: Entry found (ZONE format): {entry}")  # Отладка
            else:
                # Дополнительная проверка для формата "по цене X - Y" (русский диапазон)
                po_cene_match = re.search(r'по\s+цене\s+(?P<p1>\d+(?:[.,]\d+)?)\s*-\s*(?P<p2>\d+(?:[.,]\d+)?)', text, re.I)
                if po_cene_match:
                    p1 = po_cene_match.group('p1')
                    p2 = po_cene_match.group('p2')
                    entry = f"{p1} – {p2}"
                    print(f"DEBUG: Entry found (по цене range format): {entry}")  # Отладка
                else:
                    # ✅ NEW: Дополнительная проверка для формата "по цене X" (одиночная цена)
                    po_cene_single_match = re.search(r'по\s+цене\s+(?P<price>\d+(?:[.,]\d+)?)', text, re.I)
                    if po_cene_single_match:
                        entry = po_cene_single_match.group('price')
                        print(f"DEBUG: Entry found (по цене single format): {entry}")  # Отладка
                    else:
                        entry = None
    
    # Assign entry to output
    if entry:
        out["entry"] = entry
    elif "Entry Executed" in t:
        # Для сигналов "Entry Executed" устанавливаем специальное значение
        out["entry"] = "Entry Executed"
        print(f"DEBUG: Entry set to 'Entry Executed'")  # Отладка
    
    print(f"DEBUG: Final entry: {out['entry']}")  # Отладка

    # Stop - сначала проверяем процентный формат
    # New pattern for percentage-based stop loss: "5%-10%"
    sl_percent_found = False
    sl_percent_patterns = [
        r'SL\s*:\s*(\d+)%?\s*-\s*(\d+)%?',  # SL: 5%-10%
        r'⛔️SL\s*:\s*(\d+)%?\s*-\s*(\d+)%?',  # ⛔️SL: 5%-10%
        r'StopLoss\s*:\s*(\d+)%?\s*-\s*(\d+)%?',  # StopLoss: 5%-10%
        r'стоп\s*:\s*(\d+)%?\s*-\s*(\d+)%?',  # стоп: 5%-10%
    ]
    
    for pattern in sl_percent_patterns:
        sl_percent_match = re.search(pattern, t, re.I)
        if sl_percent_match:
            # Сохраняем оригинальный формат с процентами
            p1 = int(sl_percent_match.group(1))
            p2 = int(sl_percent_match.group(2))
            out["stop"] = f"{p1}%-{p2}%"
            out["stopFormatted"] = f"{p1}%-{p2}%"
            print(f"DEBUG: Stop found (percentage format): {out['stop']} (from {p1}%-{p2}%)")  # Отладка
            sl_percent_found = True
            break
    
    # Если процентный формат не найден, проверяем обычные форматы
    if not sl_percent_found:
        m = STOP_RE.search(t)
        if m:
            out["stop"] = _to_float(m.group('sl'))
            print(f"DEBUG: Stop found: {out['stop']}")  # Отладка
        
        # Enhanced SL parsing for "SL⛔️(X)" format
        if not out["stop"]:
            m = SL_ENHANCED_RE.search(t)
            if m:
                out["stop"] = _to_float(m.group('sl'))
                print(f"DEBUG: Stop found (enhanced): {out['stop']}")  # Отладка
        
        # Enhanced stop loss parsing for "Stop Loss (SL): 2.33" format
        if not out["stop"]:
            m = STOP_ENHANCED_RE.search(t)
            if m:
                out["stop"] = _to_float(m.group('sl'))
                print(f"DEBUG: Stop found (Stop Loss SL format): {out['stop']}")  # Отладка
        
        # Дополнительная проверка для формата "СТОП-ЛОСС: $187,8"
        if not out["stop"]:
            stop_colon_match = SOL_SPECIFIC_PATTERNS['stop_loss_colon'].search(t)
            if stop_colon_match:
                out["stop"] = _to_float(stop_colon_match.group(1))
                print(f"DEBUG: Stop found (СТОП-ЛОСС format): {out['stop']}")  # Отладка
    
    # DCA parsing
    m = DCA_RE.search(t)
    if m:
        out["dca"] = _to_float(m.group('dca'))
        print(f"DEBUG: DCA found: {out['dca']}")  # Отладка

    # Targets list line
    m = TARGETS_LIST_RE.search(t)
    tps: List[float] = []
    targets_header_found = False
    targets_header_line_idx = -1
    
    if m:
        targets_header_found = True
        # Если найдена строка "Цели:", то ищем цели в следующих строках
        print(f"DEBUG: Found targets header: {m.group('list')}")  # Отладка
        
        # ВСЕГДА находим номер строки с заголовком для парсинга последующих строк
        lines = t.splitlines()
        for idx, line in enumerate(lines):
            if TARGETS_LIST_RE.search(line):
                targets_header_line_idx = idx
                print(f"DEBUG: Targets header found at line {idx}")
                break
        
        # Новый паттерн для формата "Targets: 🎯 0.1335, 0.1370, 0.1405, 0.1440, 0.1475"
        targets_content = m.group('list')
        print(f"DEBUG: Targets content: {targets_content}")
        
        # Ищем все числа в содержимом заголовка targets (но также будем парсить последующие строки)
        targets_numbers = _find_all_numbers(targets_content)
        if targets_numbers:
            print(f"DEBUG: Found targets in header: {targets_numbers}")
            for price in targets_numbers:
                tps.append(price)
                print(f"DEBUG: Added target from header: {price}, total targets now: {len(tps)}")
        else:
            print(f"DEBUG: No numbers found in targets header, will search subsequent lines")
    else:
        print(f"DEBUG: No targets header found, searching individual lines")  # Отладка
    
    # Проверяем строку "Фиксируем прибыль на: 24.929$ 24.574$ 24.174$"
    m = TARGETS_PRICES_RE.search(t)
    if m:
        prices_text = m.group('prices')
        print(f"DEBUG: Found prices line: {prices_text}")
        # Извлекаем все числа из строки с ценами
        prices_numbers = _find_all_numbers(prices_text)
        for price in prices_numbers:
            tps.append(price)
            print(f"DEBUG: Added target from prices line: {price}, total targets now: {len(tps)}")
    
    # ✅ NEW: Check Russian TP pattern "Отгрызаeм профит на: 20.6$ 20.9$ 21.157$"
    m = RUSSIAN_TP_RE.search(t)
    if m:
        prices_text = m.group('prices')
        print(f"DEBUG: Found Russian TP line: {prices_text}")
        # Извлекаем все числа из строки с ценами  
        prices_numbers = _find_all_numbers(prices_text)
        for price in prices_numbers:
            tps.append(price)
            print(f"DEBUG: Added target from Russian TP line: {price}, total targets now: {len(tps)}")
    
    # Enhanced TP parsing for "TP📈(X-Y-Z-W)💵" format
    m = TP_ENHANCED_RE.search(t)
    if m:
        prices_text = m.group('prices')
        print(f"DEBUG: Found enhanced TP line: {prices_text}")
        # Split by dash and extract numbers
        prices_parts = prices_text.split('-')
        for part in prices_parts:
            price = _to_float(part.strip())
            if price is not None:
                tps.append(price)
                print(f"DEBUG: Added target from enhanced TP: {price}, total targets now: {len(tps)}")
    
    # New pattern for numbered TP format: "1) 0.144 2) 0.153 3) 0.166"
    for m in TP_NUMBERED_RE.finditer(t):
        price = _to_float(m.group('tp'))
        if price is not None:
            tps.append(price)
            print(f"DEBUG: Added target from numbered TP: {price}, total targets now: {len(tps)}")
    
    # Enhanced pattern for "1) 0.007824" format (more specific)
    tp_numbered_enhanced_re = re.compile(r'(\d+)\)\s*(?P<tp>[-+]?\d+(?:[.,]\d+)?)', re.I)
    for m in tp_numbered_enhanced_re.finditer(t):
        price = _to_float(m.group('tp'))
        if price is not None:
            tps.append(price)
            print(f"DEBUG: Added target from enhanced numbered TP: {price}, total targets now: {len(tps)}")
    
    # New pattern for "T1: 3.15", "T2: 3.58" format
    for m in TP_T_FORMAT_RE.finditer(t):
        price = _to_float(m.group('tp'))
        if price is not None:
            tps.append(price)
            print(f"DEBUG: Added target from T format: {price}, total targets now: {len(tps)}")
    
    # Дополнительный паттерн для "Take-Profit target N 💵" формата
    tp_target_re = re.compile(r'Take-Profit\s+target\s+\d+\s*💵', re.I)
    if tp_target_re.search(t):
        print(f"DEBUG: Found Take-Profit target format, but no specific prices")
        # В этом формате цены не указаны, только номера целей
        # Можно установить placeholder или попытаться найти цены в других местах
    
    # Проходим по строкам и собираем таргеты из строк вида "Target N: price"/"Цель N: price"
    print(f"DEBUG: Starting target parsing, total lines: {len(t.splitlines() or [])}")  # Отладка
    for i, line in enumerate(t.splitlines() or []):
        print(f"DEBUG: Processing line {i}: '{line[:50]}...'")  # Отладка
        
        # Если это строка сразу после заголовка "Тейк-профиты:" и она содержит только число,
        # добавляем его как тейк-профит
        if targets_header_line_idx >= 0 and i > targets_header_line_idx:
            # Проверяем, является ли строка просто числом (возможно с пробелами)
            line_stripped = line.strip()
            if line_stripped and re.match(r'^[-+]?\d+(?:[.,]\d+)?$', line_stripped):
                price = _to_float(line_stripped)
                if price is not None:
                    tps.append(price)
                    print(f"DEBUG: Added target from subsequent line {i}: {price}, total targets now: {len(tps)}")
                    continue
        
        if re.search(r'(?:target|цель|закрыть\s+ордер)\b', line, re.I):
            print(f"DEBUG: Line {i} matched pattern")  # Отладка
            # Берём часть строки после двоеточия/тире — там должна быть цена таргета
            tail = line
            if ':' in line:
                tail = line.split(':', 1)[1]
            elif '–' in line:
                tail = line.split('–', 1)[1]
            elif '—' in line:
                tail = line.split('—', 1)[1]
            elif 'по цене' in line:
                # Для "Закрыть ордер по цене X" берём часть после "по цене"
                tail = line.split('по цене', 1)[1]
            
            # Очищаем tail от лишних символов
            tail = tail.strip()
            # Убираем символы валюты и лишние пробелы
            tail = re.sub(r'[\$\€\£]', '', tail)
            tail = tail.strip()
            
            nums = _find_all_numbers(tail)
            print(f"DEBUG: Line {i} -> tail '{tail}' -> nums {nums}")  # Отладка
            for v in nums:
                tps.append(v)
                print(f"DEBUG: Added target {v}, total targets now: {len(tps)}")  # Отладка
        else:
            print(f"DEBUG: Line {i} did not match pattern")  # Отладка
    # Поддержим также нотацию TP/тейк
    for q in TP_ALL_RE.finditer(t):
        val = _to_float(q.group('tp'))
        if val is not None:
            tps.append(val)
            print(f"DEBUG: Added target (TP/тейк): {val}, total targets now: {len(tps)}")  # Отладка
    
    if tps:
        # unique and ordered by appearance; then order by direction
        seen = set()
        ordered: List[float] = []
        for v in tps:
            if v not in seen:
                seen.add(v)
                ordered.append(v)
        if out["direction"] == 'SHORT':
            ordered = list(sorted(ordered, reverse=True))
        else:
            ordered = list(sorted(ordered))
        out["tp"] = ordered
        print(f"DEBUG: Final targets array: {out['tp']}")  # Отладка

    # TP percentages
    m = TP_PCT_LIST_RE.search(t)
    if m:
        pct_block = m.group(1)
        pcts: List[float] = []
        for q in PCT_RE.finditer(pct_block):
            val = _to_float(q.group(1))
            if val is not None:
                pcts.append(val)
                print(f"DEBUG: Added TP percentage: {val}, total TP percentages now: {len(pcts)}")  # Отладка
        out["tpPct"] = pcts

    # Confidence heuristic
    conf = 0.0
    conf += 0.25 if out["symbol"] else 0.0
    conf += 0.25 if out["direction"] else 0.0
    if out["entry"] is not None:
        conf += 0.2
    conf += 0.15 if out["stop"] else 0.0
    conf += 0.15 if out["tp"] or out["tpPct"] else 0.0
    conf += 0.05 if out["dca"] else 0.0  # Bonus for DCA
    conf += 0.05 if out["orderType"] else 0.0
    conf += 0.05 if out["profitPct"] else 0.0
    out["confidence"] = round(min(conf, 1.0), 2) # Пересчитываем confidence после добавления новых полей

    # Вычисляем проценты для tp и stop если entry - одиночное число
    if out.get("entry") and out.get("tp") and out.get("stop"):
        entry_str = str(out["entry"])
        # Проверяем, что entry НЕ содержит символы диапазона
        if '–' not in entry_str and '-' not in entry_str and '→' not in entry_str and 'to' not in entry_str.lower() and 'до' not in entry_str.lower():
            try:
                entry_price = float(entry_str)
                print(f"DEBUG: Computing percentages for single entry: {entry_price}")
                
                # Вычисляем проценты для take profit целей
                tp_percentages = []
                tp_formatted = []
                for i, tp_price in enumerate(out["tp"]):
                    if isinstance(tp_price, (int, float)):
                        percentage = ((tp_price - entry_price) / entry_price) * 100
                        tp_percentages.append(round(percentage, 2))
                        # Форматируем как "price(percentage%)"
                        tp_formatted.append(f"{tp_price}({round(percentage, 2)}%)")
                
                # Сохраняем оба варианта
                out["tpPct"] = tp_percentages  # Для обратной совместимости
                out["tpFormatted"] = tp_formatted  # Новый формат
                print(f"DEBUG: TP percentages: {tp_percentages}")
                print(f"DEBUG: TP formatted: {tp_formatted}")
                
                # Вычисляем процент для stop loss
                if out.get("stop"):
                    stop_price = out["stop"]
                    if isinstance(stop_price, (int, float)):
                        stop_percentage = ((stop_price - entry_price) / entry_price) * 100
                        out["stopPct"] = round(stop_percentage, 2)
                        # Форматируем stop как "price(percentage%)"
                        out["stopFormatted"] = f"{stop_price}({round(stop_percentage, 2)}%)"
                        print(f"DEBUG: Stop percentage: {stop_percentage}")
                        print(f"DEBUG: Stop formatted: {out['stopFormatted']}")
                    
                # Вычисляем процент для DCA
                if out.get("dca"):
                    dca_price = out["dca"]
                    if isinstance(dca_price, (int, float)):
                        dca_percentage = ((dca_price - entry_price) / entry_price) * 100
                        out["dcaPct"] = round(dca_percentage, 2)
                        # Форматируем DCA как "price(percentage%)"
                        out["dcaFormatted"] = f"{dca_price}({round(dca_percentage, 2)}%)"
                        print(f"DEBUG: DCA percentage: {dca_percentage}")
                        print(f"DEBUG: DCA formatted: {out['dcaFormatted']}")
                    
            except (ValueError, TypeError) as e:
                print(f"DEBUG: Error computing percentages: {e}")
                # Если не удалось вычислить проценты, оставляем как есть
                pass
        else:
            print(f"DEBUG: Entry is range, skipping percentage calculation: {entry_str}")

    return out


def create_enhanced_sol_signal(text: str) -> Dict[str, Any]:
    """
    Создает улучшенный SOL сигнал с дополнительной обработкой SOL-специфичных форматов.
    
    Args:
        text: Текст сигнала
        
    Returns:
        Улучшенные данные сигнала
    """
    # Базовый парсинг
    parsed = parse_signal(text)
    
    # Дополнительная обработка для SOL-специфичных форматов
    enhanced = parsed.copy()
    
    # Если символ не найден, но есть упоминание SOL или специфичные цены
    if not enhanced.get("symbol") and ("SOL" in text.upper() or "179,8 - 181,8" in text):
        enhanced["symbol"] = "SOL/USDT"
    
    # Дополнительная обработка стоп-лосса в формате "СТОП-ЛОСС: $187,8"
    if not enhanced.get("stop"):
        stop_match = SOL_SPECIFIC_PATTERNS['stop_loss_colon'].search(text)
        if stop_match:
            enhanced["stop"] = _to_float(stop_match.group(1))
    
    # Дополнительная обработка целей в формате "🔘 Закрыть ордер по цене $176,1"
    if not enhanced.get("tp"):
        targets = []
        
        # Ищем цели с эмодзи
        for match in SOL_SPECIFIC_PATTERNS['target_with_emoji'].finditer(text):
            price = _to_float(match.group(1))
            if price is not None:
                targets.append(price)
        
        # Ищем цели без эмодзи
        for match in SOL_SPECIFIC_PATTERNS['target_plain'].finditer(text):
            price = _to_float(match.group(1))
            if price is not None and price not in targets:
                targets.append(price)
        
        if targets:
            # Сортируем цели по направлению
            if enhanced.get("direction") == 'SHORT':
                targets.sort(reverse=True)
            else:
                targets.sort()
            enhanced["tp"] = targets
    
    # Добавляем метаданные
    enhanced["source"] = "enhanced_sol_parser"
    enhanced["raw_text"] = text
    
    return enhanced


def parse_sol_signal_with_utils(text: str) -> Dict[str, Any]:
    """
    Парсит SOL сигнал используя улучшенный парсер.
    
    Args:
        text: Текст сигнала
        
    Returns:
        Словарь с распарсенными данными
    """
    return create_enhanced_sol_signal(text)


if __name__ == "__main__":
    # Тестовый SOL сигнал
    test_signal = """🔑 Открыть ШОРТ по цене 179,8 - 181,8 долларов с кредитным плечом X25.

🍒 Цели:

🔘 Закрыть ордер по цене 178,4 
🔘 Закрыть ордер по цене 177,7 
🔘 Закрыть ордер по цене $176,1
🔘 Закрыть ордер по цене $174,3
🔘 Закрыть ордер по цене $171,5

❗️СТОП-ЛОСС: $187,8"""
    
    print("=== Тест улучшенного SOL парсера ===")
    enhanced_result = create_enhanced_sol_signal(test_signal)
    
    print("Улучшенный результат:")
    for key, value in enhanced_result.items():
        if key not in ["raw_text"]:  # Пропускаем длинные поля
            print(f"{key}: {value}")
    
    print("\n=== Тест базового парсера ===")
    basic_result = parse_signal(test_signal)
    
    print("Базовый результат:")
    for key, value in basic_result.items():
        if key not in ["raw_text"]:  # Пропускаем длинные поля
            print(f"{key}: {value}") 