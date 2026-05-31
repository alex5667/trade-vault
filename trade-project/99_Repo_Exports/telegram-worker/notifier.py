#!/usr/bin/env python3
"""
Notifier модуль для отправки сигналов в Telegram бот.
Использует improved_notifier для красивого форматирования.
"""

import asyncio
from typing import Any, Dict, List, Optional

from improved_notifier import ImprovedTelegramNotifier, ENABLED

# Глобальный синглтон notifier (lazy-initialized)
_notifier: Optional[ImprovedTelegramNotifier] = None


def _get_notifier() -> ImprovedTelegramNotifier:
    """Возвращает (создаёт при необходимости) глобальный экземпляр notifier."""
    global _notifier
    if _notifier is None:
        _notifier = ImprovedTelegramNotifier()
    return _notifier

import logging
import traceback

_logger = logging.getLogger(__name__)


async def notify_parsed_signal(
    parsed: Dict[str, Any],
    raw: Dict[str, Any],
    stream_name: str = None,
    message_id: str = None,
) -> bool:
    """
    Отправляет распарсенный сигнал в Telegram бот.

    Args:
        parsed: Распарсенные данные сигнала
        raw: Сырые данные сообщения
        stream_name: Имя потока (не используется)
        message_id: ID сообщения (не используется)
    
    Returns:
        bool: True если отправка успешна
    """
    try:
        notifier = _get_notifier()
        # Форматируем сообщение
        message = notifier.format_signal_message(parsed, raw)
        
        # Отправляем уведомление
        success = await notifier.send_notification(message)
        
        if success:
            print(f"✅ notifier: сигнал {parsed.get('symbol', 'N/A')} {parsed.get('direction', 'N/A')} отправлен")
        else:
            print(f"❌ notifier: ошибка отправки сигнала {parsed.get('symbol', 'N/A')} {parsed.get('direction', 'N/A')}")
        
        return success
    except Exception as e:
        # fail-open: notifier не должен валить весь consumer
        print(f"❌ notifier: исключение при отправке: {e}")
        return False

def _split_html_message(text: str, max_length: int = 3800) -> list:
    """
    Разбивает HTML-сообщение на части, соблюдая лимит Telegram.
    Старается разбивать по границам разделов для сохранения читаемости.
    Обрабатывает экстремально длинные строки.
    
    Args:
        text: HTML текст для разбиения
        max_length: Максимальная длина одной части (дефолт 3800 для безопасности)
    
    Returns:
        list: Список частей сообщения
    """
    if len(text) <= max_length:
        return [text]
    
    lines = text.split('\n')
    chunks = []
    current_chunk = []
    current_length = 0
    
    for line in lines:
        line_length = len(line) + 1  # +1 для \n
        
        # Сценарий 1: Сама строка длиннее лимита (например, длинный JSON или лог)
        if line_length > max_length:
            # Сначала сбрасываем текущий накопленный чанк
            if current_chunk:
                chunks.append('\n'.join(current_chunk))
                current_chunk = []
                current_length = 0
            
            # Разбиваем длинную строку жестко
            # Используем max_length - 50 чтобы оставить место для индикаторов
            safe_len = max_length - 100
            line_parts = [line[i:i+safe_len] for i in range(0, len(line), safe_len)]
            
            # Все части кроме последней становятся отдельными чанками
            chunks.extend(line_parts[:-1])
            
            # Последняя часть становится началом нового чанка
            last_part = line_parts[-1]
            current_chunk = [last_part]
            current_length = len(last_part) + 1
            
        # Сценарий 2: Строка влезает, но переполняет текущий чанк
        elif current_length + line_length > max_length:
            chunks.append('\n'.join(current_chunk))
            current_chunk = [line]
            current_length = line_length
            
        # Сценарий 3: Строка влезает в текущий чанк
        else:
            current_chunk.append(line)
            current_length += line_length
    
    # Добавляем последний chunk
    if current_chunk:
        chunks.append('\n'.join(current_chunk))
    
    # Добавляем индикаторы частей если сообщение разбито
    if len(chunks) > 1:
        final_chunks = []
        for i, chunk in enumerate(chunks):
            part_indicator = f"\n\n📄 Part {i+1}/{len(chunks)}"
            final_chunks.append(chunk + part_indicator)
        return final_chunks
    
    return chunks


async def send_html_to_telegram(html_text: str, buttons: list = None) -> bool:
    """
    Отправляет HTML-форматированный текст напрямую в Telegram.
    Разбивает длинные сообщения на части вместо обрезания.
    Используется для отчетов и других системных сообщений.
    
    Args:
        html_text: HTML текст для отправки
        buttons: Опциональный список кнопок (список списков dict)
    
    Returns:
        bool: True если отправка успешна
    """
    try:
        notifier = _get_notifier()
        # Разбиваем сообщение на части если необходимо (с запасом 3800)
        chunks = _split_html_message(html_text, max_length=3800)
        
        if len(chunks) > 1:
            print(f"📨 notifier: сообщение разбито на {len(chunks)} частей ({len(html_text)} символов)")
        
        success_count = 0
        failed_count = 0
        
        for i, chunk in enumerate(chunks):
            # Кнопки прикрепляем только к последней части сообщения
            current_buttons = buttons if (buttons and i == len(chunks) - 1) else None
            
            success = await notifier.send_notification(chunk, buttons=current_buttons)
            
            if success:
                success_count += 1
                if len(chunks) > 1:
                    print(f"✅ notifier: часть {i+1}/{len(chunks)} отправлена ({len(chunk)} символов)")
            else:
                failed_count += 1
                print(f"❌ notifier: ошибка отправки части {i+1}/{len(chunks)} ({len(chunk)} символов)")
            
            # Небольшая задержка между частями для избежания rate limit
            if i < len(chunks) - 1:
                await asyncio.sleep(0.5)
        
        # Итоговый результат
        if success_count == len(chunks):
            print(f"✅ notifier: HTML отчет отправлен ({len(html_text)} символов, {len(chunks)} частей)")
            return True
        elif success_count > 0:
            print(f"⚠️ notifier: частичная отправка ({success_count}/{len(chunks)} частей)")
            stats = notifier.stats.get_stats_dict()
            print(f"   Статистика: sent={stats['sent']}, failed={stats['failed']}, success_rate={stats['success_rate']:.1f}%")
            return False
        else:
            print(f"❌ notifier: ошибка отправки HTML отчета ({len(html_text)} символов)")
            stats = notifier.stats.get_stats_dict()
            print(f"   Статистика: sent={stats['sent']}, failed={stats['failed']}, success_rate={stats['success_rate']:.1f}%")
            return False
        
    except Exception as e:
        print(f"❌ notifier: исключение при отправке HTML: {e}")
        import traceback
        traceback.print_exc()
        return False

async def delete_message_from_stream(stream_name: str, message_id: str) -> bool:
    """
    Удаляет сообщение из потока (заглушка).
    
    Args:
        stream_name: Имя потока
        message_id: ID сообщения
    
    Returns:
        bool: True если удаление успешно
    """
    # В текущей реализации сообщения не удаляются
    return True

if __name__ == "__main__":
    # Тест модуля
    async def test():
        parsed = {
            "symbol": "ICPUSDT",
            "direction": "SHORT",
            "entry": "4.7",
            "stop": "5.046",
            "tp": ["4.627", "4.55", "4.506"],
            "leverage": "19",
            "confidence": "1.0",
            "orderType": "Рыночный ордер",
            "profitPct": "78.0",
            "exchange": "BYBIT"
        }
        
        raw = {
            "chat_title": "Trading Signals",
            "username": "@trading_signals"
        }
        
        print("🧪 Тест notifier модуля:")
        result = await notify_parsed_signal(parsed, raw)
        print(f"Результат: {result}")
    
    asyncio.run(test())
