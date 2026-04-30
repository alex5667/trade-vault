#!/usr/bin/env python3
"""
Telegram Bot Commands for XAUUSD Analytics.

Adds interactive commands to telegram_labeler.py:
    /obi XAUUSD - Get OBI timeline PNG
    /depth XAUUSD - Get depth profile PNG
    /events XAUUSD - Get recent OBI events

Usage:
    This module should be imported by telegram_labeler.py
    or can be used standalone for testing.

Integration example (aiogram 3.x):
    from services.telegram_bot_commands import register_analytics_commands
    register_analytics_commands(dp, bot)
"""

import os
import aiohttp
from typing import Optional
import urllib.parse
import math
import json
import re

# Service endpoints
BOOK_ANALYTICS_URL = os.getenv("BOOK_ANALYTICS_URL", "http://127.0.0.1:8090")


async def fetch_png(endpoint: str, params: dict) -> Optional[bytes]:
    """
    Fetch PNG from book_analytics_service.
    
    Args:
        endpoint: Endpoint path (e.g. "/render/obi.png")
        params: Query parameters
        
    Returns:
        PNG bytes or None
    """
    try:
        url = f"{BOOK_ANALYTICS_URL}{endpoint}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    return await resp.read()
                else:
                    print(f"⚠️  Failed to fetch {url}: {resp.status}")
                    return None
    except Exception as e:
        print(f"⚠️  Error fetching PNG: {e}")
        return None


async def fetch_events(symbol: str, last: int = 20) -> Optional[dict]:
    """
    Fetch recent OBI events.
    
    Args:
        symbol: Symbol name
        last: Number of events
        
    Returns:
        Events dict or None
    """
    try:
        url = f"{BOOK_ANALYTICS_URL}/events/pull"
        params = {"symbol": symbol, "last": last}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    print(f"⚠️  Failed to fetch events: {resp.status}")
                    return None
    except Exception as e:
        print(f"⚠️  Error fetching events: {e}")
        return None


def register_analytics_commands(dp, bot):
    """
    Register analytics commands with aiogram dispatcher.
    
    Args:
        dp: Dispatcher instance (aiogram 3.x)
        bot: Bot instance (aiogram 3.x)
    """
    from aiogram import types
    from aiogram.filters import Command
    
    @dp.message(Command("obi"))
    async def cmd_obi(message: types.Message):
        """Send OBI timeline PNG."""
        args = message.text.split()[1:] if message.text else []
        symbol = args[0] if args else "XAUUSD"
        
        await message.answer(f"📊 Fetching OBI timeline for {symbol}...")
        
        png_bytes = await fetch_png("/render/obi.png", {"symbol": symbol, "last": 300})
        
        if png_bytes:
            photo = types.BufferedInputFile(png_bytes, filename=f"{symbol}_obi.png")
            await message.answer_photo(
                photo=photo
                caption=f"📊 {symbol} OBI Timeline (last 5 min)"
            )
        else:
            await message.answer(f"❌ Failed to fetch OBI for {symbol}")
    
    @dp.message(Command("depth"))
    async def cmd_depth(message: types.Message):
        """Send depth profile PNG."""
        args = message.text.split()[1:] if message.text else []
        symbol = args[0] if args else "XAUUSD"
        
        await message.answer(f"📊 Fetching depth profile for {symbol}...")
        
        png_bytes = await fetch_png("/render/depth.png", {"symbol": symbol})
        
        if png_bytes:
            photo = types.BufferedInputFile(png_bytes, filename=f"{symbol}_depth.png")
            await message.answer_photo(
                photo=photo
                caption=f"📊 {symbol} Depth Profile (top levels)"
            )
        else:
            await message.answer(f"❌ Failed to fetch depth for {symbol}")
    
    @dp.message(Command("events"))
    async def cmd_events(message: types.Message):
        """Send recent OBI events."""
        args = message.text.split()[1:] if message.text else []
        symbol = args[0] if args else "XAUUSD"
        
        events_data = await fetch_events(symbol, last=10)
        
        if events_data and events_data.get("events"):
            lines = [f"📊 {symbol} Recent OBI Events:\n"]
            
            for evt in events_data["events"][-10:]:
                kind_emoji = "🟢" if "up" in evt["kind"] else "🔴"
                lines.append(
                    f"{kind_emoji} {evt['kind']}: OBI={evt['obi']:.3f}, "
                    f"sustained {evt['duration_ms']}ms"
                )
            
            await message.answer("\n".join(lines))
        elif events_data and events_data.get("count") == 0:
            await message.answer(f"📊 {symbol}: No recent OBI events")
        else:
            await message.answer(f"❌ Failed to fetch events for {symbol}")
    
    print("✅ Analytics commands registered: /obi, /depth, /events")

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090/api/v1/query")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434").rstrip("/")
MODEL = os.getenv("TELEGRAM_LLM_MODEL", "deepseek-r1:14b")

async def fetch_prometheus_metric(query: str) -> Optional[float]:
    try:
        url = f"{PROMETHEUS_URL}?query={urllib.parse.quote(query)}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('status') == 'success' and data['data']['result']:
                        v = float(data['data']['result'][0]['value'][1])
                        return v if not math.isnan(v) else 0.0
                return None
    except Exception as e:
        print(f"⚠️  Error fetching prometheus metric: {e}")
        return None

async def ask_llm(prompt_text: str) -> Optional[str]:
    try:
        endpoint = f"{OLLAMA_BASE_URL}/api/generate"
        payload = {
            "model": MODEL
            "prompt": prompt_text
            "stream": False
            "options": {"temperature": 0.1, "num_predict": 400}
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(endpoint, json=payload, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    response_text = data.get("response", "").strip()
                    response_text = re.sub(r'<think>.*?</think>', '', response_text, flags=re.DOTALL).strip()
                    return response_text
    except Exception as e:
        print(f"⚠️  Error asking LLM: {e}")
        return None

def register_task_command(dp, bot=None):
    from aiogram import types
    from aiogram.filters import Command
    
    @dp.message(Command("task"))
    async def cmd_task(message: types.Message):
        args = message.text.split(maxsplit=1)[1:] if message.text else []
        question = args[0] if args else ""
        
        if not question:
            await message.answer("❌ Укажите вопрос или задачу. Например: `/task сколько сейчас открытых позиций`", parse_mode="Markdown")
            return
            
        wait_msg = await message.answer("🤖 Собираю метрики и размышляю (может занять до 60 сек)...")
        
        # Запрашиваем метрики из Prometheus
        open_pos = await fetch_prometheus_metric('sum(max by (symbol) (open_positions_count)) or vector(0)')
        pos_str = str(int(open_pos)) if open_pos is not None else "Неизвестно"
        
        prompt = (
            "Ты — торговый AI-ассистент (Antigravity). Ответь на вопрос пользователя на русском языке.\n"
            "Текущие метрики системы:\n"
            f"- Открытые реальные позиции: {pos_str}\n\n"
            f"Вопрос: {question}\n\n"
            "Отвечай кратко, по делу и профессионально. Только сухие факты."
        )
        
        answer = await ask_llm(prompt)
        
        if answer:
            await wait_msg.edit_text(f"🧠 **Antigravity AI:**\n\n{answer}", parse_mode="Markdown")
        else:
            await wait_msg.edit_text("❌ Ошибка при обращении к локальной LLM-модели (Ollama).")
    
    print("✅ System task/LLM command registered: /task")


# Alternative: Simple HTTP bot commands (without aiogram)
def create_simple_handlers():
    """
    Create simple handlers for non-aiogram bots.
    
    Returns:
        Dict of command -> handler function
    """
    async def handle_obi(chat_id: int, args: list, send_photo_func, send_message_func):
        """Handle /obi command."""
        symbol = args[0] if args else "XAUUSD"
        
        await send_message_func(chat_id, f"📊 Fetching OBI timeline for {symbol}...")
        
        png_bytes = await fetch_png("/render/obi.png", {"symbol": symbol, "last": 300})
        
        if png_bytes:
            await send_photo_func(chat_id, png_bytes, f"📊 {symbol} OBI Timeline")
        else:
            await send_message_func(chat_id, f"❌ Failed to fetch OBI for {symbol}")
    
    async def handle_depth(chat_id: int, args: list, send_photo_func, send_message_func):
        """Handle /depth command."""
        symbol = args[0] if args else "XAUUSD"
        
        await send_message_func(chat_id, f"📊 Fetching depth profile for {symbol}...")
        
        png_bytes = await fetch_png("/render/depth.png", {"symbol": symbol})
        
        if png_bytes:
            await send_photo_func(chat_id, png_bytes, f"📊 {symbol} Depth Profile")
        else:
            await send_message_func(chat_id, f"❌ Failed to fetch depth for {symbol}")
    
    async def handle_events(chat_id: int, args: list, send_message_func):
        """Handle /events command."""
        symbol = args[0] if args else "XAUUSD"
        
        events_data = await fetch_events(symbol, last=10)
        
        if events_data and events_data.get("events"):
            lines = [f"📊 {symbol} Recent OBI Events:\n"]
            
            for evt in events_data["events"][-10:]:
                kind_emoji = "🟢" if "up" in evt["kind"] else "🔴"
                lines.append(
                    f"{kind_emoji} {evt['kind']}: OBI={evt['obi']:.3f}, "
                    f"sustained {evt['duration_ms']}ms"
                )
            
            await send_message_func(chat_id, "\n".join(lines))
        elif events_data and events_data.get("count") == 0:
            await send_message_func(chat_id, f"📊 {symbol}: No recent OBI events")
        else:
            await send_message_func(chat_id, f"❌ Failed to fetch events for {symbol}")
    
    return {
        "/obi": handle_obi
        "/depth": handle_depth
        "/events": handle_events
    }


if __name__ == "__main__":
    # Test mode
    import asyncio
    
    async def test():
        print("Testing OBI PNG fetch...")
        png = await fetch_png("/render/obi.png", {"symbol": "XAUUSD", "last": 100})
        if png:
            print(f"✅ Fetched OBI PNG ({len(png)} bytes)")
            with open("/tmp/test_obi.png", "wb") as f:
                f.write(png)
            print("   Saved to /tmp/test_obi.png")
        
        print("\nTesting depth PNG fetch...")
        png = await fetch_png("/render/depth.png", {"symbol": "XAUUSD"})
        if png:
            print(f"✅ Fetched depth PNG ({len(png)} bytes)")
            with open("/tmp/test_depth.png", "wb") as f:
                f.write(png)
            print("   Saved to /tmp/test_depth.png")
        
        print("\nTesting events fetch...")
        events = await fetch_events("XAUUSD", last=5)
        if events:
            print(f"✅ Fetched {events['count']} events")
            for evt in events.get("events", []):
                print(f"   {evt['kind']}: OBI={evt['obi']:.3f}")
    
    asyncio.run(test())

