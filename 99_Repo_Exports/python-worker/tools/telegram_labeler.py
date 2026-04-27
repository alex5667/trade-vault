#!/usr/bin/env python3
"""
Telegram Labeler Bot with Analytics Commands.

Commands:
    /start - Welcome message
    /obi [SYMBOL] - Get OBI timeline PNG
    /depth [SYMBOL] - Get depth profile PNG
    /events [SYMBOL] [N] - Get recent OBI events (default N=10)
    /status [SYMBOL] - Get current OBI status

Environment:
    BOT_TOKEN - Telegram bot token (required)
    OBI_HOST - OBI service URL (default: http://127.0.0.1:8090)
    DEFAULT_SYMBOL - Default symbol (default: XAUUSD)

Usage:
    export BOT_TOKEN=123456:ABC-DEF...
    export OBI_HOST=http://127.0.0.1:8090
    python3 -m tools.telegram_labeler
"""

import os
import asyncio
import httpx
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile

# Configuration
BOT_TOKEN = os.environ["BOT_TOKEN"]
OBI_HOST = os.getenv("OBI_HOST", "http://127.0.0.1:8090")
DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "XAUUSD")

# Bot and Dispatcher
bot = Bot(BOT_TOKEN)
dp = Dispatcher()


def get_symbol_from_message(msg: Message, index: int = 1) -> str:
    """Extract symbol from message or use default."""
    parts = (msg.text or "").split()
    return (parts[index] if len(parts) > index else DEFAULT_SYMBOL).upper()


@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Welcome message with available commands."""
    text = (
        "🤖 <b>XAUUSD Analytics Bot</b>\n\n"
        "Available commands:\n"
        "• <code>/obi [SYMBOL]</code> - OBI timeline PNG\n"
        "• <code>/depth [SYMBOL]</code> - Depth profile PNG\n"
        "• <code>/events [SYMBOL] [N]</code> - Recent OBI events\n"
        "• <code>/status [SYMBOL]</code> - Current OBI status\n\n"
        f"Default symbol: <b>{DEFAULT_SYMBOL}</b>"
    )
    await message.answer(text, parse_mode="HTML")


@dp.message(Command("obi"))
async def cmd_obi(message: Message):
    """Send OBI timeline PNG."""
    symbol = get_symbol_from_message(message)
    
    await message.answer(f"📊 Fetching OBI timeline for {symbol}...")
    
    try:
        url = f"{OBI_HOST}/render/obi.png?symbol={symbol}&last=300"
        
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url)
            response.raise_for_status()
            
            # Save to temp file
            tmp_path = f"/tmp/obi_{symbol}.png"
            with open(tmp_path, "wb") as f:
                f.write(response.content)
        
        # Send photo
        photo = FSInputFile(tmp_path)
        await message.answer_photo(
            photo=photo,
            caption=f"📊 {symbol} OBI Timeline (±threshold)"
        )
    except Exception as e:
        await message.answer(f"❌ Failed to fetch OBI for {symbol}: {e}")


@dp.message(Command("depth"))
async def cmd_depth(message: Message):
    """Send depth profile PNG."""
    symbol = get_symbol_from_message(message)
    
    await message.answer(f"📊 Fetching depth profile for {symbol}...")
    
    try:
        url = f"{OBI_HOST}/render/depth.png?symbol={symbol}"
        
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url)
            response.raise_for_status()
            
            # Save to temp file
            tmp_path = f"/tmp/depth_{symbol}.png"
            with open(tmp_path, "wb") as f:
                f.write(response.content)
        
        # Send photo
        photo = FSInputFile(tmp_path)
        await message.answer_photo(
            photo=photo,
            caption=f"📊 {symbol} Depth Profile (top levels)"
        )
    except Exception as e:
        await message.answer(f"❌ Failed to fetch depth for {symbol}: {e}")


@dp.message(Command("events"))
async def cmd_events(message: Message):
    """Send recent OBI events."""
    parts = (message.text or "").split()
    symbol = (parts[1] if len(parts) > 1 else DEFAULT_SYMBOL).upper()
    last = int(parts[2]) if len(parts) > 2 else 10
    
    try:
        url = f"{OBI_HOST}/events/pull?symbol={symbol}&last={last}"
        
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(url)
            
            if response.status_code != 200:
                await message.answer(f"📊 {symbol}: No events data available")
                return
            
            data = response.json().get("events", [])
        
        if not data:
            await message.answer(f"📊 {symbol}: No recent events")
            return
        
        # Format events
        lines = [f"📊 <b>{symbol} OBI Events</b> (last {len(data)}):\n"]
        
        for evt in data:
            kind_emoji = "🟢" if "up" in evt["kind"] else "🔴"
            lines.append(
                f"{kind_emoji} <code>{evt['kind']}</code>\n"
                f"   OBI: <b>{evt['obi']:.3f}</b>\n"
                f"   Duration: {int(evt['duration_ms'])}ms\n"
            )
        
        await message.answer("\n".join(lines), parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Failed to fetch events for {symbol}: {e}")


@dp.message(Command("status"))
async def cmd_status(message: Message):
    """Send current OBI status."""
    symbol = get_symbol_from_message(message)
    
    try:
        url = f"{OBI_HOST}/features/obi?symbol={symbol}&last=1"
        
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(url)
            
            if response.status_code != 200:
                await message.answer(f"📊 {symbol}: No data from OBI service")
                return
            
            data = response.json()
        
        threshold = data.get("threshold", 0.0)
        points = data.get("points", [])
        
        if not points:
            await message.answer(f"📊 {symbol}: No OBI data yet")
            return
        
        last_point = points[-1]
        obi = last_point.get("obi_signed", 0.0)
        count = data.get("count", 0)
        
        # Status emoji
        if abs(obi) >= threshold:
            status_emoji = "🟢⬆️" if obi > 0 else "🔴⬇️"
            status_text = "Above threshold"
        else:
            status_emoji = "⚪"
            status_text = "Neutral"
        
        message_text = (
            f"{status_emoji} <b>{symbol} OBI Status</b>\n\n"
            f"Current OBI: <code>{obi:.3f}</code>\n"
            f"Threshold: ±{threshold:.2f}\n"
            f"Status: <b>{status_text}</b>\n"
            f"Data points: {count}"
        )
        
        await message.answer(message_text, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Failed to fetch status for {symbol}: {e}")


async def main():
    """Main entry point."""
    print(f"🤖 Telegram Labeler Bot starting...")
    print(f"   Bot Token: {BOT_TOKEN[:10]}***")
    print(f"   OBI Host: {OBI_HOST}")
    print(f"   Default Symbol: {DEFAULT_SYMBOL}")
    print()
    print("📱 Commands:")
    print("   /start - Welcome message")
    print("   /obi [SYMBOL] - OBI timeline PNG")
    print("   /depth [SYMBOL] - Depth profile PNG")
    print("   /events [SYMBOL] [N] - Recent events")
    print("   /status [SYMBOL] - Current status")
    print("   /task [QUESTION] - Ask the Antigravity LLM metrics questions")
    print()
    print("🚀 Bot starting polling...")
    
    from services.telegram_bot_commands import register_task_command
    register_task_command(dp, bot)
    
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

