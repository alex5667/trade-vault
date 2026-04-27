#!/usr/bin/env python3
"""
Telegram Bot Commands for EV Gate Monitoring

Добавляет команды для мониторинга EV gate через Telegram бота.

Commands:
    /ev_stats - показать статистику P(hit TP1)
    /ev_health - проверка здоровья EV gate
    /ev_tune <symbol> - рекомендации по тюнингу для символа
    
Usage:
    # Добавить в ваш existing telegram bot:
    from tools.telegram_ev_commands import register_ev_commands
    register_ev_commands(bot, redis_client)
"""

import redis
from typing import Optional
import statistics


def format_stats_message(stats: dict) -> str:
    """Форматирует статистику для Telegram."""
    lines = ["📊 *EV Gate Statistics*\n"]
    
    lines.append(f"Total keys: {stats.get('total_keys', 0)}")
    lines.append(f"Valid keys: {stats.get('valid_keys', 0)}\n")
    
    if "overall" in stats:
        ov = stats["overall"]
        lines.append("*Overall P(TP1):*")
        lines.append(f"  Mean: {ov['mean']:.3f}")
        lines.append(f"  Median: {ov['median']:.3f}")
        lines.append(f"  Range: [{ov['min']:.3f}, {ov['max']:.3f}]\n")
    
    if "by_kind" in stats:
        lines.append("*By Kind:*")
        for kind, data in sorted(stats["by_kind"].items(), key=lambda x: -x[1]["mean"]):
            lines.append(f"  {kind}: {data['mean']:.3f} (n={data['count']})")
        lines.append("")
    
    if "by_symbol" in stats:
        lines.append("*By Symbol:*")
        for sym, data in sorted(stats["by_symbol"].items(), key=lambda x: -x[1]["mean"]):
            lines.append(f"  {sym}: {data['mean']:.3f} (n={data['count']})")
    
    return "\n".join(lines)


def get_ev_stats(r: redis.Redis, min_trades: int = 10) -> dict:
    """Получает EV статистику из Redis."""
    from tools.analyze_ev_stats import fetch_all_ev_stats, analyze_stats
    
    stats = fetch_all_ev_stats(r)
    analysis = analyze_stats(stats, min_trades=min_trades)
    
    return analysis


def check_ev_health(r: redis.Redis) -> str:
    """Проверяет здоровье EV gate."""
    lines = ["🏥 *EV Gate Health Check*\n"]
    
    # 1. Check if EV stats exist
    keys = list(r.scan_iter(match="ev:tp1:*", count=10))
    if not keys:
        return "❌ No EV statistics found!\n\nPossible issues:\n- EV_TP1_ENABLED not set\n- No trades closed yet\n- stats_aggregator not running"
    
    lines.append(f"✅ Found {len(keys)} EV stat keys\n")
    
    # 2. Check for stale stats
    import time
    now_ms = int(time.time() * 1000)
    stale_threshold = 24 * 3600 * 1000  # 24 hours
    
    stale_count = 0
    for key in keys[:20]:  # Sample first 20
        last_ts_ms = r.hget(key, "last_ts_ms")
        if last_ts_ms:
            try:
                age_ms = now_ms - int(last_ts_ms)
                if age_ms > stale_threshold:
                    stale_count += 1
            except Exception:
                pass
    
    if stale_count > 0:
        lines.append(f"⚠️  {stale_count} stale stats (>24h old)")
    else:
        lines.append("✅ All stats fresh (<24h)")
    
    lines.append("")
    
    # 3. Check sample sizes
    low_sample_count = 0
    for key in keys[:20]:
        n = r.hget(key, "n")
        if n:
            try:
                if int(n) < 40:
                    low_sample_count += 1
            except Exception:
                pass
    
    if low_sample_count > 0:
        lines.append(f"⚠️  {low_sample_count} stats with n < 40 (still warming up)")
    else:
        lines.append("✅ All sampled stats have n >= 40")
    
    return "\n".join(lines)


def get_tuning_recommendations(r: redis.Redis, symbol: Optional[str] = None) -> str:
    """Генерирует рекомендации по тюнингу."""
    lines = ["🔧 *Tuning Recommendations*"]
    if symbol:
        lines[0] += f" for {symbol}"
    lines.append("")
    
    # Get stats
    pattern = f"ev:tp1:*:{symbol}:*" if symbol else "ev:tp1:*"
    keys = list(r.scan_iter(match=pattern, count=100))
    
    if not keys:
        return f"❌ No stats found for pattern: {pattern}"
    
    # Collect probabilities
    probs = []
    for key in keys:
        p_ema = r.hget(key, "p_ema")
        n = r.hget(key, "n")
        if p_ema and n:
            try:
                if int(n) >= 10:
                    probs.append(float(p_ema))
            except Exception:
                pass
    
    if not probs:
        return "❌ Insufficient data for recommendations"
    
    # Calculate percentiles
    p25 = statistics.quantiles(probs, n=4)[0] if len(probs) > 4 else min(probs)
    p50 = statistics.median(probs)
    p75 = statistics.quantiles(probs, n=4)[2] if len(probs) > 4 else max(probs)
    
    lines.append(f"P25: {p25:.3f}")
    lines.append(f"P50: {p50:.3f}")
    lines.append(f"P75: {p75:.3f}\n")
    
    # Current settings (assumed)
    current_p_min = 0.55
    current_k = 2.0
    
    # Recommendations
    if p50 < current_p_min:
        lines.append("⚠️  *Probability too low!*")
        lines.append(f"Median P(TP1) = {p50:.3f} < current p_min = {current_p_min:.3f}\n")
        lines.append("*Recommendations:*")
        lines.append(f"1. Lower EDGE_EV_P_MIN to {p25:.2f} or {(p25+p50)/2:.2f}")
        lines.append(f"2. OR lower K to {current_k*0.8:.1f} (makes threshold easier)")
        lines.append("3. OR improve signal quality")
    elif p50 > 0.65:
        lines.append("✅ *High win rate!*")
        lines.append(f"Median P(TP1) = {p50:.3f} >> p_min = {current_p_min:.3f}\n")
        lines.append("*Could be more aggressive:*")
        lines.append(f"1. Raise EDGE_EV_P_MIN to {p25:.2f} (stricter)")
        lines.append(f"2. OR raise K to {current_k*1.2:.1f} (higher threshold)")
    else:
        lines.append("✅ *Looks balanced!*")
        lines.append(f"Median P(TP1) = {p50:.3f} is reasonable\n")
        lines.append("No immediate tuning needed.")
    
    return "\n".join(lines)


def register_ev_commands(bot, redis_client: redis.Redis):
    """
    Регистрирует команды в Telegram боте.
    
    Args:
        bot: telebot.TeleBot instance
        redis_client: Redis connection
    """
    
    @bot.message_handler(commands=['ev_stats'])
    def cmd_ev_stats(message):
        try:
            stats = get_ev_stats(redis_client, min_trades=10)
            text = format_stats_message(stats)
            bot.reply_to(message, text, parse_mode='Markdown')
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")
    
    @bot.message_handler(commands=['ev_health'])
    def cmd_ev_health(message):
        try:
            health = check_ev_health(redis_client)
            bot.reply_to(message, health, parse_mode='Markdown')
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")
    
    @bot.message_handler(commands=['ev_tune'])
    def cmd_ev_tune(message):
        try:
            # Parse symbol from command (e.g., /ev_tune BTCUSDT)
            parts = message.text.split()
            symbol = parts[1] if len(parts) > 1 else None
            
            recommendations = get_tuning_recommendations(redis_client, symbol)
            bot.reply_to(message, recommendations, parse_mode='Markdown')
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")
    
    print("✅ EV Gate Telegram commands registered: /ev_stats, /ev_health, /ev_tune")


# Example usage
if __name__ == "__main__":
    import telebot
    import os
    
    # Example setup
    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    if not BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN not set")
        exit(1)
    
    r = redis.from_url(REDIS_URL, decode_responses=True)
    bot = telebot.TeleBot(BOT_TOKEN)
    
    register_ev_commands(bot, r)
    
    print("🤖 EV Gate Telegram Bot started")
    bot.infinity_polling()
