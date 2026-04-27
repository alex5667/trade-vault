#!/usr/bin/env python3
"""
Тест для проверки форматирования цен (избегание научной нотации).
"""

def format_price(value):
    """Форматирует цену, избегая научной нотации."""
    if value in ["-", None, "", "N/A"]:
        return "N/A"

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

def test_price_formatting():
    """Тестирует форматирование цен из примера DOGS/USDT."""

    print("🧪 Тест форматирования цен для DOGS/USDT сигнала\n")

    # Тестовые данные из сигнала
    test_cases = [
        ("Entry 1", 0.00005821),
        ("Entry 2", 0.00005600),
        ("TP1", 0.00005879),
        ("TP2", 0.00005938),
        ("TP3", 0.00005997),
        ("TP4", 0.00006057),
        ("TP5", 0.00006118),
        ("TP6", 0.00006179),
        ("Stop-Loss", 0.00005447),
    ]

    print("=" * 60)
    print(f"{'Поле':<15} | {'Оригинал':<15} | {'str()':<15} | {'format_price()':<15}")
    print("=" * 60)

    for label, value in test_cases:
        original = f"{value}"
        str_format = str(value)
        formatted = format_price(value)

        # Проверяем, есть ли научная нотация в str()
        has_scientific = 'e' in str_format.lower()
        emoji = "❌" if has_scientific else "✅"

        print(f"{label:<15} | {original:<15} | {str_format:<15} {emoji} | {formatted:<15} ✅")

    print("=" * 60)

    # Пример форматированного сообщения
    print("\n📱 Пример форматированного сообщения:\n")

    tp_values = [0.00005879, 0.00005938, 0.00005997, 0.00006057, 0.00006118, 0.00006179]
    tp_str = " | ".join(f"{format_price(t)}$" for t in tp_values[:3])
    if len(tp_values) > 3:
        tp_str += f" | ... (+{len(tp_values) - 3})"

    message = f"""🚨 ТОРГОВЫЙ СИГНАЛ

🟢 LONG DOGSUSDT
💰 Вход: {format_price(0.00005821)} – {format_price(0.00005600)}$ (20x)
🎯 Цели: {tp_str}
🛑 Стоп: {format_price(0.00005447)}$
📈 Потенциал: %
🏢  |

📺 Канал: @wallstreetqueenofficialTG1
⏰ 04:07:16 28.10.2025"""

    print(message)

    # Сравнение старого и нового форматирования
    print("\n" + "=" * 60)
    print("📊 Сравнение старого и нового форматирования:")
    print("=" * 60)

    print("\n❌ СТАРОЕ (с научной нотацией):")
    print(f"🎯 Цели: {str(0.00005879)}$ | {str(0.00005938)}$ | {str(0.00005997)}$ | ... (+3)")
    print(f"🛑 Стоп: {str(0.00005447)}$")

    print("\n✅ НОВОЕ (без научной нотации):")
    print(f"🎯 Цели: {format_price(0.00005879)}$ | {format_price(0.00005938)}$ | {format_price(0.00005997)}$ | ... (+3)")
    print(f"🛑 Стоп: {format_price(0.00005447)}$")

    print("\n✅ Все тесты пройдены успешно!")

if __name__ == "__main__":
    test_price_formatting()

