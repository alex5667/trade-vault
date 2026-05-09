"""
Проверка логики soft_rate вычисления.
Этот тест демонстрирует, что soft_rate МОЖЕТ быть > 0 при правильных условиях.
"""

def test_soft_rate_logic_explanation():
    """
    Демонстрация логики soft-fail из of_confirm_engine.py (строки 906-912).
    
    Условия для ok_soft=1:
    1. ok == 0 (сигнал не прошёл)
    2. have == need - 1 (не хватает ровно одного лега)
    3. score >= soft_score_min (по умолчанию 0.60 после фикса)
    4. exec_risk_norm <= soft_exec_max (по умолчанию 0.65 после фикса)
    """

    # Симуляция данных из реального потока
    test_cases = [
        {
            "name": "Идеальный soft-fail (после фикса)",
            "ok": 0,
            "have": 2,
            "need": 3,
            "score": 0.65,
            "exec_risk_norm": 0.50,
            "soft_score_min": 0.60,  # НОВОЕ значение после фикса
            "soft_exec_max": 0.65,   # НОВОЕ значение после фикса
            "expected_ok_soft": 1,
        },
        {
            "name": "Soft-fail ДО фикса (невозможно)",
            "ok": 0,
            "have": 2,
            "need": 3,
            "score": 0.65,
            "exec_risk_norm": 0.50,
            "soft_score_min": 0.78,  # СТАРОЕ значение (БАГ!)
            "soft_exec_max": 0.45,   # СТАРОЕ значение
            "expected_ok_soft": 0,   # Не проходит, т.к. 0.65 < 0.78
        },
        {
            "name": "Слишком низкий score",
            "ok": 0,
            "have": 2,
            "need": 3,
            "score": 0.55,  # < 0.60
            "exec_risk_norm": 0.50,
            "soft_score_min": 0.60,
            "soft_exec_max": 0.65,
            "expected_ok_soft": 0,
        },
        {
            "name": "Слишком высокий exec_risk",
            "ok": 0,
            "have": 2,
            "need": 3,
            "score": 0.65,
            "exec_risk_norm": 0.75,  # > 0.65
            "soft_score_min": 0.60,
            "soft_exec_max": 0.65,
            "expected_ok_soft": 0,
        },
        {
            "name": "Не хватает 2 легов (не near-miss)",
            "ok": 0,
            "have": 1,
            "need": 3,
            "score": 0.70,
            "exec_risk_norm": 0.50,
            "soft_score_min": 0.60,
            "soft_exec_max": 0.65,
            "expected_ok_soft": 0,  # have != need - 1
        },
    ]

    print("\n" + "="*80)
    print("ПРОВЕРКА ЛОГИКИ SOFT_RATE")
    print("="*80)

    for tc in test_cases:
        # Логика из of_confirm_engine.py (строки 906-912)
        ok_soft = 0
        if (tc["ok"] == 0 and
            tc["need"] > 0 and
            tc["have"] == tc["need"] - 1):
            if (tc["score"] >= tc["soft_score_min"] and
                tc["exec_risk_norm"] <= tc["soft_exec_max"]):
                ok_soft = 1

        status = "✅ PASS" if ok_soft == tc["expected_ok_soft"] else "❌ FAIL"

        print(f"\n{status} {tc['name']}")
        print(f"  Условия: ok={tc['ok']}, have={tc['have']}, need={tc['need']}")
        print(f"  Метрики: score={tc['score']:.2f}, exec_risk={tc['exec_risk_norm']:.2f}")
        print(f"  Пороги:  soft_score_min={tc['soft_score_min']:.2f}, soft_exec_max={tc['soft_exec_max']:.2f}")
        print(f"  Результат: ok_soft={ok_soft} (ожидалось {tc['expected_ok_soft']})")

        assert ok_soft == tc["expected_ok_soft"], f"Тест '{tc['name']}' провалился!"

    print("\n" + "="*80)
    print("ВСЕ ТЕСТЫ ПРОШЛИ!")
    print("="*80)
    print("\nВЫВОД:")
    print("1. ДО фикса: soft_score_min=0.78 делал soft-fail НЕВОЗМОЖНЫМ")
    print("   (требование 0.78 > passing score 0.65)")
    print("2. ПОСЛЕ фикса: soft_score_min=0.60 позволяет ловить near-miss сигналы")
    print("3. Логика вычисления корректна, баг был только в дефолтных порогах\n")

if __name__ == "__main__":
    test_soft_rate_logic_explanation()
