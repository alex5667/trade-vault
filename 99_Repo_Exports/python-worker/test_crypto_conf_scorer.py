#!/usr/bin/env python3
"""
Тест сравнения старой и новой версий _crypto_conf_scorer.
"""

import os
import tempfile
import yaml

# Импорт CryptoConfScorer только если файл существует
try:
    from regime.crypto_conf_scorer import CryptoConfScorer
    CRYPTO_CONF_SCORER_AVAILABLE = True
except ImportError:
    CRYPTO_CONF_SCORER_AVAILABLE = False


def _clamp01(x: float) -> float:
    """Clamp value to [0, 1] range"""
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


# Старая версия _crypto_conf_scorer (простая)
def _crypto_conf_scorer_old(ctx, signal_type: str):
    """
    Старая версия: простой confidence на основе z_core, micro, location
    """
    parts = {}

    # main_z
    main_z = float(abs(getattr(ctx, "main_z", getattr(ctx, "deltaSpikeZ", 0.0))))
    z_core = max(0.0, min(1.0, (main_z - 1.0) / 2.0))  # 0 при 1σ, 1 при 3σ+
    parts["z_core"] = z_core

    # micro (OBI)
    obi_z = float(abs(getattr(ctx, "obi_z", getattr(ctx, "obi_window_imbalance_z", 0.0))))
    micro = max(0.0, min(1.0, (obi_z - 0.5) / 1.5))
    parts["microstructure"] = micro

    # location (day range position)
    loc_q = float(getattr(ctx, "day_range_pos_q", 0.0))
    location = max(0.0, min(1.0, loc_q))
    parts["location"] = location

    # penalty for bad book
    penalty = 0.0
    l2_is_stale = bool(getattr(ctx, "l2_is_stale_now", False))
    if l2_is_stale:
        penalty += 0.2

    spread_bps = float(getattr(ctx, "spread_bps", 0.0))
    if spread_bps > 10.0:
        penalty += 0.2
    elif spread_bps > 5.0:
        penalty += 0.1

    parts["penalty"] = penalty

    # simple aggregation
    base = 0.5 * z_core + 0.3 * micro + 0.2 * location
    base = max(0.0, min(1.0, base - penalty))

    confidence = round(base * 100.0, 1)
    return confidence, parts


# Новая версия _crypto_conf_scorer (расширенная с жёсткими правилами)
def _crypto_conf_scorer_new(ctx, signal_type: str):
    """
    Новая версия: жёсткий confidence с ATR, weakProgress, OBI_levels, spread, market_mode
    """
    parts = {}

    # ATR quantile
    atr_q = float(getattr(ctx, "atr_q_main", getattr(ctx, "atr_quantile", 0.5)))
    atr_q = _clamp01(atr_q)

    if atr_q <= 0.0 or atr_q >= 1.0:
        atr_regime = 0.0
    elif atr_q < 0.3:
        atr_regime = (atr_q - 0.05) / (0.3 - 0.05)
    elif atr_q > 0.7:
        atr_regime = (0.95 - atr_q) / (0.95 - 0.7)
    else:
        atr_regime = 1.0

    atr_regime = _clamp01(atr_regime)
    parts["atr_regime"] = atr_regime

    hard_penalty_atr = 0.0
    if atr_q < 0.02 or atr_q > 0.98:
        hard_penalty_atr = 0.4
    elif atr_q < 0.05 or atr_q > 0.95:
        hard_penalty_atr = 0.25
    parts["hard_penalty_atr"] = hard_penalty_atr

    # main_z (with better scaling)
    main_z = float(abs(getattr(ctx, "main_z", getattr(ctx, "deltaSpikeZ", 0.0))))
    if main_z <= 1.0:
        z_core = 0.0
    elif main_z >= 4.0:
        z_core = 1.0
    else:
        z_core = (main_z - 1.0) / (4.0 - 1.0)
    z_core = _clamp01(z_core)
    parts["z_core"] = z_core
    parts["main_z"] = main_z

    # OBI persistence
    obi_levels = getattr(ctx, "OBI_windowLevels", [])
    if isinstance(obi_levels, (list, tuple)) and len(obi_levels) > 0:
        levels = [abs(float(x)) for x in obi_levels]
        strong_cnt = sum(1 for v in levels if v >= 1.0)
        obi_persist_frac = strong_cnt / max(len(levels), 1)
        obi_persist_score = _clamp01((obi_persist_frac - 0.2) / (0.7 - 0.2))
    else:
        obi_z = float(abs(getattr(ctx, "obi_z", 0.0)))
        if obi_z <= 0.5:
            obi_persist_score = 0.0
        elif obi_z >= 2.5:
            obi_persist_score = 1.0
        else:
            obi_persist_score = (obi_z - 0.5) / (2.5 - 0.5)
        obi_persist_score = _clamp01(obi_persist_score)
    parts["obi_persist"] = obi_persist_score

    # weakProgress / range_vs_atr
    weak_flag = bool(getattr(ctx, "weakProgress", False))
    weak_ratio = float(getattr(ctx, "range_vs_atr", 1.0))

    if weak_ratio <= 0.2:
        progress_score = 0.0
    elif weak_ratio >= 1.5:
        progress_score = max(0.0, (1.8 - weak_ratio) / (1.8 - 1.2))
    elif 0.4 <= weak_ratio <= 1.2:
        progress_score = 1.0
    else:
        if weak_ratio < 0.4:
            progress_score = (weak_ratio - 0.2) / (0.4 - 0.2)
        else:
            progress_score = (1.5 - weak_ratio) / (1.5 - 1.2)

    progress_score = _clamp01(progress_score)
    if weak_flag:
        progress_score *= 0.4
    parts["progress_score"] = progress_score
    parts["weak_ratio"] = weak_ratio

    # L2 quality
    spread_bps = float(getattr(ctx, "spread_bps", 0.0))
    l2_is_stale = bool(getattr(ctx, "l2_is_stale_now", False))

    if spread_bps <= 2.0:
        book_quality = 1.0
    elif spread_bps >= 12.0:
        book_quality = 0.0
    else:
        book_quality = (12.0 - spread_bps) / (12.0 - 2.0)

    if l2_is_stale:
        book_quality *= 0.3
    book_quality = _clamp01(book_quality)
    parts["book_quality"] = book_quality

    # Market mode adaptation
    st = (signal_type or "").lower()
    is_breakout = "breakout" in st
    is_absorption = "absorption" in st or "absorb" in st
    is_meanrev = "meanrev" in st or "revert" in st

    market_mode = str(getattr(ctx, "market_mode", "")).lower()
    is_momentum_mode = market_mode.startswith("momentum")
    is_meanrev_mode = market_mode.startswith("mean")

    # base weights
    w_z = 0.35
    w_obi = 0.25
    w_atr = 0.15
    w_progress = 0.15
    w_book = 0.10

    # breakout + momentum: emphasize z and OBI, deemphasize progress
    if is_breakout or is_momentum_mode:
        w_z = 0.40
        w_obi = 0.30
        w_progress = 0.05
        w_book = 0.10

    # absorption: focus on microstructure and book
    if is_absorption:
        w_obi = 0.30
        w_book = 0.15

    # mean-reversion: attention to progress and ATR regime
    if is_meanrev or is_meanrev_mode:
        w_atr = 0.20
        w_progress = 0.20

    w_sum = w_z + w_obi + w_atr + w_progress + w_book
    w_z /= w_sum
    w_obi /= w_sum
    w_atr /= w_sum
    w_progress /= w_sum
    w_book /= w_sum

    # final score with hard penalties
    base = (
        w_z * z_core
        + w_obi * obi_persist_score
        + w_atr * atr_regime
        + w_progress * progress_score
        + w_book * book_quality
    )
    base = _clamp01(base)

    hard_penalty = hard_penalty_atr
    if book_quality <= 0.1:
        hard_penalty += 0.3
    elif book_quality <= 0.3:
        hard_penalty += 0.15

    if main_z < 1.2:
        hard_penalty += 0.5

    hard_penalty = min(max(hard_penalty, 0.0), 0.9)
    parts["hard_penalty_total"] = hard_penalty

    final_score = base * (1.0 - hard_penalty)
    final_score = _clamp01(final_score)

    confidence = round(final_score * 100.0, 1)
    parts["confidence_0_1"] = final_score

    return confidence, parts

def test_compare_crypto_conf_scorers():
    """Сравниваем старую и новую версии _crypto_conf_scorer"""

    # Mock контекст для тестирования
    class MockCtx:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    test_cases = [
        {
            "name": "Идеальный сигнал (momentum breakout)",
            "ctx": MockCtx(
                atr_q_main=0.5,      # идеальный ATR режим
                main_z=3.0,          # сильный сигнал (3σ)
                OBI_windowLevels=[2.5, 2.8, 2.2],  # сильная персистентность
                weakProgress=False,
                range_vs_atr=0.8,    # хороший прогресс
                spread_bps=3.0,      # узкий спред
                l2_is_stale_now=False,
                market_mode="momentum"
            ),
            "signal_type": "breakout",
            "expected_new_better": True
        },
        {
            "name": "Средний сигнал",
            "ctx": MockCtx(
                atr_q_main=0.6,
                main_z=2.0,          # умеренный сигнал (2σ)
                OBI_windowLevels=[1.2, 1.5, 1.1],  # средняя персистентность
                weakProgress=False,
                range_vs_atr=0.9,
                spread_bps=5.0,
                l2_is_stale_now=False,
                market_mode="mixed"
            ),
            "signal_type": "breakout",
            "expected_new_better": True
        },
        {
            "name": "Плохой сигнал (экстремальный ATR)",
            "ctx": MockCtx(
                atr_q_main=0.98,     # экстремальный режим
                main_z=1.5,          # слабый сигнал
                OBI_windowLevels=[0.5, 0.6],  # слабая персистентность
                weakProgress=True,   # явный weak progress
                range_vs_atr=0.15,   # очень слабый прогресс
                spread_bps=15.0,     # широкий спред
                l2_is_stale_now=True,# stale L2
                market_mode="mixed"
            ),
            "signal_type": "breakout",
            "expected_new_better": True  # новая должна жестче штрафовать
        },
        {
            "name": "Слабый z-score",
            "ctx": MockCtx(
                atr_q_main=0.4,
                main_z=1.0,          # слишком слабый сигнал
                OBI_windowLevels=[1.8, 2.0],  # хорошая персистентность
                weakProgress=False,
                range_vs_atr=1.0,
                spread_bps=4.0,
                l2_is_stale_now=False,
                market_mode="mixed"
            ),
            "signal_type": "breakout",
            "expected_new_better": True  # новая должна жестче фильтровать слабые сигналы
        },
        {
            "name": "Перерастянутый тренд",
            "ctx": MockCtx(
                atr_q_main=0.5,
                main_z=2.5,
                OBI_windowLevels=[1.5, 1.8],
                weakProgress=False,
                range_vs_atr=1.8,     # перерастянутый тренд
                spread_bps=6.0,
                l2_is_stale_now=False,
                market_mode="meanrev"
            ),
            "signal_type": "meanrev",
            "expected_new_better": True  # новая должна учитывать перерастягивание
        }
    ]

    print("🔬 Сравнение старой и новой версий _crypto_conf_scorer")
    print("=" * 80)

    old_total = 0.0
    new_total = 0.0
    cases_count = len(test_cases)

    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📊 Тест {i}: {test_case['name']}")
        print("-" * 50)

        ctx = test_case["ctx"]
        signal_type = test_case["signal_type"]

        # Считаем старой версией
        old_conf, old_parts = _crypto_conf_scorer_old(ctx, signal_type)
        old_total += old_conf

        # Считаем новой версией
        new_conf, new_parts = _crypto_conf_scorer_new(ctx, signal_type)
        new_total += new_conf

        print(f"   Старая версия: {old_conf:.1f}%")
        print(f"   Новая версия:  {new_conf:.1f}%")
        print(f"   Разница:        {new_conf - old_conf:+.1f}%")

        # Детальный анализ компонентов
        print("\n   📈 Ключевые компоненты:")

        # Общие компоненты
        if "z_core" in old_parts and "z_core" in new_parts:
            print(f"   z_core: {old_parts['z_core']:.3f} → {new_parts['z_core']:.3f}")

        if "microstructure" in old_parts and "obi_persist" in new_parts:
            print(f"   micro: {old_parts['microstructure']:.3f} → obi_persist: {new_parts['obi_persist']:.3f}")

        # Новые компоненты
        if "atr_regime" in new_parts:
            print(f"   atr_regime: {new_parts['atr_regime']:.3f}")
        if "progress_score" in new_parts:
            print(f"   progress_score: {new_parts['progress_score']:.3f}")
        if "book_quality" in new_parts:
            print(f"   book_quality: {new_parts['book_quality']:.3f}")
        if "hard_penalty_total" in new_parts:
            print(f"   hard_penalty: {new_parts['hard_penalty_total']:.3f}")

        # Оценка
        if new_conf > old_conf:
            result = "✅ Новая лучше (более точная оценка)"
        elif new_conf < old_conf:
            result = "❌ Новая хуже (слишком консервативна)"
        else:
            result = "🟡 Равны"

        print(f"   {result}")

    # Итоговая статистика
    print("\n" + "=" * 80)
    print("📊 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"   Средний confidence (старая): {old_total/cases_count:.1f}%")
    print(f"   Средний confidence (новая):  {new_total/cases_count:.1f}%")
    print(f"   Общее улучшение:            {(new_total - old_total):+.1f}%")

    avg_improvement = (new_total - old_total) / cases_count
    print(f"   Среднее улучшение на тест:   {avg_improvement:+.1f}%")
    # Оценка качества новой системы
    if avg_improvement > 15:
        quality = "Отличное улучшение! 🎉"
    elif avg_improvement > 5:
        quality = "Хорошее улучшение 👍"
    elif avg_improvement > -5:
        quality = "Незначительные изменения"
    else:
        quality = "Возможное ухудшение ⚠️"

    print(f"   Качество новой системы: {quality}")

    return avg_improvement


def test_crypto_conf_scorer():
    """Тестируем новый CryptoConfScorer с baseline-конфигом."""

    # Создаем тестовый YAML
    test_config = {
        "crypto_conf_scorer": {
            "default": {
                "l3": {
                    "spread_max_ok_bps": 5.0,
                    "spread_hard_limit_bps": 20.0,
                    "cancel_soft": 2.0,
                    "cancel_hard": 5.0,
                    "obi_good_min": 0.6,
                    "obi_bad_max": 0.2,
                    "mp_drift_max_bps": 4.0,
                }
            },
            "by_symbol": {
                "BTCUSDT": {
                    "crypto_orderflow": {
                        "long": {
                            "l3": {
                                "spread_max_ok_bps": 3.0,
                                "spread_hard_limit_bps": 15.0,
                                "cancel_soft": 1.5,
                                "cancel_hard": 4.0,
                                "obi_good_min": 0.65,
                                "obi_bad_max": 0.25,
                                "mp_drift_max_bps": 3.5,
                            }
                        }
                    }
                }
            }
        }
    }

    # Сохраняем в временный файл
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.safe_dump(test_config, f)
        yaml_path = f.name

    try:
        # Создаем scorer
        scorer = CryptoConfScorer(yaml_path=yaml_path, reload_interval_sec=1)

        # Тестируем разные сценарии
        test_cases = [
            {
                "name": "BTC Long - Good conditions",
                "params": {
                    "symbol": "BTCUSDT",
                    "signal_family": "crypto_orderflow",
                    "direction": 1,  # long
                    "l3_spread_bps": 2.0,
                    "l3_obi_persistence_score": 0.8,
                    "l3_microprice_shift_bps_20": 1.0,
                    "l3_cancel_to_trade_bid_5s": 1.0,
                    "l3_cancel_to_trade_ask_5s": 3.0,
                    "l3_cancel_to_trade_bid_20s": 1.2,
                    "l3_cancel_to_trade_ask_20s": 2.8,
                },
                "expected_l3_score_range": (0.7, 1.0),
            },
            {
                "name": "BTC Short - Bad spread",
                "params": {
                    "symbol": "BTCUSDT",
                    "signal_family": "crypto_orderflow",
                    "direction": -1,  # short
                    "l3_spread_bps": 25.0,  # > hard limit
                    "l3_obi_persistence_score": 0.8,
                    "l3_microprice_shift_bps_20": -1.0,
                    "l3_cancel_to_trade_bid_5s": 1.0,
                    "l3_cancel_to_trade_ask_5s": 6.0,  # > hard limit
                    "l3_cancel_to_trade_bid_20s": 1.2,
                    "l3_cancel_to_trade_ask_20s": 5.5,
                },
                "expected_l3_score_range": (0.0, 0.3),
            },
            {
                "name": "Default symbol - Neutral direction",
                "params": {
                    "symbol": "UNKNOWN",
                    "signal_family": "test",
                    "direction": 0,  # neutral
                    "l3_spread_bps": 4.0,
                    "l3_obi_persistence_score": 0.5,
                    "l3_microprice_shift_bps_20": 0.5,
                    "l3_cancel_to_trade_bid_5s": 2.5,
                    "l3_cancel_to_trade_ask_5s": 2.0,
                    "l3_cancel_to_trade_bid_20s": 2.2,
                    "l3_cancel_to_trade_ask_20s": 2.1,
                },
                "expected_l3_score_range": (0.4, 0.7),
            },
        ]

        print("🧪 Testing CryptoConfScorer integration...")
        print("=" * 60)

        for test_case in test_cases:
            print(f"\n📊 {test_case['name']}")

            result = scorer.score_l3(**test_case["params"])

            l3_score = result["l3_score"]
            terms = result["terms"]
            profile = result["profile"]

            print(f"   L3 Score: {l3_score:.3f}")
            print(f"   Terms: spread={terms['spread_ok_score']:.2f}, cancel={terms['cancel_to_trade_score']:.2f}, obi={terms['obi_persistence_score']:.2f}, mp_drift={terms['microprice_drift_score']:.2f}")

            # Проверяем диапазон
            min_expected, max_expected = test_case["expected_l3_score_range"]
            if min_expected <= l3_score <= max_expected:
                print("   ✅ Score in expected range")
            else:
                print(f"   ❌ Score {l3_score:.3f} not in range [{min_expected:.1f}, {max_expected:.1f}]")

        print(f"\n🎉 All tests completed!")

    finally:
        # Очищаем временный файл
        os.unlink(yaml_path)

if __name__ == "__main__":
    print("🚀 Запуск сравнения систем confidence scoring...")
    improvement = test_compare_crypto_conf_scorers()

    print("\n" + "="*80)
    if improvement > 10:
        print("🎯 РЕЗУЛЬТАТ: Новая система значительно лучше!")
        print("   Рекомендуется использовать новую версию _crypto_conf_scorer")
    elif improvement > 0:
        print("👍 РЕЗУЛЬТАТ: Новая система немного лучше")
        print("   Можно использовать новую версию с дополнительной настройкой")
    else:
        print("⚠️  РЕЗУЛЬТАТ: Новая система требует доработки")
        print("   Возможно, смягчить некоторые пороги")

    print(f"\nСреднее улучшение: {improvement:+.1f}%")

    if CRYPTO_CONF_SCORER_AVAILABLE:
        print("\n" + "-"*80)
        print("🔄 Теперь тестируем интеграцию с CryptoConfScorer...")
        test_crypto_conf_scorer()
    else:
        print("\n" + "-"*80)
        print("⚠️  CryptoConfScorer недоступен (нет зависимостей)")
