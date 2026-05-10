#!/usr/bin/env python3
"""
Оффлайн-анализ сигналов с L3-метриками.

Анализирует корреляции между L3-метриками и результатами сигналов,
помогает оптимизировать пороги confidence scorer.
"""

import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.log import setup_logger
from regime.signal_logger import SignalLogger


class SignalAnalyzer:
    """
    Анализ сигналов с L3-метриками для оптимизации скоринга.
    """

    def __init__(self, dsn: str):
        self.logger = setup_logger("SignalAnalyzer")
        self.signal_logger = SignalLogger(dsn)

    def fetch_signal_data(self, days: int = 30) -> pd.DataFrame:
        """
        Получить данные сигналов за последние N дней.
        В реальности нужно джойнить с таблицей результатов.
        """
        # Пока получаем только сигналы
        signals = self.signal_logger.get_recent_signals(limit=10000)

        if not signals:
            self.logger.warning("No signal data found")
            return pd.DataFrame()

        df = pd.DataFrame(signals)

        # Фильтр по времени
        cutoff = datetime.now() - timedelta(days=days)
        df['ts'] = pd.to_datetime(df['ts'])
        df = df[df['ts'] >= cutoff]

        self.logger.info(f"Fetched {len(df)} signals for analysis")
        return df

    def fetch_signals_with_results(self, days: int = 30) -> pd.DataFrame:
        """
        Получить сигналы с результатами (пока заглушка).
        В реальности: джойн с trade_performance по signal_id.
        """
        # Заглушка: генерируем синтетические результаты
        df = self.fetch_signal_data(days)

        if df.empty:
            return df

        # Добавляем синтетические результаты для демонстрации
        np.random.seed(42)
        _n = len(df)

        # Симулируем результаты на основе L3-метрик
        df['pnl_r'] = 0.0
        df['is_win'] = False

        for idx, row in df.iterrows():
            # Базовый шанс успеха
            win_prob = 0.5

            # Корректируем на основе L3-метрик
            if row.get('l3_obi_persistence_score', 0) > 0.7:
                win_prob += 0.1  # Хорошая persistence
            if row.get('l3_spread_bps', 0) < 3.0:
                win_prob += 0.05  # Узкий спред
            if row.get('l3_cancel_to_trade_bid_5s', 0) < 2.0:
                win_prob += 0.05  # Нормальная активность

            # Генерируем результат
            is_win = np.random.random() < win_prob
            pnl_r = np.random.normal(0.5 if is_win else -0.3, 0.2) if is_win else np.random.normal(-0.3, 0.2)

            df.at[idx, 'pnl_r'] = pnl_r
            df.at[idx, 'is_win'] = is_win

        self.logger.info(f"Generated synthetic results for {len(df)} signals")
        return df

    def analyze_l3_correlations(self, df: pd.DataFrame) -> dict[str, float]:
        """
        Анализировать корреляции L3-метрик с результатами.
        """
        if df.empty or 'is_win' not in df.columns:
            return {}

        correlations = {}

        # Метрики для анализа
        l3_metrics = [
            'l3_spread_bps',
            'l3_obi_5', 'l3_obi_20', 'l3_obi_50',
            'l3_obi_persistence_score',
            'l3_cancel_to_trade_bid_5s', 'l3_cancel_to_trade_ask_5s',
            'l3_cancel_to_trade_bid_20s', 'l3_cancel_to_trade_ask_20s',
            'l3_microprice_shift_bps_20']

        for metric in l3_metrics:
            if metric in df.columns:
                # Корреляция с win/loss
                corr = df[metric].corr(df['is_win'])
                correlations[f"{metric}_vs_win"] = corr

                # Корреляция с PnL
                pnl_corr = df[metric].corr(df['pnl_r'])
                correlations[f"{metric}_vs_pnl"] = pnl_corr

        return correlations

    def analyze_by_quantiles(self, df: pd.DataFrame, metric: str, n_quantiles: int = 3) -> dict:
        """
        Анализ результатов по квантилям L3-метрики.
        """
        if df.empty or metric not in df.columns or 'pnl_r' not in df.columns:
            return {}

        # Разбиваем на квантили
        df = df.copy()
        df[f'{metric}_quantile'] = pd.qcut(df[metric], n_quantiles, labels=False, duplicates='drop')

        results = {}
        for q in range(n_quantiles):
            q_data = df[df[f'{metric}_quantile'] == q]
            if len(q_data) > 0:
                win_rate = q_data['is_win'].mean()
                avg_pnl = q_data['pnl_r'].mean()
                count = len(q_data)

                results[f'quantile_{q}'] = {
                    'win_rate': win_rate,
                    'avg_pnl': avg_pnl,
                    'count': count,
                    'metric_range': (q_data[metric].min(), q_data[metric].max())
                }

        return results

    def generate_recommendations(self, df: pd.DataFrame) -> list[str]:
        """
        Генерировать рекомендации по оптимизации на основе анализа.
        """
        recommendations = []

        if df.empty:
            return ["Недостаточно данных для анализа"]

        correlations = self.analyze_l3_correlations(df)

        # Анализ persistence score
        persistence_corr = correlations.get('l3_obi_persistence_score_vs_win', 0)
        if persistence_corr > 0.1 or persistence_corr < -0.1:
            recommendations.append(
                ".2f"
            )

        # Анализ спреда
        spread_corr = correlations.get('l3_spread_bps_vs_win', 0)
        if spread_corr < -0.1:
            recommendations.append(
                ".2f"
            )

        # Анализ cancel-to-trade
        c2t_corr = correlations.get('l3_cancel_to_trade_bid_5s_vs_win', 0)
        if c2t_corr < -0.1:
            recommendations.append(
                "Высокий cancel_to_trade_bid_5s коррелирует с проигрышами. "
                "Рассмотреть ужесточение порогов для этого показателя."
            )

        # Квантильный анализ OBI persistence
        obi_analysis = self.analyze_by_quantiles(df, 'l3_obi_persistence_score')
        if obi_analysis:
            q0_win = obi_analysis.get('quantile_0', {}).get('win_rate', 0)
            q2_win = obi_analysis.get('quantile_2', {}).get('win_rate', 0)

            if q2_win > q0_win + 0.1:
                recommendations.append(
                    ".1%"

                )

        return recommendations

    def run_full_analysis(self, days: int = 30) -> dict:
        """
        Запустить полный анализ сигналов.
        """
        self.logger.info(f"Starting signal analysis for last {days} days")

        # Получить данные
        df = self.fetch_signals_with_results(days)

        if df.empty:
            return {"error": "No data available"}

        # Анализ
        correlations = self.analyze_l3_correlations(df),

        # Квантильный анализ ключевых метрик
        quantile_analyses = {
            'obi_persistence': self.analyze_by_quantiles(df, 'l3_obi_persistence_score'),
            'spread': self.analyze_by_quantiles(df, 'l3_spread_bps'),
            'cancel_to_trade': self.analyze_by_quantiles(df, 'l3_cancel_to_trade_bid_5s'),
        }

        # Рекомендации
        recommendations = self.generate_recommendations(df),

        # Общая статистика
        total_signals = len(df),
        win_rate = df['is_win'].mean(),
        avg_pnl = df['pnl_r'].mean(),

        result = {
            "summary": {
                "total_signals": total_signals,
                "win_rate": win_rate,
                "avg_pnl": avg_pnl,
                "analysis_period_days": days,
            },
            "correlations": correlations,
            "quantile_analyses": quantile_analyses,
            "recommendations": recommendations,
        }

        self.logger.info(f"Analysis completed: {total_signals} signals, win rate {win_rate:.1%}")
        return result


def main():
    """Основная функция для запуска анализа."""

    # Настройки
    dsn = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
    days = int(os.getenv("ANALYSIS_DAYS", "30"))

    analyzer = SignalAnalyzer(dsn)
    result = analyzer.run_full_analysis(days)

    # Вывод результатов
    print("\n" + "="*60)
    print("📊 SIGNAL ANALYSIS RESULTS")
    print("="*60)

    if "error" in result:
        print(f"❌ {result['error']}")
        return

    summary = result["summary"]
    print("📈 Summary:")
    print(f"   Total signals: {summary['total_signals']}")
    print(".1%")
    print(".2f")

    print("\n🔗 Key Correlations:")
    correlations = result["correlations"]
    key_metrics = [
        'l3_obi_persistence_score_vs_win',
        'l3_spread_bps_vs_win',
        'l3_cancel_to_trade_bid_5s_vs_win']

    for metric in key_metrics:
        _corr = correlations.get(metric, 0)
        print(".3f")

    print("\n💡 Recommendations:")
    for rec in result["recommendations"]:
        print(f"   • {rec}")

    print("\n" + "="*60)


if __name__ == "__main__":
    main()
