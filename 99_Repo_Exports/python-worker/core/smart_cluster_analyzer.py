"""
SmartClusterAnalyzer — анализ DOM уровней.
"""

from typing import Any


class SmartClusterAnalyzer:
    @staticmethod
    def analyze_from_dom(levels: list[dict[str, float]], window: int = 6, ratio_thr: float = 2.0) -> dict[str, Any]:
        if not levels:
            return {
                "stacked_buy_levels": 0,
                "stacked_sell_levels": 0,
                "imbalance_score": 0.0,
                "absorption_score": 0.0,
                "direction": "neutral",
            }
        buy_streak = sell_streak = 0
        max_buy = max_sell = 0
        for lv in levels:
            bid = float(lv.get("bid", 0) or 0)
            ask = float(lv.get("ask", 0) or 0)
            if bid <= 0 and ask <= 0:
                continue
            if ask <= 0 and bid > 0:
                r = 999.0
            elif bid <= 0 and ask > 0:
                r = 0.0
            else:
                r = bid / max(ask, 1e-9)

            if r >= ratio_thr:
                buy_streak += 1; sell_streak = 0
            elif (1.0 / max(r, 1e-9)) >= ratio_thr:
                sell_streak += 1; buy_streak = 0
            else:
                buy_streak = 0; sell_streak = 0
            max_buy = max(max_buy, buy_streak)
            max_sell = max(max_sell, sell_streak)

        total_bid = sum(float(l.get("bid", 0) or 0) for l in levels[:window])
        total_ask = sum(float(l.get("ask", 0) or 0) for l in levels[:window])
        denom = max(total_bid + total_ask, 1.0)
        absorption_score = abs(total_bid - total_ask) / denom

        imbalance_score = (max_buy - max_sell) / float(max(max_buy, max_sell, 1))
        direction = "buy" if max_buy > max_sell else ("sell" if max_sell > max_buy else "neutral")

        return {
            "stacked_buy_levels": int(max_buy),
            "stacked_sell_levels": int(max_sell),
            "imbalance_score": float(imbalance_score),
            "absorption_score": float(absorption_score),
            "direction": direction,
        }


