"""
Record & Replay toolbox (6.2).

Цель:
  - записать детерминированный вход (ctx на bucket boundary, опционально tick/signal)
  - прогнать replay локально (без Redis/L2/L3/ATR/HTF провайдеров)
  - сравнить поведение с golden: counts, score distribution, контрольные события
"""