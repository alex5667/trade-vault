from __future__ import annotations

import bisect
import math
from dataclasses import dataclass


def _clamp01(x: float) -> float:
    return 0.0 if x <= 0.0 else (1.0 if x >= 1.0 else float(x))


@dataclass
class IsotonicCalibrator:
    # breakpoints x_k (возрастают) и значения p_k (монотонно возрастают)
    x: list[float]
    p: list[float]
    mode: str = "linear"  # "linear" or "step"

    def predict(self, xq: float) -> float:
        if not self.x or not self.p:
            return 0.5
        xq = float(xq)
        if xq <= self.x[0]:
            return float(_clamp01(self.p[0]))
        if xq >= self.x[-1]:
            return float(_clamp01(self.p[-1]))

        i = bisect.bisect_right(self.x, xq) - 1
        if self.mode == "step":
            return float(_clamp01(self.p[i]))

        # linear interpolation between (x[i], p[i]) and (x[i+1], p[i+1])
        x0, x1 = float(self.x[i]), float(self.x[i + 1])
        p0, p1 = float(self.p[i]), float(self.p[i + 1])
        if x1 <= x0 + 1e-12:
            return float(_clamp01(p0))
        t = (xq - x0) / (x1 - x0)
        return float(_clamp01(p0 + t * (p1 - p0)))


def fit_isotonic_pav(samples: list[tuple[float, int, float]]) -> IsotonicCalibrator:
    """
    samples: list of (x, y, w)
      x >= 0 (например abs(final_score))
      y in {0,1}
      w > 0
    Возвращает монотонную калибровку p(x) через PAV.
    """
    data = [(float(x), int(y), float(w)) for x, y, w in samples if math.isfinite(x) and float(w) > 0]
    data.sort(key=lambda t: t[0])
    if not data:
        return IsotonicCalibrator(x=[], p=[])

    # агрегируем одинаковые x
    xs: list[float] = []
    ps: list[float] = []
    ws: list[float] = []

    cur_x = data[0][0]
    sum_w = 0.0
    sum_yw = 0.0
    for x, y, w in data:
        if x != cur_x:
            xs.append(cur_x)
            ws.append(sum_w)
            ps.append(sum_yw / max(1e-12, sum_w))
            cur_x = x
            sum_w = 0.0
            sum_yw = 0.0
        sum_w += w
        sum_yw += w * (1.0 if y else 0.0)
    xs.append(cur_x)
    ws.append(sum_w)
    ps.append(sum_yw / max(1e-12, sum_w))

    # PAV: сливаем блоки при нарушении монотонности
    blocks: list[list[float]] = []  # [x_left, x_right, w_sum, p_value]
    for x, p, w in zip(xs, ps, ws):
        blocks.append([x, x, w, float(_clamp01(p))])
        while len(blocks) >= 2 and blocks[-2][3] > blocks[-1][3]:
            b2 = blocks.pop()
            b1 = blocks.pop()
            w_sum2 = b1[2] + b2[2]
            p_new = (b1[2] * b1[3] + b2[2] * b2[3]) / max(1e-12, w_sum2)
            blocks.append([b1[0], b2[1], w_sum2, float(_clamp01(p_new))])

    out_x: list[float] = []
    out_p: list[float] = []
    for x_l, x_r, w, p in blocks:
        out_x.append(float(x_r))
        out_p.append(float(_clamp01(p)))

    # добавим левую границу первого блока, чтобы интерполяция работала стабильно
    first_left = float(blocks[0][0])
    if out_x and first_left < out_x[0]:
        out_x = [first_left] + out_x
        out_p = [out_p[0]] + out_p

    return IsotonicCalibrator(x=out_x, p=out_p, mode="linear")
