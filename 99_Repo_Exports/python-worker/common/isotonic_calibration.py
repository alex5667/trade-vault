from __future__ import annotations

import bisect
import math
from dataclasses import dataclass
from typing import Any


def _clamp01(x: float) -> float:
    return 0.0 if x <= 0.0 else (1.0 if x >= 1.0 else float(x))


@dataclass(frozen=True)
class IsotonicCalibrator:
    """
    Монотонная калибровка p(x) по breakpoints:
      - x: возрастающие точки
      - p: неубывающие вероятности (0..1)

    mode:
      - "linear": линейная интерполяция между точками
      - "step": ступенчатая (piecewise-constant)

    Совместим с интерфейсом PlattLogitCalibrator через `apply_one`/`apply`
    (нужно для ml_confirm/decision_policy.py при подключении в meta_lr_blend).
    """
    x: list[float]
    p: list[float]
    mode: str = "linear"

    def apply_one(self, p_raw: float) -> float:
        """Compatibility shim for ml_confirm/decision_policy.py.

        decision_policy ожидает калибратор с `apply_one(p_raw) → p_cal`.
        Изотон работает на raw-вероятности (x≡p_raw, p≡p_cal).
        """
        return self.predict(float(p_raw))

    def apply(self, probs: list[float]) -> list[float]:
        return [self.apply_one(p) for p in probs]

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "isotonic",
            "x": [float(v) for v in self.x],
            "p": [float(v) for v in self.p],
            "mode": str(self.mode),
        }

    @staticmethod
    def from_dict(d: dict[str, Any]) -> IsotonicCalibrator:
        """Inverse of to_dict; fails-soft on missing keys via sanitize."""
        xs = list(d.get("x") or [])
        ps = list(d.get("p") or [])
        mode = str(d.get("mode", "linear") or "linear")
        cal = IsotonicCalibrator(x=xs, p=ps, mode=mode).sanitize()
        return cal

    def predict(self, xq: float) -> float:
        if not self.x or not self.p or len(self.x) != len(self.p):
            # fail-open нейтральная вероятность
            return 0.5

        xq = float(xq)
        if xq <= self.x[0]:
            return float(_clamp01(self.p[0]))
        if xq >= self.x[-1]:
            return float(_clamp01(self.p[-1]))

        i = bisect.bisect_right(self.x, xq) - 1
        if i < 0:
            return float(_clamp01(self.p[0]))
        if i >= len(self.x) - 1:
            return float(_clamp01(self.p[-1]))

        if self.mode == "step":
            return float(_clamp01(self.p[i]))

        # linear interpolation between (x[i], p[i]) and (x[i+1], p[i+1])
        x0, x1 = float(self.x[i]), float(self.x[i + 1])
        p0, p1 = float(self.p[i]), float(self.p[i + 1])
        if x1 <= x0 + 1e-12:
            return float(_clamp01(p0))
        t = (xq - x0) / (x1 - x0)
        return float(_clamp01(p0 + t * (p1 - p0)))

    def sanitize(self) -> IsotonicCalibrator:
        # fail-open санитация: finite, сортировка, clamp, монотонность p
        pairs = []
        for xi, pi in zip(self.x or [], self.p or []):
            if not (math.isfinite(float(xi)) and math.isfinite(float(pi))):
                continue
            pairs.append((float(xi), float(_clamp01(float(pi)))))
        if not pairs:
            return IsotonicCalibrator(x=[], p=[], mode=self.mode)
        pairs.sort(key=lambda t: t[0])
        # убираем дубликаты x (оставляем последний)
        xs: list[float] = []
        ps: list[float] = []
        last_x = None
        for x, p in pairs:
            if last_x is not None and abs(x - last_x) <= 1e-12:
                xs[-1] = x
                ps[-1] = p
            else:
                xs.append(x)
                ps.append(p)
                last_x = x
        # enforce non-decreasing p (простая проекция)
        for i in range(1, len(ps)):
            if ps[i] < ps[i - 1]:
                ps[i] = ps[i - 1]
        return IsotonicCalibrator(x=xs, p=ps, mode=self.mode)


def fit_isotonic_pav(samples: list[tuple[float, float, float]]) -> IsotonicCalibrator:
    """
    samples: list of (x, y, w)
      x >= 0 (например abs(final_score))
      y in [0..1]  (обычно 0/1, но можно подавать уже агрегированную долю win)
      w > 0  (вес, можно 1.0)
    Возвращает монотонную калибровку p(x).
    """
    # 1) sort by x
    data = []
    for x, y, w in samples:
        if not (math.isfinite(x) and math.isfinite(y) and math.isfinite(w)):
            continue
        if w <= 0:
            continue
        data.append((float(x), float(_clamp01(y)), float(w)))
    data.sort(key=lambda t: t[0])
    if not data:
        return IsotonicCalibrator(x=[], p=[])

    # 2) агрегируем одинаковые x
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
        sum_yw += w * float(y)
    xs.append(cur_x)
    ws.append(sum_w)
    ps.append(sum_yw / max(1e-12, sum_w))

    # 3) PAV: блоки с нарушением монотонности сливаем
    # blocks: (x_left, x_right, w_sum, p_value)
    blocks = []
    for x, p, w in zip(xs, ps, ws):
        blocks.append([x, x, w, float(_clamp01(p))])
        while len(blocks) >= 2 and blocks[-2][3] > blocks[-1][3]:
            b2 = blocks.pop()
            b1 = blocks.pop()
            w_sum = b1[2] + b2[2]
            # средневзвешенная вероятность
            p_new = (b1[2]*b1[3] + b2[2]*b2[3]) / max(1e-12, w_sum)
            blocks.append([b1[0], b2[1], w_sum, float(_clamp01(p_new))])

    # 4) разворачиваем в breakpoints: берём правые границы блоков как x_k
    out_x: list[float] = []
    out_p: list[float] = []
    for x_l, x_r, w_sum, p_val in blocks:
        out_x.append(float(x_r))
        out_p.append(float(_clamp01(p_val)))

    # гарантируем стартовую точку (чтобы интерполяция работала)
    first_left = float(blocks[0][0])
    if out_x and first_left < out_x[0]:
        out_x = [first_left] + out_x
        out_p = [out_p[0]] + out_p

    return IsotonicCalibrator(x=out_x, p=out_p, mode="linear")


def sanitize_breakpoints(x: list[float], p: list[float], *, mode: str = "linear") -> IsotonicCalibrator | None:
    """
    Fail-soft sanitation for externally loaded (x,p):
      - drop non-finite
      - sort by x
      - clamp p to [0..1]
      - enforce monotonicity via PAV on (x, p) with unit weights
    """
    if not x or not p or len(x) != len(p):
        return None

    tmp: list[tuple[float, float]] = []
    for xi, pi in zip(x, p):
        xi = float(xi)
        pi = float(pi)
        if not (math.isfinite(xi) and math.isfinite(pi)):
            continue
        tmp.append((xi, float(_clamp01(pi))))
    if not tmp:
        return None
    tmp.sort(key=lambda t: t[0])

    # Convert to pseudo-samples: (x, y, w) where y is probability -> use as label with weight,
    # but better: run PAV on blocks directly; simplest: approximate by two samples per point.
    # For stability and simplicity: treat each point as mean with weight 1.0 and run PAV by using
    # (x, p, w=1).
    xs: list[float] = []
    ps: list[float] = []
    ws: list[float] = []

    cur_x = tmp[0][0]
    sum_w = 0.0
    sum_p = 0.0
    for xi, pi in tmp:
        if xi != cur_x:
            xs.append(cur_x)
            ws.append(sum_w)
            ps.append(sum_p / max(1e-12, sum_w))
            cur_x = xi
            sum_w = 0.0
            sum_p = 0.0
        sum_w += 1.0
        sum_p += pi
    xs.append(cur_x)
    ws.append(sum_w)
    ps.append(sum_p / max(1e-12, sum_w))

    blocks: list[list[float]] = []
    for xi, pi, wi in zip(xs, ps, ws):
        blocks.append([float(xi), float(xi), float(wi), float(_clamp01(pi))])
        while len(blocks) >= 2 and blocks[-2][3] > blocks[-1][3]:
            b2 = blocks.pop()
            b1 = blocks.pop()
            w_sum = b1[2] + b2[2]
            p_new = (b1[2] * b1[3] + b2[2] * b2[3]) / max(1e-12, w_sum)
            blocks.append([b1[0], b2[1], w_sum, float(_clamp01(p_new))])

    out_x: list[float] = []
    out_p: list[float] = []
    for x_l, x_r, w_sum, p_val in blocks:
        out_x.append(float(x_r))
        out_p.append(float(_clamp01(p_val)))

    first_left = float(blocks[0][0])
    if out_x and first_left < out_x[0] - 1e-12:
        out_x = [first_left] + out_x
        out_p = [out_p[0]] + out_p

    if len(out_x) < 2:
        # still usable (constant), but keep it
        return IsotonicCalibrator(x=out_x, p=out_p, mode=mode)
    return IsotonicCalibrator(x=out_x, p=out_p, mode=mode)
