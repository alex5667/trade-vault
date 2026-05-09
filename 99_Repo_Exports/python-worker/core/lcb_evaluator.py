import math
from dataclasses import dataclass


@dataclass
class ArmAgg:
    """
    Online aggregation for R-multiple samples.
    Keep sum and sumsq for mean/std; robustification happens at ingestion (optional).
    """
    n: int = 0
    sum_r: float = 0.0
    sumsq_r: float = 0.0
    wins: int = 0

    def add(self, r: float) -> None:
        if not math.isfinite(r):
            return
        self.n += 1
        self.sum_r += float(r)
        self.sumsq_r += float(r) * float(r)
        if r > 0:
            self.wins += 1

    def mean(self) -> float:
        return (self.sum_r / self.n) if self.n > 0 else 0.0

    def std(self) -> float:
        if self.n < 2:
            return 0.0
        mu = self.mean()
        var = (self.sumsq_r / self.n) - mu * mu
        return math.sqrt(max(0.0, var))

    def winrate(self) -> float:
        return (self.wins / self.n) if self.n > 0 else 0.0


@dataclass
class LCBResult:
    arm: str
    n: int
    mean_r: float
    lcb_r: float
    std_r: float
    winrate: float


def _z_for_alpha(alpha: float) -> float:
    """
    alpha ~ 0.10 -> z=1.2816, alpha ~ 0.05 -> z=1.6449
    We keep a tiny lookup to avoid scipy dependency.
    """
    a = float(alpha)
    if a <= 0.051:
        return 1.6449
    if a <= 0.101:
        return 1.2816
    if a <= 0.201:
        return 0.8416
    return 0.6745


def lcb_mean(agg: ArmAgg, alpha: float = 0.10) -> float:
    """
    LCB of mean via normal approx: mean - z * std/sqrt(n)
    For heavy tails this is conservative if std inflated; still robust enough for gating.
    """
    if agg.n <= 0:
        return 0.0
    mu = agg.mean()
    if agg.n < 2:
        return mu
    sd = agg.std()
    z = _z_for_alpha(alpha)
    return float(mu - z * (sd / math.sqrt(max(1, agg.n))))


def evaluate_winner_lcb(
    *,
    stats_by_arm: dict[str, ArmAgg],
    baseline_arm: str = "A",
    min_n: int = 30,
    alpha: float = 0.10,
    min_edge_r: float = 0.05,
) -> tuple[str | None, dict[str, LCBResult], str]:
    """
    Choose winner arm by LCB(mean R).
    Rule:
      - require n >= min_n for candidate
      - winner must beat baseline by (LCB_winner - LCB_baseline) >= min_edge_r
      - if baseline has low n -> allow winner if its LCB >= +min_edge_r
    Returns:
      (winner_or_none, metrics_by_arm, reason)
    """
    baseline_arm = (baseline_arm or "A").upper()
    alpha = float(alpha)
    min_n = int(min_n)

    res: dict[str, LCBResult] = {}
    for arm, agg in (stats_by_arm or {}).items():
        a = (arm or "").upper()
        if not a:
            continue
        mu = agg.mean()
        sd = agg.std()
        lcb = lcb_mean(agg, alpha=alpha)
        res[a] = LCBResult(arm=a, n=agg.n, mean_r=mu, lcb_r=lcb, std_r=sd, winrate=agg.winrate())

    if not res:
        return None, {}, "no_data"

    base = res.get(baseline_arm)
    base_lcb = base.lcb_r if base else 0.0
    base_n = base.n if base else 0

    # candidate set
    candidates = [x for x in res.values() if x.n >= min_n]
    if not candidates:
        return None, res, f"not_ready_min_n(min_n={min_n})"

    candidates.sort(key=lambda x: x.lcb_r, reverse=True)
    top = candidates[0]

    # If baseline not ready, require absolute LCB
    if base_n < min_n:
        if top.lcb_r >= float(min_edge_r):
            return top.arm, res, f"baseline_not_ready; pick={top.arm} lcb={top.lcb_r:.3f}"
        return None, res, f"baseline_not_ready; top_lcb<{min_edge_r}"

    # baseline ready: require edge vs baseline
    edge = float(top.lcb_r - base_lcb)
    if edge >= float(min_edge_r):
        return top.arm, res, f"edge_ok({edge:.3f}>= {min_edge_r}) pick={top.arm}"
    return None, res, f"edge_too_small({edge:.3f}<{min_edge_r})"


def regime_thresholds(regime: str) -> tuple[int, float, float]:
    """
    Best-practice defaults (can be overridden by ENV in service):
      - thin/news/illiquid: stricter (more samples, lower alpha, higher edge)
      - trend: moderate
      - range/mixed: moderate
    Returns: (min_n, alpha, min_edge_r)
    """
    rg = (regime or "na").lower()
    if rg in ("thin", "news", "illiquid"):
        return 60, 0.05, 0.10
    if rg in ("trend", "trending_bull", "trending_bear"):
        return 40, 0.10, 0.07
    # range/mixed/na
    return 30, 0.10, 0.05
