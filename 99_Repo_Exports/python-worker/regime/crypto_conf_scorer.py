import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Mapping

import yaml


Direction = int  # +1, -1, 0


# === dataclasses для конфигов ===

@dataclass
class L3Thresholds:
    """
    Пороговые значения для L3-термов, вытащенные из baseline.
    """

    spread_max_ok_bps: float
    spread_hard_limit_bps: float

    cancel_soft: float
    cancel_hard: float

    obi_good_min: float     # выше этого значения – хороший устойчивый перекос
    obi_bad_max: float      # ниже этого – считаем "OBI нет"

    mp_drift_max_bps: float

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "L3Thresholds":
        return cls(
            spread_max_ok_bps=float(d["spread_max_ok_bps"])
            spread_hard_limit_bps=float(d["spread_hard_limit_bps"])
            cancel_soft=float(d["cancel_soft"])
            cancel_hard=float(d["cancel_hard"])
            obi_good_min=float(d["obi_good_min"])
            obi_bad_max=float(d["obi_bad_max"])
            mp_drift_max_bps=float(d["mp_drift_max_bps"])
        )


@dataclass
class L3Profile:
    """Профиль L3 для конкретного (symbol, family, direction)."""

    l3: L3Thresholds


@dataclass
class CryptoConfScorerConfig:
    """
    Все настройки CryptoConfScorer:
      - default_profile: общий профиль по умолчанию
      - by_symbol: overrides по символам/семействам/направлениям
    """

    default_profile: L3Profile
    by_symbol: Dict[str, Dict[str, Dict[str, L3Profile]]]  # symbol -> family -> dir_key -> profile

    @classmethod
    def from_yaml_dict(cls, root: Mapping[str, Any]) -> "CryptoConfScorerConfig":
        # ожидаем корень crypto_conf_scorer
        cfg = root.get("crypto_conf_scorer", {})
        default_cfg = cfg.get("default", {})
        default_l3_cfg = default_cfg.get("l3", {})
        default_profile = L3Profile(
            l3=L3Thresholds.from_dict(default_l3_cfg)
        )

        by_symbol: Dict[str, Dict[str, Dict[str, L3Profile]]] = {}
        syms_cfg = cfg.get("by_symbol", {}) or {}

        for symbol, sym_val in syms_cfg.items():
            by_symbol[symbol] = {}
            # sym_val: dict[family -> {long/short/neutral -> {l3: {...}}}]
            for family, fam_val in sym_val.items():
                by_symbol[symbol][family] = {}
                for dir_key, dir_val in fam_val.items():
                    l3_cfg = dir_val.get("l3", {})
                    by_symbol[symbol][family][dir_key] = L3Profile(
                        l3=L3Thresholds.from_dict(l3_cfg)
                    )

        return cls(default_profile=default_profile, by_symbol=by_symbol)

    def resolve_profile(
        self
        symbol: str
        signal_family: str
        direction: Direction
    ) -> L3Profile:
        """
        Пытаемся найти максимально специфичный профиль:
          (symbol, family, dir) -> (symbol, family, neutral) -> default
        """
        dir_key = direction_to_key(direction)

        sym_cfg = self.by_symbol.get(symbol)
        if sym_cfg is not None:
            fam_cfg = sym_cfg.get(signal_family)
            if fam_cfg is not None:
                # сначала пробуем точное направление
                prof = fam_cfg.get(dir_key)
                if prof is not None:
                    return prof
                # затем нейтральный профиль
                prof = fam_cfg.get("neutral")
                if prof is not None:
                    return prof

        # fallback: global default
        return self.default_profile


def direction_to_key(direction: Direction) -> str:
    if direction > 0:
        return "long"
    if direction < 0:
        return "short"
    return "neutral"


# === основной класс CryptoConfScorer ===

class CryptoConfScorer:
    """
    Отвечает за L3-компонент финального конф-скора (spread / cancel / OBI / microprice drift).

    - грузит YAML с baseline thresholds;
    - умеет hot-reload по mtime;
    - отдает l3_score в [0, 1] + подробный debug.
    """

    def __init__(
        self
        yaml_path: str
        reload_interval_sec: int = 60
    ) -> None:
        self.yaml_path = yaml_path
        self.reload_interval_sec = reload_interval_sec

        self._config: Optional[CryptoConfScorerConfig] = None
        self._last_loaded_mtime: float = 0.0
        self._last_checked_ts: float = 0.0

        self._load_config(force=True)

    # --- публичный API ---

    def score_l3(
        self
        *
        symbol: str
        signal_family: str
        direction: Direction
        l3_spread_bps: float
        l3_obi_persistence_score: float
        l3_microprice_shift_bps_20: float
        l3_cancel_to_trade_bid_5s: float
        l3_cancel_to_trade_ask_5s: float
        l3_cancel_to_trade_bid_20s: float
        l3_cancel_to_trade_ask_20s: float
    ) -> Dict[str, Any]:
        """
        Основной метод: возвращает dict со структурой:
          {
            "l3_score": float
            "terms": {
                "spread_ok_score": float
                "cancel_to_trade_score": float
                "obi_persistence_score": float
                "microprice_drift_score": float
            }
            "profile": { ...debug thresholds... }
          }
        """

        cfg = self._get_config()
        profile = cfg.resolve_profile(symbol, signal_family, direction)
        t = profile.l3

        # --- spread term ---
        spread_ok_score = self._spread_ok_term(
            spread_bps=l3_spread_bps
            max_ok=t.spread_max_ok_bps
            hard=t.spread_hard_limit_bps
        )

        # --- cancel-to-trade term ---
        cancel_to_trade_score = self._cancel_to_trade_term(
            bid5=l3_cancel_to_trade_bid_5s
            ask5=l3_cancel_to_trade_ask_5s
            bid20=l3_cancel_to_trade_bid_20s
            ask20=l3_cancel_to_trade_ask_20s
            soft=t.cancel_soft
            hard=t.cancel_hard
        )

        # --- OBI persistence term ---
        obi_persistence_score = self._obi_persistence_term(
            obi_persistence=l3_obi_persistence_score
            good_min=t.obi_good_min
            bad_max=t.obi_bad_max
        )

        # --- microprice drift term ---
        microprice_drift_score = self._microprice_drift_term(
            mp_shift_bps=l3_microprice_shift_bps_20
            max_bps=t.mp_drift_max_bps
        )

        # агрегируем в финальный l3_score
        # веса можно вынести в конфиг, здесь — стартовые
        w_spread = 0.35
        w_cancel = 0.25
        w_obi = 0.25
        w_mp = 0.15

        l3_score = (
            w_spread * spread_ok_score
            + w_cancel * cancel_to_trade_score
            + w_obi * obi_persistence_score
            + w_mp * microprice_drift_score
        )

        # защита от NaN/выхода за диапазон
        if l3_score != l3_score:  # NaN
            l3_score = 0.0
        l3_score = max(0.0, min(1.0, float(l3_score)))

        return {
            "l3_score": l3_score
            "terms": {
                "spread_ok_score": spread_ok_score
                "cancel_to_trade_score": cancel_to_trade_score
                "obi_persistence_score": obi_persistence_score
                "microprice_drift_score": microprice_drift_score
            }
            "profile": {
                "spread_max_ok_bps": t.spread_max_ok_bps
                "spread_hard_limit_bps": t.spread_hard_limit_bps
                "cancel_soft": t.cancel_soft
                "cancel_hard": t.cancel_hard
                "obi_good_min": t.obi_good_min
                "obi_bad_max": t.obi_bad_max
                "mp_drift_max_bps": t.mp_drift_max_bps
            }
        }

    # --- приватные helpers для термов ---

    @staticmethod
    def _spread_ok_term(
        *
        spread_bps: float
        max_ok: float
        hard: float
    ) -> float:
        """
        Идея:

        - если spread <= max_ok → score ~ 1
        - если max_ok < spread < hard → линейно спадаем до 0
        - если spread >= hard → сильный штраф → score ~ 0
        """
        if hard <= 0:
            return 0.0

        s = abs(float(spread_bps))
        if s <= max_ok:
            return 1.0

        if s >= hard:
            return 0.0

        # линейная интерполяция между max_ok и hard
        frac = (hard - s) / max(1e-9, hard - max_ok)
        return max(0.0, min(1.0, frac))

    @staticmethod
    def _cancel_to_trade_term(
        *
        bid5: float
        ask5: float
        bid20: float
        ask20: float
        soft: float
        hard: float
    ) -> float:
        """
        Cancel-to-Trade ratio:

        - агрегируем bid/ask и 5/20s в одну метрику
        - если cancel <= soft → ok = 1
        - если soft < cancel < hard → спадаем линейно до 0
        - если >= hard → 0
        """
        vals = [
            float(bid5)
            float(ask5)
            float(bid20)
            float(ask20)
        ]
        # берём max (наихудший сценарий)
        c = max(v for v in vals if v == v)  # фильтр NaN: v == v

        if c <= soft:
            return 1.0
        if c >= hard:
            return 0.0
        frac = (hard - c) / max(1e-9, hard - soft)
        return max(0.0, min(1.0, frac))

    @staticmethod
    def _obi_persistence_term(
        *
        obi_persistence: float
        good_min: float
        bad_max: float
    ) -> float:
        """
        OBI persistence score:

        - если ниже bad_max → считаем, что устойчивого перекоса нет → 0
        - если выше good_min → полноценный сигнал → 1
        - между bad_max и good_min → интерполяция 0..1
        """
        x = float(obi_persistence)
        if x <= bad_max:
            return 0.0
        if x >= good_min:
            return 1.0

        if good_min <= bad_max:
            # защита от некорректного конфига
            return 0.0

        frac = (x - bad_max) / (good_min - bad_max)
        return max(0.0, min(1.0, frac))

    @staticmethod
    def _microprice_drift_term(
        *
        mp_shift_bps: float
        max_bps: float
    ) -> float:
        """
        Microprice/fair-value drift:

        - считаем по модулю: |drift|
        - если |drift| <= max_bps → score ~ 1
        - если сильно выше max_bps → спадаем к 0
        """
        x = abs(float(mp_shift_bps))
        if max_bps <= 0:
            return 0.0

        if x <= max_bps:
            return 1.0

        # Допустим: после 2*max_bps считаем всё совсем плохо → 0
        hard = 2.0 * max_bps
        if x >= hard:
            return 0.0

        frac = (hard - x) / max(1e-9, hard - max_bps)
        return max(0.0, min(1.0, frac))

    # --- загрузка/обновление конфига ---

    def _get_config(self) -> CryptoConfScorerConfig:
        """
        Возвращает актуальный конфиг; при необходимости триггерит hot-reload.
        """
        now = time.time()
        if now - self._last_checked_ts >= self.reload_interval_sec:
            self._load_config(force=False)
            self._last_checked_ts = now

        if self._config is None:
            # защита: если что-то пошло не так с загрузкой
            raise RuntimeError("CryptoConfScorer: config is not loaded")

        return self._config

    def _load_config(self, *, force: bool) -> None:
        try:
            st = os.stat(self.yaml_path)
        except FileNotFoundError:
            if self._config is None:
                raise
            # если файл пропал после первой загрузки — оставляем старый конфиг
            return

        mtime = st.st_mtime
        if (not force) and mtime <= self._last_loaded_mtime:
            return  # нет изменений

        with open(self.yaml_path, "r", encoding="utf-8") as f:
            root = yaml.safe_load(f) or {}

        self._config = CryptoConfScorerConfig.from_yaml_dict(root)
        self._last_loaded_mtime = mtime
