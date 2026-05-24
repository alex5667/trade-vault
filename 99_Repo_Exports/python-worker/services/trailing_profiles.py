"""
Профили трейлинга, которые включаются ПОСЛЕ достижения TPn (per-profile).

Идея:
- сигнал или стратегия ставит: trail_after_tp1=true, trail_profile="rocket_v1"
- когда прилетает событие TPn_HIT — orchestrator берёт профиль и сверяет
  profile.activate_after_tp == n; если совпало — генерит команду в gateway.

Phase A (P0):
- frozen TrailingProfileV2 — будущий канонический контракт (schema_ver=2),
  поля по плану: arm_threshold_r, hard_lock_r, clear_tp_policy, allowed_regimes/symbols.
- profile_hash и policy_hash для трассировки каждого trailing-решения.
- validate_default() на старте — фатально если DEFAULT_TRAIL_PROFILE ∉ registry.
"""

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field

import redis

from common.log import setup_logger

log = setup_logger("trailing_profiles")


# ─────────────────────────────────────────────────────────────────────────────
# Стабильный хэш профиля. Используется для трассировки решений (метрики, DLQ,
# trailing_decisions hypertable).
# ─────────────────────────────────────────────────────────────────────────────
def _stable_hash(payload: dict) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:12]


@dataclass
class TrailingProfile:
    """Legacy v1 профиль трейлинга. Сохранён до полной миграции на V2.

    Поля v1 → v2:
      mode/atr_mult/points/step_points/hard_min_lock — те же.
      activate_after_tp — без изменений.
      clear_tp_policy — выводится из имени (rocket_v1 → rocket_only, иначе never).
    """

    name: str
    mode: str           # "ATR" | "POINTS" | "STEP" | "BREAKEVEN"
    atr_mult: float = 1.0
    points: float = 200.0
    hard_min_lock: float | None = None
    step_points: float | None = None
    activate_after_tp: int = 1          # 1=TP1, 2=TP2, 0=immediate
    comment: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "TrailingProfile":
        # Tolerate forward-compatible fields (TrailingProfileV2) coming from Redis.
        allowed = {f for f in cls.__dataclass_fields__}
        clean = {k: v for k, v in data.items() if k in allowed}
        return cls(**clean)

    def profile_hash(self) -> str:
        return _stable_hash(self.to_dict())

    def to_v2(self) -> "TrailingProfileV2":
        if self.name == "rocket_v1":
            clear_tp = "rocket_only"
        elif self.name == "expansion_v1":
            clear_tp = "never"
        else:
            clear_tp = "never"
        return TrailingProfileV2(
            schema_ver=2,
            name=self.name,
            mode=self.mode,
            activate_after_tp=self.activate_after_tp,
            atr_mult=self.atr_mult if self.atr_mult is not None else None,
            arm_threshold_r=None,
            hard_lock_r=None,
            clear_tp_policy=clear_tp,
            allowed_regimes=(),
            allowed_symbols=(),
            reason=self.comment or "",
        )


@dataclass(frozen=True)
class TrailingProfileV2:
    """Канонический контракт профиля (Phase A, P0).

    Frozen: hashable, immutable — безопасен для совместного использования
    между потоками/процессами. Все коллекции — tuple (не list).

    Совместимость:
      to_v1() для legacy кода;
      profile_hash() стабильный (sort_keys, no default).
    """

    schema_ver: int
    name: str
    mode: str                       # BREAKEVEN | ATR | POINTS | STEP
    activate_after_tp: int          # 0 | 1 | 2 | 3
    atr_mult: float | None
    arm_threshold_r: float | None
    hard_lock_r: float | None
    clear_tp_policy: str            # never | rocket_only | always
    allowed_regimes: tuple[str, ...] = field(default_factory=tuple)
    allowed_symbols: tuple[str, ...] = field(default_factory=tuple)
    reason: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["allowed_regimes"] = list(self.allowed_regimes)
        d["allowed_symbols"] = list(self.allowed_symbols)
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "TrailingProfileV2":
        ar = data.get("allowed_regimes") or []
        asy = data.get("allowed_symbols") or []
        return cls(
            schema_ver=int(data.get("schema_ver", 2)),
            name=str(data["name"]),
            mode=str(data["mode"]),
            activate_after_tp=int(data.get("activate_after_tp", 1)),
            atr_mult=(float(data["atr_mult"]) if data.get("atr_mult") is not None else None),
            arm_threshold_r=(float(data["arm_threshold_r"]) if data.get("arm_threshold_r") is not None else None),
            hard_lock_r=(float(data["hard_lock_r"]) if data.get("hard_lock_r") is not None else None),
            clear_tp_policy=str(data.get("clear_tp_policy", "never")),
            allowed_regimes=tuple(ar),
            allowed_symbols=tuple(asy),
            reason=str(data.get("reason", "")),
        )

    def profile_hash(self) -> str:
        return _stable_hash(self.to_dict())

    def to_v1(self) -> TrailingProfile:
        return TrailingProfile(
            name=self.name,
            mode=self.mode,
            atr_mult=self.atr_mult if self.atr_mult is not None else 1.0,
            points=200.0,
            hard_min_lock=None,
            step_points=None,
            activate_after_tp=self.activate_after_tp,
            comment=self.reason,
        )


class TrailingProfilesRegistry:
    """Единый источник правды для trailing profiles.

    Источники (порядок применения):
      1) встроенные дефолты (_init_default);
      2) Redis key `trailing:profiles` (опционально перезаписывает дефолты).

    Phase A добавляет:
      - profile_hash(name) — стабильный hash per profile;
      - policy_hash() — hash всего реестра (для трассировки rollout);
      - validate_default(name) — фатальная проверка на старте сервиса.
    """

    REDIS_KEY = "trailing:profiles"

    def __init__(self, redis_url: str | None = None):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = redis.from_url(self.redis_url, decode_responses=True)

        self._profiles: dict[str, TrailingProfile] = {}
        self._redis_key = self.REDIS_KEY

        self._init_default()
        self._load_from_redis()

        log.info("✅ TrailingProfilesRegistry initialized with %d profiles", len(self._profiles))

    # ─────────────────────────────── defaults ────────────────────────────────
    def _init_default(self):
        self._profiles["lock_and_trail"] = TrailingProfile(
            name="lock_and_trail",
            mode="ATR",
            atr_mult=1.0,
            hard_min_lock=0.0,
            activate_after_tp=1,
            comment="lock profit and trail with ATR 1.0 after TP1",
        )
        self._profiles["protective_only"] = TrailingProfile(
            name="protective_only",
            mode="BREAKEVEN",
            atr_mult=0.0,
            hard_min_lock=0.0,
            activate_after_tp=1,
            comment="Move to Breakeven + fee compensation only after TP1 is hit, no trailing",
        )
        self._profiles["range_protective"] = TrailingProfile(
            name="range_protective",
            mode="BREAKEVEN",
            atr_mult=0.0,
            hard_min_lock=0.0,
            activate_after_tp=1,
            comment="Range regime: BE + fees after TP1, no trailing",
        )
        self._profiles["rocket_v1_bear"] = TrailingProfile(
            name="rocket_v1_bear",
            mode="ATR",
            atr_mult=1.0,
            hard_min_lock=0.0,
            activate_after_tp=1,
            comment="Bear trend SHORT follow: ATR 1.0 trailing after TP1 (tighter than rocket_v1 for sharper bounces)",
        )
        self._profiles["rocket_v1"] = TrailingProfile(
            name="rocket_v1",
            mode="ATR",
            atr_mult=1.2,
            hard_min_lock=0.0,
            activate_after_tp=1,
            comment="ATR 1.2 trailing after TP1 (fallback; runtime uses calibrated value)",
        )
        self._profiles["expansion_v1"] = TrailingProfile(
            name="expansion_v1",
            mode="ATR",
            atr_mult=1.5,
            hard_min_lock=0.0,
            activate_after_tp=2,
            comment="ATR 1.5 trailing after TP2 for expansion regime",
        )
        self._profiles["wide_swing"] = TrailingProfile(
            name="wide_swing",
            mode="ATR",
            atr_mult=1.2,
            hard_min_lock=0.0,
            activate_after_tp=1,
            comment="wider ATR trail for choppy/default regime",
        )
        self._profiles["points_200"] = TrailingProfile(
            name="points_200",
            mode="POINTS",
            points=200.0,
            activate_after_tp=1,
            comment="200 pts trailing after TP1",
        )
        self._profiles["crypto_tight"] = TrailingProfile(
            name="crypto_tight",
            mode="ATR",
            atr_mult=0.5,
            hard_min_lock=0.0,
            activate_after_tp=1,
            comment="very tight ATR trail for crypto volatility",
        )

        log.debug("Initialized %d default profiles", len(self._profiles))

    # ───────────────────────────── Redis sync ────────────────────────────────
    def _load_from_redis(self):
        try:
            data = self.r.get(self._redis_key)
            if data:
                profiles_dict = json.loads(data)
                for name, profile_data in profiles_dict.items():
                    try:
                        profile = TrailingProfile.from_dict(profile_data)
                        self._profiles[name] = profile
                    except Exception as e:
                        log.warning("Failed to load profile %s from Redis: %s", name, e)
                log.info("✅ Loaded %d profiles from Redis", len(profiles_dict))
        except Exception as e:
            log.debug("No profiles in Redis or error loading: %s", e)

    def save_to_redis(self):
        try:
            profiles_dict = {name: p.to_dict() for name, p in self._profiles.items()}
            self.r.set(self._redis_key, json.dumps(profiles_dict))
            log.info("✅ Saved %d profiles to Redis", len(self._profiles))
        except Exception as e:
            log.error("Failed to save profiles to Redis: %s", e)

    # ───────────────────────────── CRUD/getters ──────────────────────────────
    def get(self, name: str) -> TrailingProfile | None:
        return self._profiles.get(name)

    def get_v2(self, name: str) -> TrailingProfileV2 | None:
        p = self._profiles.get(name)
        return p.to_v2() if p else None

    def list_names(self) -> list[str]:
        return list(self._profiles.keys())

    def add(self, profile: TrailingProfile, save_to_redis: bool = True):
        self._profiles[profile.name] = profile
        log.info("Added profile: %s", profile.name)
        if save_to_redis:
            self.save_to_redis()

    def remove(self, name: str, save_to_redis: bool = True):
        if name in self._profiles:
            del self._profiles[name]
            log.info("Removed profile: %s", name)
            if save_to_redis:
                self.save_to_redis()
        else:
            log.warning("Profile not found: %s", name)

    def get_all(self) -> dict[str, TrailingProfile]:
        return self._profiles.copy()

    # ─────────────────────────── hashes / validation ─────────────────────────
    def profile_hash(self, name: str) -> str | None:
        p = self._profiles.get(name)
        return p.profile_hash() if p else None

    def policy_hash(self) -> str:
        """Hash всех профилей. Меняется при любом изменении содержимого реестра."""
        agg = {name: p.to_dict() for name, p in sorted(self._profiles.items())}
        return _stable_hash(agg)

    def validate_default(self, default_name: str) -> None:
        """Падать на старте, если DEFAULT_TRAIL_PROFILE отсутствует в реестре.

        Это закрывает класс багов «оператор задал в compose имя профиля,
        которого нет в коде/Redis» — раньше такое всплывало только при
        реальном TP1_HIT.
        """
        if default_name not in self._profiles:
            available = ", ".join(sorted(self._profiles.keys()))
            raise ValueError(
                f"DEFAULT_TRAIL_PROFILE='{default_name}' not in registry. "
                f"Available: [{available}]"
            )
        log.info(
            "✅ DEFAULT_TRAIL_PROFILE='%s' validated (hash=%s, policy_hash=%s)",
            default_name,
            self.profile_hash(default_name),
            self.policy_hash(),
        )


if __name__ == "__main__":
    registry = TrailingProfilesRegistry()
    print("\n=== Available Profiles ===")
    for name in registry.list_names():
        profile = registry.get(name)
        if profile is None:
            continue
        print(f"\n{name} (hash={registry.profile_hash(name)}):")
        print(f"  Mode: {profile.mode}")
        print(f"  ATR mult: {profile.atr_mult}")
        print(f"  Activate after TP: {profile.activate_after_tp}")
        print(f"  Comment: {profile.comment}")
    print(f"\npolicy_hash = {registry.policy_hash()}")
