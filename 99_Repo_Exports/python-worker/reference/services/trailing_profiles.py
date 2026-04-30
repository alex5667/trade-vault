# -*- coding: utf-8 -*-
"""
Профили трейлинга, которые включаются ПОСЛЕ достижения TP1.

Идея:
- сигнал или стратегия ставит: trail_after_tp1=true, trail_profile="rocket_v1"
- когда прилетает событие TP1_HIT — мы берём профиль и генерим команду в gateway

Интегрировано с scanner_infra:
- Redis-based конфигурация
- Поддержка ATR из go-gateway
- Расширяемость для real DOM
"""

from dataclasses import dataclass, asdict
from typing import Optional, Dict, List
import json
import redis
import os

from common.log import setup_logger

log = setup_logger("trailing_profiles")


@dataclass
class TrailingProfile:
    """
    Профиль трейлинга.
    
    Attributes:
        name: Уникальное имя профиля
        mode: Режим трейлинга - "ATR" | "POINTS" | "STEP"
        atr_mult: Множитель ATR для режима ATR
        points: Фиксированные пункты для режима POINTS
        hard_min_lock: Минимальная прибыль для фиксации (в пунктах)
        step_points: Размер шага для ступенчатого трейлинга
        comment: Описание профиля
    """
    name: str
    mode: str           # "ATR" | "POINTS" | "STEP"
    atr_mult: float = 1.0
    points: float = 200.0
    hard_min_lock: Optional[float] = None  # сколько в пунктах обязательно зафиксировать
    step_points: Optional[float] = None    # для ступенчатого
    comment: str = ""
    
    def to_dict(self) -> Dict:
        """Сериализация в dict."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'TrailingProfile':
        """Десериализация из dict."""
        return cls(**data)


class TrailingProfilesRegistry:
    """
    Хранит все известные профили трейлинга.
    Можно загрузить из Redis/конфига.
    
    Redis key: trailing:profiles
    Format: JSON dict {profile_name: profile_data}
    """
    
    def __init__(self, redis_url: Optional[str] = None):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = redis.from_url(self.redis_url, decode_responses=True)
        
        self._profiles: Dict[str, TrailingProfile] = {}
        self._redis_key = "trailing:profiles"
        
        # Инициализация дефолтных профилей
        self._init_default()
        
        # Загрузка из Redis (если есть)
        self._load_from_redis()
        
        log.info("✅ TrailingProfilesRegistry initialized with %d profiles", len(self._profiles))
    
    def _init_default(self):
        """Инициализация дефолтных профилей."""
        
        # Базовый — «не отдаём TP1»
        self._profiles["lock_and_trail"] = TrailingProfile(
            name="lock_and_trail"
            mode="ATR"
            atr_mult=0.8,       # SL = mid - 0.8*ATR
            hard_min_lock=0.0
            comment="lock profit and trail with ATR 0.8"
        )

        # Ракетный — для сильных ходов по XAUUSD и крипте
        self._profiles["rocket_v1"] = TrailingProfile(
            name="rocket_v1"
            mode="ATR"
            atr_mult=0.6,       # SL = entry ± 0.6*ATR (для трейлинга после TP1)
            hard_min_lock=0.0
            comment="ATR 0.6 trailing, TP1=0.78 ATR (default for crypto and XAUUSD)"
        )

        # Более безопасный — если рынок шумный
        self._profiles["wide_swing"] = TrailingProfile(
            name="wide_swing"
            mode="ATR"
            atr_mult=1.2
            hard_min_lock=0.0
            comment="wider ATR trail for choppy regime"
        )

        # Фиксированный по пунктам — на случай если ATR нет в MT5
        self._profiles["points_200"] = TrailingProfile(
            name="points_200"
            mode="POINTS"
            points=200.0
            comment="200 pts trailing"
        )
        
        # Агрессивный для криптовалют
        self._profiles["crypto_tight"] = TrailingProfile(
            name="crypto_tight"
            mode="ATR"
            atr_mult=0.5
            hard_min_lock=0.0
            comment="very tight ATR trail for crypto volatility"
        )
        
        log.debug("Initialized %d default profiles", len(self._profiles))
    
    def _load_from_redis(self):
        """Загрузка профилей из Redis."""
        try:
            data = self.r.get(self._redis_key)
            if data:
                profiles_dict = json.loads(data)
                for name, profile_data in profiles_dict.items():
                    try:
                        profile = TrailingProfile.from_dict(profile_data)
                        self._profiles[name] = profile
                        log.debug("Loaded profile from Redis: %s", name)
                    except Exception as e:
                        log.warning("Failed to load profile %s from Redis: %s", name, e)
                
                log.info("✅ Loaded %d profiles from Redis", len(profiles_dict))
        except Exception as e:
            log.debug("No profiles in Redis or error loading: %s", e)
    
    def save_to_redis(self):
        """Сохранение профилей в Redis."""
        try:
            profiles_dict = {
                name: profile.to_dict() 
                for name, profile in self._profiles.items()
            }
            self.r.set(self._redis_key, json.dumps(profiles_dict))
            log.info("✅ Saved %d profiles to Redis", len(self._profiles))
        except Exception as e:
            log.error("Failed to save profiles to Redis: %s", e)
    
    def get(self, name: str) -> Optional[TrailingProfile]:
        """Получить профиль по имени."""
        return self._profiles.get(name)
    
    def list_names(self) -> List[str]:
        """Список всех доступных профилей."""
        return list(self._profiles.keys())
    
    def add(self, profile: TrailingProfile, save_to_redis: bool = True):
        """
        Добавить новый профиль.
        
        Args:
            profile: Профиль для добавления
            save_to_redis: Сохранить в Redis после добавления
        """
        self._profiles[profile.name] = profile
        log.info("Added profile: %s", profile.name)
        
        if save_to_redis:
            self.save_to_redis()
    
    def remove(self, name: str, save_to_redis: bool = True):
        """
        Удалить профиль.
        
        Args:
            name: Имя профиля
            save_to_redis: Сохранить в Redis после удаления
        """
        if name in self._profiles:
            del self._profiles[name]
            log.info("Removed profile: %s", name)
            
            if save_to_redis:
                self.save_to_redis()
        else:
            log.warning("Profile not found: %s", name)
    
    def get_all(self) -> Dict[str, TrailingProfile]:
        """Получить все профили."""
        return self._profiles.copy()


if __name__ == "__main__":
    # Тестирование
    registry = TrailingProfilesRegistry()
    
    print("\n=== Available Profiles ===")
    for name in registry.list_names():
        profile = registry.get(name)
        print(f"\n{name}:")
        print(f"  Mode: {profile.mode}")
        print(f"  ATR mult: {profile.atr_mult}")
        print(f"  Points: {profile.points}")
        print(f"  Comment: {profile.comment}")
    
    # Сохранение в Redis
    registry.save_to_redis()
    print("\n✅ Profiles saved to Redis")

