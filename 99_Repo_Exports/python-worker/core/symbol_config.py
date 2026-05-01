"""
Symbol Configuration Manager - Управление конфигурацией для каждого символа

Поддерживает:
- Динамическую конфигурацию через Redis
- Разные настройки для  BTCUSD, ETHUSD, etc.
- Валидацию параметров
- Defaults для разных типов инструментов
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, Optional, List
import json
from enum import Enum
from core.redis_keys import RedisStreams as RS


class SymbolType(Enum):
    """Тип торгового инструмента"""
    FOREX_METAL = "forex_metal"  #  XAGUSD
    CRYPTO_USD = "crypto_usd"     # BTCUSD, ETHUSD
    CRYPTO_USDT = "crypto_usdt"   # BTCUSDT, ETHUSDT
    FOREX_PAIR = "forex_pair"     # EURUSD, GBPUSD


@dataclass
class DOMConfig:
    """Конфигурация Depth of Market"""
    vendor: str = "BINANCE"
    depth: int = 15
    mock_mid_price: float = 0.0
    mock_tick_size: float = 0.01
    
    # Redis streams
    book_stream: str = ""
    book_last_key: str = ""
    
    def __post_init__(self):
        """Auto-generate stream names if not provided"""
        if not self.book_stream and hasattr(self, '_symbol'):
            self.book_stream = f"stream:book_{self._symbol}"
        if not self.book_last_key and hasattr(self, '_symbol'):
            self.book_last_key = f"book:levels:{self._symbol}"


@dataclass
class ATRConfig:
    """Конфигурация Average True Range"""
    source: str = "ticks"  # ticks, candles
    timeframe: str = "1m"  # 1m, 5m, 15m
    period: int = 14
    
    # Multipliers for SL/TP
    sl_multiplier: float = 1.5
    tp_multipliers: List[float] = field(default_factory=lambda: [2.0, 3.0, 4.0])
    
    # Thresholds
    dist_atr_threshold: float = 0.5  # Distance from pivot as ATR multiplier


@dataclass
class AccountConfig:
    """Конфигурация торгового счета"""
    deposit_usd: float = 100.0
    leverage: int = 1000
    risk_percent: float = 5.0  # % of account per trade
    
    # Symbol-specific
    contract_size: float = 100.0  # 100 for  1 for crypto
    lot_step: float = 0.01
    min_lot: float = 0.01
    max_lot: float = 10.0


@dataclass
class OrderFlowConfig:
    """Конфигурация OrderFlow анализа"""
    # Delta thresholds
    delta_threshold_moderate: float = 100.0
    delta_threshold_extreme: float = 300.0
    delta_window: int = 120  # ticks
    
    # OBI (Order Book Imbalance)
    obi_threshold: float = 0.3
    obi_depth_levels: int = 5
    
    # Weak Progress detection
    weak_progress_bar_range_atr_ratio: float = 0.10
    
    # Iceberg detection
    iceberg_duration_seconds: float = 1.5
    iceberg_refresh_min_abs: float = 1.0
    
    # Signal generation
    min_signal_interval_seconds: int = 60  # Minimum pause between signals
    
    # Redis read settings
    read_count: int = 100
    read_block_ms: int = 1000


@dataclass
class TelegramConfig:
    """Конфигурация уведомлений Telegram"""
    use_buttons: bool = False
    notify_stream: str = RS.NOTIFY_TELEGRAM
    snap_prefix: str = "signal:snap"
    snap_ttl: int = 21600  # 6 hours


@dataclass
class SymbolConfig:
    """
    Полная конфигурация для торгового символа.
    
    Используется для динамического создания handlers с индивидуальными настройками.
    """
    symbol: str
    symbol_type: SymbolType
    
    # Sub-configs
    dom: DOMConfig
    atr: ATRConfig
    account: AccountConfig
    orderflow: OrderFlowConfig
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    
    # Metadata
    enabled: bool = True
    is_custom: bool = False  # True если конфигурация была изменена вручную
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
    
    def to_dict(self) -> Dict:
        """Serialize to dict"""
        return {
            'symbol': self.symbol,
            'symbol_type': self.symbol_type.value,
            'dom': asdict(self.dom),
            'atr': asdict(self.atr),
            'account': asdict(self.account),
            'orderflow': asdict(self.orderflow),
            'telegram': asdict(self.telegram),
            'enabled': self.enabled,
            'is_custom': self.is_custom,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'SymbolConfig':
        """Deserialize from dict"""
        return cls(
            symbol=data['symbol'],
            symbol_type=SymbolType(data['symbol_type']),
            dom=DOMConfig(**data['dom']),
            atr=ATRConfig(**data['atr']),
            account=AccountConfig(**data['account']),
            orderflow=OrderFlowConfig(**data['orderflow']),
            telegram=TelegramConfig(**data.get('telegram', {})),
            enabled=data.get('enabled', True),
            is_custom=data.get('is_custom', False),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at')
        )
    
    def to_json(self) -> str:
        """Serialize to JSON"""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'SymbolConfig':
        """Deserialize from JSON"""
        return cls.from_dict(json.loads(json_str))
    
    def to_instrument_config(self) -> 'OrderFlowConfig':
        """
        Конвертирует SymbolConfig в OrderFlowConfig из instrument_config.py
        для использования в handlers.
        
        Returns:
            OrderFlowConfig из instrument_config.py
        """
        try:
            from core.instrument_config import OrderFlowConfig as InstrumentOrderFlowConfig
        except ImportError:
            raise ImportError("Cannot import OrderFlowConfig from instrument_config")
        
        # Конвертируем orderflow config из symbol_config.py в instrument_config.py
        of_cfg = self.orderflow
        
        # Маппинг полей между двумя версиями OrderFlowConfig
        return InstrumentOrderFlowConfig(
            symbol=self.symbol,
            delta_window_ticks=of_cfg.delta_window,
            delta_z_threshold=3.0,  # Default, так как нет в symbol_config.OrderFlowConfig
            weak_progress_atr=of_cfg.weak_progress_bar_range_atr_ratio,
            obi_threshold=of_cfg.obi_threshold,
            obi_min_duration=2.0,  # Default, так как нет в symbol_config.OrderFlowConfig
            iceberg_refresh_count=2,  # Default, так как нет в symbol_config.OrderFlowConfig
            iceberg_min_duration=of_cfg.iceberg_duration_seconds,
            iceberg_refresh_min_abs=of_cfg.iceberg_refresh_min_abs,
            dist_atr_threshold=0.5,  # Default, так как нет в symbol_config.OrderFlowConfig
            min_signal_interval_sec=of_cfg.min_signal_interval_seconds,  # Конвертируем _seconds -> _sec
            read_count=of_cfg.read_count,
            read_block_ms=of_cfg.read_block_ms,
            metadata={}
        )


# ═══════════════════════════════════════════════════════════════════
# FACTORY: Создание конфигураций по умолчанию
# ═══════════════════════════════════════════════════════════════════

class SymbolConfigFactory:
    """Factory для создания конфигураций с defaults для разных типов символов"""
    
    @staticmethod
    def create_xauusd_config() -> SymbolConfig:
        """Конфигурация для  (Gold)"""
        return SymbolConfig(
            symbol="",
            symbol_type=SymbolType.FOREX_METAL,
            dom=DOMConfig(
                vendor="BINANCE",
                depth=15,
                mock_mid_price=3955.0,
                mock_tick_size=0.1,
                book_stream="stream:book_",
                book_last_key="book:levels:"
            ),
            atr=ATRConfig(
                source="ticks",
                timeframe="1m",
                period=14,
                sl_multiplier=1.5,
                tp_multipliers=[2.0, 3.0, 4.0],
                dist_atr_threshold=0.5
            ),
            account=AccountConfig(
                deposit_usd=100.0,
                leverage=1000,
                risk_percent=5.0,
                contract_size=100.0,  # Troy oz
                lot_step=0.01,
                min_lot=0.01,
                max_lot=10.0
            ),
            orderflow=OrderFlowConfig(
                delta_threshold_moderate=100.0,
                delta_threshold_extreme=300.0,
                delta_window=120,
                obi_threshold=0.3,
                obi_depth_levels=5,
                weak_progress_bar_range_atr_ratio=0.10,
                iceberg_duration_seconds=1.5,
                iceberg_refresh_min_abs=1.0,
                min_signal_interval_seconds=60,
                read_count=100,
                read_block_ms=1000
            ),
            telegram=TelegramConfig(
                use_buttons=False,
                notify_stream=RS.NOTIFY_TELEGRAM,
                snap_prefix="signal:snap",
                snap_ttl=21600
            )
        )
    
    @staticmethod
    def create_btcusd_config() -> SymbolConfig:
        """Конфигурация для BTCUSD (Bitcoin)"""
        return SymbolConfig(
            symbol="BTCUSD",
            symbol_type=SymbolType.CRYPTO_USD,
            dom=DOMConfig(
                vendor="BINANCE",
                depth=20,  # Больше уровней для крипты
                mock_mid_price=45000.0,
                mock_tick_size=0.01,
                book_stream="stream:book_BTCUSD",
                book_last_key="book:levels:BTCUSD"
            ),
            atr=ATRConfig(
                source="ticks",
                timeframe="1m",
                period=14,
                sl_multiplier=2.0,  # Больше для крипты (выше волатильность)
                tp_multipliers=[3.0, 5.0, 8.0],
                dist_atr_threshold=0.7
            ),
            account=AccountConfig(
                deposit_usd=100.0,
                leverage=1000,
                risk_percent=5.0,
                contract_size=1.0,  # 1 BTC
                lot_step=0.001,
                min_lot=0.001,
                max_lot=1.0
            ),
            orderflow=OrderFlowConfig(
                delta_threshold_moderate=500.0,  # Выше для BTC
                delta_threshold_extreme=1500.0,
                delta_window=120,
                obi_threshold=0.4,
                obi_depth_levels=10,  # Больше уровней
                weak_progress_bar_range_atr_ratio=0.15,
                iceberg_duration_seconds=2.0,
                iceberg_refresh_min_abs=5.0,
                min_signal_interval_seconds=120,  # Реже сигналы
                read_count=100,
                read_block_ms=1000
            ),
            telegram=TelegramConfig(
                use_buttons=False,
                notify_stream=RS.NOTIFY_TELEGRAM,
                snap_prefix="signal:snap",
                snap_ttl=21600
            )
        )
    
    @staticmethod
    def create_ethusd_config() -> SymbolConfig:
        """Конфигурация для ETHUSD (Ethereum)"""
        return SymbolConfig(
            symbol="ETHUSD",
            symbol_type=SymbolType.CRYPTO_USD,
            dom=DOMConfig(
                vendor="BINANCE",
                depth=20,
                mock_mid_price=2500.0,
                mock_tick_size=0.01,
                book_stream="stream:book_ETHUSD",
                book_last_key="book:levels:ETHUSD"
            ),
            atr=ATRConfig(
                source="ticks",
                timeframe="1m",
                period=14,
                sl_multiplier=2.0,
                tp_multipliers=[3.0, 5.0, 8.0],
                dist_atr_threshold=0.7
            ),
            account=AccountConfig(
                deposit_usd=100.0,
                leverage=1000,
                risk_percent=5.0,
                contract_size=1.0,  # 1 ETH
                lot_step=0.01,
                min_lot=0.01,
                max_lot=10.0
            ),
            orderflow=OrderFlowConfig(
                delta_threshold_moderate=300.0,
                delta_threshold_extreme=1000.0,
                delta_window=120,
                obi_threshold=0.4,
                obi_depth_levels=10,
                weak_progress_bar_range_atr_ratio=0.15,
                iceberg_duration_seconds=2.0,
                iceberg_refresh_min_abs=2.0,
                min_signal_interval_seconds=120,
                read_count=100,
                read_block_ms=1000
            ),
            telegram=TelegramConfig(
                use_buttons=False,
                notify_stream=RS.NOTIFY_TELEGRAM,
                snap_prefix="signal:snap",
                snap_ttl=21600
            )
        )
    
    @staticmethod
    def create_generic_crypto_config(
        symbol: str,
        mid_price: float,
        contract_size: float = 1.0,
        lot_step: float = 0.001
    ) -> SymbolConfig:
        """
        Создает базовую конфигурацию для любой криптовалюты.
        
        Args:
            symbol: Название символа (BNBUSD, SOLUSD, etc)
            mid_price: Примерная цена для mock
            contract_size: Размер контракта (обычно 1.0)
            lot_step: Шаг лота
        """
        return SymbolConfig(
            symbol=symbol,
            symbol_type=SymbolType.CRYPTO_USD,
            dom=DOMConfig(
                vendor="BINANCE",
                depth=20,
                mock_mid_price=mid_price,
                mock_tick_size=0.01,
                book_stream=f"stream:book_{symbol}",
                book_last_key=f"book:levels:{symbol}"
            ),
            atr=ATRConfig(
                source="ticks",
                timeframe="1m",
                period=14,
                sl_multiplier=2.0,
                tp_multipliers=[3.0, 5.0, 8.0],
                dist_atr_threshold=0.7
            ),
            account=AccountConfig(
                deposit_usd=100.0,
                leverage=1000,
                risk_percent=5.0,
                contract_size=contract_size,
                lot_step=lot_step,
                min_lot=lot_step,
                max_lot=100.0
            ),
            orderflow=OrderFlowConfig(
                delta_threshold_moderate=200.0,
                delta_threshold_extreme=800.0,
                delta_window=120,
                obi_threshold=0.4,
                obi_depth_levels=10,
                weak_progress_bar_range_atr_ratio=0.15,
                iceberg_duration_seconds=2.0,
                iceberg_refresh_min_abs=1.0,
                min_signal_interval_seconds=120,
                read_count=100,
                read_block_ms=1000
            ),
            telegram=TelegramConfig(
                use_buttons=False,
                notify_stream=RS.NOTIFY_TELEGRAM,
                snap_prefix="signal:snap",
                snap_ttl=21600
            )
        )
    
    @staticmethod
    def create_from_symbol(symbol: str, custom_params: Optional[Dict] = None) -> SymbolConfig:
        """
        Создает конфигурацию на основе символа с возможностью переопределения параметров.
        
        Args:
            symbol: Название символа
            custom_params: Кастомные параметры для переопределения defaults
        
        Returns:
            SymbolConfig with defaults + custom params
        """
        # Определяем тип символа и создаем базовую конфигурацию
        if symbol == REMOVE_ME:
            config = SymbolConfigFactory.create_xauusd_config()
        elif symbol == "BTCUSD":
            config = SymbolConfigFactory.create_btcusd_config()
        elif symbol == "ETHUSD":
            config = SymbolConfigFactory.create_ethusd_config()
        elif symbol.endswith("USD") or symbol.endswith("USDT"):
            # Generic crypto
            mid_price = custom_params.get('mid_price', 100.0) if custom_params else 100.0
            config = SymbolConfigFactory.create_generic_crypto_config(symbol, mid_price)
        else:
            raise ValueError(f"Unknown symbol type: {symbol}")
        
        # Применяем кастомные параметры если есть
        if custom_params:
            config_dict = config.to_dict()
            
            # Deep merge custom params
            for section, params in custom_params.items():
                if section in config_dict and isinstance(params, dict):
                    config_dict[section].update(params)
            
            config = SymbolConfig.from_dict(config_dict)
        
        return config


# ═══════════════════════════════════════════════════════════════════
# ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    """Примеры создания конфигураций"""
    
    # Пример 1:  с defaults
    xau_config = SymbolConfigFactory.create_xauusd_config()
    print("===  Config ===")
    print(xau_config.to_json())
    print()
    
    # Пример 2: BTCUSD с defaults
    btc_config = SymbolConfigFactory.create_btcusd_config()
    print("=== BTCUSD Config ===")
    print(btc_config.to_json())
    print()
    
    # Пример 3: Custom crypto (BNBUSD)
    bnb_config = SymbolConfigFactory.create_generic_crypto_config(
        symbol="BNBUSD",
        mid_price=300.0,
        contract_size=1.0,
        lot_step=0.01
    )
    print("=== BNBUSD Config ===")
    print(bnb_config.to_json())
    print()
    
    # Пример 4: Custom параметры
    sol_config = SymbolConfigFactory.create_from_symbol(
        "SOLUSD",
        custom_params={
            'mid_price': 150.0,
            'dom': {
                'depth': 25,
                'mock_mid_price': 150.0
            },
            'atr': {
                'sl_multiplier': 2.5
            },
            'orderflow': {
                'delta_threshold_extreme': 1000.0
            }
        }
    )
    print("=== SOLUSD Config (custom) ===")
    print(sol_config.to_json())

