"""
Configuration and input validation for handlers.

Extracted from BaseOrderFlowHandler to follow Single Responsibility Principle.
Provides validation for:
- Symbol names
- Configuration objects
- Infrastructure components
"""

import logging
from typing import Any, Optional


class InvalidSymbolError(Exception):
    """Raised when symbol is invalid."""
    pass


class MissingConfigError(Exception):
    """Raised when configuration is missing or invalid."""
    pass


class DependencyError(Exception):
    """Raised when required dependencies are unavailable."""
    pass


class ConfigValidator:
    """
    Validates handler configuration and inputs.
    
    Responsibilities:
    - Validate symbol names
    - Validate configuration objects
    - Validate infrastructure components
    - Check logical consistency
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize config validator.
        
        Args:
            logger: Optional logger for warnings
        """
        self.logger = logger or logging.getLogger(__name__)
    
    def validate_symbol(self, symbol: str) -> None:
        """
        Validate symbol name.
        
        Args:
            symbol: Trading symbol to validate
            
        Raises:
            InvalidSymbolError: If symbol is invalid
        """
        if not symbol or not isinstance(symbol, str) or len(symbol.strip()) == 0:
            raise InvalidSymbolError(
                f"Invalid symbol: '{symbol}'. Must be non-empty string."
            )
    
    def validate_source_name(self, source_name: str) -> None:
        """
        Validate source name.
        
        Args:
            source_name: Source name to validate
            
        Raises:
            ValueError: If source_name is invalid
        """
        if not source_name or not isinstance(source_name, str):
            raise ValueError(
                f"Invalid source_name: '{source_name}'. Must be non-empty string."
            )
    
    def validate_signal_stream_prefix(self, signal_stream_prefix: str) -> None:
        """
        Validate signal stream prefix.
        
        Args:
            signal_stream_prefix: Stream prefix to validate
            
        Raises:
            ValueError: If prefix is invalid
        """
        if not signal_stream_prefix or not isinstance(signal_stream_prefix, str):
            raise ValueError(
                f"Invalid signal_stream_prefix: '{signal_stream_prefix}'. "
                "Must be non-empty string."
            )
    
    def validate_config(self, config: Any) -> None:
        """
        Validate configuration object for logical consistency.
        
        Args:
            config: Configuration object to validate
            
        Raises:
            MissingConfigError: If required config attributes are missing
            ValueError: If config values are invalid
        """
        if not hasattr(config, 'symbol') or not config.symbol:
            raise MissingConfigError("Config must have valid 'symbol' attribute.")
        
        # Validate critical thresholds
        required_attrs = [
            'main_z_threshold'
            'breakout_z_threshold'
            'obi_threshold'
            'delta_bucket_ms'
        ]
        
        for attr in required_attrs:
            if hasattr(config, attr):
                value = getattr(config, attr)
                
                if attr.endswith('_threshold'):
                    if not isinstance(value, (int, float)) or value < 0:
                        raise ValueError(
                            f"Config.{attr} must be non-negative number, got: {value}"
                        )
                elif attr == 'delta_bucket_ms':
                    if not isinstance(value, int) or value <= 0:
                        raise ValueError(
                            f"Config.{attr} must be positive integer, got: {value}"
                        )
        
        # Validate logical consistency
        if hasattr(config, 'main_z_threshold') and hasattr(config, 'breakout_z_threshold'):
            main_z = getattr(config, 'main_z_threshold', 0)
            breakout_z = getattr(config, 'breakout_z_threshold', 0)
            
            if breakout_z <= main_z:
                self.logger.warning(
                    "Breakout Z threshold should be higher than main Z threshold: "
                    f"main={main_z}, breakout={breakout_z}"
                )
    
    def validate_inputs(
        self
        symbol: str
        config: Optional[Any]
        source_name: str
        signal_stream_prefix: str
    ) -> None:
        """
        Validate all handler inputs.
        
        Args:
            symbol: Trading symbol
            config: Optional configuration object
            source_name: Data source name
            signal_stream_prefix: Signal stream prefix
            
        Raises:
            InvalidSymbolError: If symbol is invalid
            ValueError: If other inputs are invalid
            MissingConfigError: If config is invalid
        """
        self.validate_symbol(symbol)
        self.validate_source_name(source_name)
        self.validate_signal_stream_prefix(signal_stream_prefix)
        
        if config is not None:
            self.validate_config(config)
    
    def validate_redis(self, redis: Any) -> None:
        """
        Validate Redis connection.
        
        Args:
            redis: Redis client instance
            
        Raises:
            DependencyError: If Redis is not initialized
        """
        if redis is None:
            raise DependencyError("Redis not initialized")
    
    def validate_stream(self, stream_name: str, stream_value: Any) -> None:
        """
        Validate stream configuration.
        
        Args:
            stream_name: Name of the stream (for error messages)
            stream_value: Stream value to validate
            
        Raises:
            DependencyError: If stream is not configured
        """
        if not stream_value:
            raise DependencyError(f"{stream_name} not configured")
    
    def validate_infrastructure(
        self
        redis: Any
        tick_stream: str
        book_stream: str
        cache_service: Any
    ) -> None:
        """
        Validate all infrastructure components.
        
        Args:
            redis: Redis client
            tick_stream: Tick stream name
            book_stream: Book stream name
            cache_service: Cache service instance
            
        Raises:
            DependencyError: If any component is missing
        """
        self.validate_redis(redis)
        self.validate_stream("tick_stream", tick_stream)
        self.validate_stream("book_stream", book_stream)
        
        if cache_service is None:
            raise DependencyError("CacheService not initialized")
