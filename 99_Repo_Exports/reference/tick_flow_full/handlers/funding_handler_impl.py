#!/usr/bin/env python3
"""Funding Data Handler.

Extended for P0 derivatives context:
- keeps legacy raw funding keys untouched (`binance:fundingRate:<SYMBOL>`)
- additionally writes a minimal partial normalized payload under
  `ctx:deriv_source:funding:<SYMBOL>` so the collector can merge stream-fed
  funding with REST-polled basis / OI without parsing heterogeneous payloads in
  multiple places.
"""

import json
import sys
import time
from typing import List, Dict, Any

from services.orderflow.derivatives_context import partial_funding_payload_from_exchange


class FundingDataHandler:
    """Funding-rate handler for Binance stream payloads."""

    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.partial_prefix = "ctx:deriv_source:funding:"
        self.partial_ttl_s = 3600

    def handle_funding_stream_data(self, data) -> None:
        try:
            funding_data = self._extract_funding_data(data)
            if not funding_data:
                print("⚠️ Нет данных funding rates для обработки")
                return
            self._save_funding_data(funding_data)
        except Exception as e:
            print(f"❌ FundingHandler: Ошибка обработки funding rates: {e}")
            sys.stdout.flush()

    def _extract_funding_data(self, data) -> List[Dict]:
        funding_data = []
        if isinstance(data, list):
            # Если data уже список funding rates
            funding_data = data
        elif isinstance(data, str):
            # Если data строка JSON
            funding_data = json.loads(data)
        elif isinstance(data, dict):
            # Если data словарь, ищем ключ 'funding_rates' или используем весь словарь
            funding_data = data.get('funding_rates', [data] if data else [])
        return funding_data

    def _save_funding_data(self, funding_data: List[Dict]) -> None:
        try:
            now_ms = int(time.time() * 1000)
            for funding in funding_data:
                symbol = self._extract_symbol_from_funding(funding)
                if not symbol:
                    continue
                # Legacy raw key used by existing tools/reporters.
                key = f"binance:fundingRate:{symbol}"
                value = json.dumps(funding)
                self.redis_client.setex(key, 3600, value)

                # New partial normalized source for P0 derivatives context collector.
                try:
                    partial = partial_funding_payload_from_exchange(funding, venue="binance", now_ms=now_ms)
                    if partial.get("symbol"):
                        pkey = f"{self.partial_prefix}{symbol}"
                        self.redis_client.setex(pkey, int(self.partial_ttl_s), json.dumps(partial, ensure_ascii=False))
                except Exception:
                    # Fail-open: collector can still use public REST polling.
                    pass
        except Exception as e:
            print(f"❌ FundingHandler: Ошибка сохранения funding rates: {e}")
            sys.stdout.flush()

    def _extract_symbol_from_funding(self, funding) -> str:
        symbol = ''
        if isinstance(funding, dict):
            symbol = funding.get('symbol', '')
        elif isinstance(funding, str):
            try:
                funding_dict = json.loads(funding)
                symbol = funding_dict.get('symbol', '')
            except json.JSONDecodeError:
                pass
        return str(symbol or '').upper()

    def validate_funding_data(self, data: Dict) -> bool:
        if not isinstance(data, dict):
            return False
        # Проверяем обязательные поля
        required_fields = ['symbol']
        for field in required_fields:
            if field not in data:
                return False
        return True

    def get_funding_info(self, symbol: str) -> Dict:
        try:
            key = f"binance:fundingRate:{str(symbol).upper()}"
            data = self.redis_client.get(key)
            if data:
                return json.loads(data)
            else:
                return {}
        except Exception as e:
            print(f"❌ FundingHandler: Ошибка получения данных funding rate для {symbol}: {e}")
            return {}

    def get_all_funding_rates(self) -> List[Dict]:
        """
        Получает все funding rates из Redis
        
        Returns:
            List[Dict]: Список всех funding rates
        """
        try:
            pattern = "binance:fundingRate:*"
            keys = self.redis_client.keys(pattern)
            
            funding_rates = []
            for key in keys:
                data = self.redis_client.get(key)
                if data:
                    funding_rates.append(json.loads(data))
            
            return funding_rates
            
        except Exception as e:
            print(f"❌ FundingHandler: Ошибка получения всех funding rates: {e}")
            return []
    
    def get_high_funding_rates(self, threshold: float = 0.01) -> List[Dict]:
        """
        Получает funding rates выше порогового значения
        
        Args:
            threshold: Пороговое значение (по умолчанию 1%)
            
        Returns:
            List[Dict]: Список funding rates выше порога
        """
        try:
            all_funding = self.get_all_funding_rates()
            high_funding = []
            
            for funding in all_funding:
                rate = funding.get('lastFundingRate', 0)
                try:
                    rate_float = float(rate)
                    if abs(rate_float) > threshold:
                        high_funding.append(funding)
                except (ValueError, TypeError):
                    continue
            
            return high_funding
            
        except Exception as e:
            print(f"❌ FundingHandler: Ошибка получения высоких funding rates: {e}")
            return []
    
    def get_funding_rate_summary(self) -> Dict:
        """
        Получает сводку по funding rates
        
        Returns:
            Dict: Сводка с количеством положительных/отрицательных rates
        """
        try:
            all_funding = self.get_all_funding_rates()
            
            positive_count = 0
            negative_count = 0
            total_count = len(all_funding)
            
            for funding in all_funding:
                rate = funding.get('lastFundingRate', 0)
                try:
                    rate_float = float(rate)
                    if rate_float > 0:
                        positive_count += 1
                    elif rate_float < 0:
                        negative_count += 1
                except (ValueError, TypeError):
                    continue
            
            return {
                'total': total_count,
                'positive': positive_count,
                'negative': negative_count,
                'neutral': total_count - positive_count - negative_count
            }
            
        except Exception as e:
            print(f"❌ FundingHandler: Ошибка получения сводки funding rates: {e}")
            return {'total': 0, 'positive': 0, 'negative': 0, 'neutral': 0} 