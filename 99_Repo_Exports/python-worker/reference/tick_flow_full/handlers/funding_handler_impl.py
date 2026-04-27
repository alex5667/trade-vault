#!/usr/bin/env python3
"""
Funding Data Handler
Обработчик данных funding rates от Binance
"""

import json
import sys
from typing import List, Dict, Any


class FundingDataHandler:
    """
    Обработчик данных funding rates от Binance
    """
    
    def __init__(self, redis_client):
        """
        Инициализация обработчика funding rates
        
        Args:
            redis_client: Клиент Redis
        """
        self.redis_client = redis_client
    
    def handle_funding_stream_data(self, data) -> None:
        """
        Обрабатывает данные funding rates из стрима
        
        Args:
            data: Данные funding rates (может быть список или строка)
        """
        try:
            funding_data = self._extract_funding_data(data)
            
            if not funding_data:
                print("⚠️ Нет данных funding rates для обработки")
                return
                
            # Закомментировано для уменьшения шума в логах
            # print(f"💰 FundingHandler: Получены funding rates: {len(funding_data)} записей")
            # sys.stdout.flush()
            
            # Сохраняем данные в Redis
            self._save_funding_data(funding_data)
            
        except Exception as e:
            print(f"❌ FundingHandler: Ошибка обработки funding rates: {e}")
            sys.stdout.flush()
    
    def _extract_funding_data(self, data) -> List[Dict]:
        """
        Извлекает данные funding rates из различных форматов
        
        Args:
            data: Данные в различных форматах
            
        Returns:
            List[Dict]: Список данных funding rates
        """
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
        """
        Сохраняет данные funding rates в Redis
        
        Args:
            funding_data: Список данных funding rates
        """
        try:
            for funding in funding_data:
                symbol = self._extract_symbol_from_funding(funding)
                
                if symbol:
                    key = f"binance:fundingRate:{symbol}"
                    value = json.dumps(funding)
                    self.redis_client.setex(key, 3600, value)  # TTL 1 час
                    
        except Exception as e:
            print(f"❌ FundingHandler: Ошибка сохранения funding rates: {e}")
            sys.stdout.flush()
    
    def _extract_symbol_from_funding(self, funding) -> str:
        """
        Извлекает символ из данных funding rate
        
        Args:
            funding: Данные funding rate
            
        Returns:
            str: Символ или пустая строка
        """
        symbol = ''
        
        if isinstance(funding, dict):
            symbol = funding.get('symbol', '')
        elif isinstance(funding, str):
            try:
                funding_dict = json.loads(funding)
                symbol = funding_dict.get('symbol', '')
            except json.JSONDecodeError:
                pass
        
        return symbol
    
    def validate_funding_data(self, data: Dict) -> bool:
        """
        Валидирует данные funding rate
        
        Args:
            data: Данные funding rate
            
        Returns:
            bool: True если данные валидны
        """
        if not isinstance(data, dict):
            return False
        
        # Проверяем обязательные поля
        required_fields = ['symbol']
        for field in required_fields:
            if field not in data:
                return False
        
        return True
    
    def get_funding_info(self, symbol: str) -> Dict:
        """
        Получает информацию о funding rate из Redis
        
        Args:
            symbol: Символ торговой пары
            
        Returns:
            Dict: Данные funding rate или пустой словарь
        """
        try:
            key = f"binance:fundingRate:{symbol}"
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