#!/usr/bin/env python3
"""
Pairs Data Handler
Обработчик данных новых торговых пар от Binance
"""

import json
import sys
from typing import Callable, List, Dict


class PairsDataHandler:
    """
    Обработчик данных новых торговых пар от Binance
    """
    
    def __init__(self, ws_callback: Callable[[list], None]):
        """
        Инициализация обработчика новых пар
        
        Args:
            ws_callback: Функция обратного вызова для обновления WebSocket подключений
        """
        self.ws_callback = ws_callback
    
    def handle_new_pairs_data(self, data) -> None:
        """
        Обрабатывает данные о новых торговых парах
        
        Args:
            data: Данные о новых парах (может быть список или строка)
        """
        try:
            new_pairs = self._extract_pairs_data(data)
            
            if not new_pairs:
                print("⚠️ Нет данных новых пар для обработки")
                return
                
            print(f"🔗 PairsHandler: Получены новые пары: {len(new_pairs)} шт.")
            sys.stdout.flush()
            
            # Фильтруем только USDT пары
            filtered_pairs = self._filter_usdt_pairs(new_pairs)
            
            if filtered_pairs:
                print(f"📡 PairsHandler: Отправка {len(filtered_pairs)} отфильтрованных пар для WebSocket")
                sys.stdout.flush()
                self.ws_callback(filtered_pairs)
            else:
                print("⚠️ PairsHandler: Нет подходящих USDT пар для подключения")
                
        except Exception as e:
            print(f"❌ PairsHandler: Ошибка обработки новых пар: {e}")
            sys.stdout.flush()
    
    def _extract_pairs_data(self, data) -> List[str]:
        """
        Извлекает данные новых пар из различных форматов
        
        Args:
            data: Данные в различных форматах
            
        Returns:
            List[str]: Список новых пар
        """
        new_pairs = []
        
        if isinstance(data, list):
            # Если data уже список новых пар
            new_pairs = data
        elif isinstance(data, str):
            # Если data строка JSON
            new_pairs = json.loads(data)
        elif isinstance(data, dict):
            # Если data словарь, ищем ключ 'pairs' или используем весь словарь
            new_pairs = data.get('pairs', [data] if data else [])
        
        return new_pairs
    
    def _filter_usdt_pairs(self, pairs: List[str]) -> List[str]:
        """
        Фильтрует только USDT пары
        
        Args:
            pairs: Список всех пар
            
        Returns:
            List[str]: Список отфильтрованных USDT пар
        """
        filtered_pairs = []
        
        for pair in pairs:
            if self._validate_pair_symbol(pair):
                filtered_pairs.append(pair)
        
        return filtered_pairs
    
    def _validate_pair_symbol(self, symbol: str) -> bool:
        """
        Валидирует символ торговой пары
        
        Args:
            symbol: Символ торговой пары
            
        Returns:
            bool: True если символ валиден
        """
        if not isinstance(symbol, str):
            return False
        
        # Проверяем, что символ заканчивается на USDT
        if not symbol.endswith('USDT'):
            return False
        
        # Исключаем UP/DOWN токены
        if 'UP' in symbol or 'DOWN' in symbol:
            return False
        
        # Проверяем минимальную длину
        if len(symbol) < 5:
            return False
        
        return True
    
    def validate_pairs_data(self, data: List[str]) -> bool:
        """
        Валидирует данные новых пар
        
        Args:
            data: Список новых пар
            
        Returns:
            bool: True если данные валидны
        """
        if not isinstance(data, list):
            return False
        
        # Проверяем, что все элементы - строки
        for pair in data:
            if not isinstance(pair, str):
                return False
        
        return True
    
    def get_unique_pairs(self, pairs: List[str]) -> List[str]:
        """
        Получает уникальные пары из списка
        
        Args:
            pairs: Список пар
            
        Returns:
            List[str]: Список уникальных пар
        """
        return list(set(pairs))
    
    def sort_pairs_by_volume(self, pairs: List[str], volume_data: Dict[str, float]) -> List[str]:
        """
        Сортирует пары по объему торгов
        
        Args:
            pairs: Список пар
            volume_data: Данные об объемах торгов
            
        Returns:
            List[str]: Отсортированный список пар
        """
        def get_volume(pair):
            return volume_data.get(pair, 0.0)
        
        return sorted(pairs, key=get_volume, reverse=True)
    
    def filter_active_pairs(self, pairs: List[str], min_volume: float = 1000000) -> List[str]:
        """
        Фильтрует активные пары по минимальному объему
        
        Args:
            pairs: Список пар
            min_volume: Минимальный объем торгов
            
        Returns:
            List[str]: Список активных пар
        """
        # Здесь можно добавить логику фильтрации по объему
        # Пока возвращаем все пары
        return pairs
    
    def get_pairs_summary(self, pairs: List[str]) -> Dict:
        """
        Получает сводку по парам
        
        Args:
            pairs: Список пар
            
        Returns:
            Dict: Сводка с информацией о парах
        """
        usdt_pairs = [p for p in pairs if p.endswith('USDT')]
        btc_pairs = [p for p in pairs if p.endswith('BTC')]
        eth_pairs = [p for p in pairs if p.endswith('ETH')]
        other_pairs = [p for p in pairs if not p.endswith(('USDT', 'BTC', 'ETH'))]
        
        return {
            'total': len(pairs),
            'usdt': len(usdt_pairs),
            'btc': len(btc_pairs),
            'eth': len(eth_pairs),
            'other': len(other_pairs)
        }
    
    def format_pairs_for_display(self, pairs: List[str], max_display: int = 10) -> str:
        """
        Форматирует пары для отображения
        
        Args:
            pairs: Список пар
            max_display: Максимальное количество для отображения
            
        Returns:
            str: Отформатированная строка
        """
        if not pairs:
            return "нет пар"
        
        if len(pairs) <= max_display:
            return ", ".join(pairs)
        else:
            displayed = pairs[:max_display]
            remaining = len(pairs) - max_display
            return f"{', '.join(displayed)} и еще {remaining} пар" 