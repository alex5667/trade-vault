import json
import sys
from typing import List, Set
from .redis_client import get_redis
from .redis_keys import RedisStreams as RS


class WebSocketManager:
    """
    Менеджер для управления WebSocket подключениями
    """
    
    def __init__(self):
        """Инициализация менеджера WebSocket"""
        self.connected_ws_pairs: Set[str] = set()
        self.redis_client = get_redis()
    
    def update_ws_connections(self, symbols: List[str]) -> None:
        """
        Отправляет запрос в Go для подписки на новые WebSocket соединения
        
        Args:
            symbols: Список символов торговых пар для подключения
        """
        if not symbols:
            return
        
        # Преобразуем символы в нижний регистр для единообразия
        symbols = [s.lower() for s in symbols]
        
        # Отсеиваем символы, которые уже подключены
        new_symbols = [s for s in symbols if s not in self.connected_ws_pairs]
        if not new_symbols:
            print(f"⚠️ Все символы уже подключены: {symbols}")
            sys.stdout.flush()
            return
        
        # Добавляем новые символы в список подключенных
        for s in new_symbols:
            self.connected_ws_pairs.add(s)
        
        try:
            # Формируем сообщение для Go - используем простой массив строк
            message = json.dumps(new_symbols)
            
            # Выводим отладочную информацию
            print(f"🚀 Отправка запроса на WS: {message}")
            sys.stdout.flush()
            
            # Отправляем запрос в Redis
            recipients = self.redis_client.publish(RS.WS_NEW_PAIRS, message)
            print(f"📡 Отправлен запрос на новые WS [{recipients} получателей]: {new_symbols}")
            sys.stdout.flush()
            
            # Проверяем результат публикации
            if recipients == 0:
                print("⚠️ Нет получателей для ws:new_pairs. Проверьте, запущен ли Go-воркер")
                sys.stdout.flush()
                
        except Exception as e:
            print(f"❌ Ошибка отправки запроса на подписку: {e}")
            sys.stdout.flush()
            # Удаляем символы из списка подключенных при ошибке
            for s in new_symbols:
                self.connected_ws_pairs.discard(s)
    
    def get_connected_pairs(self) -> Set[str]:
        """
        Возвращает множество подключенных торговых пар
        
        Returns:
            Set[str]: Множество символов подключенных пар
        """
        return self.connected_ws_pairs.copy()
    
    def clear_connections(self) -> None:
        """Очищает список подключенных соединений"""
        self.connected_ws_pairs.clear()
        print("🗑️ Список подключенных WebSocket соединений очищен")
        sys.stdout.flush() 