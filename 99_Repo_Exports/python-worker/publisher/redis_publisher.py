import json
from typing import Dict, Any
from core.redis_client import get_redis

# Redis клиент будет инициализирован при первом использовании
redis_client = None

def _get_redis_client():
    """Lazy initialization of Redis client"""
    global redis_client
    if redis_client is None:
        redis_client = get_redis()
    return redis_client

def publish_to_redis(channel: str, message: Any) -> bool:
    """
    Публикует сообщение в указанный канал Redis
    
    Args:
        channel (str): Канал для публикации
        message (Any): Сообщение (будет сериализовано в JSON)
    
    Returns:
        bool: True, если публикация успешна, иначе False
    """
    try:
        # Если сообщение не строка, преобразуем его в JSON
        if not isinstance(message, str):
            message = json.dumps(message)
        
        # Публикуем сообщение
        recipients = _get_redis_client().publish(channel, message)
        
        print(f"📢 Опубликовано в Redis канал '{channel}': {message[:100]}... ({recipients} получателей)")
        return True
    except Exception as e:
        print(f"❌ Ошибка публикации в Redis канал '{channel}': {e}")
        return False 