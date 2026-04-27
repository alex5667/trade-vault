import sys
import threading
from abc import ABC, abstractmethod
from typing import Union


class BaseHandler(ABC):
    """
    Базовый класс для всех обработчиков сигналов
    """
    
    def __init__(self, name: str):
        """
        Инициализация базового обработчика
        
        Args:
            name: Имя обработчика для логирования
        """
        self.name = name
        self.is_running = False
        self.thread: Union[threading.Thread, None] = None
        
    def start(self) -> None:
        """Запускает обработчик в отдельном потоке"""
        if self.is_running:
            print(f"⚠️ {self.name} уже запущен")
            return
            
        self.is_running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        print(f"🚀 {self.name} запущен")
        sys.stdout.flush()
        
    def stop(self) -> None:
        """Останавливает обработчик"""
        if not self.is_running:
            print(f"⚠️ {self.name} уже остановлен")
            return
            
        self.is_running = False
        print(f"⛔ {self.name} остановлен")
        sys.stdout.flush()
        
        # Ждем завершения потока
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5.0)
    
    @abstractmethod
    def _run(self) -> None:
        """
        Основная функция обработчика (должна быть переопределена в наследниках)
        """
        pass
    
    def _log(self, message: str, level: str = "INFO") -> None:
        """
        Логирование с именем обработчика
        
        Args:
            message: Сообщение для логирования
            level: Уровень логирования (INFO, WARNING, ERROR)
        """
        emoji_map = {
            "INFO": "ℹ️",
            "WARNING": "⚠️", 
            "ERROR": "❌",
            "SUCCESS": "✅"
        }
        emoji = emoji_map.get(level, "📝")
        print(f"{emoji} {self.name}: {message}")
        sys.stdout.flush() 