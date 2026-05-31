#!/usr/bin/env python3
"""
Исправленная версия multithreaded_worker.py с функциональностью подписки на каналы
И УЛУЧШЕНИЯМИ ДЛЯ ПРЕДОТВРАЩЕНИЯ "ЗАСЫПАНИЯ"
"""

import asyncio
import logging
import os
import signal
import sys
import time
import traceback
from typing import Dict, List, Optional, Tuple

from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneCodeInvalidError

# Загружаем переменные окружения из .env файла
from dotenv import load_dotenv
load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.config import load_settings
from app.channel_status import ChannelStatusChecker
from app.parse_utils import parse_signal
from app.channel_monitor import ChannelMonitor
from app.alert_system import AlertSystem
from channel_poller import ChannelPoller
import redis

def _normalize_channel_name(name: Optional[str]) -> str:
    """Нормализует имя канала для сопоставления: убирает @, пробелы, подчёркивания и приводит к нижнему регистру."""
    if not name:
        return ""
    return name.strip().lstrip('@').replace('_', '').replace(' ', '').lower()

class ChannelGroup:
    """Группа каналов для многопоточной обработки."""
    
    def __init__(self, group_id: int, channels: List[str]):
        self.id = group_id
        self.channels = channels
        self.status = "idle"
        self.thread_id = None
        self.message_count = 0
        self.last_activity = None
        self.error_count = 0

class MultithreadedTelegramWorker:
    """Многопоточный worker для обработки сообщений из Telegram каналов."""
    
    def __init__(self):
        """Инициализирует worker."""
        # Создаем logger для инициализации
        self.setup_logging()
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        self.settings = load_settings()
        self.redis = redis.Redis.from_url(self.settings.redis_url, decode_responses=True)
        self.status_checker = ChannelStatusChecker(self.redis, self.logger)
        self.channel_groups: List[ChannelGroup] = []
        # ⚡ OPTIMIZATION: Bounded queue prevents OOM under message flood
        # maxsize=1000 provides backpressure if consumer can't keep up
        self.message_queue = asyncio.Queue(maxsize=1000)
        self.running = False
        self.thread_pool = None
        
        # НОВЫЕ ПОЛЯ ДЛЯ ПРЕДОТВРАЩЕНИЯ "ЗАСЫПАНИЯ"
        self.last_message_time = time.time()
        self.last_health_check = time.time()
        self.connection_errors = 0
        self.max_connection_errors = 5
        self.health_check_interval = 20  # секунды (уменьшено с 30)
        self.keep_alive_interval = 30    # секунды (уменьшено с 60)
        self.force_updates_interval = 30 # принудительная проверка обновлений
        self.max_idle_time = 180         # 3 минуты без сообщений (уменьшено с 5)
        
        self.stats = {
            'total_messages': 0,
            'parsed_messages': 0,
            'errors': 0,
            'start_time': time.time(),
            'last_message_time': time.time(),
            'connection_status': 'disconnected',
            'health_checks': 0,
            'reconnections': 0
        }
        
        # Message logging counter - log only every N messages
        self.message_log_counter = 0
        self.MESSAGE_LOG_INTERVAL = 100  # Log every 100th message
        
        # Единый авторизованный клиент для всех потоков
        self.main_client: Optional[TelegramClient] = None
        
        # Channel Poller для active polling (events не работают для каналов!)
        self.channel_poller: Optional[ChannelPoller] = None
        self.channel_entities = []  # Сохраняем entities для polling
        
        # Логирование уже настроено выше
        
        # Инициализация мониторинга каналов (после logger)
        self.channel_monitor = ChannelMonitor(self.redis, self.logger)
        
        # Инициализация системы алертов
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_ids = os.getenv('TELEGRAM_NOTIFY_CHAT_IDS', '').split(',')
        chat_ids = [cid.strip() for cid in chat_ids if cid.strip()]
        
        self.alert_system = AlertSystem(
            redis_client=self.redis,
            logger=self.logger,
            bot_token=bot_token,
            chat_ids=chat_ids
        )
        
        # Обработчики сигналов
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def setup_logging(self):
        """Настраивает логирование."""
        # Disable console logging completely, only log to file
        file_handler = logging.FileHandler('multithreaded_worker.log')
        file_handler.setLevel(logging.INFO)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
            handlers=[file_handler]
        )
        
        # Suppress noisy telethon connection logs ("Attempt 1 at connecting failed...")
        logging.getLogger('telethon').setLevel(logging.WARNING)
        
        # self.logger уже создан в __init__
    
    def _signal_handler(self, signum, frame):
        """Обработчик сигналов для корректного завершения."""
        self.logger.info(f"Получен сигнал {signum}, завершаем работу...")
        self.stop()
    
    async def initialize_main_client(self):
        """Инициализирует и авторизует основной клиент."""
        try:
            print("🔌 [DEBUG] Начало initialize_main_client...")
            sys.stdout.flush()
            self.logger.info("🔐 Инициализация основного Telegram клиента...")
            
            # Получаем настройки из переменных окружения
            api_id = os.getenv('TG_API_ID')
            api_hash = os.getenv('TG_API_HASH')
            session_name = os.getenv('TG_SESSION')
            sessions_dir = os.getenv('SESSIONS_DIR', './sessions')
            
            print(f"📝 [DEBUG] API ID: {api_id}, Session: {session_name}, Dir: {sessions_dir}")
            sys.stdout.flush()
            
            if not all([api_id, api_hash, session_name]):
                error_msg = "❌ Отсутствуют обязательные переменные окружения для Telegram"
                print(error_msg)
                sys.stdout.flush()
                self.logger.error(error_msg)
                return False
            
            # Создаем путь к файлу сессии (Telethon автоматически добавит .session)
            session_path = os.path.join(sessions_dir, session_name)
            
            print(f"📁 [DEBUG] Путь к сессии: {session_path}")
            sys.stdout.flush()
            self.logger.info(f"📁 Используем сессию: {session_path}")
            
            # Проверяем существование файла сессии
            print(f"🔍 [DEBUG] Проверяем файл: {session_path}.session")
            sys.stdout.flush()
            
            if not os.path.exists(session_path + '.session'):
                error_msg = f"❌ Файл сессии не найден: {session_path}.session"
                print(error_msg)
                sys.stdout.flush()
                self.logger.error(error_msg)
                return False
            
            print(f"✅ [DEBUG] Файл сессии найден!")
            sys.stdout.flush()
            
            # Создаем клиент с правильными настройками для контейнера
            print("🔨 [DEBUG] Создаем TelegramClient...")
            sys.stdout.flush()
            self.main_client = TelegramClient(
                session_path,
                int(api_id),
                api_hash,
                device_model="Scanner Infra Worker",
                system_version="Linux",
                app_version="1.0.0",
                lang_code="en",
                # Настройки для работы в контейнере
                connection_retries=3,
                retry_delay=1,
                timeout=30,
                # Отключаем интерактивный режим
                request_retries=3
            )
            
            # Подключаемся к Telegram БЕЗ интерактивного ввода с timeout
            try:
                print("🔌 [DEBUG] Подключаемся к Telegram...")
                sys.stdout.flush()
                
                await asyncio.wait_for(
                    self.main_client.connect(),
                    timeout=30.0
                )
                print("✅ [DEBUG] Подключение установлено!")
                sys.stdout.flush()
                self.logger.info("✅ Подключение к Telegram установлено")
                
                # Проверяем подключение
                if not self.main_client.is_connected():
                    error_msg = "❌ Не удалось подключиться к Telegram"
                    print(error_msg)
                    sys.stdout.flush()
                    self.logger.error(error_msg)
                    return False
                
                print("🔐 [DEBUG] Проверяем авторизацию...")
                sys.stdout.flush()
                
                # Проверяем авторизацию с timeout
                is_authorized = await asyncio.wait_for(
                    self.main_client.is_user_authorized(),
                    timeout=10.0
                )
                
                if not is_authorized:
                    error_msg = "❌ Сессия не авторизована. Необходимо создать новую сессию через setup-telegram-session.py"
                    print(error_msg)
                    sys.stdout.flush()
                    self.logger.error(error_msg)
                    return False
                
                print("✅ [DEBUG] Клиент авторизован!")
                sys.stdout.flush()
                self.logger.info("✅ Telegram клиент авторизован")
                
            except asyncio.TimeoutError as timeout_error:
                error_msg = f"⏰ Timeout при подключении к Telegram: {timeout_error}"
                print(error_msg)
                sys.stdout.flush()
                self.logger.error(error_msg)
                return False
            except Exception as start_error:
                error_msg = f"❌ Ошибка при подключении клиента: {start_error}"
                print(error_msg)
                sys.stdout.flush()
                self.logger.error(error_msg)
                return False
            
            print("✅ [DEBUG] Успешно подключились к Telegram")
            sys.stdout.flush()
            self.logger.info("✅ Успешно подключились к Telegram")
            self.stats['connection_status'] = 'connected'
            
            # НЕ регистрируем обработчик здесь - сделаем это ПОСЛЕ подписки на каналы
            # чтобы знать точный список каналов для мониторинга
            
            return True
            
        except FloodWaitError as e:
            wait_time = e.seconds
            self.logger.warning(f"⚠️ Flood wait: ждем {wait_time} секунд")
            await asyncio.sleep(wait_time)
            return await self.initialize_main_client()
            
        except SessionPasswordNeededError:
            self.logger.error("❌ Требуется пароль двухфакторной аутентификации")
            return False
            
        except PhoneCodeInvalidError:
            self.logger.error("❌ Неверный код подтверждения телефона")
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации клиента: {e}")
            self.connection_errors += 1
            return False
    
    async def subscribe_to_channels(self) -> List[str]:
        """Подписывается на активные каналы."""
        try:
            self.logger.info("📡 Подписка на активные каналы...")
            
            # Получаем список активных каналов
            self.logger.info("🔍 Получаем список активных каналов...")
            active_channels = self.status_checker.get_active_channels()
            self.logger.info(f"✅ Получено {len(active_channels)} активных каналов: {active_channels[:5]}...")
            
            if not active_channels:
                self.logger.warning("⚠️ Нет активных каналов для подписки")
                return []
            
            subscribed_channels = []
            
            # 🎯 SENIOR DEV: Track subscription attempts
            print(f"\n{'='*80}")
            print(f"📡 [SUBSCRIPTION] Начинаем подписку на {len(active_channels)} активных каналов")
            print(f"{'='*80}\n")
            sys.stdout.flush()
            
            for idx, channel in enumerate(active_channels, 1):
                try:
                    # Убираем @ если есть
                    channel_name = channel.lstrip('@')
                    
                    # Progress indicator
                    print(f"[{idx}/{len(active_channels)}] Подписка на @{channel_name}...", end=' ')
                    sys.stdout.flush()
                    
                    # Пытаемся присоединиться к каналу
                    try:
                        await self.main_client(JoinChannelRequest(channel_name))
                        subscribed_channels.append(channel)
                        print(f"✅")
                        sys.stdout.flush()
                        self.logger.info(f"✅ Присоединились к каналу: @{channel_name}")
                        
                    except FloodWaitError as e:
                        wait_time = e.seconds
                        print(f"⏳ FloodWait({wait_time}s)")
                        sys.stdout.flush()
                        self.logger.info(f"⏳ FloodWait {wait_time}s для @{channel_name}")
                        await asyncio.sleep(wait_time)
                        
                        # Повторная попытка после ожидания
                        try:
                            await self.main_client(JoinChannelRequest(channel_name))
                            subscribed_channels.append(channel)
                            print(f"✅ (retry)")
                            sys.stdout.flush()
                            self.logger.info(f"✅ Присоединились к @{channel_name} после FloodWait")
                        except Exception as retry_e:
                            print(f"❌ {type(retry_e).__name__}")
                            sys.stdout.flush()
                            self.logger.warning(f"❌ Не удалось присоединиться к @{channel_name} после FloodWait: {retry_e}")
                            
                    except Exception as e:
                        print(f"❌ {type(e).__name__}")
                        sys.stdout.flush()
                        self.logger.warning(f"❌ Не удалось присоединиться к @{channel_name}: {e}")
                        
                    # Небольшая задержка между запросами
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    print(f"❌ OUTER: {type(e).__name__}")
                    sys.stdout.flush()
                    self.logger.error(f"❌ Ошибка при подписке на {channel}: {e}")
                    continue
            
            # 🎯 SENIOR DEV: Summary report
            print(f"\n{'='*80}")
            print(f"📊 [SUBSCRIPTION REPORT]")
            print(f"{'='*80}")
            print(f"  Активных каналов из Redis: {len(active_channels)}")
            print(f"  Успешно подписано: {len(subscribed_channels)}")
            print(f"  Не подписано: {len(active_channels) - len(subscribed_channels)}")
            print(f"  Success rate: {len(subscribed_channels)/len(active_channels)*100:.1f}%")
            print(f"{'='*80}\n")
            sys.stdout.flush()
            
            self.logger.info(f"✅ Успешно подписано на {len(subscribed_channels)}/{len(active_channels)} каналов")
            
            # Инициализируем каналы в мониторинге
            for channel in subscribed_channels:
                self.channel_monitor.add_channel(channel)
            
            # Загружаем статистику каналов из Redis
            self.channel_monitor.load_channel_stats()
            
            # ВАЖНО: Регистрируем обработчик событий ПОСЛЕ подписки на каналы
            print(f"📡 [DEBUG] Регистрируем обработчик событий...")
            sys.stdout.flush()
            
            # Получаем entity для каждого подписанного канала
            # Senior Dev approach: with timeout, rate limiting, and proper error handling
            channel_entities = []
            print(f"📡 [DEBUG] Получаем entities для {len(subscribed_channels)} каналов...")
            sys.stdout.flush()
            
            failed_entities = []
            for idx, channel in enumerate(subscribed_channels):
                try:
                    channel_name = channel.lstrip('@')
                    
                    # 🎯 SENIOR DEV: Exponential backoff retry with circuit breaker pattern
                    max_retries = 3
                    base_timeout = 30.0  # ⬆️ Increased from 10s to 30s
                    
                    for attempt in range(max_retries):
                        try:
                            # Calculate timeout with exponential backoff
                            timeout = base_timeout * (1.5 ** attempt)
                            
                            entity = await asyncio.wait_for(
                                self.main_client.get_entity(channel_name),
                                timeout=timeout
                            )
                            channel_entities.append(entity)
                            
                            # 📊 Progress reporting every 10 channels
                            if len(channel_entities) % 10 == 0:
                                progress_pct = (len(channel_entities) / len(subscribed_channels)) * 100
                                print(f"   ... получено {len(channel_entities)}/{len(subscribed_channels)} entities ({progress_pct:.0f}%)")
                                sys.stdout.flush()
                            
                            # ✅ Success - break retry loop
                            break
                            
                        except asyncio.TimeoutError:
                            if attempt < max_retries - 1:
                                wait = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                                self.logger.warning(f"⏰ Timeout для {channel} (попытка {attempt + 1}/{max_retries}), ждем {wait}s")
                                await asyncio.sleep(wait)
                            else:
                                self.logger.error(f"❌ Все {max_retries} попытки исчерпаны для {channel}")
                                failed_entities.append((channel, f"timeout_after_{max_retries}_retries"))
                                
                        except FloodWaitError as e:
                            wait_time = min(e.seconds, 300)  # Cap at 5 minutes
                            self.logger.warning(f"🛑 FloodWaitError для {channel}: ждем {wait_time}s (попытка {attempt + 1}/{max_retries})")
                            await asyncio.sleep(wait_time)
                            # Continue to next retry iteration
                            
                        except Exception as e:
                            if attempt < max_retries - 1:
                                self.logger.warning(f"⚠️ Ошибка для {channel}: {type(e).__name__} (попытка {attempt + 1}/{max_retries})")
                                await asyncio.sleep(2 ** attempt)
                            else:
                                self.logger.error(f"❌ Финальная ошибка для {channel}: {e}")
                                failed_entities.append((channel, f"{type(e).__name__}_{str(e)[:50]}"))
                    
                    # 🎯 SENIOR DEV: Adaptive rate limiting based on success/failure
                    if idx < len(subscribed_channels) - 1:
                        # Adaptive delay: increase if we have failures
                        failure_rate = len(failed_entities) / (idx + 1) if (idx + 1) > 0 else 0
                        base_delay = 0.3  # Increased from 200ms to 300ms
                        adaptive_delay = base_delay * (1 + failure_rate)
                        await asyncio.sleep(min(adaptive_delay, 2.0))  # Cap at 2s
                            
                except ValueError as ve:
                    # Channel not found or invalid
                    self.logger.warning(f"⚠️ Невалидный канал {channel}: {ve}")
                    failed_entities.append((channel, "invalid"))
                except Exception as e:
                    # Catch-all for unexpected errors
                    self.logger.error(f"❌ Неожиданная ошибка для {channel}: {type(e).__name__}: {e}")
                    failed_entities.append((channel, str(e)))
            
            print(f"✅ [DEBUG] Получено {len(channel_entities)} channel entities")
            sys.stdout.flush()
            
            # 🎯 SENIOR DEV: Enhanced metrics and monitoring
            success_count = len(channel_entities)
            failure_count = len(failed_entities)
            total_count = len(subscribed_channels)
            success_rate = (success_count / total_count * 100) if total_count > 0 else 0
            
            # Report failed entities for investigation
            if failed_entities:
                print(f"⚠️ [DEBUG] Не удалось получить {failure_count} entities ({100-success_rate:.1f}%):")
                for channel, reason in failed_entities[:10]:  # Show first 10
                    print(f"   - {channel}: {reason}")
                if len(failed_entities) > 10:
                    print(f"   ... и еще {len(failed_entities) - 10}")
                sys.stdout.flush()
                
                # 🎯 SENIOR DEV: Circuit Breaker - track failures per channel
                try:
                    pipeline = self.redis.pipeline()
                    
                    # Save failed entities with timestamp and reason
                    for channel, reason in failed_entities:
                        failure_key = f"telegram:channel:{channel}:failures"
                        pipeline.lpush(failure_key, f"{int(time.time())}:{reason}")
                        pipeline.ltrim(failure_key, 0, 9)  # Keep last 10 failures
                        pipeline.expire(failure_key, 86400)  # 24 hours TTL
                        
                        # Increment failure counter
                        counter_key = f"telegram:channel:{channel}:failure_count"
                        pipeline.incr(counter_key)
                        pipeline.expire(counter_key, 3600)  # 1 hour TTL
                    
                    # Save global metrics
                    metrics_key = f"telegram:subscription:metrics:{int(time.time())}"
                    pipeline.hset(metrics_key, mapping={
                        'total': total_count,
                        'success': success_count,
                        'failure': failure_count,
                        'success_rate': f"{success_rate:.2f}",
                        'timestamp': int(time.time())
                    })
                    pipeline.expire(metrics_key, 86400 * 7)  # Keep for 7 days
                    
                    pipeline.execute()
                    self.logger.info(f"📊 Метрики подписки сохранены в Redis")
                    
                except Exception as redis_e:
                    self.logger.warning(f"⚠️ Не удалось сохранить метрики в Redis: {redis_e}")
            
            # !!!! CRITICAL FIX !!!!
            # Event handlers НЕ РАБОТАЮТ для telegram каналов!
            # Используем ACTIVE POLLING вместо events
            if channel_entities:
                # Success rate calculation (Senior Dev: always measure)
                success_rate = (len(channel_entities) / len(subscribed_channels)) * 100 if subscribed_channels else 0
                print(f"📊 [DEBUG] Success rate: {success_rate:.1f}% ({len(channel_entities)}/{len(subscribed_channels)})")
                sys.stdout.flush()
                
                # Сохраняем entities для использования в polling
                self.channel_entities = channel_entities
                
                # Инициализируем Channel Poller для ACTIVE POLLING
                print(f"🔄 [DEBUG] Запуск ACTIVE POLLING для {len(channel_entities)} каналов...")
                sys.stdout.flush()
                
                self.channel_poller = ChannelPoller(self.main_client, self.logger)
                self.channel_poller.set_message_callback(self._handle_all_messages)
                
                print(f"✅ [DEBUG] Channel Poller инициализирован для {len(channel_entities)} каналов")
                print(f"🔄 [DEBUG] Polling interval: {self.channel_poller.poll_interval}s")
                self.logger.info(f"Channel Poller initialized for {len(channel_entities)} channels")
                sys.stdout.flush()
            else:
                # Критическая ситуация
                self.logger.error("❌ CRITICAL: Не удалось получить ни одного entity!")
                print(f"❌ [DEBUG] CRITICAL: Нет entities для polling!")
                sys.stdout.flush()
            
            # Отправляем алерт о запуске системы
            self.alert_system.alert_system_startup(len(subscribed_channels))
            return subscribed_channels
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка подписки на каналы: {e}")
            return []
    
    def calculate_optimal_threads(self, channel_count: int) -> Tuple[int, int]:
        """Вычисляет оптимальное количество потоков и каналов на поток."""
        if channel_count <= 10:
            return 1, channel_count
        elif channel_count <= 20:
            return 2, (channel_count + 1) // 2
        elif channel_count <= 40:
            return 4, (channel_count + 3) // 4
        elif channel_count <= 60:
            return 6, (channel_count + 5) // 6
        else:
            return 8, (channel_count + 7) // 8
    
    def create_channel_groups(self, channels: List[str]) -> List[ChannelGroup]:
        """Создает группы каналов для многопоточной обработки."""
        channel_count = len(channels)
        optimal_threads, channels_per_thread = self.calculate_optimal_threads(channel_count)
        
        self.logger.info(f"Создаем {optimal_threads} потоков по {channels_per_thread} каналов")
        
        groups = []
        for i in range(optimal_threads):
            start_idx = i * channels_per_thread
            end_idx = min(start_idx + channels_per_thread, channel_count)
            group_channels = channels[start_idx:end_idx]
            
            group = ChannelGroup(i, group_channels)
            groups.append(group)
            
            self.logger.info(f"Группа {i}: {len(group_channels)} каналов")
        
        return groups
    
    async def _handle_all_messages(self, ev: events.NewMessage.Event):
        """Обрабатывает все сообщения и определяет группу."""
        try:
            msg = ev.message
            chat = await ev.get_chat()
            chat_id = getattr(chat, 'id', None)
            chat_title = getattr(chat, 'title', '') or getattr(chat, 'username', '') or ''
            username = f"@{getattr(chat, 'username', '')}" if getattr(chat, 'username', '') else None
            
            # Определяем группу для этого канала
            group = self._find_group_for_channel(username or chat_title)
            if not group:
                # Если группа не найдена, создаем временную для обработки
                group = ChannelGroup(group_id=-1, channels=[username or chat_title])
                self.logger.warning(f"⚠️ Канал {username or chat_title} не найден в группах, создана временная группа.")
            
            text = msg.message or msg.raw_text or ""
            
            # Increment message counter
            self.message_log_counter += 1
            
            # Silent processing - log only to file
            if self.message_log_counter % self.MESSAGE_LOG_INTERVAL == 0:
                self.logger.info(f"📨 Сообщение #{self.message_log_counter} от {username or chat_title}, всего: {self.stats['total_messages']}")
            
            # Обновляем мониторинг канала
            channel_name = username or chat_title
            if channel_name:
                self.channel_monitor.update_channel_activity(channel_name, msg.date.timestamp())
            
            # Обновляем статистику группы и время последнего сообщения
            group.message_count += 1
            group.last_activity = time.time()
            self.stats['total_messages'] += 1
            self.last_message_time = time.time()
            self.stats['last_message_time'] = self.last_message_time
            
            # Отправляем в очередь для обработки в основном потоке
            message_data = {
                "chat_id": str(chat_id) if chat_id is not None else "",
                "chat_title": str(chat_title) if chat_title else "",
                "username": str(username) if username else "",
                "msg_id": str(msg.id) if msg.id is not None else "",
                "timestamp": str(int(msg.date.timestamp() * 1000)) if msg.date else "",
                "text": str(text) if text else "",
                "group_id": str(group.id) if group.id is not None else ""
            }
            
            await self.message_queue.put(message_data)  # ✅ ASYNC put
            
        except Exception as e:
            print(f"\n❌ [ERROR] _handle_all_messages failed: {e}")
            import traceback
            traceback.print_exc()
            sys.stdout.flush()
            self.stats['errors'] += 1
            self.logger.error(f"Ошибка обработки сообщения: {e}")
    
    def _find_group_for_channel(self, channel_identifier: str) -> Optional[ChannelGroup]:
        """Находит группу для указанного канала."""
        ident_norm = _normalize_channel_name(channel_identifier)
        if not ident_norm:
            return None
        for group in self.channel_groups:
            for configured in group.channels:
                if _normalize_channel_name(configured) == ident_norm:
                    return group
        return None
    
    # НОВЫЕ МЕТОДЫ ДЛЯ ПРЕДОТВРАЩЕНИЯ "ЗАСЫПАНИЯ"
    
    async def health_check(self):
        """Проверяет здоровье соединения и worker."""
        while self.running:
            try:
                current_time = time.time()
                
                # Проверяем соединение с Telegram
                if self.main_client and not self.main_client.is_connected():
                    print(f"\n⚠️ ПОТЕРЯНО СОЕДИНЕНИЕ! Переподключаемся...")
                    sys.stdout.flush()
                    self.logger.warning("⚠️ Потеряно соединение с Telegram, пытаемся переподключиться...")
                    await self.reconnect()
                
                # Проверяем время последнего сообщения
                time_since_last_message = current_time - self.last_message_time
                if time_since_last_message > self.max_idle_time:
                    print(f"\n⚠️ ДОЛГО НЕТ СООБЩЕНИЙ ({time_since_last_message:.0f}s), проверяем соединение...")
                    sys.stdout.flush()
                    self.logger.warning(f"⚠️ Долго нет сообщений ({time_since_last_message:.0f}s), проверяем соединение...")
                    await self.check_connection_health()
                
                # Обновляем статистику
                self.stats['health_checks'] += 1
                self.last_health_check = current_time
                
                # Логируем статус каждые 5 проверок (чаще, т.к. интервал уменьшен)
                if self.stats['health_checks'] % 5 == 0:
                    uptime = current_time - self.stats['start_time']
                    idle_time = current_time - self.last_message_time
                    status_msg = (f"💓 Health check #{self.stats['health_checks']}: "
                                 f"uptime={uptime/60:.1f}m, "
                                 f"idle={idle_time:.0f}s, "
                                 f"соединение={self.stats['connection_status']}, "
                                 f"сообщений={self.stats['total_messages']}, "
                                 f"ошибок={self.stats['errors']}")
                    print(f"\n{status_msg}")
                    sys.stdout.flush()
                    self.logger.info(status_msg)
                
                await asyncio.sleep(self.health_check_interval)
                
            except Exception as e:
                print(f"❌ Ошибка health check: {e}")
                sys.stdout.flush()
                self.logger.error(f"❌ Ошибка health check: {e}")
                await asyncio.sleep(self.health_check_interval)
    
    async def keep_alive(self):
        """Отправляет keep-alive сигналы для поддержания соединения."""
        ping_count = 0
        while self.running:
            try:
                if self.main_client and self.main_client.is_connected():
                    # Отправляем ping для поддержания соединения
                    try:
                        start = time.time()
                        me = await self.main_client.get_me()
                        elapsed = (time.time() - start) * 1000
                        
                        if me:
                            ping_count += 1
                            # Логируем каждый 5-й пинг
                            if ping_count % 5 == 0:
                                print(f"💓 Keep-alive ping #{ping_count} OK ({elapsed:.0f}ms)")
                                sys.stdout.flush()
                            self.logger.debug(f"💓 Keep-alive ping #{ping_count} успешен ({elapsed:.0f}ms)")
                    except Exception as e:
                        print(f"⚠️ Keep-alive ping #{ping_count} FAILED: {e}")
                        sys.stdout.flush()
                        self.logger.warning(f"⚠️ Keep-alive ping не удался: {e}")
                        await self.check_connection_health()
                else:
                    print("⚠️ Keep-alive: клиент не подключен")
                    sys.stdout.flush()
                    self.logger.warning("⚠️ Keep-alive: клиент не подключен")
                
                await asyncio.sleep(self.keep_alive_interval)
                
            except Exception as e:
                print(f"❌ Ошибка keep-alive: {e}")
                sys.stdout.flush()
                self.logger.error(f"❌ Ошибка keep-alive: {e}")
                await asyncio.sleep(self.keep_alive_interval)
    
    async def check_connection_health(self):
        """Проверяет и восстанавливает здоровье соединения."""
        try:
            if not self.main_client:
                self.logger.error("❌ Клиент не инициализирован")
                return False
            
            # Проверяем базовое соединение
            if not self.main_client.is_connected():
                self.logger.warning("⚠️ Клиент отключен, пытаемся переподключиться...")
                return await self.reconnect()
            
            # Проверяем авторизацию
            try:
                me = await self.main_client.get_me()
                if not me:
                    self.logger.warning("⚠️ Пользователь не авторизован, переподключаемся...")
                    return await self.reconnect()
                
                self.stats['connection_status'] = 'connected'
                self.connection_errors = 0
                return True
                
            except Exception as e:
                self.logger.warning(f"⚠️ Ошибка проверки авторизации: {e}")
                return await self.reconnect()
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки здоровья соединения: {e}")
            return False
    
    async def reconnect(self):
        """Переподключается к Telegram."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                retry_count += 1
                print(f"\n🔄 ПЕРЕПОДКЛЮЧЕНИЕ К TELEGRAM (попытка {retry_count}/{max_retries})...")
                sys.stdout.flush()
                self.logger.info(f"🔄 Попытка переподключения к Telegram ({retry_count}/{max_retries})...")
                self.stats['connection_status'] = 'reconnecting'
                
                # Закрываем старое соединение
                if self.main_client:
                    try:
                        await self.main_client.disconnect()
                        print("✅ Старое соединение закрыто")
                        sys.stdout.flush()
                    except Exception as e:
                        print(f"⚠️ Ошибка закрытия старого соединения: {e}")
                        sys.stdout.flush()
                
                # Пауза перед повторной попыткой
                if retry_count > 1:
                    wait_time = retry_count * 2
                    print(f"⏳ Ожидание {wait_time}s перед повторной попыткой...")
                    sys.stdout.flush()
                    await asyncio.sleep(wait_time)
                
                # Инициализируем заново
                print("🔨 Инициализация нового клиента...")
                sys.stdout.flush()
                if await self.initialize_main_client():
                    # Переподписываемся на каналы
                    print("📡 Переподписка на каналы...")
                    sys.stdout.flush()
                    active_channels = await self.subscribe_to_channels()
                    if active_channels:
                        self.channel_groups = self.create_channel_groups(active_channels)
                        print(f"✅ ПЕРЕПОДКЛЮЧЕНИЕ УСПЕШНО! Каналов: {len(active_channels)}")
                        sys.stdout.flush()
                        self.logger.info(f"✅ Переподключение успешно (попытка {retry_count})")
                        self.stats['reconnections'] += 1
                        self.stats['connection_status'] = 'connected'
                        return True
                
                print(f"❌ Попытка {retry_count} не удалась")
                sys.stdout.flush()
                
            except Exception as e:
                print(f"❌ Ошибка при попытке {retry_count}: {e}")
                sys.stdout.flush()
                self.logger.error(f"❌ Ошибка переподключения (попытка {retry_count}): {e}")
        
        print(f"❌ ВСЕ {max_retries} ПОПЫТОК ПЕРЕПОДКЛЮЧЕНИЯ НЕ УДАЛИСЬ!")
        sys.stdout.flush()
        self.logger.error("❌ Все попытки переподключения не удались")
        self.stats['connection_status'] = 'failed'
        return False
    
    async def monitor_connection(self):
        """Мониторит качество соединения."""
        while self.running:
            try:
                if self.main_client and self.main_client.is_connected():
                    # Проверяем качество соединения
                    try:
                        start_time = time.time()
                        me = await self.main_client.get_me()
                        response_time = (time.time() - start_time) * 1000
                        
                        if response_time > 5000:  # больше 5 секунд
                            self.logger.warning(f"⚠️ Медленный ответ от Telegram: {response_time:.0f}ms")
                        
                    except Exception as e:
                        self.logger.warning(f"⚠️ Проблема с соединением: {e}")
                        await self.check_connection_health()
                
                await asyncio.sleep(60)  # проверяем каждую минуту
                
            except Exception as e:
                self.logger.error(f"❌ Ошибка мониторинга соединения: {e}")
                await asyncio.sleep(60)
    

    async def process_message_queue(self):
        """Обрабатывает очередь сообщений в основном потоке."""
        self.logger.info("MessageQueue processor started")
        message_count = 0
        while self.running:
            try:
                try:
                    message_data = await asyncio.wait_for(
                        self.message_queue.get(),
                        timeout=1.0
                    )
                    message_count += 1
                except asyncio.TimeoutError:
                    continue
                await self._process_message(message_data)
            except Exception as e:
                print(f"\n\u274c [QUEUE ERROR] {e}")
                traceback.print_exc()
                sys.stdout.flush()
                self.logger.error(f"Ошибка обработки очереди сообщений: {e}")
                self.stats['errors'] += 1
    
    async def _process_message(self, message_data: Dict):
        """Обрабатывает сообщение в основном потоке."""
        print(f"🔍 [DEBUG] _process_message started")
        sys.stdout.flush()
        
        try:
            print(f"🔍 [DEBUG] Creating raw dict...")
            sys.stdout.flush()
            
            # 1) Записываем сырое сообщение
            raw = {
                "chat_id": message_data["chat_id"],
                "chat_title": message_data["chat_title"],
                "username": message_data["username"],
                "msg_id": message_data["msg_id"],
                "timestamp": message_data["timestamp"],
                "text": message_data["text"],
                "thread_group": message_data["group_id"]
            }
            
            print(f"🔍 [DEBUG] Raw dict created, writing to Redis...")
            sys.stdout.flush()
            
            # Все поля в raw уже strings, но на всякий случай проверим
            try:
                msg_id = self.redis.xadd(self.settings.raw_stream, raw)
                print(f"\n📝 СООБЩЕНИЕ СОХРАНЕНО В REDIS")
                print(f"   Stream: {self.settings.raw_stream}")
                print(f"   Message ID: {msg_id}")
                print(f"   Канал: {raw['username']}")
                print(f"   Длина: {len(raw['text'])} символов\n")
                sys.stdout.flush()
            except Exception as e:
                print(f"❌ [REDIS ERROR] Failed to save raw: {e}")
                print(f"❌ [DEBUG] Raw data types: {[(k, type(v).__name__, v) for k, v in raw.items()]}")
                sys.stdout.flush()
                self.logger.error(f"❌ Ошибка записи raw в Redis: {e}")
                # Конвертируем все в strings принудительно
                raw_safe = {k: str(v) if v is not None else "" for k, v in raw.items()}
                msg_id = self.redis.xadd(self.settings.raw_stream, raw_safe)
                print(f"✅ [REDIS] Raw message saved (после конвертации), ID: {msg_id}")
                sys.stdout.flush()
            
            # 2) Парсим сигнал используя parse_signal
            text_to_parse = message_data["text"]
            print(f"\n✅ [PARSER ENABLED] Парсим сообщение...")
            sys.stdout.flush()
            
            # Используем реальный парсер
            self.logger.info(f"DEBUG: Пытаемся распарсить текст: {text_to_parse[:200]}...")
            parsed = parse_signal(text_to_parse)
            self.logger.info(f"DEBUG: Результат парсинга: {parsed}")
            
            parsed.update({
                "chat_id": message_data["chat_id"],
                "chat_title": message_data["chat_title"],
                "username": message_data["username"],
                "channel": (message_data["chat_title"] or (message_data["username"] or "")),
                "msg_id": message_data["msg_id"],
                "timestamp": message_data["timestamp"],
                "thread_group": message_data["group_id"]
            })
            
            # Проверяем обязательные поля (более гибкая проверка)
            has_direction = bool(parsed.get("direction"))
            has_entry = (parsed.get("entry") is not None)
            has_symbol = bool(parsed.get("symbol"))
            
            self.logger.info(f"DEBUG: has_direction={has_direction}, has_symbol={has_symbol}, has_entry={has_entry}")
            
            # ✅ ПАРСЕР ВКЛЮЧЕН - отправляем только валидные сигналы
            # Проверяем наличие direction и symbol
            if has_direction and has_symbol:  # Отправляем только валидные сигналы
                # ЛОГИРУЕМ ОТПРАВКУ В БОТ
                print(f"\n{'='*80}")
                print(f"📤 ОТПРАВКА В БОТ (ВАЛИДНЫЙ СИГНАЛ)")
                print(f"{'='*80}")
                print(f"📍 Канал: {message_data.get('username') or message_data.get('chat_title')}")
                print(f"📝 Длина текста: {len(message_data.get('text', ''))} символов")
                print(f"⏰ Время: {message_data.get('timestamp')}")
                print(f"{'='*80}")
                print(f"🤖 Сообщение будет отправлено в Telegram бот")
                print(f"{'='*80}\n")
                sys.stdout.flush()
                
                self.logger.info(f"DEBUG: Сигнал считается валидным, публикуем в Redis.")
                
                # Конвертируем все значения в strings для Redis (Senior Dev Fix)
                parsed_for_redis = {}
                for key, value in parsed.items():
                    if value is None:
                        parsed_for_redis[key] = ""
                    elif isinstance(value, bool):
                        parsed_for_redis[key] = "true" if value else "false"
                    elif isinstance(value, (list, dict)):
                        parsed_for_redis[key] = str(value)
                    else:
                        parsed_for_redis[key] = str(value)
                
                # Записываем в parsed stream
                self.redis.xadd(self.settings.parsed_stream, parsed_for_redis)
                
                # Отправляем алерт об успешном парсинге
                channel_name = message_data.get('username') or message_data.get('chat_title')
                if channel_name:
                    self.alert_system.send_alert(
                        message=f"Успешно распарсен сигнал {parsed.get('symbol')} {parsed.get('direction')} от {channel_name}",
                        alert_type="success",
                        channel=channel_name,
                        data={"symbol": parsed.get('symbol'), "direction": parsed.get('direction')},
                        send_telegram=False  # Не спамим в Telegram
                    )
                
                # ✅ ОТПРАВЛЯЕМ РАСПАРСЕННЫЙ СИГНАЛ В БОТ
                notify_data = {
                    'type': 'trading_signal',
                    'channel': str(message_data.get('username') or message_data.get('chat_title') or 'Unknown Channel'),
                    'text': str(message_data.get('text', '')),
                    'timestamp': str(message_data.get('timestamp', int(time.time() * 1000))),
                    'msg_id': str(message_data.get('msg_id', '')),
                    'parsed': 'true',  # ✅ УСПЕШНО РАСПАРСЕН
                    # Реальные данные из парсера
                    'symbol': str(parsed.get('symbol') or ''),
                    'direction': str(parsed.get('direction') or ''),
                    'entry': str(parsed.get('entry') or ''),
                    'stop': str(parsed.get('stop') or ''),
                    'tp': str(parsed.get('tp', [])),
                    'leverage': str(parsed.get('leverage') or ''),
                    'confidence': str(parsed.get('confidence', 0)),
                    'source': str(message_data.get('username') or message_data.get('chat_title') or 'Unknown Channel'),
                    'orderType': str(parsed.get('orderType') or ''),
                    'profitPct': str(parsed.get('profitPct') or ''),
                    'exchange': str(parsed.get('exchange') or ''),
                    'timeframe': str(parsed.get('timeframe') or '')
                }
                
                self.redis.xadd('notify:telegram', notify_data)
                
                # ЛОГИРУЕМ ОТПРАВКУ В БОТ
                print(f"\n{'='*80}")
                print(f"🤖 ОТПРАВЛЕНО В БОТ (notify:telegram) - TRADING SIGNAL")
                print(f"{'='*80}")
                print(f"📍 Канал: {notify_data['channel']}")
                print(f"📝 Текст: {notify_data['text'][:100]}...")
                print(f"⏰ Timestamp: {notify_data['timestamp']}")
                print(f"🔖 Type: {notify_data['type']}")
                print(f"✅ Парсинг: УСПЕШЕН (parsed='true')")
                print(f"🎯 Symbol: {notify_data['symbol']}, Direction: {notify_data['direction']}")
                print(f"💰 Entry: {notify_data['entry']}, Stop: {notify_data['stop']}")
                print(f"🎯 TP: {notify_data['tp']}")
                print(f"⚡ Leverage: {notify_data['leverage']}x, Confidence: {notify_data['confidence']}")
                print(f"{'='*80}\n")
                sys.stdout.flush()
                
                self.stats['parsed_messages'] += 1
                self.logger.info(f"✅ Сигнал от {message_data['username']} успешно распарсен: {parsed.get('symbol')} {parsed.get('direction')}")
            else:
                # ЛОГИРУЕМ НЕУСПЕШНЫЙ ПАРСИНГ
                print(f"\n{'='*80}")
                print(f"⚠️ ПАРСИНГ НЕ УДАЛСЯ")
                print(f"{'='*80}")
                print(f"Канал: {message_data.get('username') or message_data.get('chat_title')}")
                print(f"has_direction: {has_direction}")
                print(f"has_symbol: {has_symbol}")
                print(f"has_entry: {has_entry}")
                print(f"Parsed data: {parsed}")
                print(f"{'='*80}\n")
                sys.stdout.flush()
                
                self.logger.debug(f"Сообщение от {message_data['username']} не является валидным сигналом (отсутствуют direction или symbol)")
                
                # Отправляем алерт о невалидном сигнале
                channel_name = message_data.get('username') or message_data.get('chat_title')
                if channel_name:
                    self.alert_system.send_alert(
                        message=f"Невалидный сигнал от {channel_name}: отсутствуют direction или symbol",
                        alert_type="warning",
                        channel=channel_name,
                        data={"text": text_to_parse[:200]},
                        send_telegram=False
                    )
                
        except Exception as e:
            print(f"\n❌ [ERROR] Ошибка обработки сообщения: {e}")
            import traceback
            traceback.print_exc()
            sys.stdout.flush()
            self.logger.error(f"Ошибка обработки сообщения: {e}")
            self.stats['errors'] += 1
    
    async def start(self):
        """
        Запускает worker с правильной архитектурой для long-running service.
        
        Senior Dev Approach:
        - Structured concurrency с asyncio.gather()
        - Proper lifecycle management
        - Graceful shutdown
        - Individual error handling для каждой задачи
        """
        try:
            print("🚀 [SENIOR] Запуск telegram-worker с enterprise архитектурой")
            sys.stdout.flush()
            self.logger.info("🚀 Запуск многопоточного telegram-worker")
            
            # ============================================================
            # ФАЗА 1: ИНИЦИАЛИЗАЦИЯ
            # ============================================================
            print("🔐 [INIT] Инициализация Telegram клиента...")
            sys.stdout.flush()
            
            if not await self.initialize_main_client():
                self.logger.error("❌ Ошибка инициализации клиента")
                return False
            
            print("✅ [INIT] Telegram клиент готов")
            sys.stdout.flush()
            
            # ============================================================
            # ФАЗА 2: ПОДПИСКА НА КАНАЛЫ
            # ============================================================
            print("📡 [INIT] Подписка на каналы...")
            sys.stdout.flush()
            
            self.running = True
            active_channels = await self.subscribe_to_channels()
            while not active_channels and self.running:
                self.logger.warning("⚠️ Нет активных каналов, ожидание 60 секунд перед повторной проверкой...")
                print("⚠️ Нет активных каналов, ожидание 60 секунд...")
                sys.stdout.flush()
                await asyncio.sleep(60)
                active_channels = await self.subscribe_to_channels()
                
            if not self.running:
                return False
            
            self.channel_groups = self.create_channel_groups(active_channels)
            self.running = True
            
            print(f"✅ [INIT] Подписано на {len(active_channels)} каналов")
            sys.stdout.flush()
            
            # ============================================================
            # ФАЗА 3: ЗАПУСК BACKGROUND TASKS (TRUE CONCURRENCY)
            # ============================================================
            print("\n" + "="*80)
            print("🚀 [LAUNCH] Запуск background tasks (параллельно)...")
            print("="*80 + "\n")
            sys.stdout.flush()
            
            # Senior Dev Critical Fix: ВСЕ задачи должны быть обернуты в _wrap_task!
            # Это предотвращает молчаливые падения и блокировку event loop
            tasks = []
            task_names = []
            
            # 1. Message Queue Processor
            task = asyncio.create_task(
                self._wrap_task(self.process_message_queue(), "MessageQueueProcessor")
            )
            tasks.append(task)
            task_names.append("MessageQueueProcessor")
            
            # 2. Health Check
            try:
                task = asyncio.create_task(
                    self._wrap_task(self.health_check(), "HealthCheck")
                )
                tasks.append(task)
                task_names.append("HealthCheck")
                print(f"✅ [TASK] HealthCheck создан")
                sys.stdout.flush()
            except Exception as e:
                print(f"❌ [TASK] HealthCheck failed to create: {e}")
                import traceback
                traceback.print_exc()
                sys.stdout.flush()
            
            # 3. Keep Alive
            try:
                task = asyncio.create_task(
                    self._wrap_task(self.keep_alive(), "KeepAlive")
                )
                tasks.append(task)
                task_names.append("KeepAlive")
                print(f"✅ [TASK] KeepAlive создан")
                sys.stdout.flush()
            except Exception as e:
                print(f"❌ [TASK] KeepAlive failed to create: {e}")
                import traceback
                traceback.print_exc()
                sys.stdout.flush()
            
            # 4. Connection Monitor
            try:
                task = asyncio.create_task(
                    self._wrap_task(self.monitor_connection(), "ConnectionMonitor")
                )
                tasks.append(task)
                task_names.append("ConnectionMonitor")
                print(f"✅ [TASK] ConnectionMonitor создан")
                sys.stdout.flush()
            except Exception as e:
                print(f"❌ [TASK] ConnectionMonitor failed to create: {e}")
                import traceback
                traceback.print_exc()
                sys.stdout.flush()
            
            # 5. Channel Monitor
            try:
                task = asyncio.create_task(
                    self._wrap_task(self.monitor_channels(), "ChannelMonitor")
                )
                tasks.append(task)
                task_names.append("ChannelMonitor")
                print(f"✅ [TASK] ChannelMonitor создан")
                sys.stdout.flush()
            except Exception as e:
                print(f"❌ [TASK] ChannelMonitor failed to create: {e}")
                import traceback
                traceback.print_exc()
                sys.stdout.flush()
            
            # 6. КРИТИЧЕСКИ ВАЖНО: Channel Poller (ACTIVE POLLING)
            if self.channel_poller and self.channel_entities:
                print(f"🔄 [LAUNCH] Добавляем Channel Poller для {len(self.channel_entities)} каналов")
                sys.stdout.flush()
                # CRITICAL FIX: Оборачиваем в _wrap_task для error handling
                task = asyncio.create_task(
                    self._wrap_task(
                        self.channel_poller.poll_all_channels(self.channel_entities),
                        "ChannelPoller"
                    )
                )
                tasks.append(task)
                task_names.append("ChannelPoller")
                print(f"✅ [TASK] ChannelPoller запущен с error handling")
                sys.stdout.flush()
            else:
                print(f"⚠️ [WARNING] Channel Poller не добавлен!")
                sys.stdout.flush()
            
            # 7. Main Loop (heartbeat)
            try:
                task = asyncio.create_task(
                    self._wrap_task(self._main_loop(), "MainLoop")
                )
                tasks.append(task)
                task_names.append("MainLoop")
                print(f"✅ [TASK] MainLoop создан")
                sys.stdout.flush()
            except Exception as e:
                print(f"❌ [TASK] MainLoop failed to create: {e}")
                import traceback
                traceback.print_exc()
                sys.stdout.flush()
            
            print(f"✅ [LAUNCH] Создано {len(tasks)} параллельных задач")
            print("="*80 + "\n")
            sys.stdout.flush()
            
            # CRITICAL: Даем event loop запустить все задачи НЕМЕДЛЕННО!
            # await asyncio.sleep(0) передает control event loop
            print("⚡ [DEBUG] Передаем control event loop для запуска задач...")
            sys.stdout.flush()
            
            print("⚡ [DEBUG] Выполняю await asyncio.sleep(0)...")
            sys.stdout.flush()
            await asyncio.sleep(0)
            
            print("⚡ [DEBUG] asyncio.sleep(0) завершён")
            sys.stdout.flush()
            
            # Ждем секунду, чтобы задачи успели начать выполнение
            print("⚡ [DEBUG] Выполняю await asyncio.sleep(1)...")
            sys.stdout.flush()
            await asyncio.sleep(1)
            
            print("⚡ [DEBUG] asyncio.sleep(1) завершён")
            sys.stdout.flush()
            
            # ============================================================
            # ФАЗА 4: ВЫПОЛНЕНИЕ (KEEP ALIVE FOREVER)
            # ============================================================
            # Senior Dev Critical Fix: НЕ используем gather() для бесконечных задач!
            # Просто держим программу живой - задачи уже работают в background
            print("\n" + "="*80)
            print("🏃 [RUNNING] Worker активен - все задачи работают параллельно!")
            print("💤 [INFO] Главный поток переходит в режим ожидания...")
            print("="*80 + "\n")
            sys.stdout.flush()
            
            # CRITICAL FIX: Используем КОРОТКИЙ sleep для частого переключения контекста!
            # Event loop должен часто переключаться между задачами
            try:
                while self.running:
                    await asyncio.sleep(1)  # короткий sleep для частого переключения
            except KeyboardInterrupt:
                self.logger.info("🛑 Получен сигнал остановки")
            finally:
                print("\n" + "="*80)
                print("🛑 [SHUTDOWN] Получен сигнал завершения...")
                print("="*80)
                sys.stdout.flush()
                self.running = False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка в start(): {e}", exc_info=True)
            return False
    
    async def _wrap_task(self, coro, task_name: str):
        """
        Обертка для задачи с error handling и logging.
        
        Senior Dev Pattern: каждая задача должна:
        1. Логировать свой запуск НЕМЕДЛЕННО (до await)
        2. Обрабатывать свои исключения
        3. Не падать молча
        4. Логировать завершение
        
        CRITICAL FIX: Логирование ДО await, чтобы не блокировать другие задачи!
        """
        # Логируем СРАЗУ, не дожидаясь выполнения корутины
        print(f"✅ [TASK] {task_name} запущен")
        sys.stdout.flush()
        self.logger.info(f"✅ Task '{task_name}' started")
        
        try:
            await coro
            
            print(f"🏁 [TASK] {task_name} завершен")
            sys.stdout.flush()
            self.logger.info(f"🏁 Task '{task_name}' completed")
            
        except asyncio.CancelledError:
            print(f"🛑 [TASK] {task_name} отменен")
            sys.stdout.flush()
            self.logger.info(f"🛑 Task '{task_name}' cancelled")
            raise  # re-raise для правильной обработки
            
        except Exception as e:
            print(f"❌ [TASK] {task_name} упал с ошибкой: {e}")
            import traceback
            traceback.print_exc()
            sys.stdout.flush()
            self.logger.error(f"❌ Task '{task_name}' failed: {e}", exc_info=True)
            raise  # re-raise для visibility
    
    async def _main_loop(self):
        """
        Главный цикл - heartbeat для поддержания работы.
        """
        heartbeat_count = 0
        while self.running:
            await asyncio.sleep(10)  # heartbeat каждые 10 секунд
            
            heartbeat_count += 1
            if heartbeat_count % 6 == 0:  # каждую минуту
                uptime = time.time() - self.stats['start_time']
                print(f"💓 [HEARTBEAT] Worker alive: uptime={uptime/60:.1f}m, messages={self.stats['total_messages']}")
                sys.stdout.flush()
            
            # Проверяем соединение
            if self.main_client and not self.main_client.is_connected():
                self.logger.warning("⚠️ Соединение потеряно в main loop")
                try:
                    await self.main_client.connect()
                    self.logger.info("✅ Переподключено")
                except Exception as e:
                    self.logger.error(f"❌ Ошибка переподключения: {e}")
                    await asyncio.sleep(5)
    
    async def monitor_channels(self):
        """Мониторинг активности каналов."""
        while self.running:
            try:
                # Проверяем неактивные каналы
                inactive_channels = self.channel_monitor.get_inactive_channels()
                
                for channel_name in inactive_channels:
                    channel = self.channel_monitor.channels.get(channel_name)
                    if channel and channel.is_active:
                        # Канал стал неактивным
                        inactive_hours = (time.time() - channel.last_message_time) / 3600
                        self.alert_system.alert_channel_inactive(channel_name, inactive_hours)
                        channel.is_active = False
                
                # Очищаем старые алерты
                self.alert_system.cleanup_old_alerts(days=7)
                
                await asyncio.sleep(300)  # Проверяем каждые 5 минут
                
            except Exception as e:
                self.logger.error(f"❌ Ошибка в мониторинге каналов: {e}")
                await asyncio.sleep(60)

    def stop(self):
        """Останавливает worker."""
        self.logger.info("🛑 Остановка worker...")
        self.running = False
        
        if self.main_client and self.main_client.is_connected():
            try:
                # Просто закрываем соединение без создания задачи
                self.main_client.disconnect()
            except Exception as e:
                self.logger.error(f"Ошибка при отключении клиента: {e}")
        
        self.logger.info("✅ Worker остановлен")

async def main():
    """Основная функция."""
    worker = MultithreadedTelegramWorker()
    
    try:
        await worker.start()
    except KeyboardInterrupt:
        worker.logger.info("Получен сигнал прерывания")
    finally:
        worker.stop()

if __name__ == "__main__":
    asyncio.run(main()) 