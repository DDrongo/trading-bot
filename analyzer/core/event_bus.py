# core/event_bus.py
"""
🔌 Шина событий для связи между модулями
"""

import asyncio
import logging
from enum import Enum
from typing import Dict, List, Callable, Any
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger('event_bus')


class EventType(Enum):
    """Типы событий системы"""
    # Сессии
    SESSION_STARTED = "SESSION_STARTED"
    SESSION_COMPLETED = "SESSION_COMPLETED"

    # Сигналы
    TRADING_SIGNAL_GENERATED = "TRADING_SIGNAL_GENERATED"
    SIGNAL_EXPIRED = "SIGNAL_EXPIRED"
    SIGNAL_CANCELLED = "SIGNAL_CANCELLED"

    # Торговля
    TRADE_EXECUTED = "TRADE_EXECUTED"
    TRADE_CLOSED = "TRADE_CLOSED"
    POSITION_UPDATED = "POSITION_UPDATED"

    # Анализ
    LIQUIDITY_CHECK_PASSED = "LIQUIDITY_CHECK_PASSED"
    SCREEN1_ANALYSIS_COMPLETE = "SCREEN1_ANALYSIS_COMPLETE"
    SCREEN2_ANALYSIS_COMPLETE = "SCREEN2_ANALYSIS_COMPLETE"
    SCREEN3_ANALYSIS_COMPLETE = "SCREEN3_ANALYSIS_COMPLETE"

    # Системные
    ERROR_OCCURRED = "ERROR_OCCURRED"
    WARNING_OCCURRED = "WARNING_OCCURRED"
    SYSTEM_STARTED = "SYSTEM_STARTED"
    SYSTEM_STOPPED = "SYSTEM_STOPPED"

    # Данные
    MARKET_DATA_UPDATED = "MARKET_DATA_UPDATED"
    ORDER_BOOK_UPDATED = "ORDER_BOOK_UPDATED"


@dataclass
class Event:
    """Событие системы"""
    event_type: EventType
    data: Dict[str, Any]
    source: str
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        return {
            'event_type': self.event_type.value,
            'data': self.data,
            'source': self.source,
            'timestamp': self.timestamp.isoformat()
        }


class EventBus:
    """
    Паттерн Publisher/Subscriber для связи модулей
    Потокобезопасная реализация с асинхронной обработкой
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(EventBus, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._subscribers: Dict[EventType, List[Callable]] = {}
        self._event_queue = asyncio.Queue()
        self._is_running = False
        self._task = None
        self._initialized = True

        logger.info("✅ EventBus инициализирован")

    async def start(self):
        """Запуск обработчика событий"""
        if self._is_running:
            return

        self._is_running = True
        self._task = asyncio.create_task(self._event_processor())
        logger.info("🚀 EventBus запущен")

    async def stop(self):
        """Остановка обработчика событий"""
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 EventBus остановлен")

    async def _event_processor(self):
        """Асинхронный обработчик событий"""
        while self._is_running:
            try:
                event = await self._event_queue.get()

                # Получаем подписчиков для этого типа события
                subscribers = self._subscribers.get(event.event_type, [])

                if not subscribers:
                    logger.debug(f"Нет подписчиков для события {event.event_type.value}")
                    self._event_queue.task_done()
                    continue

                # Запускаем обработку у всех подписчиков
                tasks = []
                for callback in subscribers:
                    task = asyncio.create_task(self._safe_callback(callback, event))
                    tasks.append(task)

                # Ждем завершения всех обработчиков
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

                self._event_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в обработчике событий: {e}")

    async def _safe_callback(self, callback: Callable, event: Event):
        """Безопасный вызов callback с обработкой исключений"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(event)
            else:
                callback(event)
        except Exception as e:
            logger.error(f"Ошибка в обработчике события {event.event_type.value}: {e}")

    def subscribe(self, event_type: EventType, callback: Callable):
        """Подписка на события определенного типа"""
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []

        if callback not in self._subscribers[event_type]:
            self._subscribers[event_type].append(callback)
            logger.debug(f"Добавлен подписчик на {event_type.value}")

    def unsubscribe(self, event_type: EventType, callback: Callable):
        """Отписка от событий"""
        if event_type in self._subscribers and callback in self._subscribers[event_type]:
            self._subscribers[event_type].remove(callback)
            logger.debug(f"Удален подписчик с {event_type.value}")

    async def publish(self, event_type: EventType, data: Dict[str, Any], source: str):
        """Публикация события"""
        event = Event(event_type=event_type, data=data, source=source)

        # Логируем важные события
        if event_type in [EventType.ERROR_OCCURRED, EventType.TRADING_SIGNAL_GENERATED,
                          EventType.TRADE_EXECUTED]:
            logger.info(f"📢 {event_type.value}: {data.get('symbol', 'system')}")

        # Добавляем в очередь на обработку
        await self._event_queue.put(event)
        logger.debug(f"Опубликовано событие: {event_type.value} от {source}")

    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики шины событий"""
        return {
            'is_running': self._is_running,
            'queue_size': self._event_queue.qsize(),
            'subscribers_count': sum(len(subs) for subs in self._subscribers.values()),
            'event_types': [et.value for et in self._subscribers.keys()]
        }


# Глобальный экземпляр шины событий
event_bus = EventBus()

# Экспорт для импорта в другие модули
__all__ = ['EventType', 'Event', 'EventBus', 'event_bus']