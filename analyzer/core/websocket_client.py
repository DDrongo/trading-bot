# analyzer/core/websocket_client.py (НОВЫЙ ФАЙЛ)
"""
🔌 WEBSOCKET КЛИЕНТ ДЛЯ BYBIT
ФАЗА 1.3.6: Реальное время для раннего входа
"""

import asyncio
import json
import logging
import websockets
from typing import List, Dict, Optional, Callable, Any
from datetime import datetime

logger = logging.getLogger('websocket_client')


class BybitWebSocketClient:
    """
    WebSocket клиент для Bybit V5 API
    Поддержка:
    - Подписка на ticker для нескольких символов
    - Автоматическое переподключение при разрыве
    - Обработка обновлений цены через колбэки
    """

    def __init__(self, symbols: List[str] = None):
        """
        Инициализация WebSocket клиента

        Args:
            symbols: Список символов для подписки (опционально)
        """
        self.symbols = symbols or []
        self.callbacks: List[Callable] = []
        self.websocket = None
        self.running = False
        self.reconnect_task = None
        self.ping_task = None

        # Настройки
        self.url = "wss://stream.bybit.com/v5/public/linear"
        self.reconnect_interval = 5  # секунд между попытками
        self.ping_interval = 20  # секунд между ping
        self.ping_timeout = 10  # секунд ожидания pong

        logger.info(f"✅ BybitWebSocketClient создан (символов: {len(self.symbols)})")

    def add_symbols(self, symbols: List[str]):
        """Добавить символы для подписки"""
        new_symbols = [s for s in symbols if s not in self.symbols]
        if new_symbols:
            self.symbols.extend(new_symbols)
            logger.info(f"➕ Добавлены символы: {new_symbols}")
            # Если уже подключены, обновляем подписку
            if self.running and self.websocket:
                asyncio.create_task(self._subscribe_new(new_symbols))

    def remove_symbols(self, symbols: List[str]):
        """Удалить символы из подписки"""
        for symbol in symbols:
            if symbol in self.symbols:
                self.symbols.remove(symbol)
        logger.info(f"➖ Удалены символы: {symbols}")
        # Если уже подключены, обновляем подписку
        if self.running and self.websocket:
            asyncio.create_task(self._unsubscribe(symbols))

    def on_price_update(self, callback: Callable):
        """
        Регистрация обработчика обновлений цены

        Args:
            callback: Асинхронная функция с сигнатурой async def callback(symbol: str, price: float)
        """
        self.callbacks.append(callback)
        logger.info(f"➕ Зарегистрирован обработчик цены (всего: {len(self.callbacks)})")

    async def connect(self):
        """Подключение к WebSocket и запуск прослушивания"""
        logger.info("🔌 Подключение к WebSocket Bybit...")

        self.running = True

        while self.running:
            try:
                self.websocket = await websockets.connect(
                    self.url,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout
                )
                logger.info("✅ WebSocket подключен")

                # Подписываемся на символы
                await self._subscribe_all()

                # Запускаем прослушивание
                await self._listen()

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"⚠️ WebSocket разорван: {e}")
                await self._reconnect()

            except Exception as e:
                logger.error(f"❌ Ошибка WebSocket: {e}")
                await self._reconnect()

    async def _subscribe_all(self):
        """Подписка на все символы"""
        if not self.symbols:
            logger.info("⚠️ Нет символов для подписки")
            return

        for symbol in self.symbols:
            await self._subscribe_symbol(symbol)

        logger.info(f"📡 Подписка оформлена на {len(self.symbols)} символов: {self.symbols}")

    async def _subscribe_symbol(self, symbol: str):
        """Подписка на один символ"""
        try:
            msg = {
                "op": "subscribe",
                "args": [f"tickers.{symbol}"]
            }
            await self.websocket.send(json.dumps(msg))
            logger.debug(f"📡 Подписка на {symbol}")
        except Exception as e:
            logger.error(f"❌ Ошибка подписки на {symbol}: {e}")

    async def _subscribe_new(self, symbols: List[str]):
        """Подписка на новые символы"""
        for symbol in symbols:
            await self._subscribe_symbol(symbol)
        logger.info(f"📡 Подписка на новые символы: {symbols}")

    async def _unsubscribe(self, symbols: List[str]):
        """Отписка от символов"""
        try:
            for symbol in symbols:
                msg = {
                    "op": "unsubscribe",
                    "args": [f"tickers.{symbol}"]
                }
                await self.websocket.send(json.dumps(msg))
                logger.debug(f"📡 Отписка от {symbol}")
            logger.info(f"📡 Отписка от символов: {symbols}")
        except Exception as e:
            logger.error(f"❌ Ошибка отписки: {e}")

    async def _listen(self):
        """Прослушивание входящих сообщений"""
        while self.running and self.websocket:
            try:
                message = await asyncio.wait_for(
                    self.websocket.recv(),
                    timeout=self.ping_interval + 5
                )
                data = json.loads(message)

                # Обработка сообщения
                await self._process_message(data)

            except asyncio.TimeoutError:
                # Таймаут ожидания сообщения, отправляем ping
                if self.websocket:
                    try:
                        await self.websocket.ping()
                        logger.debug("📡 Ping отправлен")
                    except:
                        logger.warning("⚠️ Не удалось отправить ping")
                continue

            except websockets.exceptions.ConnectionClosed:
                logger.warning("⚠️ WebSocket закрыт")
                break

            except Exception as e:
                logger.error(f"❌ Ошибка при получении сообщения: {e}")
                await asyncio.sleep(1)

    async def _process_message(self, data: Dict[str, Any]):
        """
        Обработка полученного сообщения

        Формат сообщения ticker:
        {
            "topic": "tickers.BTCUSDT",
            "type": "snapshot",
            "data": {
                "symbol": "BTCUSDT",
                "lastPrice": "50000.00",
                ...
            }
        }
        """
        try:
            # Проверяем, что это сообщение ticker
            if 'topic' in data and 'tickers' in data['topic']:
                ticker_data = data.get('data', {})
                symbol = ticker_data.get('symbol')
                price_str = ticker_data.get('lastPrice')

                if symbol and price_str:
                    price = float(price_str)

                    # Логируем только каждые 10 обновлений для снижения шума
                    if not hasattr(self, '_update_counter'):
                        self._update_counter = {}
                    self._update_counter[symbol] = self._update_counter.get(symbol, 0) + 1
                    if self._update_counter[symbol] % 10 == 0:
                        logger.debug(f"📊 {symbol} цена: {price:.6f}")

                    # Вызываем все колбэки
                    for callback in self.callbacks:
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(symbol, price)
                            else:
                                callback(symbol, price)
                        except Exception as e:
                            logger.error(f"❌ Ошибка в колбэке: {e}")

        except Exception as e:
            logger.error(f"❌ Ошибка обработки сообщения: {e}")

    async def _reconnect(self):
        """Переподключение при разрыве"""
        if not self.running:
            return

        logger.info(f"🔄 Переподключение через {self.reconnect_interval} сек...")
        await asyncio.sleep(self.reconnect_interval)

        # Закрываем старый сокет
        if self.websocket:
            try:
                await self.websocket.close()
            except:
                pass

        # Переподключаемся
        await self.connect()

    async def close(self):
        """Закрытие соединения"""
        logger.info("🔌 Закрытие WebSocket...")
        self.running = False

        if self.websocket:
            try:
                await self.websocket.close()
            except:
                pass

        # Очищаем колбэки
        self.callbacks.clear()

        logger.info("✅ WebSocket закрыт")

    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики клиента"""
        return {
            'running': self.running,
            'symbols_count': len(self.symbols),
            'symbols': self.symbols,
            'callbacks_count': len(self.callbacks),
            'url': self.url,
            'reconnect_interval': self.reconnect_interval
        }


# Глобальный экземпляр (будет создан в orchestrator)
websocket_client = None

__all__ = ['BybitWebSocketClient']