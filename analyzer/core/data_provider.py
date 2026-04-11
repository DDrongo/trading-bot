# analyzer/core/data_provider.py
"""
📦 DATA PROVIDER - Единый источник данных для всего бота
ФАЗА 1.3.9.1:
- Исправлен get_order_book (использует self.__client)
- Добавлена защита от rate limit (задержки + семафор)
"""

import logging
import time
import asyncio
from random import uniform
from typing import Dict, List, Optional, Any

logger = logging.getLogger('data_provider')


class DataProvider:
    """
    Единый источник данных для всего приложения.
    Паттерн Singleton: один экземпляр на всё приложение.
    Ленивая инициализация: клиент создаётся только при первом обращении.
    """

    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._client = None
        self._config = None
        self._cache: Dict[str, tuple] = {}
        self._cache_ttl = 60  # секунд
        self._price_cache_ttl = 5  # секунд для текущей цены

        # ФАЗА 1.3.9.1: Rate limit защита
        self._request_semaphore = None  # Инициализируется в configure
        self._request_delay = 0.5  # секунд между запросами

        self._initialized = True

        logger.info("📦 DataProvider создан (Singleton)")

    def configure(self, config: dict) -> None:
        """Передать конфигурацию (вызывается при инициализации бота)"""
        self._config = config

        # ФАЗА 1.3.9.1: Настройки rate limit из конфига
        api_config = config.get('api', {})
        self._request_delay = api_config.get('request_delay', 0.5)
        max_concurrent = api_config.get('max_concurrent_requests', 3)
        self._request_semaphore = asyncio.Semaphore(max_concurrent)

        logger.info("📦 DataProvider сконфигурирован")
        logger.info(f"   Request delay: {self._request_delay} сек")
        logger.info(f"   Max concurrent: {max_concurrent}")

    async def _delay(self):
        """Случайная задержка для избежания rate limit"""
        actual_delay = uniform(self._request_delay * 0.7, self._request_delay * 1.3)
        await asyncio.sleep(actual_delay)

    async def _get_client(self):
        """Ленивая инициализация BybitAPIClient"""
        if self._client is None:
            if self._config is None:
                raise RuntimeError("DataProvider не сконфигурирован. Вызовите configure() сначала.")

            from analyzer.core.api_client_bybit import BybitAPIClient
            self._client = BybitAPIClient(self._config)
            await self._client.initialize()
            logger.info("✅ DataProvider: BybitAPIClient инициализирован")
        return self._client

    def _get_cached(self, key: str, ttl: int = None) -> Optional[Any]:
        """Получение из кэша"""
        if ttl is None:
            ttl = self._cache_ttl

        if key in self._cache:
            data, timestamp = self._cache[key]
            if time.time() - timestamp < ttl:
                logger.debug(f"♻️ Кэш попадание: {key}")
                return data
            else:
                del self._cache[key]
        return None

    def _set_cached(self, key: str, data: Any) -> None:
        """Сохранение в кэш"""
        self._cache[key] = (data, time.time())

    async def get_current_price(self, symbol: str, force_refresh: bool = False) -> Optional[float]:
        """Получить текущую цену символа"""
        try:
            cache_key = f"price_{symbol}"

            if not force_refresh:
                cached = self._get_cached(cache_key, ttl=self._price_cache_ttl)
                if cached is not None:
                    return cached

            async with self._request_semaphore:
                await self._delay()

                client = await self._get_client()
                price = await client.get_current_price(symbol, force_refresh=force_refresh)

                if price:
                    self._set_cached(cache_key, price)

                return price

        except Exception as e:
            logger.error(f"❌ Ошибка получения цены {symbol}: {e}")
            return None

    async def get_klines(self, symbol: str, interval: str, limit: int = 100) -> List:
        """Получить свечи"""
        try:
            cache_key = f"klines_{symbol}_{interval}_{limit}"
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

            async with self._request_semaphore:
                await self._delay()

                client = await self._get_client()
                klines = await client.get_klines(symbol, interval, limit)

                if klines:
                    self._set_cached(cache_key, klines)

                return klines

        except Exception as e:
            logger.error(f"❌ Ошибка получения свечей {symbol} {interval}: {e}")
            return []

    async def get_24h_ticker(self, symbol: str) -> Dict[str, Any]:
        """Получить 24h тикер"""
        try:
            cache_key = f"ticker_{symbol}"
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

            async with self._request_semaphore:
                await self._delay()

                client = await self._get_client()
                ticker = await client.get_24h_ticker(symbol)

                if ticker:
                    self._set_cached(cache_key, ticker)

                return ticker

        except Exception as e:
            logger.error(f"❌ Ошибка получения тикера {symbol}: {e}")
            return {}

    async def get_all_symbols(self) -> List[str]:
        """Получить все символы"""
        try:
            cache_key = "all_symbols"
            cached = self._get_cached(cache_key, ttl=3600)  # 1 час
            if cached is not None:
                return cached

            async with self._request_semaphore:
                await self._delay()

                client = await self._get_client()
                symbols = await client.get_all_symbols()

                if symbols:
                    self._set_cached(cache_key, symbols)

                return symbols

        except Exception as e:
            logger.error(f"❌ Ошибка получения списка символов: {e}")
            return []

    async def check_symbol_exists(self, symbol: str) -> bool:
        """Проверить существование символа"""
        try:
            client = await self._get_client()
            return await client.check_symbol_exists(symbol)
        except Exception as e:
            logger.error(f"❌ Ошибка проверки символа {symbol}: {e}")
            return False

    async def get_tick_size(self, symbol: str) -> float:
        """Получить tick_size символа"""
        try:
            cache_key = f"tick_size_{symbol}"
            cached = self._get_cached(cache_key, ttl=3600)
            if cached is not None:
                return cached

            async with self._request_semaphore:
                await self._delay()

                client = await self._get_client()
                tick_size = await client.get_tick_size(symbol)

                if tick_size:
                    self._set_cached(cache_key, tick_size)

                return tick_size

        except Exception as e:
            logger.error(f"❌ Ошибка получения tick_size для {symbol}: {e}")
            return 0.0001

    # ========== ФАЗА 1.3.9.1: ИСПРАВЛЕННЫЙ get_order_book ==========
    async def get_order_book(self, symbol: str, limit: int = 10) -> Optional[Dict]:
        """
        Получает стакан ордеров

        Args:
            symbol: Символ (например, 'BTCUSDT')
            limit: Глубина стакана (1-200)

        Returns:
            Dict с полями bids и asks, или None при ошибке
        """
        try:
            cache_key = f"orderbook_{symbol}_{limit}"
            cached = self._get_cached(cache_key, ttl=2)  # Кэш на 2 секунды
            if cached is not None:
                return cached

            async with self._request_semaphore:
                await self._delay()

                client = await self._get_client()

                # Проверяем наличие метода get_orderbook у клиента
                if hasattr(client, 'get_orderbook'):
                    orderbook = await client.get_orderbook(symbol, limit)
                elif hasattr(client, 'get_order_book'):
                    orderbook = await client.get_order_book(symbol, limit)
                else:
                    logger.error(f"❌ API клиент не имеет метода get_orderbook/get_order_book")
                    return None

                if orderbook:
                    self._set_cached(cache_key, orderbook)

                return orderbook

        except Exception as e:
            logger.error(f"❌ Ошибка получения стакана {symbol}: {e}")
            return None

    def clear_cache(self) -> None:
        """Очистить кэш"""
        self._cache.clear()
        logger.info("🧹 Кэш DataProvider очищен")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Получить статистику кэша"""
        return {
            'cache_size': len(self._cache),
            'cache_keys': list(self._cache.keys())
        }

    async def close(self) -> None:
        """Закрыть соединения"""
        if self._client:
            await self._client.close()
            logger.info("✅ DataProvider: BybitAPIClient закрыт")


# Глобальный экземпляр (Singleton)
data_provider = DataProvider()

__all__ = ['DataProvider', 'data_provider']