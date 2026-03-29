# analyzer/core/data_provider.py
"""
📦 DATA PROVIDER - Единый источник данных для всего бота
ФАЗА 1.3.7: Singleton, ленивая инициализация, кеширование
"""

import logging
import time
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
        self._initialized = True

        logger.info("📦 DataProvider создан (Singleton)")

    def configure(self, config: dict) -> None:
        """Передать конфигурацию (вызывается при инициализации бота)"""
        self._config = config
        logger.info("📦 DataProvider сконфигурирован")

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
        """
        Получить текущую цену символа

        Args:
            symbol: Символ (например, BTCUSDT)
            force_refresh: Принудительное обновление (игнорировать кэш)

        Returns:
            Текущая цена или None
        """
        try:
            cache_key = f"price_{symbol}"

            if not force_refresh:
                cached = self._get_cached(cache_key, ttl=self._price_cache_ttl)
                if cached is not None:
                    return cached

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

            client = await self._get_client()
            tick_size = await client.get_tick_size(symbol)

            if tick_size:
                self._set_cached(cache_key, tick_size)

            return tick_size

        except Exception as e:
            logger.error(f"❌ Ошибка получения tick_size для {symbol}: {e}")
            return 0.0001

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