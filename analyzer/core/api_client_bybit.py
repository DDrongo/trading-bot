# analyzer/core/api_client_bybit.py
"""
🌐 API клиент для работы с биржей Bybit (V5 API)
ФАЗА 1.3.5: ИСПРАВЛЕНИЯ
- Исправлена обработка ошибок "symbol invalid" (проверка существования символа)
- Добавлена проверка существования символа перед запросом
- Улучшено кэширование exchange_info
- Добавлен метод check_symbol_exists
"""

import asyncio
import logging
import time
import hmac
import hashlib
import json
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from urllib.parse import urlencode

import aiohttp
from aiohttp import ClientSession, ClientTimeout

logger = logging.getLogger('api_client_bybit')


class BybitAPIClient:
    def __init__(self, config=None):
        self.config = config or {}

        # Получаем настройки API из конфига
        api_config = self.config.get('api', {})
        system_config = self.config.get('system', {})

        # РЕЖИМЫ РАБОТЫ
        self.mode = self.config.get('mode', 'paper')

        # Тип рынка (linear = USDT фьючерсы, spot = спот)
        self.market_type = self.config.get('market_type', 'linear')

        # API ключи (только для LIVE)
        self.api_key = api_config.get('api_key', '')
        self.api_secret = api_config.get('api_secret', '')

        # Настройки API
        self.testnet = api_config.get('testnet', False)
        self.timeout = api_config.get('timeout', 30)
        self.rate_limit = api_config.get('rate_limit', 100)
        self.retry_attempts = api_config.get('retry_attempts', 3)
        self.retry_delay = api_config.get('retry_delay', 1)
        self.recv_window = api_config.get('recv_window', 5000)

        # Настройки запросов
        requests_config = api_config.get('requests', {})
        self.orderbook_limits = requests_config.get('orderbook_limits', [1, 5, 10, 20, 50, 100])
        self.max_kline_limit = requests_config.get('max_kline_limit', 1000)
        self.default_order_type = requests_config.get('default_order_type', 'GTC')

        # Настройки кэширования
        cache_config = api_config.get('cache', {})
        self._cache_ttl = cache_config.get('ttl_seconds', 60)
        self._price_cache_ttl = 5  # секунд для текущей цены

        if self.mode == 'paper':
            self.base_url = "https://api.bybit.com"
            logger.info(f"📊 Режим: PAPER (публичные данные Bybit), Рынок: {self.market_type}")

        elif self.mode == 'live':
            if self.testnet:
                self.base_url = "https://api-testnet.bybit.com"
                logger.info(f"🧪 Режим: LIVE (тестовая сеть Bybit), Рынок: {self.market_type}")
            else:
                self.base_url = "https://api.bybit.com"
                logger.info(f"🚀 Режим: LIVE (реальная сеть Bybit), Рынок: {self.market_type}")

        # Сессия HTTP
        self.session: Optional[ClientSession] = None

        # Лимиты и кэш
        self.request_count = 0
        self.last_reset = time.time()
        self.rate_limiter = asyncio.Semaphore(self.rate_limit)

        # Кэш для часто запрашиваемых данных
        self._cache: Dict[str, tuple] = {}

        # Кэш для tick_size и существующих символов
        self._tick_size_cache: Dict[str, float] = {}
        self._symbols_cache: Optional[List[str]] = None
        self._exchange_info_cache_time: Optional[float] = None
        self._exchange_info_ttl = 3600  # 1 час

        self._initialized = False

        logger.info(f"✅ BybitAPIClient создан (Timeout: {self.timeout}s, Rate Limit: {self.rate_limit}/s)")

    async def initialize(self) -> bool:
        """Инициализация клиента"""
        try:
            timeout = ClientTimeout(total=self.timeout)
            self.session = ClientSession(timeout=timeout)

            if await self._test_connection():
                self._initialized = True
                await self._load_exchange_info()
                logger.info("✅ BybitAPIClient инициализирован")
                return True
            else:
                logger.error("❌ Не удалось подключиться к Bybit API")
                return False

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации API клиента: {e}")
            return False

    async def _test_connection(self) -> bool:
        """Тестирование соединения с API"""
        try:
            url = f"{self.base_url}/v5/market/time"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('retCode', 1) == 0
                return False
        except:
            return True

    async def _load_exchange_info(self) -> None:
        """Загрузка информации о бирже для получения tick_size и списка символов"""
        try:
            current_time = time.time()

            if (self._exchange_info_cache_time and
                    current_time - self._exchange_info_cache_time < self._exchange_info_ttl):
                logger.debug("♻️ Используем кэшированную exchange_info")
                return

            params = {
                'category': self.market_type,
                'status': 'Trading'
            }

            data = await self._make_public_request("GET", "/v5/market/instruments-info", params)

            if data and 'list' in data:
                symbols_list = []
                for instrument in data['list']:
                    symbol = instrument.get('symbol')
                    if symbol:
                        symbols_list.append(symbol)
                        tick_size = instrument.get('priceFilter', {}).get('tickSize')
                        if tick_size:
                            try:
                                self._tick_size_cache[symbol] = float(tick_size)
                            except (ValueError, TypeError):
                                self._tick_size_cache[symbol] = 0.0001
                        else:
                            self._tick_size_cache[symbol] = 0.0001

                self._symbols_cache = symbols_list
                self._exchange_info_cache_time = current_time
                logger.info(f"✅ Загружена exchange_info: {len(symbols_list)} символов")
            else:
                logger.warning("⚠️ Не удалось получить exchange_info")

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки exchange_info: {e}")

    async def check_symbol_exists(self, symbol: str) -> bool:
        """
        Проверка существования символа на бирже

        Args:
            symbol: Символ (например, BTCUSDT)

        Returns:
            True если символ существует, False если нет
        """
        try:
            # Загружаем список символов если нужно
            if self._symbols_cache is None:
                await self._load_exchange_info()

            if self._symbols_cache:
                exists = symbol in self._symbols_cache
                if not exists:
                    logger.warning(f"⚠️ Символ {symbol} не найден на Bybit {self.market_type.upper()}")
                return exists

            # Если нет кэша, пробуем прямой запрос
            params = {
                'category': self.market_type,
                'symbol': symbol
            }
            data = await self._make_public_request("GET", "/v5/market/tickers", params)
            return data and 'list' in data and len(data['list']) > 0

        except Exception as e:
            logger.error(f"❌ Ошибка проверки символа {symbol}: {e}")
            return False

    async def get_tick_size(self, symbol: str) -> float:
        """
        Получение tick_size для символа (шаг цены)

        Args:
            symbol: Символ (например, BTCUSDT)

        Returns:
            Tick size (минимальный шаг цены)
        """
        if symbol in self._tick_size_cache:
            return self._tick_size_cache[symbol]

        await self._load_exchange_info()
        return self._tick_size_cache.get(symbol, 0.0001)

    async def get_current_price(self, symbol: str, force_refresh: bool = False) -> Optional[float]:
        """
        Получение текущей цены символа

        ✅ ФАЗА 1.3.5: Добавлена проверка существования символа

        Args:
            symbol: Символ (например, BTCUSDT)
            force_refresh: Принудительное обновление (игнорировать кэш)

        Returns:
            Текущая цена или None в случае ошибки
        """
        try:
            # Проверяем существование символа
            if not await self.check_symbol_exists(symbol):
                logger.warning(f"⚠️ Символ {symbol} не существует на Bybit {self.market_type.upper()}")
                return None

            cache_key = f"current_price_{symbol}"

            if not force_refresh:
                cached = self._get_cached_data(cache_key, ttl=self._price_cache_ttl)
                if cached is not None:
                    logger.debug(f"♻️ Кэш текущей цены {symbol}: {cached}")
                    return cached

            params = {
                'category': self.market_type,
                'symbol': symbol
            }

            data = await self._make_public_request("GET", "/v5/market/tickers", params)

            if data and 'list' in data and len(data['list']) > 0:
                ticker = data['list'][0]
                last_price = ticker.get('lastPrice')

                if last_price:
                    price = float(last_price)
                    self._set_cached_data(cache_key, price)
                    logger.debug(f"✅ Текущая цена {symbol}: {price}")
                    return price
                else:
                    logger.warning(f"⚠️ Нет lastPrice для {symbol}")
                    return None
            else:
                logger.warning(f"⚠️ Нет данных тикера для {symbol}")
                return None

        except Exception as e:
            logger.error(f"❌ Ошибка получения текущей цены {symbol}: {e}")
            return None

    def _generate_signature(self, params: Dict, timestamp: int) -> str:
        """Генерация подписи для Bybit API"""
        param_str = f"{timestamp}{self.api_key}{self.rate_limit * 1000}"

        if params:
            param_str += urlencode(params)

        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        return signature

    async def _make_public_request(self, method: str, endpoint: str,
                                   params: Dict = None) -> Dict[str, Any]:
        """
        Выполнение публичного HTTP запроса
        """
        url = f"{self.base_url}{endpoint}"

        if params is None:
            params = {}

        await self._check_rate_limit()

        for attempt in range(self.retry_attempts):
            try:
                async with self.rate_limiter:
                    logger.debug(f"📡 {method} {endpoint} (попытка {attempt + 1})")

                    async with self.session.request(
                            method=method,
                            url=url,
                            params=params if method == "GET" else None,
                            json=params if method != "GET" else None
                    ) as response:

                        self.request_count += 1

                        if response.status == 200:
                            data = await response.json()

                            if data.get('retCode') == 0:
                                return data.get('result', data)
                            else:
                                error_msg = data.get('retMsg', 'Unknown error')
                                logger.error(f"❌ Bybit API error: {error_msg}")
                                raise Exception(f"Bybit API Error: {error_msg}")
                        else:
                            error_text = await response.text()
                            logger.error(f"❌ HTTP ошибка {response.status}: {error_text}")

                            if attempt < self.retry_attempts - 1:
                                wait_time = self.retry_delay * (2 ** attempt)
                                logger.warning(f"⚠️ Повтор через {wait_time} секунд...")
                                await asyncio.sleep(wait_time)
                            else:
                                raise Exception(f"HTTP Error after {self.retry_attempts} attempts: {error_text}")

            except asyncio.TimeoutError:
                logger.error(f"⏰ Таймаут запроса {endpoint} (попытка {attempt + 1})")
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise Exception(f"Timeout after {self.retry_attempts} attempts")

            except Exception as e:
                logger.error(f"❌ Ошибка запроса {endpoint}: {e}")
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise

        raise Exception(f"Failed after {self.retry_attempts} attempts")

    async def _make_private_request(self, method: str, endpoint: str,
                                    params: Dict = None) -> Dict[str, Any]:
        """
        Выполнение приватного HTTP запроса (с подписью)
        """
        if not self.api_key or not self.api_secret:
            raise Exception("API ключи не настроены для приватных запросов")

        url = f"{self.base_url}{endpoint}"

        if params is None:
            params = {}

        timestamp = int(time.time() * 1000)
        params['api_key'] = self.api_key
        params['timestamp'] = timestamp
        params['recv_window'] = self.recv_window

        signature = self._generate_signature(params, timestamp)
        params['sign'] = signature

        await self._check_rate_limit()

        for attempt in range(self.retry_attempts):
            try:
                async with self.rate_limiter:
                    logger.debug(f"🔐 {method} {endpoint} (попытка {attempt + 1})")

                    headers = {
                        'X-BAPI-API-KEY': self.api_key,
                        'X-BAPI-SIGN': signature,
                        'X-BAPI-TIMESTAMP': str(timestamp),
                        'X-BAPI-RECV-WINDOW': str(self.recv_window),
                        'Content-Type': 'application/json'
                    }

                    async with self.session.request(
                            method=method,
                            url=url,
                            params=params if method == "GET" else None,
                            json=params if method != "GET" else None,
                            headers=headers
                    ) as response:

                        self.request_count += 1

                        if response.status == 200:
                            data = await response.json()

                            if data.get('retCode') == 0:
                                return data.get('result', data)
                            else:
                                error_msg = data.get('retMsg', 'Unknown error')
                                logger.error(f"❌ Bybit API error: {error_msg}")
                                raise Exception(f"Bybit API Error: {error_msg}")
                        else:
                            error_text = await response.text()
                            logger.error(f"❌ HTTP ошибка {response.status}: {error_text}")

                            if attempt < self.retry_attempts - 1:
                                wait_time = self.retry_delay * (2 ** attempt)
                                logger.warning(f"⚠️ Повтор через {wait_time} секунд...")
                                await asyncio.sleep(wait_time)
                            else:
                                raise Exception(f"HTTP Error after {self.retry_attempts} attempts: {error_text}")

            except asyncio.TimeoutError:
                logger.error(f"⏰ Таймаут запроса {endpoint} (попытка {attempt + 1})")
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise Exception(f"Timeout after {self.retry_attempts} attempts")

            except Exception as e:
                logger.error(f"❌ Ошибка запроса {endpoint}: {e}")
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise

        raise Exception(f"Failed after {self.retry_attempts} attempts")

    async def _check_rate_limit(self):
        """Проверка лимита запросов для Bybit"""
        current_time = time.time()

        if current_time - self.last_reset > 1:
            self.request_count = 0
            self.last_reset = current_time

        if self.request_count >= self.rate_limit:
            wait_time = 1 - (current_time - self.last_reset)
            if wait_time > 0:
                logger.warning(f"⚠️ Лимит запросов Bybit достигнут, ждем {wait_time:.2f} секунд")
                await asyncio.sleep(wait_time)
                self.request_count = 0
                self.last_reset = time.time()

    def _get_cached_data(self, cache_key: str, ttl: int = None) -> Optional[Any]:
        """Получение данных из кэша"""
        if ttl is None:
            ttl = self._cache_ttl

        if cache_key in self._cache:
            data, timestamp = self._cache[cache_key]
            if time.time() - timestamp < ttl:
                logger.debug(f"♻️ Кэш попадание: {cache_key}")
                return data
            else:
                del self._cache[cache_key]
        return None

    def _set_cached_data(self, cache_key: str, data: Any):
        """Сохранение данных в кэш"""
        self._cache[cache_key] = (data, time.time())

    async def get_klines(self, symbol: str, interval: str, limit: int = 100) -> List:
        """
        Получение исторических свечей (klines/candles)

        ✅ ФАЗА 1.3.5: Добавлена проверка существования символа
        """
        try:
            # Проверяем существование символа
            if not await self.check_symbol_exists(symbol):
                logger.warning(f"⚠️ Символ {symbol} не существует, пропускаем")
                return []

            cache_key = f"klines_{symbol}_{interval}_{limit}"
            cached = self._get_cached_data(cache_key)
            if cached:
                return cached

            interval_map = {
                '1m': '1', '3m': '3', '5m': '5', '15m': '15', '30m': '30',
                '1h': '60', '2h': '120', '4h': '240', '6h': '360', '12h': '720',
                '1d': 'D', '1w': 'W', '1M': 'M'
            }

            bybit_interval = interval_map.get(interval, interval)

            params = {
                'category': self.market_type,
                'symbol': symbol,
                'interval': bybit_interval,
                'limit': min(limit, self.max_kline_limit)
            }

            data = await self._make_public_request("GET", "/v5/market/kline", params)

            klines = []
            if data and 'list' in data:
                for candle in data['list']:
                    klines.append([
                        candle[0],  # timestamp
                        candle[1],  # open
                        candle[2],  # high
                        candle[3],  # low
                        candle[4],  # close
                        candle[5],  # volume
                        candle[0],  # close_time
                        "0",
                        0,
                        0,
                        0,
                        "0"
                    ])

            self._set_cached_data(cache_key, klines)
            logger.debug(f"✅ Получено {len(klines)} свечей {symbol} {interval}")
            return klines

        except Exception as e:
            logger.error(f"❌ Ошибка получения свечей {symbol} {interval}: {e}")
            return []

    async def get_24h_ticker(self, symbol: str) -> Dict[str, Any]:
        """Получение 24-часовой статистики по символу"""
        try:
            cache_key = f"ticker_{symbol}"
            cached = self._get_cached_data(cache_key)
            if cached:
                return cached

            params = {
                'category': self.market_type,
                'symbol': symbol
            }

            data = await self._make_public_request("GET", "/v5/market/tickers", params)

            result = {}
            if data and 'list' in data and data['list']:
                ticker = data['list'][0]
                result = {
                    'symbol': ticker.get('symbol', symbol),
                    'lastPrice': ticker.get('lastPrice', '0'),
                    'volume': ticker.get('volume24h', '0'),
                    'highPrice': ticker.get('highPrice24h', '0'),
                    'lowPrice': ticker.get('lowPrice24h', '0'),
                    'priceChange': ticker.get('price24hPcnt', '0'),
                    'count': ticker.get('turnover24h', '0')
                }

            self._set_cached_data(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"❌ Ошибка получения тикера {symbol}: {e}")
            return {}

    async def get_order_book(self, symbol: str, limit: int = 10) -> Dict[str, List]:
        """Получение стакана заявок"""
        try:
            if limit not in self.orderbook_limits:
                closest_limit = min(self.orderbook_limits, key=lambda x: abs(x - limit))
                logger.debug(f"⚠️ Лимит {limit} недопустим, используем ближайший: {closest_limit}")
                limit = closest_limit

            cache_key = f"orderbook_{symbol}_{limit}"
            cached = self._get_cached_data(cache_key)
            if cached:
                return cached

            params = {
                'category': self.market_type,
                'symbol': symbol,
                'limit': limit
            }

            data = await self._make_public_request("GET", "/v5/market/orderbook", params)

            result = {'bids': [], 'asks': []}
            if data:
                result['bids'] = data.get('b', [])
                result['asks'] = data.get('a', [])

            self._set_cached_data(cache_key, result)
            logger.debug(f"✅ Получен стакан {symbol} (глубина: {limit})")
            return result

        except Exception as e:
            logger.error(f"❌ Ошибка получения стакана {symbol}: {e}")
            return {'bids': [], 'asks': []}

    async def get_exchange_info(self) -> Dict[str, Any]:
        """Получение информации о бирже (торговые пары)"""
        try:
            cache_key = "exchange_info"
            cached = self._get_cached_data(cache_key)
            if cached:
                return cached

            params = {
                'category': self.market_type,
                'status': 'Trading'
            }

            data = await self._make_public_request("GET", "/v5/market/instruments-info", params)

            self._set_cached_data(cache_key, data)

            symbol_count = len(data.get('list', [])) if data else 0
            logger.info(f"✅ Получена информация о бирже ({symbol_count} пар)")
            return data

        except Exception as e:
            logger.error(f"❌ Ошибка получения информации о бирже: {e}")
            return {}

    async def get_all_symbols(self) -> List[str]:
        """Получение списка всех торговых пар"""
        try:
            if self._symbols_cache is not None:
                return self._symbols_cache

            await self._load_exchange_info()
            return self._symbols_cache or []

        except Exception as e:
            logger.error(f"❌ Ошибка получения списка символов: {e}")
            return []

    async def place_order(self, symbol: str, side: str, quantity: float,
                          order_type: str = "Market", price: float = None) -> Dict[str, Any]:
        """Размещение ордера на Bybit"""
        try:
            params = {
                'category': self.market_type,
                'symbol': symbol,
                'side': side.capitalize(),
                'orderType': order_type.capitalize(),
                'qty': str(quantity),
                'timeInForce': self.default_order_type
            }

            if order_type.lower() == "limit" and price:
                params['price'] = str(price)

            response = await self._make_private_request("POST", "/v5/order/create", params)

            logger.info(f"✅ Ордер размещен: {symbol} {side} {quantity} @ {price or 'market'}")
            return response

        except Exception as e:
            logger.error(f"❌ Ошибка размещения ордера {symbol}: {e}")
            return {}

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Отмена ордера"""
        try:
            params = {
                'category': self.market_type,
                'symbol': symbol,
                'orderId': order_id
            }

            response = await self._make_private_request("POST", "/v5/order/cancel", params)

            logger.info(f"✅ Ордер отменен: {symbol} #{order_id}")
            return response.get('retCode', 1) == 0

        except Exception as e:
            logger.error(f"❌ Ошибка отмены ордера {symbol}: {e}")
            return False

    async def get_order_status(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """Получение статуса ордера"""
        try:
            params = {
                'category': self.market_type,
                'symbol': symbol,
                'orderId': order_id
            }

            response = await self._make_private_request("GET", "/v5/order/history", params)
            return response

        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса ордера {symbol}: {e}")
            return {}

    async def get_account_info(self) -> Dict[str, Any]:
        """Получение информации об аккаунте (балансы)"""
        try:
            params = {
                'accountType': 'UNIFIED' if self.market_type == 'linear' else 'SPOT'
            }

            response = await self._make_private_request("GET", "/v5/account/wallet-balance", params)
            return response

        except Exception as e:
            logger.error(f"❌ Ошибка получения информации об аккаунте: {e}")
            return {}

    async def close(self):
        """Закрытие соединений"""
        if self.session:
            await self.session.close()
            logger.info("✅ Сессия Bybit API закрыта")

    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики API клиента"""
        return {
            'request_count': self.request_count,
            'rate_limit': self.rate_limit,
            'cache_size': len(self._cache),
            'cache_ttl': self._cache_ttl,
            'initialized': self._initialized,
            'testnet': self.testnet,
            'exchange': 'Bybit',
            'market_type': self.market_type,
            'timeout': self.timeout,
            'retry_attempts': self.retry_attempts,
            'recv_window': self.recv_window,
            'tick_size_cache_size': len(self._tick_size_cache),
            'symbols_cache_size': len(self._symbols_cache) if self._symbols_cache else 0
        }


# Адаптер для совместимости с системой
class APIClient:
    """
    Адаптер API клиента для системы (Bybit версия)
    """

    def __init__(self, config=None):
        self.config = config or {}
        self.bybit_client = BybitAPIClient(config)
        self._initialized = False

    async def initialize(self) -> bool:
        """Инициализация API клиента"""
        result = await self.bybit_client.initialize()
        self._initialized = result
        return result

    async def get_current_price(self, symbol: str, force_refresh: bool = False) -> Optional[float]:
        """Получение текущей цены"""
        return await self.bybit_client.get_current_price(symbol, force_refresh)

    async def get_tick_size(self, symbol: str) -> float:
        """Получение tick_size"""
        return await self.bybit_client.get_tick_size(symbol)

    async def check_symbol_exists(self, symbol: str) -> bool:
        """Проверка существования символа"""
        return await self.bybit_client.check_symbol_exists(symbol)

    async def get_klines(self, symbol: str, interval: str, limit: int = 100) -> List:
        """Получение свечей"""
        return await self.bybit_client.get_klines(symbol, interval, limit)

    async def get_24h_ticker(self, symbol: str) -> Dict[str, Any]:
        """Получение 24h тикера"""
        return await self.bybit_client.get_24h_ticker(symbol)

    async def get_order_book(self, symbol: str, limit: int = 10) -> Dict[str, List]:
        """Получение стакана"""
        return await self.bybit_client.get_order_book(symbol, limit)

    async def get_exchange_info(self) -> Dict[str, Any]:
        """Получение информации о бирже"""
        return await self.bybit_client.get_exchange_info()

    async def get_all_symbols(self) -> List[str]:
        """Получение всех символов"""
        return await self.bybit_client.get_all_symbols()

    async def place_order(self, symbol: str, side: str, quantity: float,
                          order_type: str = "MARKET", price: float = None) -> Dict[str, Any]:
        """Размещение ордера"""
        return await self.bybit_client.place_order(symbol, side, quantity, order_type, price)

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Отмена ордера"""
        return await self.bybit_client.cancel_order(symbol, order_id)

    async def get_order_status(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """Получение статуса ордера"""
        return await self.bybit_client.get_order_status(symbol, order_id)

    async def get_account_info(self) -> Dict[str, Any]:
        """Получение информации об аккаунте"""
        return await self.bybit_client.get_account_info()

    async def close(self):
        """Закрытие соединений"""
        await self.bybit_client.close()

    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики"""
        stats = self.bybit_client.get_stats()
        stats['client_type'] = 'Bybit'
        return stats


__all__ = ['APIClient', 'BybitAPIClient']