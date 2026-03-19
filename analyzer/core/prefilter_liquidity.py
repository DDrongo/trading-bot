
# core/prefilter_liquidity.py
"""
🎯 ПРЕФИЛЬТР ЛИКВИДНОСТИ - быстрая проверка перед трехэкранным анализом
"""

import logging
import asyncio
from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger('liquidity_prefilter')


@dataclass
class LiquidityMetrics:
    """Метрики ликвидности символа"""
    symbol: str
    volume_24h_usd: float = 0.0
    current_price: float = 0.0
    avg_spread_pct: Optional[float] = None
    order_book_depth_usd: Optional[float] = None
    number_of_trades: Optional[int] = None
    last_updated: datetime = field(default_factory=datetime.now)
    passed: bool = False
    fail_reason: str = ""

    def to_dict(self) -> Dict[str, any]:
        return {
            "symbol": self.symbol,
            "volume_24h_usd": self.volume_24h_usd,
            "current_price": self.current_price,
            "avg_spread_pct": self.avg_spread_pct,
            "order_book_depth_usd": self.order_book_depth_usd,
            "number_of_trades": self.number_of_trades,
            "last_updated": self.last_updated.isoformat(),
            "passed": self.passed,
            "fail_reason": self.fail_reason
        }


@dataclass
class PrefilterResult:
    """Результат работы префильтра"""
    total_symbols: int = 0
    passed_symbols: int = 0
    failed_symbols: int = 0
    filtered_symbols: List[str] = field(default_factory=list)
    metrics_by_symbol: Dict[str, LiquidityMetrics] = field(default_factory=dict)
    execution_time_seconds: float = 0.0

    def to_dict(self) -> Dict[str, any]:
        return {
            "total_symbols": self.total_symbols,
            "passed_symbols": self.passed_symbols,
            "failed_symbols": self.failed_symbols,
            "filtered_symbols": self.filtered_symbols,
            "execution_time_seconds": self.execution_time_seconds,
            "metrics_by_symbol": {k: v.to_dict() for k, v in self.metrics_by_symbol.items()}
        }


class LiquidityPrefilter:
    """
    Быстрая проверка ликвидности перед дорогим трехэкранным анализом
    """

    def __init__(self, api_client, config=None):
        self.api = api_client
        self.config = config or {}

        # Получаем конфигурацию из секции analysis.prefilter
        analysis_config = self.config.get('analysis', {})
        prefilter_config = analysis_config.get('prefilter', {})

        # Конфигурационные пороги
        self.MIN_24H_VOLUME_USD = analysis_config.get('min_24h_volume_usd', 1_000_000)
        self.MIN_PRICE = analysis_config.get('min_price', 0.01)
        self.MAX_SPREAD_PCT = analysis_config.get('max_spread_pct', 1.0)

        # Параметры префильтра
        self.CHECK_ORDERBOOK = prefilter_config.get('check_orderbook', False)
        self.MIN_ORDERBOOK_DEPTH = prefilter_config.get('min_orderbook_depth', 10_000)
        self.MIN_PRICE_THRESHOLD = prefilter_config.get('min_price_threshold', 0.10)
        self.BATCH_SIZE = prefilter_config.get('batch_size', 20)
        self.ORDERBOOK_LIMIT = prefilter_config.get('orderbook_limit', 10)

        # Черный и белый списки
        self.BLACKLIST = set(prefilter_config.get('blacklist', [
            "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "USDPUSDT",
            "FDUSDUSDT", "DAIUSDT", "USDSUSDT"
        ]))
        self.WHITELIST = set(prefilter_config.get('whitelist', []))

        # Кэш для результатов проверки
        self._checked_symbols: Dict[str, LiquidityMetrics] = {}
        self._cache_ttl_hours = prefilter_config.get('cache_ttl_hours', 1)

        logger.info(f"✅ LiquidityPrefilter создан. "
                    f"Минимум: ${self.MIN_24H_VOLUME_USD:,.0f}, "
                    f"цена > ${self.MIN_PRICE:.2f}, "
                    f"порог цены: ${self.MIN_PRICE_THRESHOLD:.2f}, "
                    f"размер пачки: {self.BATCH_SIZE}")

    async def filter_symbols(self, symbols: List[str]) -> PrefilterResult:
        """
        Основной метод фильтрации символов по ликвидности

        Args:
            symbols: Список символов для проверки

        Returns:
            PrefilterResult с отфильтрованными символами и метриками
        """
        start_time = datetime.now()
        logger.info(f"🔍 Префильтр ликвидности: начинаем проверку {len(symbols)} символов")

        result = PrefilterResult(total_symbols=len(symbols))

        # Очищаем устаревший кэш
        self._clean_old_cache()

        # Проверяем символы параллельно (пачками)
        passed_symbols = []

        for i in range(0, len(symbols), self.BATCH_SIZE):
            batch = symbols[i:i + self.BATCH_SIZE]
            logger.debug(f"Обрабатываю пачку {i // self.BATCH_SIZE + 1}: {len(batch)} символов")

            # Создаем задачи для параллельной обработки
            tasks = [self._check_symbol_async(symbol) for symbol in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Обрабатываем результаты
            for symbol_result in batch_results:
                if isinstance(symbol_result, Exception):
                    logger.warning(f"Ошибка при проверке символа: {symbol_result}")
                    continue

                symbol, metrics = symbol_result
                result.metrics_by_symbol[symbol] = metrics

                if metrics.passed:
                    passed_symbols.append(symbol)
                    logger.debug(f"✅ {symbol} прошел: ${metrics.volume_24h_usd:,.0f}")
                else:
                    logger.debug(f"❌ {symbol} не прошел: {metrics.fail_reason}")

        # Обновляем статистику результата
        result.passed_symbols = len(passed_symbols)
        result.failed_symbols = result.total_symbols - result.passed_symbols
        result.filtered_symbols = passed_symbols
        result.execution_time_seconds = (datetime.now() - start_time).total_seconds()

        # 🔧 ИСПРАВЛЕНИЕ БАГА: защита от деления на ноль
        if result.total_symbols > 0:
            passed_percent = result.passed_symbols / result.total_symbols * 100
        else:
            passed_percent = 0.0

        logger.info(f"📊 Префильтр завершен: {result.total_symbols} → {result.passed_symbols} символов "
                    f"({passed_percent:.1f}%) "
                    f"за {result.execution_time_seconds:.2f} сек")

        return result

    async def _check_symbol_async(self, symbol: str) -> Tuple[str, LiquidityMetrics]:
        """Асинхронная проверка одного символа"""
        try:
            # Пропускаем черный список
            if symbol in self.BLACKLIST:
                return symbol, LiquidityMetrics(
                    symbol=symbol,
                    passed=False,
                    fail_reason="В черном списке"
                )

            # Проверяем кэш (если есть свежий результат)
            cached = self._get_cached_metrics(symbol)
            if cached:
                return symbol, cached

            # Проверяем ликвидность
            metrics = await self._check_symbol_liquidity(symbol)

            # Сохраняем в кэш
            self._checked_symbols[symbol] = metrics

            return symbol, metrics

        except Exception as e:
            logger.error(f"Ошибка проверки символа {symbol}: {e}")
            return symbol, LiquidityMetrics(
                symbol=symbol,
                passed=False,
                fail_reason=f"Ошибка проверки: {str(e)[:50]}"  # 🔧 Ограничение длины
            )

    async def _check_symbol_liquidity(self, symbol: str) -> LiquidityMetrics:
        """Проверка ликвидности одного символа"""
        metrics = LiquidityMetrics(symbol=symbol)

        try:
            # 1. Получаем 24h тикер (1 запрос)
            logger.debug(f"📡 Получаем тикер для {symbol}")
            ticker = await self.api.get_24h_ticker(symbol)

            if not ticker:
                metrics.fail_reason = "Не удалось получить тикер"
                logger.warning(f"❌ Нет тикера для {symbol}")
                return metrics

            # 2. Извлекаем данные (Bybit V5 специфика)
            logger.debug(f"🔍 Парсинг данных для {symbol}")
            try:
                volume_str = str(ticker.get('volume', '0')).replace(',', '')
                volume = float(volume_str) if volume_str and volume_str != '0' else 0.0

                last_price_str = str(ticker.get('lastPrice', '0')).replace(',', '')
                last_price = float(last_price_str) if last_price_str and last_price_str != '0' else 0.0

                logger.debug(f"📊 {symbol}: объем={volume}, цена={last_price}")

                # ✅ КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Проверка минимальной цены
                if last_price < self.MIN_PRICE_THRESHOLD:
                    metrics.volume_24h_usd = volume * last_price
                    metrics.current_price = last_price
                    metrics.fail_reason = f"Цена ${last_price:.4f} < ${self.MIN_PRICE_THRESHOLD:.2f}"
                    logger.warning(f"❌ {symbol}: цена слишком низкая: ${last_price:.4f}")
                    return metrics

                # Рассчитываем объем в USD
                volume_usd = volume * last_price

            except (ValueError, TypeError, AttributeError) as e:
                logger.error(f"❌ Ошибка парсинга данных для {symbol}: {e}, ticker: {ticker}")
                metrics.fail_reason = f"Ошибка парсинга данных: {str(e)[:50]}"
                return metrics

            # Сохраняем базовые метрики
            metrics.volume_24h_usd = volume_usd
            metrics.current_price = last_price

            # 3. Быстрые проверки порогов
            if volume_usd < self.MIN_24H_VOLUME_USD:
                metrics.fail_reason = f"Объём ${volume_usd:,.0f} < ${self.MIN_24H_VOLUME_USD:,.0f}"
                logger.warning(f"❌ {symbol}: недостаточный объем: ${volume_usd:,.0f}")
                return metrics

            if last_price < self.MIN_PRICE:
                metrics.fail_reason = f"Цена ${last_price:.4f} < ${self.MIN_PRICE:.2f}"
                logger.warning(f"❌ {symbol}: цена ниже минимума: ${last_price:.4f}")
                return metrics

            # 4. Опционально: проверяем стакан (если включено в конфиге)
            if self.CHECK_ORDERBOOK:
                try:
                    logger.debug(f"📊 Проверка стакана для {symbol}")
                    order_book = await self.api.get_order_book(symbol, limit=self.ORDERBOOK_LIMIT)

                    if order_book and 'bids' in order_book and 'asks' in order_book and order_book['bids'] and \
                            order_book['asks']:
                        # Рассчитываем глубину стакана на первых 5 уровнях
                        bid_depth = sum(float(bid[0]) * float(bid[1]) for bid in order_book['bids'][:5])
                        ask_depth = sum(float(ask[0]) * float(ask[1]) for ask in order_book['asks'][:5])
                        avg_depth = (bid_depth + ask_depth) / 2

                        metrics.order_book_depth_usd = avg_depth

                        # Проверяем спред
                        best_bid = float(order_book['bids'][0][0])
                        best_ask = float(order_book['asks'][0][0])
                        spread_pct = (best_ask - best_bid) / best_bid * 100
                        metrics.avg_spread_pct = spread_pct

                        logger.debug(f"📊 {symbol}: спред={spread_pct:.2f}%, глубина=${avg_depth:,.0f}")

                        if spread_pct > self.MAX_SPREAD_PCT:
                            metrics.fail_reason = f"Спред {spread_pct:.2f}% > {self.MAX_SPREAD_PCT}%"
                            logger.warning(f"❌ {symbol}: слишком большой спред: {spread_pct:.2f}%")
                            return metrics

                        if avg_depth < self.MIN_ORDERBOOK_DEPTH:
                            metrics.fail_reason = f"Глубина стакана ${avg_depth:,.0f} < ${self.MIN_ORDERBOOK_DEPTH:,.0f}"
                            logger.warning(f"❌ {symbol}: недостаточная глубина: ${avg_depth:,.0f}")
                            return metrics
                    else:
                        logger.warning(f"⚠️ {symbol}: пустой или некорректный стакан")

                except Exception as e:
                    logger.warning(f"⚠️ Не удалось проверить стакан для {symbol}: {e}")
                    # Не считаем это критической ошибкой для быстрой фильтрации

            # 5. Дополнительные проверки (если есть данные)
            if 'count' in ticker:
                try:
                    count_str = str(ticker['count']).replace(',', '')
                    metrics.number_of_trades = int(float(count_str))
                    logger.debug(f"📊 {symbol}: сделок={metrics.number_of_trades}")
                except (ValueError, TypeError) as e:
                    logger.debug(f"⚠️ Не удалось получить количество сделок для {symbol}: {e}")
                    # Не критично

            # 6. Все проверки пройдены
            metrics.passed = True
            metrics.last_updated = datetime.now()

            logger.info(f"✅ {symbol} прошел ликвидность: ${volume_usd:,.0f}, ${last_price:.2f}")

            return metrics

        except Exception as e:
            metrics.fail_reason = f"Исключение: {str(e)[:100]}"
            logger.error(f"❌ Общая ошибка проверки {symbol}: {e}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return metrics

    def _get_cached_metrics(self, symbol: str) -> Optional[LiquidityMetrics]:
        """Получение метрик из кэша если они свежие"""
        if symbol not in self._checked_symbols:
            return None

        cached = self._checked_symbols[symbol]
        cache_age = datetime.now() - cached.last_updated

        if cache_age < timedelta(hours=self._cache_ttl_hours):
            logger.debug(f"♻️ Использую кэш для {symbol} (возраст: {cache_age.total_seconds():.0f} сек)")
            return cached

        # Кэш устарел, удаляем
        del self._checked_symbols[symbol]
        return None

    def _clean_old_cache(self):
        """Очистка устаревшего кэша"""
        now = datetime.now()
        expired_symbols = []

        for symbol, metrics in self._checked_symbols.items():
            cache_age = now - metrics.last_updated
            if cache_age >= timedelta(hours=self._cache_ttl_hours):
                expired_symbols.append(symbol)

        for symbol in expired_symbols:
            del self._checked_symbols[symbol]

        if expired_symbols:
            logger.debug(f"🧹 Очищен кэш для {len(expired_symbols)} символов")

    def get_metrics_for_symbol(self, symbol: str) -> Optional[LiquidityMetrics]:
        """Получение метрик для конкретного символа"""
        return self._checked_symbols.get(symbol)

    def get_cache_stats(self) -> Dict[str, any]:
        """Статистика кэша"""
        return {
            'cache_size': len(self._checked_symbols),
            'symbols': list(self._checked_symbols.keys()),
            'passed_count': sum(1 for m in self._checked_symbols.values() if m.passed)
        }

    async def quick_check(self, symbol: str) -> bool:
        """Быстрая проверка одного символа (без детальных метрик)"""
        try:
            # Пропускаем черный список
            if symbol in self.BLACKLIST:
                return False

            # Проверяем кэш
            cached = self._get_cached_metrics(symbol)
            if cached:
                return cached.passed

            # Быстрая проверка по тикеру
            ticker = await self.api.get_24h_ticker(symbol)
            if not ticker:
                return False

            # Парсим данные (упрощенная версия)
            try:
                volume_str = str(ticker.get('volume', '0')).replace(',', '')
                volume = float(volume_str) if volume_str else 0.0

                last_price_str = str(ticker.get('lastPrice', '0')).replace(',', '')
                last_price = float(last_price_str) if last_price_str else 0.0

                volume_usd = volume * last_price
            except (ValueError, TypeError):
                return False

            # Только базовые проверки для скорости
            if volume_usd < self.MIN_24H_VOLUME_USD:
                return False

            if last_price < self.MIN_PRICE:
                return False

            # Сохраняем в кэш простой результат
            simple_metrics = LiquidityMetrics(
                symbol=symbol,
                volume_24h_usd=volume_usd,
                current_price=last_price,
                passed=True
            )
            self._checked_symbols[symbol] = simple_metrics

            return True

        except Exception as e:
            logger.error(f"Ошибка быстрой проверки {symbol}: {e}")
            return False


# Экспорт для импорта в другие модули
__all__ = ['LiquidityPrefilter', 'PrefilterResult', 'LiquidityMetrics']
