# core/three_screen_analyzer.py
"""
🎯 КООРДИНАТОР ТРЕХЭКРАННОГО АНАЛИЗА (С ХРАНЕНИЕМ В БД)
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any

from .screen1_trend_analyzer import Screen1TrendAnalyzer
from .screen2_entry_zones import Screen2EntryZonesAnalyzer
from .screen3_signal_generator import Screen3SignalGenerator
from .event_bus import EventType, event_bus
from .data_classes import (
    Screen1Result, Screen2Result, Screen3Result,
    ThreeScreenAnalysis
)

logger = logging.getLogger('three_screen_analyzer')


class ThreeScreenAnalyzer:
    """
    🎯 Главный координатор трехэкранного анализа (с поддержкой хранения)
    """

    def __init__(self, api_client, config):
        self.api_client = api_client
        self.config = config

        # Получаем параметры из конфига
        analysis_config = config.get('analysis', {})
        caching_config = analysis_config.get('caching', {})

        # Создаем анализаторы для каждого экрана
        self.screen1_analyzer = Screen1TrendAnalyzer(config)
        self.screen2_analyzer = Screen2EntryZonesAnalyzer(config)
        self.screen3_analyzer = Screen3SignalGenerator(config)

        # Кэш для расчетов
        self._calculation_cache = {}
        self._cache_max_size = caching_config.get('calculation_cache_size', 100)
        self._cache_hits = 0
        self._cache_misses = 0

        self._initialized = False
        self._analysis_start_time = None

        logger.info(f"✅ ThreeScreenAnalyzer создан с раздельными анализаторами")

    def _get_cache_key(self, symbol: str, timeframe: str, calculation_type: str) -> str:
        key = f"{symbol}_{timeframe}_{calculation_type}"
        logger.debug(f"Сгенерирован ключ кэша: {key}")
        return key

    def _get_cached_calculation(self, cache_key: str) -> Any:
        if cache_key in self._calculation_cache:
            self._cache_hits += 1
            logger.debug(f"🚀 Кэш попадание: {cache_key}")
            return self._calculation_cache[cache_key]
        self._cache_misses += 1
        logger.debug(f"❌ Кэш промах: {cache_key}")
        return None

    def _set_cached_calculation(self, cache_key: str, value: Any) -> None:
        if len(self._calculation_cache) >= self._cache_max_size:
            oldest_key = next(iter(self._calculation_cache))
            del self._calculation_cache[oldest_key]
            logger.debug(f"🧹 Очистка кэша: {oldest_key}")

        self._calculation_cache[cache_key] = value
        logger.debug(f"💾 Сохранено в кэш: {cache_key}")

    def get_cache_stats(self) -> Dict[str, Any]:
        total = self._cache_hits + self._cache_misses
        stats = {
            'cache_size': len(self._calculation_cache),
            'cache_hits': self._cache_hits,
            'cache_misses': self._cache_misses,
            'hit_ratio': self._cache_hits / total if total > 0 else 0,
        }
        logger.debug(f"Статистика кэша: {stats}")
        return stats

    async def initialize(self) -> bool:
        """Инициализация анализатора"""
        logger.info("🚀 Начало инициализации ThreeScreenAnalyzer")

        try:
            # Инициализация API клиента если нужно
            if hasattr(self.api_client, 'initialize') and not getattr(self.api_client, '_initialized', False):
                logger.debug("Инициализация API клиента...")
                init_success = await self.api_client.initialize()
                if not init_success:
                    logger.error("❌ Не удалось инициализировать API клиент")
                    return False
                logger.debug("✅ API клиент инициализирован")

            self._initialized = True
            logger.info("✅ ThreeScreenAnalyzer успешно инициализирован")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации ThreeScreenAnalyzer: {e}")
            return False

    def _validate_klines_data(self, klines: List, timeframe: str) -> bool:
        """Валидация данных свечей"""
        logger.debug(f"Валидация данных {timeframe}")

        try:
            if not klines or len(klines) == 0:
                logger.warning(f"⚠️ Пустые данные klines для таймфрейма {timeframe}")
                return False

            # Проверяем структуру свечей
            for i, kline in enumerate(klines[:3]):  # Проверяем только первые 3 для скорости
                if len(kline) < 7:
                    logger.warning(f"⚠️ Неполная структура klines {i} для {timeframe}: "
                                   f"{len(kline)} полей вместо 7")
                    return False

                try:
                    open_price = float(kline[1])
                    high_price = float(kline[2])
                    low_price = float(kline[3])
                    close_price = float(kline[4])
                    volume = float(kline[5])

                    # Проверка корректности цен
                    if any(p <= 0 for p in [open_price, high_price, low_price, close_price] if p != 0):
                        logger.warning(f"⚠️ Некорректная цена в свече {i}: "
                                       f"O={open_price}, H={high_price}, L={low_price}, C={close_price}")
                        return False

                    if high_price < low_price:
                        logger.warning(f"⚠️ High цена меньше Low в свече {i}: {high_price} < {low_price}")
                        return False

                    if high_price < max(open_price, close_price):
                        logger.warning(f"⚠️ High цена меньше Open/Close в свече {i}: "
                                       f"{high_price} < {max(open_price, close_price)}")
                        return False

                    if low_price > min(open_price, close_price):
                        logger.warning(f"⚠️ Low цена больше Open/Close в свече {i}: "
                                       f"{low_price} > {min(open_price, close_price)}")
                        return False

                    # Проверка объема (не критично, но логируем)
                    if volume < 0:
                        logger.debug(f"⚠️ Отрицательный объем в свече {i}: {volume}")

                except (ValueError, TypeError, IndexError) as e:
                    logger.warning(f"⚠️ Ошибка преобразования данных свечи {i}: {e}")
                    return False

            # ✅ ДОБАВЛЕННАЯ ПРОВЕРКА: проверяем, что есть движение цены
            if len(klines) >= 10:
                try:
                    # Берем последние 10 свечей для проверки волатильности
                    recent_klines = klines[-10:]
                    closes = [float(k[4]) for k in recent_klines]

                    if len(closes) >= 2:
                        price_range = max(closes) - min(closes)
                        avg_price = sum(closes) / len(closes)

                        # Проверяем, что цена не заморожена
                        if avg_price > 0:
                            volatility_pct = price_range / avg_price * 100

                            if volatility_pct < 0.01:  # Движение меньше 0.01%
                                logger.warning(
                                    f"⚠️ Подозрительно маленькое движение цены {timeframe}: {volatility_pct:.3f}%")
                                logger.warning(f"   Диапазон цен: {min(closes):.6f} - {max(closes):.6f}")
                                logger.warning(f"   Последние цены: {closes[-3:]}")

                                # Для M5/M15 можем быть более строгими
                                if timeframe in ['5m', '15m'] and volatility_pct < 0.005:
                                    logger.warning(f"❌ Отклоняем данные {timeframe}: цена не двигается")
                                    return False

                            logger.debug(f"✅ Волатильность {timeframe}: {volatility_pct:.3f}%")

                            # Проверяем, что последняя цена отличается от предыдущей
                            if len(closes) >= 2:
                                last_price_change = abs(closes[-1] - closes[-2]) / closes[-2] * 100
                                if last_price_change < 0.001:  # Меньше 0.001%
                                    logger.debug(
                                        f"⚠️ Последняя цена не изменилась на {timeframe}: {last_price_change:.4f}%")
                        else:
                            logger.warning(f"⚠️ Средняя цена равна 0 для {timeframe}")

                except Exception as e:
                    logger.warning(f"⚠️ Ошибка проверки волатильности {timeframe}: {e}")
                    # Не считаем это критической ошибкой, продолжаем

            # Проверка на одинаковые свечи (возможная ошибка данных)
            if len(klines) >= 5:
                try:
                    sample_klines = klines[:5]
                    # Проверяем, что все цены открытия не одинаковые
                    opens = [float(k[1]) for k in sample_klines]
                    if len(set(opens)) == 1:
                        logger.warning(f"⚠️ Все цены открытия одинаковые для {timeframe}: {opens[0]}")
                        # Это может быть ошибка API

                    # Проверяем, что есть изменение цены
                    price_changes = []
                    for i in range(1, min(5, len(klines))):
                        prev_close = float(klines[-i][4])
                        current_close = float(klines[-i - 1][4]) if len(klines) > i else prev_close
                        if prev_close > 0:
                            change = abs(current_close - prev_close) / prev_close * 100
                            price_changes.append(change)

                    if price_changes and max(price_changes) < 0.001:  # Меньше 0.001%
                        logger.warning(
                            f"⚠️ Очень маленькие изменения цены на {timeframe}: max {max(price_changes):.4f}%")

                except Exception as e:
                    logger.debug(f"⚠️ Ошибка дополнительной проверки данных {timeframe}: {e}")

            logger.debug(f"✅ Данные {timeframe} прошли валидацию")
            return True

        except Exception as e:
            logger.error(f"❌ Критическая ошибка валидации данных {timeframe}: {e}")
            return False

    async def _get_klines_for_analysis(self, symbol: str) -> Dict[str, List]:
        """Получение данных с API для всех таймфреймов"""
        logger.info(f"🔍 Получение данных для {symbol}")

        try:
            # Получаем параметры из конфига
            analysis_config = self.config.get('analysis', {})
            orchestration_config = analysis_config.get('orchestration', {})
            kline_limits = orchestration_config.get('kline_limits', {})
            min_timeframes = orchestration_config.get('min_timeframes_for_analysis', 3)

            # Используем лимиты из конфига
            timeframes = {
                '1d': kline_limits.get('1d', 100),
                '4h': kline_limits.get('4h', 50),
                '1h': kline_limits.get('1h', 50),
                '15m': kline_limits.get('15m', 30),
                '5m': kline_limits.get('5m', 30)
            }

            klines_data = {}
            successful_timeframes = 0

            for tf, limit in timeframes.items():
                try:
                    logger.debug(f"🔍 Запрос данных {symbol} {tf}, лимит: {limit}")

                    klines = await self.api_client.get_klines(
                        symbol=symbol,
                        interval=tf,
                        limit=limit
                    )

                    if not self._validate_klines_data(klines, tf):
                        logger.warning(f"❌ Данные {symbol} {tf} не прошли валидацию")
                        continue

                    klines_data[tf] = klines
                    successful_timeframes += 1
                    logger.debug(f"✅ Получено {len(klines)} валидных свечей {tf} для {symbol}")

                except Exception as e:
                    logger.error(f"❌ Ошибка получения данных {symbol} {tf}: {e}")
                    continue

            if successful_timeframes < min_timeframes:
                logger.error(f"❌ Недостаточно данных для анализа {symbol}: "
                             f"{successful_timeframes}/{len(timeframes)} таймфреймов (мин: {min_timeframes})")
                return {}

            logger.info(f"✅ Успешно получены данные для {symbol}: "
                        f"{successful_timeframes}/{len(timeframes)} таймфреймов")
            return klines_data

        except Exception as e:
            logger.error(f"❌ Критическая ошибка получения данных для {symbol}: {e}")
            return {}

    async def analyze_symbol(self, symbol: str) -> Optional[ThreeScreenAnalysis]:
        """Основной метод анализа символа"""
        logger.info(f"🚀 Начинаем трехэкранный анализ для {symbol}")

        # Засекаем время начала анализа
        self._analysis_start_time = datetime.now()

        if not self._initialized:
            logger.warning("Анализатор не инициализирован, выполняю инициализацию...")
            init_success = await self.initialize()
            if not init_success:
                logger.error(f"❌ ThreeScreenAnalyzer не инициализирован для {symbol}")
                return None

        try:
            # Получаем данные для всех таймфреймов
            klines_data = await self._get_klines_for_analysis(symbol)
            if not klines_data:
                logger.warning(f"Не удалось получить данные для {symbol}")
                return None

            # ЭКРАН 1 - Анализ тренда
            logger.info(f"📊 ЗАПУСК ЭКРАН 1 для {symbol}")
            screen1_result = await self._analyze_screen1(symbol, klines_data)
            if not screen1_result.passed:
                logger.info(f"❌ {symbol} не прошел ЭКРАН 1 (тренд)")
                return await self._create_final_analysis(symbol, screen1_result)

            # ЭКРАН 2 - Зоны входа
            logger.info(f"🎯 ЗАПУСК ЭКРАН 2 для {symbol}")
            screen2_result = await self._analyze_screen2(symbol, klines_data, screen1_result)
            if not screen2_result.passed:
                logger.info(f"❌ {symbol} не прошел ЭКРАН 2 (зоны входа)")
                return await self._create_final_analysis(symbol, screen1_result, screen2_result)

            # ЭКРАН 3 - Сигналы
            logger.info(f"⚡ ЗАПУСК ЭКРАН 3 для {symbol}")
            screen3_result = await self._analyze_screen3(symbol, klines_data, screen1_result, screen2_result)

            return await self._create_final_analysis(symbol, screen1_result, screen2_result, screen3_result)

        except Exception as e:
            logger.error(f"Ошибка при анализе {symbol}: {str(e)}")

            asyncio.create_task(event_bus.publish(EventType.ERROR_OCCURRED, {
                "error": str(e),
                "symbol": symbol,
                "function": "analyze_symbol"
            }, source="three_screen_analyzer"))

            return None

    async def _analyze_screen1(self, symbol: str, klines_data: Dict) -> Screen1Result:
        """Запуск анализа первого экрана"""
        d1_klines = klines_data.get('1d', [])
        return self.screen1_analyzer.analyze_daily_trend(symbol, d1_klines)

    async def _analyze_screen2(self, symbol: str, klines_data: Dict,
                               screen1_result: Screen1Result) -> Screen2Result:
        """Запуск анализа второго экрана"""
        h4_klines = klines_data.get('4h', [])
        h1_klines = klines_data.get('1h', [])
        return self.screen2_analyzer.analyze_entry_zones(
            symbol, h4_klines, h1_klines, screen1_result.trend_direction
        )

    async def _analyze_screen3(self, symbol: str, klines_data: Dict,
                               screen1_result: Screen1Result,
                               screen2_result: Screen2Result) -> Screen3Result:
        """Запуск анализа третьего экрана"""
        m15_klines = klines_data.get('15m', [])
        m5_klines = klines_data.get('5m', [])
        return self.screen3_analyzer.generate_signal(
            symbol, m15_klines, m5_klines, screen1_result, screen2_result
        )

    async def _create_final_analysis(self, symbol: str, screen1: Screen1Result,
                                     screen2: Optional[Screen2Result] = None,
                                     screen3: Optional[Screen3Result] = None) -> ThreeScreenAnalysis:
        """Создание финального анализа с расчетом времени"""
        logger.info(f"Создание финального анализа для {symbol}")

        # Рассчитываем время выполнения анализа
        analysis_duration = 0.0
        if self._analysis_start_time:
            analysis_duration = (datetime.now() - self._analysis_start_time).total_seconds()

        # ✅ ИСПРАВЛЕНО: убран параметр analysis_duration_seconds
        analysis = ThreeScreenAnalysis(
            symbol=symbol,
            screen1=screen1,
            screen2=screen2 or Screen2Result(),
            screen3=screen3 or Screen3Result()
        )

        # Расчет общей уверенности
        confidences = [screen1.confidence_score]
        if screen2 and screen2.passed:
            confidences.append(screen2.confidence)
        if screen3 and screen3.passed:
            confidences.append(screen3.confidence)

        analysis.overall_confidence = sum(confidences) / len(confidences) if confidences else 0.0

        # Проверка прохождения всех экранов
        should_trade = all([
            screen1.passed,
            screen2.passed if screen2 else False,
            screen3.passed if screen3 else False
        ])

        # Получаем параметры проверки качества из конфига
        analysis_config = self.config.get('analysis', {})
        min_rr_ratio = analysis_config.get('min_rr_ratio', 1.2)
        max_risk_per_trade_pct = analysis_config.get('max_risk_per_trade_pct', 2.0)

        # Дополнительная проверка качества для экрана 3
        if should_trade and screen3:
            rr_ratio = screen3.indicators.get('risk_reward_ratio', 0)
            risk_pct = screen3.indicators.get('risk_pct', 0)

            if rr_ratio < min_rr_ratio:
                logger.warning(f"❌ Сигнал {symbol} имеет плохой R/R: {rr_ratio:.2f}:1 (мин: {min_rr_ratio})")
                should_trade = False
            elif risk_pct > max_risk_per_trade_pct:
                logger.warning(
                    f"❌ Сигнал {symbol} имеет слишком большой риск: {risk_pct:.1f}% (макс: {max_risk_per_trade_pct}%)")
                should_trade = False

        analysis.should_trade = should_trade

        status = "✅ ПРОШЕЛ ВСЕ ЭКРАНЫ" if analysis.should_trade else "❌ ОСТАНОВЛЕН"

        if screen3 and analysis.should_trade:
            rr = screen3.indicators.get('risk_reward_ratio', 0)
            risk = screen3.indicators.get('risk_pct', 0)
            logger.info(f"{status} {symbol} - Уверенность: {analysis.overall_confidence:.1%}, "
                        f"R/R: {rr:.2f}:1, Риск: {risk:.1f}%, Время: {analysis_duration:.2f} сек")
        else:
            logger.info(f"{status} {symbol} - Общая уверенность: {analysis.overall_confidence:.1%}, "
                        f"Время: {analysis_duration:.2f} сек")

        # ✅ СОХРАНЯЕМ В БД ЕСЛИ СИГНАЛ ПРОШЕЛ ТРЕТИЙ ЭКРАН
        try:
            from .signal_repository import signal_repository

            # Сохраняем ВСЕ сигналы, прошедшие экран 3
            if screen3 and screen3.passed:
                logger.info(f"💾 Сохраняем сигнал {symbol} в БД...")
                signal_id = await signal_repository.save_signal(analysis)
                if signal_id:
                    logger.info(f"✅ Сигнал {symbol} сохранен (ID: {signal_id})")
                else:
                    logger.warning(f"⚠️ Не удалось сохранить сигнал {symbol}")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения сигнала {symbol}: {e}")

        return analysis

    async def create_signal_from_analysis(self, analysis: ThreeScreenAnalysis):
        """Создание торгового сигнала из анализа (для совместимости)"""
        logger.info(f"Преобразование анализа в Signal для {analysis.symbol}")

        try:
            # Импортируем классы сигналов если они есть
            try:
                from core.data_classes import Signal, Direction, SignalStatus
            except ImportError:
                logger.warning("Модуль data_classes не найден, пропускаем создание Signal")
                return None

            if not analysis or not analysis.should_trade:
                logger.warning("Анализ не прошел или не должен торговаться")
                return None

            if analysis.screen3.signal_type == "BUY":
                direction = Direction.LONG
            elif analysis.screen3.signal_type == "SELL":
                direction = Direction.SHORT
            else:
                logger.warning(f"Неизвестный тип сигнала: {analysis.screen3.signal_type}")
                return None

            # Получаем параметры из конфига
            signal_generation_config = self.config.get('analysis', {}).get('signal_generation', {})

            default_position_size = signal_generation_config.get('default_position_size', 100)
            default_margin_mode = signal_generation_config.get('default_margin_mode', "cross")
            default_leverage = signal_generation_config.get('default_leverage', 10)
            default_total_capital = signal_generation_config.get('default_total_capital', 1000)

            signal = Signal(
                symbol=analysis.symbol,
                strategy="three_screen",
                direction=direction,
                status=SignalStatus.PENDING,
                confidence=analysis.overall_confidence,
                three_screen_analysis=analysis,
                entry_prices=[analysis.screen3.entry_price],
                stop_loss=analysis.screen3.stop_loss,
                take_profit_levels=[analysis.screen3.take_profit],
                position_size=default_position_size,
                margin_mode=default_margin_mode,
                leverage=default_leverage,
                total_capital=default_total_capital
            )

            logger.info(f"✅ Создан Signal для {analysis.symbol}: {direction.value}")
            return signal

        except Exception as e:
            logger.error(f"❌ Ошибка создания Signal: {e}")
            return None


# Экспорт для импорта в другие модули
__all__ = [
    'ThreeScreenAnalyzer',
    'ThreeScreenAnalysis',  # Экспортируем из data_classes
]