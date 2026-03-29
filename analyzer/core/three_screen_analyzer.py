# analyzer/core/three_screen_analyzer.py (ПОЛНОСТЬЮ - С DATAPROVIDER)

import logging
from datetime import datetime
from typing import List, Dict, Optional, Any

from .screen1_trend_analyzer import Screen1TrendAnalyzer, Screen1Result
from .screen2_entry_zones import Screen2Analyzer
from .data_classes import Screen2Result
from .screen3_signal_generator import Screen3SignalGenerator, Screen3Result
from .event_bus import EventType, event_bus
from .data_classes import ThreeScreenAnalysis

logger = logging.getLogger('three_screen_analyzer')


class ThreeScreenAnalyzer:

    def __init__(self, config, data_provider=None):
        """
        Args:
            config: Конфигурация
            data_provider: Экземпляр DataProvider (опционально, используется глобальный)
        """
        self.config = config
        self.data_provider = data_provider or globals().get('data_provider')
        if self.data_provider is None:
            from analyzer.core.data_provider import data_provider as global_dp
            self.data_provider = global_dp

        analysis_config = config.get('analysis', {})
        caching_config = analysis_config.get('caching', {})

        self.signal_types_config = analysis_config.get('signal_types', {})
        self.m15_config = self.signal_types_config.get('m15', {})
        self.max_slippage_pct = self.m15_config.get('max_slippage_pct', 1.0)

        self.screen1_analyzer = Screen1TrendAnalyzer(config)
        self.screen2_analyzer = Screen2Analyzer(config)
        self.screen3_analyzer = Screen3SignalGenerator(config)

        self._calculation_cache = {}
        self._cache_max_size = caching_config.get('calculation_cache_size', 100)
        self._cache_hits = 0
        self._cache_misses = 0

        self._initialized = False
        self._analysis_start_time = None

        logger.info(f"✅ ThreeScreenAnalyzer создан (Фаза 1.3.7) — использует DataProvider")

    def _get_cache_key(self, symbol: str, timeframe: str, calculation_type: str) -> str:
        return f"{symbol}_{timeframe}_{calculation_type}"

    def _get_cached_calculation(self, cache_key: str) -> Any:
        if cache_key in self._calculation_cache:
            self._cache_hits += 1
            return self._calculation_cache[cache_key]
        self._cache_misses += 1
        return None

    def _set_cached_calculation(self, cache_key: str, value: Any) -> None:
        if len(self._calculation_cache) >= self._cache_max_size:
            oldest_key = next(iter(self._calculation_cache))
            del self._calculation_cache[oldest_key]
        self._calculation_cache[cache_key] = value

    def get_cache_stats(self) -> Dict[str, Any]:
        total = self._cache_hits + self._cache_misses
        return {
            'cache_size': len(self._calculation_cache),
            'cache_hits': self._cache_hits,
            'cache_misses': self._cache_misses,
            'hit_ratio': self._cache_hits / total if total > 0 else 0,
        }

    async def initialize(self) -> bool:
        logger.info("🚀 Начало инициализации ThreeScreenAnalyzer")

        try:
            self._initialized = True
            logger.info("✅ ThreeScreenAnalyzer успешно инициализирован")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации ThreeScreenAnalyzer: {e}")
            return False

    def _validate_klines_data(self, klines: List, timeframe: str) -> bool:
        try:
            if not klines or len(klines) == 0:
                return False

            for i, kline in enumerate(klines[:3]):
                if len(kline) < 7:
                    return False

                try:
                    open_price = float(kline[1])
                    high_price = float(kline[2])
                    low_price = float(kline[3])
                    close_price = float(kline[4])

                    if any(p <= 0 for p in [open_price, high_price, low_price, close_price] if p != 0):
                        return False

                    if high_price < low_price:
                        return False

                except (ValueError, TypeError, IndexError):
                    return False

            return True

        except Exception as e:
            logger.error(f"❌ Ошибка валидации данных {timeframe}: {e}")
            return False

    async def _get_klines_for_analysis(self, symbol: str) -> Dict[str, List]:
        logger.info(f"🔍 Получение данных для {symbol} через DataProvider")

        try:
            analysis_config = self.config.get('analysis', {})
            orchestration_config = analysis_config.get('orchestration', {})
            kline_limits = orchestration_config.get('kline_limits', {})
            min_timeframes = orchestration_config.get('min_timeframes_for_analysis', 3)

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
                    klines = await self.data_provider.get_klines(symbol, tf, limit)

                    if not self._validate_klines_data(klines, tf):
                        continue

                    klines_data[tf] = klines
                    successful_timeframes += 1

                except Exception as e:
                    logger.error(f"❌ Ошибка получения данных {symbol} {tf}: {e}")
                    continue

            if successful_timeframes < min_timeframes:
                logger.error(f"❌ Недостаточно данных для анализа {symbol}: {successful_timeframes}/{len(timeframes)}")
                return {}

            logger.info(f"✅ Успешно получены данные для {symbol}: {successful_timeframes}/{len(timeframes)}")
            return klines_data

        except Exception as e:
            logger.error(f"❌ Ошибка получения данных для {symbol}: {e}")
            return {}

    async def analyze_symbol(self, symbol: str) -> Optional[ThreeScreenAnalysis]:
        logger.info(f"🚀 Начинаем трехэкранный анализ для {symbol}")

        self._analysis_start_time = datetime.now()

        if not self._initialized:
            init_success = await self.initialize()
            if not init_success:
                logger.error(f"❌ ThreeScreenAnalyzer не инициализирован для {symbol}")
                return None

        try:
            klines_data = await self._get_klines_for_analysis(symbol)
            if not klines_data:
                logger.warning(f"Не удалось получить данные для {symbol}")
                return None

            screen1_result = await self._analyze_screen1(symbol, klines_data)

            if not screen1_result.passed:
                return await self._create_final_analysis(symbol, screen1_result)

            screen2_result = await self._analyze_screen2(symbol, klines_data, screen1_result)

            if not screen2_result.passed:
                return await self._create_final_analysis(symbol, screen1_result, screen2_result)

            screen3_result = await self._analyze_screen3(symbol, klines_data, screen1_result, screen2_result)

            if screen3_result and screen3_result.passed:
                signal_subtype = getattr(screen3_result, 'signal_subtype', 'M15')

                if signal_subtype == 'M15':
                    current_price = await self.data_provider.get_current_price(symbol, force_refresh=True)
                    if current_price:
                        deviation_pct = abs(
                            current_price - screen3_result.entry_price) / screen3_result.entry_price * 100

                        if deviation_pct > self.max_slippage_pct:
                            logger.warning(
                                f"⚠️ M15 сигнал для {symbol} отклонён: отклонение {deviation_pct:.2f}% > {self.max_slippage_pct}%"
                            )
                            screen3_result.passed = False
                            screen3_result.rejection_reason = f"Отклонение цены {deviation_pct:.2f}% > {self.max_slippage_pct}%"

            return await self._create_final_analysis(symbol, screen1_result, screen2_result, screen3_result)

        except Exception as e:
            logger.error(f"Ошибка при анализе {symbol}: {str(e)}")
            await event_bus.publish(EventType.ERROR_OCCURRED, {
                "error": str(e),
                "symbol": symbol,
                "function": "analyze_symbol"
            }, source="three_screen_analyzer")
            return None

    async def _analyze_screen1(self, symbol: str, klines_data: Dict) -> Screen1Result:
        d1_klines = klines_data.get('1d', [])
        return self.screen1_analyzer.analyze_daily_trend(symbol, d1_klines)

    async def _analyze_screen2(self, symbol: str, klines_data: Dict,
                               screen1_result: Screen1Result) -> Screen2Result:
        """Запуск анализа второго экрана"""
        h4_klines = klines_data.get('4h', [])
        h1_klines = klines_data.get('1h', [])

        # Преобразование: список списков → список словарей
        def convert_klines(klines_list):
            if not klines_list:
                return []
            result = []
            for k in klines_list:
                result.append({
                    'timestamp': k[0],
                    'open': float(k[1]),
                    'high': float(k[2]),
                    'low': float(k[3]),
                    'close': float(k[4]),
                    'volume': float(k[5]) if len(k) > 5 else 0
                })
            return result

        h4_data = convert_klines(h4_klines)

        # Получаем текущую цену
        current_price = screen1_result.indicators.get('current_price', 0)
        if current_price == 0 and h4_data:
            current_price = h4_data[-1]['close']

        # Вызываем analyze
        result = self.screen2_analyzer.analyze(
            h4_data, screen1_result.trend_direction, current_price, symbol
        )

        # Преобразуем результат в Screen2Result
        screen2 = Screen2Result()
        screen2.passed = result.get('success', False)
        screen2.confidence = result.get('score', 0) / 5.0 if result.get('score') else 0
        screen2.zone_low = result.get('zone_low', 0)
        screen2.zone_high = result.get('zone_high', 0)
        screen2.screen2_score = result.get('score', 0)
        screen2.expected_pattern = result.get('expected_pattern', '')
        screen2.rejection_reason = result.get('reason', '')

        return screen2

    async def _analyze_screen3(self, symbol: str, klines_data: Dict,
                               screen1_result: Screen1Result,
                               screen2_result: Screen2Result) -> Screen3Result:
        m15_klines = klines_data.get('15m', [])
        m5_klines = klines_data.get('5m', [])

        # Преобразование для screen3
        def convert_klines(klines_list):
            if not klines_list:
                return []
            result = []
            for k in klines_list:
                result.append([
                    k[0], float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]) if len(k) > 5 else 0
                ])
            return result

        m15_converted = convert_klines(m15_klines)
        m5_converted = convert_klines(m5_klines)

        return self.screen3_analyzer.generate_signal(
            symbol, m15_converted, m5_converted, screen1_result, screen2_result
        )

    async def _create_final_analysis(self, symbol: str, screen1: Screen1Result,
                                     screen2: Optional[Screen2Result] = None,
                                     screen3: Optional[Screen3Result] = None) -> ThreeScreenAnalysis:
        logger.info(f"Создание финального анализа для {symbol}")

        analysis_duration = 0.0
        if self._analysis_start_time:
            analysis_duration = (datetime.now() - self._analysis_start_time).total_seconds()

        analysis = ThreeScreenAnalysis(
            symbol=symbol,
            screen1=screen1,
            screen2=screen2 or Screen2Result(),
            screen3=screen3 or Screen3Result()
        )

        if screen2:
            analysis.zone_low = getattr(screen2, 'zone_low', 0.0)
            analysis.zone_high = getattr(screen2, 'zone_high', 0.0)
            analysis.expected_pattern = getattr(screen2, 'expected_pattern', '')
            analysis.screen2_score = getattr(screen2, 'screen2_score', 0)

        confidences = [screen1.confidence_score]
        if screen2 and screen2.passed:
            confidences.append(screen2.confidence)
        if screen3 and screen3.passed:
            confidences.append(screen3.confidence)

        analysis.overall_confidence = sum(confidences) / len(confidences) if confidences else 0.0

        should_trade = all([
            screen1.passed,
            screen2.passed if screen2 else False,
            screen3.passed if screen3 else False
        ])

        if should_trade and screen3:
            rr_ratio = screen3.indicators.get('risk_reward_ratio', 0)
            risk_pct = screen3.indicators.get('risk_pct', 0)

            analysis_config = self.config.get('analysis', {})
            min_rr_ratio = self.m15_config.get('min_rr_ratio', 3.0)
            max_risk_per_trade_pct = analysis_config.get('max_risk_per_trade_pct', 2.0)

            if rr_ratio < (min_rr_ratio - 0.01):
                should_trade = False
                screen3.passed = False
                screen3.rejection_reason = f"R/R {rr_ratio:.2f}:1 < {min_rr_ratio}:1"
            elif risk_pct > max_risk_per_trade_pct:
                should_trade = False
                screen3.passed = False
                screen3.rejection_reason = f"Риск {risk_pct:.1f}% > {max_risk_per_trade_pct}%"

        analysis.should_trade = should_trade

        status = "✅ ПРОШЕЛ ВСЕ ЭКРАНЫ" if analysis.should_trade else "❌ ОСТАНОВЛЕН"

        if screen3 and analysis.should_trade:
            rr = screen3.indicators.get('risk_reward_ratio', 0)
            risk = screen3.indicators.get('risk_pct', 0)
            logger.info(f"{status} {symbol} - Уверенность: {analysis.overall_confidence:.1%}, "
                        f"R/R: {rr:.2f}:1, Риск: {risk:.1f}%, Время: {analysis_duration:.2f} сек")
        else:
            rejection_msg = getattr(screen3, 'rejection_reason', '') if screen3 else ''
            if rejection_msg:
                logger.info(f"{status} {symbol} - Причина: {rejection_msg}")
            else:
                logger.info(f"{status} {symbol} - Время: {analysis_duration:.2f} сек")

        if screen3 and screen3.passed and should_trade:
            try:
                from .signal_repository import signal_repository

                signal_id = await signal_repository.save_signal(analysis)
                if signal_id:
                    logger.info(f"✅ M15 сигнал {symbol} сохранен (ID: {signal_id})")

                    await event_bus.publish(
                        EventType.TRADING_SIGNAL_GENERATED,
                        {
                            'signal_id': signal_id,
                            'symbol': symbol,
                            'signal_type': screen3.signal_type,
                            'entry_price': screen3.entry_price,
                            'stop_loss': screen3.stop_loss,
                            'take_profit': screen3.take_profit,
                            'confidence': analysis.overall_confidence,
                            'risk_reward_ratio': screen3.indicators.get('risk_reward_ratio', 0),
                            'signal_subtype': 'M15',
                            'order_type': 'MARKET',
                            'expiration_time': screen3.expiration_time.isoformat() if screen3.expiration_time else None
                        },
                        'three_screen_analyzer'
                    )
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения сигнала {symbol}: {e}")

        return analysis

    async def create_signal_from_analysis(self, analysis: ThreeScreenAnalysis):
        logger.info(f"Преобразование анализа в Signal для {analysis.symbol}")

        try:
            from .data_classes import Signal, Direction, SignalStatus

            if not analysis or not analysis.should_trade:
                return None

            if analysis.screen3.signal_type == "BUY":
                direction = Direction.LONG
            elif analysis.screen3.signal_type == "SELL":
                direction = Direction.SHORT
            else:
                return None

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

            return signal

        except Exception as e:
            logger.error(f"❌ Ошибка создания Signal: {e}")
            return None


__all__ = ['ThreeScreenAnalyzer', 'ThreeScreenAnalysis']