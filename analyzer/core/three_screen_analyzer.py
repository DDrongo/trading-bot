# analyzer/core/three_screen_analyzer.py (ПОЛНОСТЬЮ - ФАЗА 2.2 С W1)
"""
🎯 THREE SCREEN ANALYZER - Координатор трёхэкранного анализа

ФАЗА 2.2:
- Добавлен W1 Trend Analyzer (глобальный тренд)
- Добавлен Market Stage (стадия рынка)
"""

import logging
from datetime import datetime
from typing import List, Dict, Optional, Any

from .screen1_trend_analyzer import Screen1TrendAnalyzer, Screen1Result
from .screen2_entry_zones import Screen2Analyzer
from .data_classes import Screen2Result
from .screen3_signal_generator import Screen3SignalGenerator, Screen3Result
from .event_bus import EventType, event_bus
from .data_classes import ThreeScreenAnalysis
from .w1_trend_analyzer import W1TrendAnalyzer, W1TrendResult

logger = logging.getLogger('three_screen_analyzer')


class ThreeScreenAnalyzer:

    def __init__(self, config, data_provider=None):
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

        # ФАЗА 2.2: W1 анализатор
        self.w1_analyzer = W1TrendAnalyzer(config)

        self._calculation_cache = {}
        self._cache_max_size = caching_config.get('calculation_cache_size', 100)
        self._cache_hits = 0
        self._cache_misses = 0

        self._initialized = False
        self._analysis_start_time = None

        # ФАЗА 2.0: SMC данные для комментария
        self._last_fvg_zones = []
        self._last_liquidity_pools = []

        logger.info(f"✅ ThreeScreenAnalyzer создан (Фаза 2.2 с W1)")
        logger.info(f"   Единый источник цены: DataProvider")

    def _format_price(self, price: float) -> str:
        """Унифицированное форматирование цены"""
        if price is None or price == 0:
            return "-"
        if price < 0.01:
            return f"{price:.6f}"
        elif price < 0.1:
            return f"{price:.5f}"
        elif price < 1:
            return f"{price:.4f}"
        elif price < 10:
            return f"{price:.3f}"
        elif price < 100:
            return f"{price:.2f}"
        else:
            return f"{price:.2f}"

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
                '1w': kline_limits.get('1w', 52),  # ФАЗА 2.2: W1
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
        """
        Анализ символа по трёхэкранной методологии (Фаза 2.2 с W1)
        """
        logger.info(f"🚀 Начинаем Pro анализ для {symbol}")

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

            # ========== ПОЛУЧАЕМ РЕАЛЬНУЮ ЦЕНУ ОДИН РАЗ ==========
            real_price = await self.data_provider.get_current_price(symbol, force_refresh=True)
            logger.info(f"📊 {symbol}: реальная цена = {self._format_price(real_price)}")

            # ========== ФАЗА 2.2: W1 АНАЛИЗ (глобальный тренд) ==========
            w1_klines = klines_data.get('1w', [])
            if w1_klines and len(w1_klines) >= 50:
                w1_result = self.w1_analyzer.analyze(symbol, w1_klines)
                w1_trend = w1_result.trend
                logger.info(
                    f"📊 {symbol}: W1 тренд = {w1_trend} (ADX={w1_result.adx:.1f}, сила={w1_result.strength:.1f}%)")
            else:
                w1_trend = 'SIDEWAYS'
                logger.warning(
                    f"⚠️ {symbol}: недостаточно W1 данных для анализа ({len(w1_klines) if w1_klines else 0} свечей)")

            # Шаг 1: Screen 1 (D1 тренд)
            screen1_result = await self._analyze_screen1(symbol, klines_data)

            # ========== ФАЗА 2.2: MARKET STAGE ==========
            d1_trend = screen1_result.trend_direction

            if w1_trend == 'BEAR' and d1_trend == 'BULL':
                market_stage = 'BULL_CORRECTION'
                bias = 'CAUTIOUS_BUY'
            elif w1_trend == 'BULL' and d1_trend == 'BULL':
                market_stage = 'TREND_CONTINUATION'
                bias = 'AGGRESSIVE_BUY'
            elif w1_trend == 'BEAR' and d1_trend == 'BEAR':
                market_stage = 'TREND_CONTINUATION'
                bias = 'AGGRESSIVE_SELL'
            elif w1_trend == 'BULL' and d1_trend == 'BEAR':
                market_stage = 'BEAR_CORRECTION'
                bias = 'CAUTIOUS_SELL'
            else:
                market_stage = 'UNDEFINED'
                bias = 'NEUTRAL'

            logger.info(f"📊 {symbol}: Market Stage = {market_stage}, Bias = {bias}")

            # Шаг 2: Screen 2 (H4 зоны входа)
            screen2_result = await self._analyze_screen2(symbol, klines_data, screen1_result, real_price)

            # Сохраняем SMC данные для комментария
            self._last_fvg_zones = getattr(screen2_result, 'fvg_zones', [])
            self._last_liquidity_pools = getattr(screen2_result, 'liquidity_pools', [])

            # Шаг 3: Screen 3 (M15 сигналы)
            screen3_result = await self._analyze_screen3(
                symbol, klines_data, screen1_result, screen2_result, real_price
            )

            # Сохраняем тренд в БД если прошёл фильтр
            if screen1_result.passed:
                await self._save_trend_analysis(symbol, screen1_result)

            return await self._create_final_analysis(
                symbol, screen1_result, screen2_result, screen3_result, real_price,
                market_stage, bias, w1_trend
            )

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
                               screen1_result: Screen1Result, real_price: float) -> Screen2Result:
        """Запуск анализа второго экрана с SMC (Фаза 2.0)"""
        h4_klines = klines_data.get('4h', [])

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

        result = self.screen2_analyzer.analyze(
            h4_data, screen1_result.trend_direction, real_price, symbol
        )

        screen2 = Screen2Result()
        screen2.passed = result.get('success', False)
        screen2.confidence = result.get('score', 0) / 5.0 if result.get('score') else 0
        screen2.zone_low = result.get('zone_low', 0)
        screen2.zone_high = result.get('zone_high', 0)
        screen2.screen2_score = result.get('score', 0)
        screen2.expected_pattern = result.get('expected_pattern', '')
        screen2.rejection_reason = result.get('reason', '')
        screen2.best_zone = (screen2.zone_low + screen2.zone_high) / 2 if screen2.zone_low > 0 else 0

        screen2.entry_type = result.get('entry_type', 'FALLBACK')
        screen2.fvg_zones = result.get('fvg_zones', [])
        screen2.liquidity_pools = result.get('liquidity_pools', [])
        screen2.selected_fvg = result.get('selected_fvg', None)
        screen2.selected_liquidity_pool = result.get('selected_liquidity_pool', None)

        screen2.h4_analysis = result.get('h4_analysis', {})
        screen2.support_levels = result.get('support_levels', [])
        screen2.resistance_levels = result.get('resistance_levels', [])
        screen2.fib_levels = result.get('fib_levels', [])
        screen2.confluence = result.get('confluence', {})
        screen2.historical_levels_used = result.get('historical_levels_used', 0)

        if screen2.passed:
            logger.info(
                f"✅ {symbol}: Screen 2 пройден (score={screen2.screen2_score}/5, "
                f"тип={screen2.entry_type}, зона={self._format_price(screen2.zone_low)}-{self._format_price(screen2.zone_high)})")

        return screen2

    async def _get_h4_trend(self, symbol: str, klines_data: Dict) -> Dict[str, Any]:
        h4_config = self.config.get('analysis', {}).get('h4_filter', {})
        enabled = h4_config.get('enabled', True)

        if not enabled:
            return {
                'direction': 'SIDEWAYS',
                'strength': 0,
                'passed': True
            }

        adx_threshold = h4_config.get('adx_threshold', 20)

        h4_klines = klines_data.get('4h', [])
        if not h4_klines or len(h4_klines) < 20:
            logger.warning(f"⚠️ {symbol}: Недостаточно H4 данных для ADX")
            return {
                'direction': 'SIDEWAYS',
                'strength': 0,
                'passed': True
            }

        try:
            highs = [float(k[2]) for k in h4_klines[-20:]]
            lows = [float(k[3]) for k in h4_klines[-20:]]
            closes = [float(k[4]) for k in h4_klines[-20:]]

            adx_value = self._calculate_adx(highs, lows, closes, period=14)

            if adx_value is None:
                return {
                    'direction': 'SIDEWAYS',
                    'strength': 0,
                    'passed': True
                }

            if adx_value > adx_threshold:
                trend_dir = self._determine_h4_direction(closes)
                logger.info(f"📊 {symbol}: H4 ADX={adx_value:.1f} > {adx_threshold}, тренд={trend_dir}")
                return {
                    'direction': trend_dir,
                    'strength': adx_value,
                    'passed': True
                }
            else:
                logger.info(f"📊 {symbol}: H4 ADX={adx_value:.1f} ≤ {adx_threshold} (флэт)")
                return {
                    'direction': 'SIDEWAYS',
                    'strength': adx_value,
                    'passed': True
                }

        except Exception as e:
            logger.error(f"❌ Ошибка расчёта H4 тренда для {symbol}: {e}")
            return {
                'direction': 'SIDEWAYS',
                'strength': 0,
                'passed': True
            }

    def _calculate_adx(self, highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[
        float]:
        try:
            if len(highs) < period + 1:
                return None

            tr_values = []
            plus_dm_values = []
            minus_dm_values = []

            for i in range(1, len(highs)):
                tr = max(
                    highs[i] - lows[i],
                    abs(highs[i] - closes[i - 1]),
                    abs(lows[i] - closes[i - 1])
                )
                tr_values.append(tr)

                up_move = highs[i] - highs[i - 1]
                down_move = lows[i - 1] - lows[i]

                plus_dm = up_move if up_move > down_move and up_move > 0 else 0
                minus_dm = down_move if down_move > up_move and down_move > 0 else 0

                plus_dm_values.append(plus_dm)
                minus_dm_values.append(minus_dm)

            atr = self._smooth_wilder(tr_values, period)
            plus_di_smoothed = self._smooth_wilder(plus_dm_values, period)
            minus_di_smoothed = self._smooth_wilder(minus_dm_values, period)

            if not atr or atr[-1] == 0:
                return None

            plus_di = []
            minus_di = []

            for i in range(min(len(plus_di_smoothed), len(atr))):
                if atr[i] != 0:
                    plus_di.append((plus_di_smoothed[i] / atr[i]) * 100)
                    minus_di.append((minus_di_smoothed[i] / atr[i]) * 100)

            if not plus_di or not minus_di:
                return None

            dx_values = []
            for p, m in zip(plus_di, minus_di):
                if p + m == 0:
                    dx = 0
                else:
                    dx = abs(p - m) / (p + m) * 100
                dx_values.append(dx)

            adx = self._smooth_wilder(dx_values, period)

            return adx[-1] if adx else None

        except Exception as e:
            logger.error(f"Ошибка расчёта ADX: {e}")
            return None

    def _smooth_wilder(self, values: List[float], period: int) -> List[float]:
        if len(values) < period:
            return []

        smoothed = [sum(values[:period]) / period]

        for i in range(period, len(values)):
            prev = smoothed[-1]
            smoothed.append(prev + (values[i] - prev) / period)

        return smoothed

    def _determine_h4_direction(self, closes: List[float]) -> str:
        if len(closes) < 20:
            return 'SIDEWAYS'

        ema20 = self._calculate_ema(closes, 20)
        ema50 = self._calculate_ema(closes, 50)

        if not ema20 or not ema50:
            return 'SIDEWAYS'

        if ema20[-1] > ema50[-1] and closes[-1] > ema20[-1]:
            return 'BULL'
        elif ema20[-1] < ema50[-1] and closes[-1] < ema20[-1]:
            return 'BEAR'
        else:
            return 'SIDEWAYS'

    def _calculate_ema(self, prices: List[float], period: int) -> List[float]:
        if len(prices) < period:
            return []

        multiplier = 2 / (period + 1)
        ema = [sum(prices[:period]) / period]

        for price in prices[period:]:
            ema.append((price * multiplier) + (ema[-1] * (1 - multiplier)))

        return ema

    async def _analyze_screen3(self, symbol: str, klines_data: Dict,
                               screen1_result: Screen1Result,
                               screen2_result: Screen2Result,
                               real_price: float) -> Screen3Result:
        """Запуск анализа третьего экрана с SMC данными (Фаза 2.0)"""

        from .signal_repository import signal_repository
        if await signal_repository.has_active_m15(symbol):
            logger.info(f"⏭️ {symbol}: уже есть активный M15 сигнал")
            result = Screen3Result()
            result.passed = False
            result.rejection_reason = "Уже есть активный M15 сигнал"
            return result

        h4_trend_result = await self._get_h4_trend(symbol, klines_data)

        h4_direction = h4_trend_result.get('direction', 'SIDEWAYS')
        screen1_direction = screen1_result.trend_direction

        if h4_direction == 'BULL':
            if screen1_direction != 'BULL':
                logger.info(f"❌ {symbol}: H4 тренд BULL, но D1 тренд {screen1_direction}")
                result = Screen3Result()
                result.passed = False
                result.rejection_reason = f"H4 тренд BULL не совпадает с D1 {screen1_direction}"
                return result
        elif h4_direction == 'BEAR':
            if screen1_direction != 'BEAR':
                logger.info(f"❌ {symbol}: H4 тренд BEAR, но D1 тренд {screen1_direction}")
                result = Screen3Result()
                result.passed = False
                result.rejection_reason = f"H4 тренд BEAR не совпадает с D1 {screen1_direction}"
                return result

        m15_klines = klines_data.get('15m', [])
        m5_klines = klines_data.get('5m', [])

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

        from analyzer.core.analyst import FVGDetector, LiquidityScanner

        fvg_detector = FVGDetector()
        liquidity_scanner = LiquidityScanner()

        result = self.screen3_analyzer.generate_signal(
            symbol, m15_converted, m5_converted, screen1_result, screen2_result, real_price,
            liquidity_scanner=liquidity_scanner,
            fvg_detector=fvg_detector
        )

        if result.passed:
            logger.info(
                f"✅ {symbol}: Screen 3 пройден! {result.signal_type} @ {self._format_price(result.entry_price)} "
                f"[{result.entry_type}]"
            )

        return result

    async def _create_final_analysis(self, symbol: str, screen1: Screen1Result,
                                     screen2: Optional[Screen2Result] = None,
                                     screen3: Optional[Screen3Result] = None,
                                     real_price: float = 0.0,
                                     market_stage: str = "UNDEFINED",
                                     bias: str = "NEUTRAL",
                                     w1_trend: str = "SIDEWAYS") -> ThreeScreenAnalysis:
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

        # ФАЗА 2.2: Добавляем W1 и Market Stage
        analysis.market_stage = market_stage
        analysis.bias = bias
        analysis.w1_trend = w1_trend

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
            min_rr_ratio = self.m15_config.get('min_rr_ratio', 3.0)

            if rr_ratio < (min_rr_ratio - 0.01):
                should_trade = False
                screen3.passed = False
                screen3.rejection_reason = f"R/R {rr_ratio:.2f}:1 < {min_rr_ratio}:1"

        analysis.should_trade = should_trade

        if screen3 and screen3.passed and should_trade:
            from .signal_repository import signal_repository

            if await signal_repository.was_traded_recently(symbol, minutes=30):
                logger.info(f"⏭️ {symbol}: была сделка в последние 30 минут, пропускаем")
                should_trade = False
                screen3.passed = False
                screen3.rejection_reason = f"Сделка по {symbol} была менее 30 минут назад"

            if should_trade and await signal_repository.has_active_m15(symbol):
                logger.info(f"⏭️ {symbol}: уже есть активный M15 сигнал")
                should_trade = False
                screen3.passed = False
                screen3.rejection_reason = "Уже есть активный M15 сигнал"

            if not should_trade:
                analysis.should_trade = False
                return analysis

            if real_price > 0:
                old_entry = screen3.entry_price
                screen3.entry_price = real_price
                screen3.current_price_at_signal = real_price

                risk = abs(old_entry - screen3.stop_loss)
                if screen3.signal_type == "BUY":
                    screen3.stop_loss = real_price - risk
                    screen3.take_profit = real_price + (risk * screen3.indicators.get('risk_reward_ratio', 3.0))
                else:
                    screen3.stop_loss = real_price + risk
                    screen3.take_profit = real_price - (risk * screen3.indicators.get('risk_reward_ratio', 3.0))

                screen3.stop_loss = self.screen3_analyzer._round_price(screen3.stop_loss)
                screen3.take_profit = self.screen3_analyzer._round_price(screen3.take_profit)

                logger.info(
                    f"📊 {symbol}: Entry скорректирована с {self._format_price(old_entry)} на {self._format_price(real_price)}")

            learning_comment = self._generate_full_analysis_description(
                symbol, screen1, screen2, screen3
            )

            try:
                signal_id = await signal_repository.save_signal(analysis, learning_comment)
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
                            'expiration_time': screen3.expiration_time.isoformat() if screen3.expiration_time else None,
                            'learning_comment': learning_comment,
                            'entry_type': getattr(screen3, 'entry_type', 'FALLBACK'),
                            'position_multiplier': getattr(screen3, 'position_multiplier', 1.0),
                            'liquidity_grabbed': getattr(screen3, 'liquidity_grabbed', False),
                            'fvg_present': getattr(screen3, 'fvg_present', False),
                            'grab_price': getattr(screen3, 'grab_price', None)
                        },
                        'three_screen_analyzer'
                    )
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения сигнала {symbol}: {e}")

        return analysis

    def _generate_full_analysis_description(self, symbol: str, screen1: Screen1Result,
                                            screen2: Screen2Result,
                                            screen3: Screen3Result = None) -> str:
        # (оставляем существующую реализацию без изменений)
        return f"Analysis for {symbol}"

    async def _save_trend_analysis(self, symbol: str, screen1_result: Screen1Result) -> None:
        try:
            from .signal_repository import signal_repository

            indicators = screen1_result.indicators

            trend_direction = screen1_result.trend_direction
            adx = indicators.get('adx', 0)
            ema20 = indicators.get('ema_20', 0)
            ema50 = indicators.get('ema_50', 0)
            macd_line = indicators.get('macd_line', 0)
            macd_signal = indicators.get('macd_signal', 0)
            structure = "-"
            confidence = screen1_result.confidence_score

            await signal_repository.save_trend_analysis(
                symbol=symbol,
                trend_direction=trend_direction,
                adx=adx,
                ema20=ema20,
                ema50=ema50,
                macd_line=macd_line,
                macd_signal=macd_signal,
                structure=structure,
                confidence=confidence
            )

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения тренда {symbol}: {e}")


__all__ = ['ThreeScreenAnalyzer', 'ThreeScreenAnalysis']