# analyzer/core/three_screen_analyzer.py (ПОЛНОСТЬЮ - ФАЗА 2.0 SMC)
"""
🎯 THREE SCREEN ANALYZER - Координатор трёхэкранного анализа

ФАЗА 1.4.1:
- ИСПРАВЛЕНО: единый источник цены (DataProvider)
- ИСПРАВЛЕНО: Entry = реальная цена
- ИСПРАВЛЕНО: унифицирован формат цен (4 знака)
- ИСПРАВЛЕНО: пересчёт SL/TP от реальной цены
- ДОБАВЛЕНО: отслеживание пробоев в WATCH

ФАЗА 1.5.2:
- Универсальный генератор комментариев для WATCH и M15
- Отображение исторических уровней, фибоначчи, confluence
- Полный обучающий комментарий для всех типов сигналов
- Добавлены дата/время в H4 анализ (импульс/коррекция)
- Исправлена терминология: «Позиция цены» вместо «Текущая позиция»

ФАЗА 2.0 - SMC (Smart Money Concepts):
- 🆕 Обновлён комментарий для отображения FVG зон
- 🆕 Обновлён комментарий для отображения пулов ликвидности
- 🆕 Отображение типа входа (SNIPER/TREND/LEGACY)
- 🆕 Отображение информации о Liquidity Grab
- 🆕 Передача SMC данных в screen3
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

        self._calculation_cache = {}
        self._cache_max_size = caching_config.get('calculation_cache_size', 100)
        self._cache_hits = 0
        self._cache_misses = 0

        self._initialized = False
        self._analysis_start_time = None

        # ФАЗА 2.0: SMC данные для комментария
        self._last_fvg_zones = []
        self._last_liquidity_pools = []

        logger.info(f"✅ ThreeScreenAnalyzer создан (Фаза 2.0 SMC) — Pro режим активирован")
        logger.info(f"   Единый источник цены: DataProvider")
        logger.info(f"   🆕 SMC комментарии: FVG + Liquidity Pools")

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
        Анализ символа по трёхэкранной методологии (Фаза 2.0 SMC)
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

            # Шаг 1: Screen 1 (D1 тренд)
            screen1_result = await self._analyze_screen1(symbol, klines_data)

            # Шаг 2: Screen 2 (H4 зоны входа) - передаём реальную цену
            screen2_result = await self._analyze_screen2(symbol, klines_data, screen1_result, real_price)

            # Сохраняем SMC данные для комментария
            self._last_fvg_zones = getattr(screen2_result, 'fvg_zones', [])
            self._last_liquidity_pools = getattr(screen2_result, 'liquidity_pools', [])

            # Шаг 3: Screen 3 (M15 сигналы) - передаём реальную цену и SMC данные
            screen3_result = await self._analyze_screen3(
                symbol, klines_data, screen1_result, screen2_result, real_price
            )

            # Сохраняем тренд в БД если прошёл фильтр
            if screen1_result.passed:
                await self._save_trend_analysis(symbol, screen1_result)

            return await self._create_final_analysis(symbol, screen1_result, screen2_result, screen3_result, real_price)

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

        # Используем SMC анализ
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

        # ФАЗА 2.0: Сохраняем SMC данные в screen2_result
        screen2.entry_type = result.get('entry_type', 'LEGACY')
        screen2.fvg_zones = result.get('fvg_zones', [])
        screen2.liquidity_pools = result.get('liquidity_pools', [])
        screen2.selected_fvg = result.get('selected_fvg', None)
        screen2.selected_liquidity_pool = result.get('selected_liquidity_pool', None)

        # Сохраняем дополнительные данные для обучающего комментария
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

        # Проверка дубликатов перед генерацией
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

        # ФАЗА 2.0: Передаём SMC данные в Screen3
        from analyzer.core.analyst import FVGDetector, LiquidityScanner

        fvg_detector = FVGDetector()
        liquidity_scanner = LiquidityScanner()

        # Конвертируем H4 данные для детекторов
        h4_klines = klines_data.get('4h', [])
        h4_converted = []
        for k in h4_klines:
            h4_converted.append({
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4]),
                'timestamp': k[0]
            })

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
                                     real_price: float = 0.0) -> ThreeScreenAnalysis:
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
            min_rr_ratio = self.m15_config.get('min_rr_ratio', 3.0)

            if rr_ratio < (min_rr_ratio - 0.01):
                should_trade = False
                screen3.passed = False
                screen3.rejection_reason = f"R/R {rr_ratio:.2f}:1 < {min_rr_ratio}:1"

        analysis.should_trade = should_trade

        if screen3 and screen3.passed and should_trade:
            # ========== ПРОВЕРКА ДУБЛИКАТОВ ==========
            from .signal_repository import signal_repository

            # Проверяем, была ли сделка в последние 30 минут
            if await signal_repository.was_traded_recently(symbol, minutes=30):
                logger.info(f"⏭️ {symbol}: была сделка в последние 30 минут, пропускаем")
                should_trade = False
                screen3.passed = False
                screen3.rejection_reason = f"Сделка по {symbol} была менее 30 минут назад"

            # Проверяем, есть ли активный M15 сигнал
            if should_trade and await signal_repository.has_active_m15(symbol):
                logger.info(f"⏭️ {symbol}: уже есть активный M15 сигнал")
                should_trade = False
                screen3.passed = False
                screen3.rejection_reason = "Уже есть активный M15 сигнал"

            if not should_trade:
                analysis.should_trade = False
                return analysis

            # ========== ИСПОЛЬЗУЕМ РЕАЛЬНУЮ ЦЕНУ ==========
            if real_price > 0:
                # Перезаписываем entry_price на реальную цену
                old_entry = screen3.entry_price
                screen3.entry_price = real_price
                screen3.current_price_at_signal = real_price

                # Пересчитываем SL и TP от реальной цены
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

            # Генерируем обучающий комментарий (Фаза 2.0 с SMC)
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
                            # ФАЗА 2.0: SMC данные
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

    def _generate_full_analysis_description(
            self,
            symbol: str,
            screen1: Screen1Result,
            screen2: Screen2Result,
            screen3: Screen3Result = None
    ) -> str:
        """
        Генерация полного описания для обучающей системы (ФАЗА 2.0 SMC)
        Включает FVG зоны, пулы ликвидности, тип входа
        """

        # D1 тренд
        d1_trend = screen1.trend_direction
        d1_adx = screen1.indicators.get('adx', 0)
        d1_ema20 = screen1.indicators.get('ema_20', 0)
        d1_ema50 = screen1.indicators.get('ema_50', 0)

        if d1_adx > 25:
            strength = "СИЛЬНЫЙ"
        elif d1_adx > 20:
            strength = "УМЕРЕННЫЙ"
        else:
            strength = "СЛАБЫЙ"

        description = f"""
═══════════════════════════════════════════════════════════════
📊 АНАЛИЗ {symbol} {'(WATCH)' if screen3 is None or not screen3.passed else ''}
═══════════════════════════════════════════════════════════════

🎯 D1 ТРЕНД
───────────────────────────────────────────────────────────────
  Направление:     {d1_trend}
  Сила:            {strength} (ADX = {d1_adx:.1f})
  EMA20:           {self._format_price(d1_ema20)}
  EMA50:           {self._format_price(d1_ema50)}
  💡 EMA20 {'выше' if d1_ema20 > d1_ema50 else 'ниже'} EMA50 — {'восходящий' if d1_trend == 'BULL' else 'нисходящий'} тренд подтверждён
"""

        # H4 анализ (импульс/коррекция) с датами
        h4_analysis = getattr(screen2, 'h4_analysis', {})
        if h4_analysis:
            phase = h4_analysis.get('phase', 'UNKNOWN')
            impulse = h4_analysis.get('impulse', {})
            correction = h4_analysis.get('correction', {})

            description += f"""
📈 H4 АНАЛИЗ
───────────────────────────────────────────────────────────────
  Фаза:            {phase}
"""
            if impulse:
                start_time = impulse.get('start_time', '?')
                end_time = impulse.get('end_time', '?')
                candles = impulse.get('candles_count', '?')

                description += f"""
  Последний импульс ({impulse.get('direction', 'N/A')}):
    Начало:        {self._format_price(impulse.get('start_price', 0))} ({start_time}) — {candles} свечей назад
    Конец:         {self._format_price(impulse.get('end_price', 0))} ({end_time})
    Изменение:     {impulse.get('change_pct', 0):+.2f}%
"""
            if correction:
                start_time = correction.get('start_time', '?')
                candles = correction.get('candles_count', '?')

                description += f"""
  Текущая коррекция ({correction.get('direction', 'N/A')}):
    Начало:        {self._format_price(correction.get('start_price', 0))} ({start_time}) — {candles} свечей назад
    Текущая цена:  {self._format_price(correction.get('current_price', 0))}
    Изменение:     {correction.get('change_pct', 0):+.2f}%
"""

        # ========== ФАЗА 2.0: FVG ЗОНЫ ==========
        fvg_zones = getattr(screen2, 'fvg_zones', [])
        if fvg_zones:
            description += f"""
🕳️ FVG АНАЛИЗ (Fair Value Gaps)
───────────────────────────────────────────────────────────────
"""
            for i, fvg in enumerate(fvg_zones[:5], 1):
                fvg_type = "БЫЧИЙ" if fvg.get('type') == 'bullish' else "МЕДВЕЖИЙ"
                age = fvg.get('age', '?')
                strength = fvg.get('strength', 'WEAK')
                strength_icon = "💪" if strength == "STRONG" else "📊" if strength == "NORMAL" else "🕰️"

                description += f"  {i}. {fvg_type} FVG: {self._format_price(fvg.get('low', 0))} - {self._format_price(fvg.get('high', 0))}\n"
                description += f"     {strength_icon} Возраст: {age} свечей, сила: {strength}\n"

        # ========== ФАЗА 2.0: ПУЛЫ ЛИКВИДНОСТИ ==========
        liquidity_pools = getattr(screen2, 'liquidity_pools', [])
        if liquidity_pools:
            description += f"""
💧 ЗОНЫ ЛИКВИДНОСТИ (Liquidity Pools)
───────────────────────────────────────────────────────────────
"""
            for i, pool in enumerate(liquidity_pools[:5], 1):
                pool_type = "SELL_SIDE (стопы лонгистов)" if pool.get(
                    'type') == 'SELL_SIDE' else "BUY_SIDE (стопы шортистов)"
                touches = pool.get('touches', '?')
                strength = pool.get('strength', 'NORMAL')

                description += f"  {i}. {pool_type}\n"
                description += f"     Уровень: {self._format_price(pool.get('price', 0))}, касаний: {touches}, сила: {strength}\n"

        # ========== ФАЗА 2.0: ТИП ВХОДА ==========
        entry_type = getattr(screen2, 'entry_type', 'LEGACY')
        entry_type_icon = "🎯" if entry_type == "SNIPER" else "📈" if entry_type == "TREND" else "📊"

        description += f"""
🎯 ТАКТИКА ВХОДА
───────────────────────────────────────────────────────────────
  Тип:            {entry_type_icon} {entry_type}
"""

        if entry_type == "SNIPER":
            selected_fvg = getattr(screen2, 'selected_fvg', None)
            selected_pool = getattr(screen2, 'selected_liquidity_pool', None)

            description += f"""  Условие:       Ждём прокол пула ликвидности и возврат в FVG
  Ожидаемый паттерн: PIN_BAR
"""
            if selected_pool:
                description += f"  Пул ликвидности: {self._format_price(selected_pool.get('price', 0))}\n"
            if selected_fvg:
                description += f"  FVG зона:      {self._format_price(selected_fvg.get('low', 0))} - {self._format_price(selected_fvg.get('high', 0))}\n"

        elif entry_type == "TREND":
            description += f"""  Условие:       Цена в FVG зоне
  Ожидаемый паттерн: ENGULFING
  Размер позиции: 50% (половинный)
"""
        else:
            description += f"""  Условие:       Стандартная логика (исторические уровни)
  Размер позиции: 75% (стандартный)
"""

        # Исторические уровни
        support_levels = getattr(screen2, 'support_levels', [])
        resistance_levels = getattr(screen2, 'resistance_levels', [])

        hist_supports = [s for s in support_levels if s.get('source') == 'HISTORICAL']
        hist_resistances = [r for r in resistance_levels if r.get('source') == 'HISTORICAL']

        if hist_supports:
            description += f"""
🏛️ ИСТОРИЧЕСКИЕ ПОДДЕРЖКИ
───────────────────────────────────────────────────────────────
"""
            for i, sup in enumerate(hist_supports[:5], 1):
                touches = sup.get('touches', '?')
                timeframe = sup.get('timeframe', '?')
                strength = sup.get('strength', 'WEAK')
                price = sup.get('price', 0)
                description += f"  {i}. {self._format_price(price)} ({strength}, {timeframe}, {touches} кас.)\n"

        if hist_resistances:
            description += f"""
🏛️ ИСТОРИЧЕСКИЕ СОПРОТИВЛЕНИЯ
───────────────────────────────────────────────────────────────
"""
            for i, res in enumerate(hist_resistances[:5], 1):
                touches = res.get('touches', '?')
                timeframe = res.get('timeframe', '?')
                strength = res.get('strength', 'WEAK')
                price = res.get('price', 0)
                description += f"  {i}. {self._format_price(price)} ({strength}, {timeframe}, {touches} кас.)\n"

        # Локальные H4 уровни
        h4_supports = [s for s in support_levels if s.get('source') != 'HISTORICAL'][:3]
        h4_resistances = [r for r in resistance_levels if r.get('source') != 'HISTORICAL'][:3]

        if h4_supports or h4_resistances:
            description += f"""
📈 ЛОКАЛЬНЫЕ УРОВНИ (H4)
───────────────────────────────────────────────────────────────
"""
            if h4_supports:
                description += "  Поддержки: "
                description += ", ".join([self._format_price(s.get('price', 0)) for s in h4_supports])
                description += "\n"
            if h4_resistances:
                description += "  Сопротивления: "
                description += ", ".join([self._format_price(r.get('price', 0)) for r in h4_resistances])
                description += "\n"

        # Уровни Фибоначчи
        fib_levels = getattr(screen2, 'fib_levels', [])
        if fib_levels:
            description += f"""
📐 УРОВНИ ФИБОНАЧЧИ (от импульса)
───────────────────────────────────────────────────────────────
"""
            for fib in fib_levels[:4]:
                description += f"  {fib.get('level', 0):.3f}: {self._format_price(fib.get('price', 0))} ({fib.get('strength', 'WEAK')})\n"

        # Совпадения уровней (Confluence)
        confluence = getattr(screen2, 'confluence', {})
        hist_used = getattr(screen2, 'historical_levels_used', 0)

        description += f"""
✅ СОВПАДЕНИЯ УРОВНЕЙ (CONFLUENCE)
───────────────────────────────────────────────────────────────
"""
        if confluence and confluence.get('has_confluence', False):
            description += f"  🎯 {confluence.get('description', '')}\n"
            description += f"  💡 Совпадение уровней = СИЛЬНАЯ ЗОНА!\n"
        else:
            description += f"  Нет явных совпадений\n"

        if hist_used > 0:
            description += f"  ⭐ Использовано исторических уровней: {hist_used}\n"

        # Зона входа
        description += f"""
🎯 ЗОНА ВХОДА
───────────────────────────────────────────────────────────────
  Нижняя граница: {self._format_price(screen2.zone_low)}
  Верхняя граница: {self._format_price(screen2.zone_high)}
  Score Screen2:   {screen2.screen2_score}/8
  Ожидаемый паттерн: {screen2.expected_pattern}
"""

        # Текущая позиция цены
        if screen3 and screen3.current_price_at_signal > 0:
            current_price = screen3.current_price_at_signal
        elif screen3 and screen3.entry_price > 0:
            current_price = screen3.entry_price
        else:
            current_price = (screen2.zone_low + screen2.zone_high) / 2

        # Определяем позицию цены относительно зоны
        if current_price > screen2.zone_high:
            diff_pct = (current_price - screen2.zone_high) / screen2.zone_high * 100
            position = f"▲ ВЫШЕ зоны на {diff_pct:.1f}%"
            in_zone = False
        elif current_price < screen2.zone_low:
            diff_pct = (screen2.zone_low - current_price) / screen2.zone_low * 100
            position = f"▼ НИЖЕ зоны на {diff_pct:.1f}%"
            in_zone = False
        else:
            position = "● В ЗОНЕ"
            in_zone = True

        description += f"""
📍 ПОЗИЦИЯ ЦЕНЫ (на {datetime.now().strftime('%d.%m.%Y %H:%M')})
───────────────────────────────────────────────────────────────
  Текущая цена:    {self._format_price(current_price)}
  Позиция цены:    {position}
"""

        # M15 сигнал (если есть)
        if screen3 and screen3.passed:
            # ФАЗА 2.0: Отображаем информацию о Liquidity Grab
            liquidity_grabbed = getattr(screen3, 'liquidity_grabbed', False)
            grab_price = getattr(screen3, 'grab_price', None)

            description += f"""
⚡ M15 СИГНАЛ [{entry_type}]
───────────────────────────────────────────────────────────────
  Направление:     {screen3.signal_type}
  Паттерн:         {screen3.trigger_pattern}
  Уверенность:     {screen3.confidence:.1%}
  Entry:           {self._format_price(screen3.entry_price)}
  Stop Loss:       {self._format_price(screen3.stop_loss)}
  Take Profit:     {self._format_price(screen3.take_profit)}
  R/R:             {screen3.indicators.get('risk_reward_ratio', 0):.2f}:1
  Размер позиции:  {getattr(screen3, 'position_multiplier', 1.0):.0%}
"""
            if liquidity_grabbed and grab_price:
                description += f"""
  💧 Liquidity Grab: ДА (прокол {self._format_price(grab_price)})
"""
            else:
                description += f"""
  💧 Liquidity Grab: НЕТ
"""
        else:
            # WATCH статус с правильной логикой
            if in_zone:
                watch_status = "✅ Цена В ЗОНЕ! Ждём подтверждающий паттерн на M15"
                watch_action = "🔍 Ожидаемый паттерн: PIN_BAR, ENGULFING, MORNING_STAR\n  ⚡ При появлении паттерна — автоматическое открытие позиции"
            else:
                watch_status = f"❌ Вход НЕВОЗМОЖЕН — цена {'выше' if current_price > screen2.zone_high else 'ниже'} зоны"
                watch_action = f"📉 Ждём {'снижения' if current_price > screen2.zone_high else 'роста'} цены в зону {self._format_price(screen2.zone_low)}-{self._format_price(screen2.zone_high)}"

            description += f"""
⏳ СТАТУС WATCH
───────────────────────────────────────────────────────────────
  {watch_status}
  {watch_action}
  🔔 При входе в зону — автоматический поиск паттерна на M15
"""

        description += f"""
═══════════════════════════════════════════════════════════════
"""
        return description

    async def _save_trend_analysis(self, symbol: str, screen1_result: Screen1Result) -> None:
        """Сохраняет результаты анализа тренда D1 в БД"""
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