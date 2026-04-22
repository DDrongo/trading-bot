# analyzer/core/screen2_entry_zones.py (ПОЛНОСТЬЮ - ФАЗА 2.0 SMC)
"""
screen2_entry_zones.py - поиск зон входа (H4)

ФАЗА 1.5.1 - ИНТЕГРАЦИЯ ИСТОРИЧЕСКИХ УРОВНЕЙ:
- Загрузка уровней W1 (2 года, мин. 5 касаний) и D1 (1 год, мин. 3 касания)
- Приоритет: W1 > D1 > H4
- Confluence (совпадение на нескольких ТФ) = VERY_STRONG
- SL ставится за исторический уровень
- Учитываются только уровни, вызывавшие разворот (не проколы)

ФАЗА 1.5.2:
- Добавлены start_time, end_time, candles_count в импульс и коррекцию
- Точные координаты для поиска на графике

ФАЗА 2.0 - SMC (Smart Money Concepts):
- 🆕 Интеграция FVG Detector (Fair Value Gaps)
- 🆕 Интеграция Liquidity Scanner (бассейны ликвидности)
- 🆕 Приоритет зон: SNIPER (FVG + Liquidity) > TREND (FVG) > LEGACY (исторические уровни)
- 🆕 Возвращает entry_type для Position Manager
"""

import logging
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import sqlite3
import os
from pathlib import Path

logger = logging.getLogger(__name__)


class Screen2Analyzer:
    """Анализатор зон входа (Экран 2) с историческими уровнями и SMC"""

    # ========== КОНФИГУРАЦИЯ ИСТОРИЧЕСКИХ УРОВНЕЙ ==========
    HISTORICAL_CONFIG = {
        'W1': {
            'years_back': 2.0,
            'min_touches': 5,
            'strength': 'VERY_STRONG',
            'priority': 3,
            'weight': 2.0,
            'min_reversal_pct': 3.0
        },
        'D1': {
            'years_back': 1.0,
            'min_touches': 3,
            'strength': 'STRONG',
            'priority': 2,
            'weight': 1.5,
            'min_reversal_pct': 2.0
        }
    }

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.min_score = 3

        analysis_config = self.config.get('analysis', {})

        # Фильтр ширины диапазона
        range_filter_config = analysis_config.get('range_filter', {})
        self.range_filter_enabled = range_filter_config.get('enabled', False)
        self.range_lookback = range_filter_config.get('lookback', 50)
        self.range_min_width_pct = range_filter_config.get('min_width_pct', 2.0)
        self.range_max_width_pct = range_filter_config.get('max_width_pct', 10.0)

        # Проверка стороны зоны
        zone_side_config = analysis_config.get('zone_side_check', {})
        self.zone_side_check_enabled = zone_side_config.get('enabled', True)

        # Допуск на ложный пробой
        self.false_breakout_tolerance = 2.0

        # ========== ФАЗА 1.5.1: ИНИЦИАЛИЗАЦИЯ БД ==========
        self.db_path = self._get_db_path()
        self._init_db()

        # ========== ФАЗА 2.0: SMC КОМПОНЕНТЫ ==========
        self._fvg_detector = None
        self._liquidity_scanner = None

        logger.info(f"✅ Screen2Analyzer инициализирован (Фаза 2.0 SMC)")
        logger.info(f"   Исторические уровни: ВКЛЮЧЕНЫ")
        logger.info(
            f"   W1: {self.HISTORICAL_CONFIG['W1']['years_back']} лет, мин. {self.HISTORICAL_CONFIG['W1']['min_touches']} касаний")
        logger.info(
            f"   D1: {self.HISTORICAL_CONFIG['D1']['years_back']} год, мин. {self.HISTORICAL_CONFIG['D1']['min_touches']} касания")
        logger.info(f"   БД: {self.db_path}")
        logger.info(f"   🆕 SMC: FVG Detector + Liquidity Scanner")
        logger.info(f"   🆕 Приоритет: SNIPER > TREND > LEGACY")

    def _get_db_path(self) -> str:
        """Получение пути к БД исторических уровней (ОТДЕЛЬНАЯ БД)"""
        try:
            historical_config = self.config.get('historical_data', {})
            db_path = historical_config.get('db_path')

            if db_path:
                from pathlib import Path
                project_root = Path(__file__).parent.parent.parent
                return str(project_root / db_path)

            from pathlib import Path
            project_root = Path(__file__).parent.parent.parent
            return str(project_root / "data/historical.db")
        except:
            return "data/historical.db"

    def _init_db(self):
        """Проверка существования таблицы historical_levels"""
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='historical_levels'"
            )
            if not cursor.fetchone():
                logger.warning(
                    "⚠️ Таблица historical_levels не найдена. Запустите 'python tools/run_backtest.py collect-levels'")
            conn.close()
        except Exception as e:
            logger.warning(f"⚠️ Не удалось проверить БД: {e}")

    def _get_fvg_detector(self):
        """Ленивая инициализация FVG детектора"""
        if self._fvg_detector is None:
            try:
                from analyzer.core.analyst.fvg_detector import FVGDetector
                self._fvg_detector = FVGDetector()
                logger.debug("✅ FVGDetector инициализирован")
            except ImportError as e:
                logger.warning(f"⚠️ Не удалось импортировать FVGDetector: {e}")
                self._fvg_detector = None
        return self._fvg_detector

    def _get_liquidity_scanner(self):
        """Ленивая инициализация Liquidity Scanner"""
        if self._liquidity_scanner is None:
            try:
                from analyzer.core.analyst.liquidity_scanner import LiquidityScanner
                self._liquidity_scanner = LiquidityScanner()
                logger.debug("✅ LiquidityScanner инициализирован")
            except ImportError as e:
                logger.warning(f"⚠️ Не удалось импортировать LiquidityScanner: {e}")
                self._liquidity_scanner = None
        return self._liquidity_scanner

    def _load_historical_levels(self, symbol: str) -> Tuple[List[Dict], List[Dict]]:
        """
        Загрузка СИЛЬНЫХ исторических уровней из БД

        Returns:
            (supports, resistances) - только уровни, вызывавшие разворот
        """
        supports = []
        resistances = []

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row

            cursor = conn.execute("""
                SELECT * FROM historical_levels 
                WHERE symbol = ? 
                AND strength IN ('STRONG', 'VERY_STRONG')
                ORDER BY 
                    CASE strength 
                        WHEN 'VERY_STRONG' THEN 1 
                        WHEN 'STRONG' THEN 2 
                    END,
                    timeframe DESC,
                    touches DESC
            """, (symbol,))

            rows = cursor.fetchall()

            for row in rows:
                level_dict = {
                    'price': row['price'],
                    'strength': row['strength'],
                    'type': row['level_type'],
                    'touches': row['touches'],
                    'timeframe': row['timeframe'],
                    'source': 'HISTORICAL',
                    'priority': 3 if row['timeframe'] == 'W1' else 2
                }

                if row['level_type'] == 'SUPPORT':
                    supports.append(level_dict)
                else:
                    resistances.append(level_dict)

            conn.close()

            if supports or resistances:
                logger.info(
                    f"📊 {symbol}: загружено исторических уровней — поддержек: {len(supports)}, сопротивлений: {len(resistances)}")
                for s in supports[:3]:
                    logger.debug(
                        f"   Поддержка: {s['price']:.4f} ({s['strength']}, {s['timeframe']}, {s['touches']} кас.)")
                for r in resistances[:3]:
                    logger.debug(
                        f"   Сопротивление: {r['price']:.4f} ({r['strength']}, {r['timeframe']}, {r['touches']} кас.)")

            return supports, resistances

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки исторических уровней {symbol}: {e}")
            return [], []

    def analyze(
            self,
            h4_data: List[dict],
            trend_direction: str,
            current_price: float,
            symbol: str
    ) -> Dict[str, Any]:
        """
        Анализирует H4 данные и возвращает зоны входа

        ФАЗА 1.5.1: Интеграция исторических уровней
        ФАЗА 1.5.2: Добавлены даты в импульс/коррекцию
        ФАЗА 2.0: SMC приоритет (SNIPER > TREND > LEGACY)
        """
        if not h4_data or len(h4_data) < 20:
            return {
                'success': False,
                'score': 0,
                'reason': 'Недостаточно данных H4'
            }

        try:
            if not self._validate_symbol(symbol):
                return {
                    'success': False,
                    'score': 0,
                    'reason': f'Невалидный символ: {symbol}'
                }

            # ========== ФАЗА 2.0: СНАЧАЛА ПРОВЕРЯЕМ SMC ЗОНЫ ==========
            smc_result = self._analyze_smc_zones(h4_data, trend_direction, current_price, symbol)

            if smc_result.get('success'):
                logger.info(f"✅ {symbol}: SMC зона найдена — {smc_result.get('entry_type')}")
                return smc_result

            # ========== ФАЗА 1.5.1: ЗАГРУЖАЕМ ИСТОРИЧЕСКИЕ УРОВНИ ==========
            hist_supports, hist_resistances = self._load_historical_levels(symbol)

            # ========== АНАЛИЗ ИМПУЛЬСА/КОРРЕКЦИИ НА H4 ==========
            h4_analysis = self._analyze_h4_impulse_correction(h4_data, trend_direction, current_price)

            if h4_analysis['phase'] == 'IMPULSE':
                logger.info(f"❌ {symbol}: H4 в фазе ИМПУЛЬС (не коррекция) — ждём отката")
                return {
                    'success': False,
                    'score': 0,
                    'reason': f"H4 в фазе ИМПУЛЬС, ждём коррекции",
                    'h4_analysis': h4_analysis
                }

            # ========== ФИЛЬТР ШИРИНЫ ДИАПАЗОНА ==========
            if self.range_filter_enabled:
                range_width_pct = self._calculate_range_width(h4_data)
                if range_width_pct is not None:
                    if range_width_pct < self.range_min_width_pct:
                        return {
                            'success': False, 'score': 0,
                            'reason': f'Диапазон слишком узкий: {range_width_pct:.1f}%'
                        }
                    if range_width_pct > self.range_max_width_pct:
                        return {
                            'success': False, 'score': 0,
                            'reason': f'Диапазон слишком широкий: {range_width_pct:.1f}%'
                        }

            # ========== НАХОДИМ УРОВНИ ==========
            h4_supports = self._find_support_levels(h4_data)
            h4_resistances = self._find_resistance_levels(h4_data)

            nearby_hist_supports = self._filter_nearby_levels(hist_supports, current_price, 15.0, 'SUPPORT')
            nearby_hist_resistances = self._filter_nearby_levels(hist_resistances, current_price, 15.0, 'RESISTANCE')

            all_supports = self._merge_levels_with_priority(h4_supports, nearby_hist_supports)
            all_resistances = self._merge_levels_with_priority(h4_resistances, nearby_hist_resistances)

            logger.info(
                f"📊 {symbol}: поддержек — H4: {len(h4_supports)}, ист: {len(nearby_hist_supports)} → всего: {len(all_supports)}")
            logger.info(
                f"📊 {symbol}: сопротивлений — H4: {len(h4_resistances)}, ист: {len(nearby_hist_resistances)} → всего: {len(all_resistances)}")

            fib_levels = self._calculate_fibonacci_from_impulse(h4_analysis['impulse'], trend_direction)

            all_zones = fib_levels + all_supports + all_resistances
            volume_zones = self._analyze_volume_zones(h4_data, all_zones)

            score = self._calculate_score(
                fib_levels, all_supports, all_resistances,
                nearby_hist_supports, nearby_hist_resistances,
                volume_zones, h4_data, trend_direction
            )

            best_zone = self._select_best_zone_with_priority(
                fib_levels, all_supports, all_resistances,
                volume_zones, trend_direction, current_price
            )

            if not best_zone:
                return {
                    'success': False,
                    'score': score,
                    'reason': 'Не найдено подходящих зон'
                }

            if self.zone_side_check_enabled:
                zone_check_passed, zone_check_reason = self._check_zone_side_with_tolerance(
                    best_zone, trend_direction, current_price, symbol
                )

                if not zone_check_passed:
                    return {
                        'success': False,
                        'score': score,
                        'reason': zone_check_reason
                    }

            expected_pattern = self._determine_expected_pattern(h4_data, best_zone, trend_direction)

            success = score >= self.min_score

            if success:
                zone_source = best_zone.get('source', 'H4')
                zone_strength = best_zone.get('strength', 'WEAK')

                logger.info(f"✅ {symbol}: ЭКРАН 2 пройден (score={score}/8) [LEGACY]")
                logger.info(f"   Зона: {best_zone.get('low', best_zone.get('price', 0)):.4f}-{best_zone.get('high', best_zone.get('price', 0)):.4f}")
                logger.info(f"   Источник: {zone_source}, сила: {zone_strength}")

                return {
                    'success': True,
                    'score': score,
                    'zone_low': best_zone.get('low', best_zone.get('price', 0)),
                    'zone_high': best_zone.get('high', best_zone.get('price', 0)),
                    'expected_pattern': expected_pattern,
                    'reason': f'Score {score}/8',
                    'entry_type': 'LEGACY',
                    'h4_analysis': h4_analysis,
                    'support_levels': all_supports,
                    'resistance_levels': all_resistances,
                    'fib_levels': fib_levels,
                    'historical_levels_used': len(nearby_hist_supports) + len(nearby_hist_resistances)
                }
            else:
                logger.info(f"❌ {symbol}: ЭКРАН 2 не пройден — score={score}/8 < {self.min_score}")
                return {
                    'success': False,
                    'score': score,
                    'reason': f'Score {score}/8 < {self.min_score}'
                }

        except Exception as e:
            logger.error(f"❌ Ошибка анализа Screen2 для {symbol}: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'score': 0,
                'reason': str(e)
            }

    # ========== ФАЗА 2.0: SMC МЕТОДЫ ==========

    def _analyze_smc_zones(self, h4_data, trend_direction, current_price, symbol):
        fvg_detector = self._get_fvg_detector()
        liquidity_scanner = self._get_liquidity_scanner()

        if fvg_detector is None:
            return {'success': False, 'reason': 'FVGDetector не доступен'}

        converted_klines = self._convert_to_klines_dict(h4_data)
        fvg_zones = fvg_detector.find_fvg(converted_klines)

        if not fvg_zones:
            return {'success': False, 'reason': 'FVG зоны не найдены'}

        logger.debug(f"🕳️ {symbol}: найдено {len(fvg_zones)} FVG зон")

        if liquidity_scanner is not None:
            liquidity_pools = liquidity_scanner.find_liquidity_pools(converted_klines)

            if liquidity_pools:
                logger.debug(f"💧 {symbol}: найдено {len(liquidity_pools)} пулов ликвидности")
                sniper_zone = self._find_sniper_zone(fvg_zones, liquidity_pools, trend_direction, current_price)

                if sniper_zone:
                    selected_fvg = sniper_zone.get('fvg_zone', {})
                    logger.info(f"🎯 {symbol}: SNIPER зона найдена!")
                    logger.info(f"   FVG: {selected_fvg.get('low', 0):.4f}-{selected_fvg.get('high', 0):.4f}")
                    logger.info(f"   Liquidity Pool: {sniper_zone.get('liquidity_pool', {}).get('price', 0):.4f}")

                    return {
                        'success': True,
                        'score': 8,
                        'zone_low': sniper_zone['low'],
                        'zone_high': sniper_zone['high'],
                        'expected_pattern': 'PIN_BAR',
                        'reason': 'Sniper entry: FVG above/below liquidity pool',
                        'entry_type': 'SNIPER',
                        'fvg_zones': fvg_zones,
                        'liquidity_pools': liquidity_pools,
                        'selected_fvg': {
                            'low': selected_fvg.get('low', 0),
                            'high': selected_fvg.get('high', 0),
                            'type': selected_fvg.get('type', 'bullish'),
                            'age': selected_fvg.get('age', 0),
                            'strength': selected_fvg.get('strength', 'NORMAL'),
                            'formed_at': selected_fvg.get('formed_at_dt', None)
                        },
                        'selected_liquidity_pool': sniper_zone.get('liquidity_pool')
                    }

        trend_zone = self._find_nearest_fvg_zone(fvg_zones, trend_direction, current_price)

        if trend_zone:
            logger.info(f"📈 {symbol}: TREND зона найдена (ближайший FVG)")
            return {
                'success': True,
                'score': 6,
                'zone_low': trend_zone['low'],
                'zone_high': trend_zone['high'],
                'expected_pattern': 'ENGULFING',
                'reason': 'Trend entry: nearest FVG',
                'entry_type': 'TREND',
                'fvg_zones': fvg_zones,
                'selected_fvg': trend_zone
            }

        return {'success': False, 'reason': 'SMC зоны не найдены'}

    def _convert_to_klines_dict(self, h4_data: List[dict]) -> List[Dict[str, Any]]:
        """Конвертирует H4 данные в формат для детекторов"""
        converted = []
        for k in h4_data:
            converted.append({
                'open': k['open'],
                'high': k['high'],
                'low': k['low'],
                'close': k['close'],
                'timestamp': k.get('timestamp', 0)
            })
        return converted

    def _find_sniper_zone(
            self,
            fvg_zones: List[Dict],
            liquidity_pools: List[Dict],
            trend_direction: str,
            current_price: float
    ) -> Optional[Dict]:
        """Находит идеальную зону для Sniper входа с диагностикой"""

        logger.info(f"🔍 [SNIPER DIAG] Начинаем поиск для {trend_direction}")
        logger.info(f"   FVG зон найдено: {len(fvg_zones)}")
        logger.info(f"   Пуллов ликвидности найдено: {len(liquidity_pools)}")

        if not fvg_zones:
            logger.info(f"❌ [SNIPER DIAG] Нет FVG зон → SNIPER невозможен")
            return None

        if not liquidity_pools:
            logger.info(f"❌ [SNIPER DIAG] Нет пулов ликвидности → SNIPER невозможен")
            return None

        if trend_direction == 'BULL':
            sell_pools = [p for p in liquidity_pools if p['type'] == 'SELL_SIDE']
            logger.info(f"   SELL_SIDE пулов (под ценой): {len(sell_pools)}")

            for pool in sell_pools:
                pool_price = pool['price']
                logger.info(f"   Проверяем пул {pool_price:.4f} (касаний: {pool['touches']})")

                for fvg in fvg_zones:
                    if fvg['type'] == 'bullish' and fvg['low'] > pool_price:
                        distance_pct = (fvg['low'] - pool_price) / pool_price * 100
                        logger.info(f"      ✅ Найден bullish FVG {fvg['low']:.4f}-{fvg['high']:.4f} выше пула")
                        logger.info(f"      Расстояние: {distance_pct:.2f}%")
                        if distance_pct <= 5.0:
                            logger.info(f"      ✅ SNIPER УСЛОВИЯ ВЫПОЛНЕНЫ!")
                            return {
                                'low': fvg['low'],
                                'high': fvg['high'],
                                'entry_type': 'SNIPER',
                                'liquidity_pool': pool,
                                'fvg_zone': fvg,
                                'distance_pct': distance_pct
                            }
                        else:
                            logger.info(f"      ❌ Слишком далеко: {distance_pct:.2f}% > 5%")
                    else:
                        logger.info(f"      FVG {fvg['type']} не подходит (нужен bullish)")
        else:
            # BEAR логика аналогично
            buy_pools = [p for p in liquidity_pools if p['type'] == 'BUY_SIDE']
            logger.info(f"   BUY_SIDE пулов (над ценой): {len(buy_pools)}")

            for pool in buy_pools:
                pool_price = pool['price']
                logger.info(f"   Проверяем пул {pool_price:.4f} (касаний: {pool['touches']})")

                for fvg in fvg_zones:
                    if fvg['type'] == 'bearish' and fvg['high'] < pool_price:
                        distance_pct = (pool_price - fvg['high']) / pool_price * 100
                        logger.info(f"      ✅ Найден bearish FVG {fvg['low']:.4f}-{fvg['high']:.4f} ниже пула")
                        logger.info(f"      Расстояние: {distance_pct:.2f}%")
                        if distance_pct <= 5.0:
                            logger.info(f"      ✅ SNIPER УСЛОВИЯ ВЫПОЛНЕНЫ!")
                            return {
                                'low': fvg['low'],
                                'high': fvg['high'],
                                'entry_type': 'SNIPER',
                                'liquidity_pool': pool,
                                'fvg_zone': fvg,
                                'distance_pct': distance_pct
                            }
                        else:
                            logger.info(f"      ❌ Слишком далеко: {distance_pct:.2f}% > 5%")

        logger.info(f"❌ [SNIPER DIAG] Подходящая пара FVG + Liquidity Pool не найдена")
        return None

    def _find_nearest_fvg_zone(
            self,
            fvg_zones: List[Dict],
            trend_direction: str,
            current_price: float
    ) -> Optional[Dict]:
        """Находит ближайшую FVG зону к текущей цене"""
        if trend_direction == 'BULL':
            bullish_zones = [z for z in fvg_zones if z['type'] == 'bullish']

            if not bullish_zones:
                return None

            # Ищем зону ниже цены (поддержка)
            below = [z for z in bullish_zones if z['high'] < current_price]
            if below:
                nearest = max(below, key=lambda x: x['high'])
                distance_pct = (current_price - nearest['high']) / current_price * 100
                logger.debug(f"📈 Ближайший bullish FVG ниже: {nearest['low']:.4f}-{nearest['high']:.4f} (расст: {distance_pct:.2f}%)")
                return nearest

            # Если нет ниже, берём самую близкую сверху
            nearest = min(bullish_zones, key=lambda x: x['low'])
            distance_pct = (nearest['low'] - current_price) / current_price * 100
            logger.debug(f"📈 Ближайший bullish FVG выше: {nearest['low']:.4f}-{nearest['high']:.4f} (расст: {distance_pct:.2f}%)")
            return nearest

        elif trend_direction == 'BEAR':
            bearish_zones = [z for z in fvg_zones if z['type'] == 'bearish']

            if not bearish_zones:
                return None

            # Ищем зону выше цены (сопротивление)
            above = [z for z in bearish_zones if z['low'] > current_price]
            if above:
                nearest = min(above, key=lambda x: x['low'])
                distance_pct = (nearest['low'] - current_price) / current_price * 100
                logger.debug(f"📉 Ближайший bearish FVG выше: {nearest['low']:.4f}-{nearest['high']:.4f} (расст: {distance_pct:.2f}%)")
                return nearest

            # Если нет выше, берём самую близкую снизу
            nearest = max(bearish_zones, key=lambda x: x['high'])
            distance_pct = (current_price - nearest['high']) / current_price * 100
            logger.debug(f"📉 Ближайший bearish FVG ниже: {nearest['low']:.4f}-{nearest['high']:.4f} (расст: {distance_pct:.2f}%)")
            return nearest

        return None

    # ========== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ (LEGACY) ==========

    def _filter_nearby_levels(self, levels: List[Dict], current_price: float,
                              max_distance_pct: float, level_type: str) -> List[Dict]:
        if not levels or current_price <= 0:
            return []

        filtered = []
        for level in levels:
            price = level.get('price', 0)
            if price <= 0:
                continue

            distance_pct = abs(price - current_price) / current_price * 100

            if distance_pct <= max_distance_pct:
                filtered.append(level)

        return filtered

    def _merge_levels_with_priority(self, h4_levels: List[Dict], hist_levels: List[Dict]) -> List[Dict]:
        all_levels = []

        for level in hist_levels:
            level_copy = level.copy()
            if 'priority' not in level_copy:
                level_copy['priority'] = 3 if level.get('timeframe') == 'W1' else 2
            all_levels.append(level_copy)

        for level in h4_levels:
            level_copy = level.copy()
            level_copy['priority'] = 1
            level_copy['source'] = 'H4'
            all_levels.append(level_copy)

        return self._group_levels_with_priority(all_levels)

    def _group_levels_with_priority(self, levels: List[Dict], tolerance: float = 0.015) -> List[Dict]:
        if not levels:
            return []

        sorted_levels = sorted(levels, key=lambda x: x.get('price', 0))
        grouped = []
        current_group = [sorted_levels[0]]

        for level in sorted_levels[1:]:
            if abs(level['price'] - current_group[-1]['price']) / current_group[-1]['price'] < tolerance:
                current_group.append(level)
            else:
                grouped.append(self._create_merged_level(current_group))
                current_group = [level]

        if current_group:
            grouped.append(self._create_merged_level(current_group))

        return sorted(grouped, key=lambda x: x.get('priority', 0), reverse=True)

    def _create_merged_level(self, group: List[Dict]) -> Dict:
        avg_price = sum(l['price'] for l in group) / len(group)
        max_priority = max(l.get('priority', 0) for l in group)

        strengths = [l.get('strength', 'WEAK') for l in group]
        if 'VERY_STRONG' in strengths:
            strength = 'VERY_STRONG'
        elif 'STRONG' in strengths:
            strength = 'STRONG'
        else:
            strength = 'WEAK'

        sources = set(l.get('source', 'H4') for l in group)
        source = 'HISTORICAL' if 'HISTORICAL' in sources else 'H4'
        confluence = len(sources) > 1

        timeframes = [l.get('timeframe', '') for l in group if l.get('timeframe')]
        if 'W1' in timeframes:
            best_timeframe = 'W1'
        elif 'D1' in timeframes:
            best_timeframe = 'D1'
        else:
            best_timeframe = timeframes[0] if timeframes else 'H4'

        return {
            'price': avg_price,
            'low': avg_price * 0.995,
            'high': avg_price * 1.005,
            'strength': strength,
            'priority': max_priority,
            'source': source,
            'confluence': confluence,
            'touches': sum(l.get('touches', 1) for l in group),
            'type': group[0].get('type', 'SUPPORT'),
            'timeframe': best_timeframe
        }

    def _calculate_score(self, fib_levels, supports, resistances,
                         hist_supports, hist_resistances,
                         volume_zones, h4_data, trend_direction) -> int:
        score = 0

        if fib_levels:
            score += 1
        if supports or resistances:
            score += 1

        if hist_supports or hist_resistances:
            score += 1
        all_hist = hist_supports + hist_resistances
        if any(l.get('strength') == 'VERY_STRONG' for l in all_hist):
            score += 1
            logger.debug(f"⭐ Бонус за VERY_STRONG уровень")

        all_zones = fib_levels + supports + resistances
        strong_zones = [z for z in all_zones if z.get('strength') in ['STRONG', 'VERY_STRONG']]
        if strong_zones:
            score += 1

        all_zones_with_confluence = supports + resistances
        if any(z.get('confluence', False) for z in all_zones_with_confluence):
            score += 1
            logger.debug(f"🎯 Бонус за confluence")

        if volume_zones.get('confirmed', 0) >= 2:
            score += 1

        if self._check_trend_structure(h4_data, trend_direction):
            score += 1

        return score

    def _select_best_zone_with_priority(self, fib_zones, supports, resistances,
                                        volume_data, trend_direction, current_price) -> Optional[Dict]:
        candidates = []

        for zone in supports + resistances + fib_zones:
            zone_copy = zone.copy()
            if 'priority' not in zone_copy:
                zone_copy['priority'] = 0
            if 'low' not in zone_copy:
                price = zone_copy.get('price', 0)
                zone_copy['low'] = price * 0.995
                zone_copy['high'] = price * 1.005

            if trend_direction == 'BULL':
                if zone_copy.get('type') == 'SUPPORT' and zone_copy.get('price', 0) < current_price:
                    candidates.append(zone_copy)
            else:
                if zone_copy.get('type') == 'RESISTANCE' and zone_copy.get('price', 0) > current_price:
                    candidates.append(zone_copy)

        if not candidates:
            return None

        def sort_key(z):
            priority = z.get('priority', 0)
            strength_score = 3 if z.get('strength') == 'VERY_STRONG' else (2 if z.get('strength') == 'STRONG' else 1)
            confluence_bonus = 2 if z.get('confluence', False) else 0
            return priority * 10 + strength_score * 5 + confluence_bonus

        candidates.sort(key=sort_key, reverse=True)

        return candidates[0]

    def _validate_symbol(self, symbol: str) -> bool:
        if not symbol:
            return False
        invalid_symbols = ['4USDT', '0USDT', 'USDTUSDT', 'USDCUSDT', 'BUSDUSDT']
        if symbol in invalid_symbols:
            return False
        if len(symbol) < 5:
            return False
        if not symbol.endswith('USDT'):
            return False
        return True

    def _analyze_h4_impulse_correction(self, h4_data: List[dict], d1_trend: str,
                                       current_price: float) -> Dict[str, Any]:
        if len(h4_data) < 30:
            return {'phase': 'UNKNOWN', 'impulse': None, 'correction': None, 'h4_direction': 'UNKNOWN'}

        closes = [c['close'] for c in h4_data]
        ema20 = self._calculate_ema(closes, 20)
        ema50 = self._calculate_ema(closes, 50)

        if not ema20 or not ema50:
            return {'phase': 'UNKNOWN', 'impulse': None, 'correction': None, 'h4_direction': 'UNKNOWN'}

        h4_direction = 'UP' if ema20[-1] > ema50[-1] else 'DOWN'
        swings = self._find_swing_points(h4_data)

        if d1_trend == 'BULL':
            if h4_direction == 'UP':
                phase = 'IMPULSE'
                impulse = self._find_last_impulse_up(swings, h4_data)
                correction = None
            else:
                phase = 'CORRECTION'
                impulse = self._find_last_impulse_up(swings, h4_data)
                correction = self._find_current_correction_down(impulse, h4_data, current_price)
        else:
            if h4_direction == 'DOWN':
                phase = 'IMPULSE'
                impulse = self._find_last_impulse_down(swings, h4_data)
                correction = None
            else:
                phase = 'CORRECTION'
                impulse = self._find_last_impulse_down(swings, h4_data)
                correction = self._find_current_correction_up(impulse, h4_data, current_price)

        return {
            'phase': phase,
            'impulse': impulse,
            'correction': correction,
            'h4_direction': h4_direction,
            'ema20': ema20[-1] if ema20 else 0,
            'ema50': ema50[-1] if ema50 else 0
        }

    def _calculate_ema(self, prices: List[float], period: int) -> List[float]:
        if len(prices) < period:
            return []
        multiplier = 2 / (period + 1)
        ema = [sum(prices[:period]) / period]
        for price in prices[period:]:
            ema.append((price * multiplier) + (ema[-1] * (1 - multiplier)))
        return ema

    def _find_swing_points(self, h4_data: List[dict], lookback: int = 5) -> Dict[str, List[Dict]]:
        highs = [c['high'] for c in h4_data]
        lows = [c['low'] for c in h4_data]
        times = [c.get('timestamp', i) for i, c in enumerate(h4_data)]

        swing_highs = []
        swing_lows = []

        for i in range(lookback, len(h4_data) - lookback):
            if highs[i] == max(highs[i - lookback:i + lookback + 1]):
                swing_highs.append({'price': highs[i], 'index': i, 'time': times[i]})
            if lows[i] == min(lows[i - lookback:i + lookback + 1]):
                swing_lows.append({'price': lows[i], 'index': i, 'time': times[i]})

        return {'highs': swing_highs, 'lows': swing_lows}

    def _find_last_impulse_up(self, swings: Dict, h4_data: List[dict]) -> Optional[Dict]:
        if len(swings['lows']) < 1 or len(swings['highs']) < 1:
            return None
        recent_lows = sorted(swings['lows'], key=lambda x: x['index'], reverse=True)
        recent_highs = sorted(swings['highs'], key=lambda x: x['index'], reverse=True)

        total_candles = len(h4_data)

        for low in recent_lows[:3]:
            for high in recent_highs[:3]:
                if high['index'] > low['index']:
                    start_time = low.get('time')
                    end_time = high.get('time')

                    try:
                        start_ts = int(start_time) if start_time else None
                        start_dt = datetime.fromtimestamp(start_ts / 1000) if start_ts else datetime.now()
                    except (ValueError, TypeError):
                        start_dt = datetime.now()

                    try:
                        end_ts = int(end_time) if end_time else None
                        end_dt = datetime.fromtimestamp(end_ts / 1000) if end_ts else datetime.now()
                    except (ValueError, TypeError):
                        end_dt = datetime.now()

                    candles_count = high['index'] - low['index']
                    candles_ago_start = total_candles - low['index'] - 1

                    return {
                        'start_price': low['price'],
                        'end_price': high['price'],
                        'start_index': low['index'],
                        'end_index': high['index'],
                        'start_time': start_dt.strftime('%d.%m.%Y %H:%M'),
                        'end_time': end_dt.strftime('%d.%m.%Y %H:%M'),
                        'candles_count': candles_count,
                        'candles_ago_start': candles_ago_start,
                        'change_pct': (high['price'] - low['price']) / low['price'] * 100,
                        'direction': 'UP'
                    }
        return None

    def _find_last_impulse_down(self, swings: Dict, h4_data: List[dict]) -> Optional[Dict]:
        if len(swings['highs']) < 1 or len(swings['lows']) < 1:
            return None
        recent_highs = sorted(swings['highs'], key=lambda x: x['index'], reverse=True)
        recent_lows = sorted(swings['lows'], key=lambda x: x['index'], reverse=True)

        total_candles = len(h4_data)

        for high in recent_highs[:3]:
            for low in recent_lows[:3]:
                if low['index'] > high['index']:
                    start_time = high.get('time')
                    end_time = low.get('time')

                    try:
                        start_ts = int(start_time) if start_time else None
                        start_dt = datetime.fromtimestamp(start_ts / 1000) if start_ts else datetime.now()
                    except (ValueError, TypeError):
                        start_dt = datetime.now()

                    try:
                        end_ts = int(end_time) if end_time else None
                        end_dt = datetime.fromtimestamp(end_ts / 1000) if end_ts else datetime.now()
                    except (ValueError, TypeError):
                        end_dt = datetime.now()

                    candles_count = low['index'] - high['index']
                    candles_ago_start = total_candles - high['index'] - 1

                    return {
                        'start_price': high['price'],
                        'end_price': low['price'],
                        'start_index': high['index'],
                        'end_index': low['index'],
                        'start_time': start_dt.strftime('%d.%m.%Y %H:%M'),
                        'end_time': end_dt.strftime('%d.%m.%Y %H:%M'),
                        'candles_count': candles_count,
                        'candles_ago_start': candles_ago_start,
                        'change_pct': (high['price'] - low['price']) / high['price'] * 100,
                        'direction': 'DOWN'
                    }
        return None

    def _find_current_correction_down(self, impulse: Optional[Dict], h4_data: List[dict],
                                      current_price: float) -> Optional[Dict]:
        if not impulse:
            return None

        total_candles = len(h4_data)
        start_index = impulse.get('end_index', 0)
        candles_count = total_candles - start_index - 1
        start_time = impulse.get('end_time', '?')

        return {
            'start_price': impulse['end_price'],
            'current_price': current_price,
            'start_index': start_index,
            'start_time': start_time,
            'candles_count': candles_count,
            'change_pct': (current_price - impulse['end_price']) / impulse['end_price'] * 100,
            'direction': 'DOWN'
        }

    def _find_current_correction_up(self, impulse: Optional[Dict], h4_data: List[dict],
                                    current_price: float) -> Optional[Dict]:
        if not impulse:
            return None

        total_candles = len(h4_data)
        start_index = impulse.get('end_index', 0)
        candles_count = total_candles - start_index - 1
        start_time = impulse.get('end_time', '?')

        return {
            'start_price': impulse['end_price'],
            'current_price': current_price,
            'start_index': start_index,
            'start_time': start_time,
            'candles_count': candles_count,
            'change_pct': (current_price - impulse['end_price']) / impulse['end_price'] * 100,
            'direction': 'UP'
        }

    def _calculate_fibonacci_from_impulse(self, impulse: Optional[Dict], trend_direction: str) -> List[dict]:
        if not impulse:
            return []
        start_price = impulse['start_price']
        end_price = impulse['end_price']
        fib_levels = []
        levels = [0.382, 0.5, 0.618, 0.786]

        if trend_direction == 'BULL':
            diff = end_price - start_price
            for level in levels:
                price = end_price - diff * level
                fib_levels.append({
                    'low': price * 0.995, 'high': price * 1.005, 'price': price,
                    'level': level, 'strength': 'STRONG' if level in [0.5, 0.618] else 'WEAK',
                    'type': 'FIBONACCI', 'priority': 1
                })
        else:
            diff = start_price - end_price
            for level in levels:
                price = end_price + diff * level
                fib_levels.append({
                    'low': price * 0.995, 'high': price * 1.005, 'price': price,
                    'level': level, 'strength': 'STRONG' if level in [0.5, 0.618] else 'WEAK',
                    'type': 'FIBONACCI', 'priority': 1
                })
        return fib_levels

    def _calculate_range_width(self, h4_data: List[dict]) -> Optional[float]:
        if not h4_data or len(h4_data) < self.range_lookback:
            return None
        recent_data = h4_data[-self.range_lookback:]
        lows = [c.get('low', 0) for c in recent_data if c.get('low', 0) > 0]
        highs = [c.get('high', 0) for c in recent_data if c.get('high', 0) > 0]
        if not lows or not highs:
            return None
        range_low = min(lows)
        range_high = max(highs)
        if range_low <= 0:
            return None
        return (range_high - range_low) / range_low * 100

    def _find_support_levels(self, data: List[dict]) -> List[dict]:
        supports = []
        lows = [c['low'] for c in data]
        for i in range(5, len(data) - 5):
            if lows[i] < min(lows[i - 5:i]) and lows[i] < min(lows[i + 1:i + 6]):
                supports.append(
                    {'price': lows[i], 'strength': 'WEAK', 'type': 'SUPPORT', 'priority': 1, 'source': 'H4'})
        return self._group_levels(supports)[:3]

    def _find_resistance_levels(self, data: List[dict]) -> List[dict]:
        resistances = []
        highs = [c['high'] for c in data]
        for i in range(5, len(data) - 5):
            if highs[i] > max(highs[i - 5:i]) and highs[i] > max(highs[i + 1:i + 6]):
                resistances.append(
                    {'price': highs[i], 'strength': 'WEAK', 'type': 'RESISTANCE', 'priority': 1, 'source': 'H4'})
        return self._group_levels(resistances)[:3]

    def _group_levels(self, levels: List[dict], tolerance: float = 0.01) -> List[dict]:
        if not levels:
            return []
        sorted_levels = sorted(levels, key=lambda x: x['price'])
        grouped = []
        current_group = [sorted_levels[0]]
        for level in sorted_levels[1:]:
            if abs(level['price'] - current_group[-1]['price']) / current_group[-1]['price'] < tolerance:
                current_group.append(level)
            else:
                avg_price = sum(l['price'] for l in current_group) / len(current_group)
                strength = 'STRONG' if len(current_group) >= 3 else 'WEAK'
                grouped.append({
                    'price': avg_price, 'strength': strength, 'type': current_group[0]['type'],
                    'count': len(current_group), 'priority': 1, 'source': 'H4'
                })
                current_group = [level]
        if current_group:
            avg_price = sum(l['price'] for l in current_group) / len(current_group)
            strength = 'STRONG' if len(current_group) >= 3 else 'WEAK'
            grouped.append({
                'price': avg_price, 'strength': strength, 'type': current_group[0]['type'],
                'count': len(current_group), 'priority': 1, 'source': 'H4'
            })
        return grouped

    def _analyze_volume_zones(self, data: List[dict], zones: List[dict]) -> Dict[str, Any]:
        confirmed = 0
        for zone in zones:
            zone_low = zone.get('low', zone.get('price', 0)) * 0.995
            zone_high = zone.get('high', zone.get('price', 0)) * 1.005
            volume_spikes = 0
            for candle in data[-50:]:
                if zone_low <= candle['close'] <= zone_high:
                    avg_volume = np.mean([c['volume'] for c in data[-20:]])
                    if candle['volume'] > avg_volume * 1.5:
                        volume_spikes += 1
            if volume_spikes >= 2:
                confirmed += 1
                zone['volume_confirmed'] = True
            else:
                zone['volume_confirmed'] = False
        return {'confirmed': confirmed, 'total_zones': len(zones)}

    def _check_trend_structure(self, data: List[dict], trend_direction: str) -> bool:
        if len(data) < 20:
            return False
        highs = [c['high'] for c in data[-20:]]
        lows = [c['low'] for c in data[-20:]]
        if trend_direction == 'BULL':
            higher_highs = all(highs[i] < highs[i + 1] for i in range(len(highs) - 1) if i % 3 == 0)
            higher_lows = all(lows[i] < lows[i + 1] for i in range(len(lows) - 1) if i % 3 == 0)
            return higher_highs or higher_lows
        else:
            lower_highs = all(highs[i] > highs[i + 1] for i in range(len(highs) - 1) if i % 3 == 0)
            lower_lows = all(lows[i] > lows[i + 1] for i in range(len(lows) - 1) if i % 3 == 0)
            return lower_highs or lower_lows

    def _check_broken_levels(self, data: List[dict], levels: List[dict]) -> bool:
        if not levels or len(data) < 5:
            return False
        current_price = data[-1]['close']
        broken = 0
        for level in levels:
            level_price = level.get('price', 0)
            if current_price > level_price and level.get('type') == 'RESISTANCE':
                broken += 1
            elif current_price < level_price and level.get('type') == 'SUPPORT':
                broken += 1
        return broken >= 2

    def _check_zone_side_with_tolerance(self, best_zone: dict, trend_direction: str,
                                        current_price: float, symbol: str) -> tuple:
        if not best_zone:
            return False, "Нет зоны для проверки"
        zone_low = best_zone.get('low', 0)
        zone_high = best_zone.get('high', 0)
        if zone_low <= 0 or zone_high <= 0:
            return False, "Некорректные границы зоны"
        if trend_direction == "BULL":
            min_allowed = zone_low * (1 - self.false_breakout_tolerance / 100)
            if current_price >= min_allowed:
                if current_price < zone_low:
                    return True, f"BUY: ЛОЖНЫЙ ПРОБОЙ! Цена {current_price:.4f} ниже зоны"
                elif current_price <= zone_high:
                    return True, f"BUY: цена {current_price:.4f} В ЗОНЕ"
                else:
                    return True, f"BUY: цена {current_price:.4f} ВЫШЕ зоны"
            else:
                return False, f"Серьёзный пробой: цена {current_price:.4f} < {min_allowed:.4f}"
        elif trend_direction == "BEAR":
            max_allowed = zone_high * (1 + self.false_breakout_tolerance / 100)
            if current_price <= max_allowed:
                if current_price > zone_high:
                    return True, f"SELL: ЛОЖНЫЙ ПРОБОЙ! Цена {current_price:.4f} выше зоны"
                elif current_price >= zone_low:
                    return True, f"SELL: цена {current_price:.4f} В ЗОНЕ"
                else:
                    return True, f"SELL: цена {current_price:.4f} НИЖЕ зоны"
            else:
                return False, f"Серьёзный пробой: цена {current_price:.4f} > {max_allowed:.4f}"
        else:
            return True, "SIDEWAYS: оба направления разрешены"

    def _determine_expected_pattern(self, data: List[dict], zone: dict, trend_direction: str) -> str:
        if trend_direction == "BULL":
            expected = "PIN_BAR"
        else:
            expected = "ENGULFING"
        if zone.get('strength') == 'STRONG' or zone.get('strength') == 'VERY_STRONG':
            expected = "MORNING_STAR" if trend_direction == "BULL" else "EVENING_STAR"
        return expected

    def _add_impulse_to_watch(self, symbol: str, h4_analysis: Dict, trend_direction: str, current_price: float):
        impulse = h4_analysis.get('impulse', {})
        if impulse:
            logger.info(f"👀 {symbol}: импульс {impulse.get('direction')} добавлен в WATCH. Ждём коррекции.")


__all__ = ['Screen2Analyzer']