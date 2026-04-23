# analyzer/core/screen2_entry_zones.py (ПОЛНОСТЬЮ - ФАЗА 2.0 SMC + SNIPER ПРОКОЛ)
"""
screen2_entry_zones.py - поиск зон входа (H4)

ФАЗА 1.5.1 - ИНТЕГРАЦИЯ ИСТОРИЧЕСКИХ УРОВНЕЙ
ФАЗА 2.0 - SMC (FVG, Liquidity, SNIPER/TREND)
ФАЗА 2.2 - ИСПРАВЛЕНИЯ: проверка прокола ликвидности для SNIPER
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

        range_filter_config = analysis_config.get('range_filter', {})
        self.range_filter_enabled = range_filter_config.get('enabled', False)
        self.range_lookback = range_filter_config.get('lookback', 50)
        self.range_min_width_pct = range_filter_config.get('min_width_pct', 2.0)
        self.range_max_width_pct = range_filter_config.get('max_width_pct', 10.0)

        zone_side_config = analysis_config.get('zone_side_check', {})
        self.zone_side_check_enabled = zone_side_config.get('enabled', True)

        self.false_breakout_tolerance = 2.0

        self.db_path = self._get_db_path()
        self._init_db()

        self._fvg_detector = None
        self._liquidity_scanner = None
        self._last_candles = None  # ← ДЛЯ ПРОВЕРКИ ПРОКОЛА

        logger.info(f"✅ Screen2Analyzer инициализирован (Фаза 2.2 с SNIPER проколом)")

    def _get_db_path(self) -> str:
        try:
            from pathlib import Path
            project_root = Path(__file__).parent.parent
            return str(project_root / "core_v2" / "historical.db")
        except:
            return "data/historical.db"

    def _init_db(self):
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            conn = sqlite3.connect(self.db_path)
            conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='historical_levels'")
            conn.close()
        except Exception as e:
            logger.warning(f"⚠️ Не удалось проверить БД: {e}")

    def _get_fvg_detector(self):
        if self._fvg_detector is None:
            try:
                from core_v2.analyst.fvg_detector import FVGDetector
                self._fvg_detector = FVGDetector()
            except ImportError as e:
                logger.warning(f"⚠️ Не удалось импортировать FVGDetector: {e}")
                self._fvg_detector = None
        return self._fvg_detector

    def _get_liquidity_scanner(self):
        if self._liquidity_scanner is None:
            try:
                from core_v2.analyst.liquidity_scanner import LiquidityScanner
                self._liquidity_scanner = LiquidityScanner()
            except ImportError as e:
                logger.warning(f"⚠️ Не удалось импортировать LiquidityScanner: {e}")
                self._liquidity_scanner = None
        return self._liquidity_scanner

    # ========== ПРОВЕРКА ПРОКОЛА ЛИКВИДНОСТИ ==========

    def _check_liquidity_grab(self, candles: List[Dict], pool_price: float, pool_type: str, lookback: int = 5) -> tuple:
        """Проверяет, был ли прокол пула ликвидности и возврат"""
        recent_candles = candles[-lookback:] if len(candles) > lookback else candles

        if pool_type == 'SELL_SIDE':
            for candle in recent_candles:
                if candle.get('low', 0) < pool_price:
                    if candle.get('close', 0) > pool_price:
                        return True, candle.get('low', 0)
        else:
            for candle in recent_candles:
                if candle.get('high', 0) > pool_price:
                    if candle.get('close', 0) < pool_price:
                        return True, candle.get('high', 0)

        return False, None

    # ========== ФИЛЬТРАЦИЯ FVG ПО КАЧЕСТВУ (по ДАТЕ) ==========

    def _check_fvg_visited_by_date(self, fvg_zone: Dict, candles: List[Dict]) -> bool:
        zone_low = fvg_zone.get('low', 0)
        zone_high = fvg_zone.get('high', 0)
        formed_at = fvg_zone.get('formed_at', 0)

        if zone_low == 0 or zone_high == 0 or formed_at == 0:
            return False

        for candle in candles:
            candle_ts = candle.get('timestamp', 0)
            if candle_ts <= formed_at:
                continue
            candle_low = candle.get('low', 0)
            candle_high = candle.get('high', 0)
            if candle_low <= zone_high and candle_high >= zone_low:
                return True
        return False

    def _count_fvg_touches_by_date(self, fvg_zone: Dict, candles: List[Dict]) -> int:
        zone_low = fvg_zone.get('low', 0)
        zone_high = fvg_zone.get('high', 0)
        formed_at = fvg_zone.get('formed_at', 0)

        if zone_low == 0 or zone_high == 0 or formed_at == 0:
            return 0

        touches = 0
        for candle in candles:
            candle_ts = candle.get('timestamp', 0)
            if candle_ts <= formed_at:
                continue
            candle_low = candle.get('low', 0)
            candle_high = candle.get('high', 0)
            if candle_low <= zone_high and candle_high >= zone_low:
                touches += 1
        return touches

    def _filter_fvg_by_quality(self, fvg_zones: List[Dict], candles: List[Dict], max_age: int = 30) -> List[Dict]:
        quality_zones = []
        for zone in fvg_zones:
            age = zone.get('age', 999)
            if age >= max_age:
                continue
            touches = self._count_fvg_touches_by_date(zone, candles)
            if touches > 1:
                continue
            zone['quality'] = 'FRESH'
            quality_zones.append(zone)
        return quality_zones

    # ========== SNIPER ЗОНА С ПРОВЕРКОЙ ПРОКОЛА ==========

    def _find_sniper_zone(self, fvg_zones, liquidity_pools, trend_direction, current_price):
        if not fvg_zones or not liquidity_pools:
            return None

        candles = getattr(self, '_last_candles', None)
        if not candles:
            return None

        if trend_direction == 'BULL':
            sell_pools = [p for p in liquidity_pools if p['type'] == 'SELL_SIDE']
            for pool in sell_pools:
                pool_price = pool['price']
                is_grab, grab_price = self._check_liquidity_grab(candles, pool_price, 'SELL_SIDE')
                if not is_grab:
                    continue
                for fvg in fvg_zones:
                    if fvg['type'] == 'bullish' and fvg['low'] > pool_price:
                        distance_pct = (fvg['low'] - pool_price) / pool_price * 100
                        if distance_pct <= 5.0:
                            return {
                                'low': fvg['low'],
                                'high': fvg['high'],
                                'entry_type': 'SNIPER',
                                'liquidity_pool': pool,
                                'fvg_zone': fvg,
                                'grab_price': grab_price
                            }
        else:
            buy_pools = [p for p in liquidity_pools if p['type'] == 'BUY_SIDE']
            for pool in buy_pools:
                pool_price = pool['price']
                is_grab, grab_price = self._check_liquidity_grab(candles, pool_price, 'BUY_SIDE')
                if not is_grab:
                    continue
                for fvg in fvg_zones:
                    if fvg['type'] == 'bearish' and fvg['high'] < pool_price:
                        distance_pct = (pool_price - fvg['high']) / pool_price * 100
                        if distance_pct <= 5.0:
                            return {
                                'low': fvg['low'],
                                'high': fvg['high'],
                                'entry_type': 'SNIPER',
                                'liquidity_pool': pool,
                                'fvg_zone': fvg,
                                'grab_price': grab_price
                            }
        return None

    def _find_nearest_fvg_zone(self, fvg_zones, trend_direction, current_price):
        if not fvg_zones:
            return None
        if trend_direction == 'BULL':
            bullish_zones = [z for z in fvg_zones if z['type'] == 'bullish']
            if not bullish_zones:
                return None
            below = [z for z in bullish_zones if z['high'] < current_price]
            if below:
                return max(below, key=lambda x: x['high'])
            return min(bullish_zones, key=lambda x: x['low'])
        else:
            bearish_zones = [z for z in fvg_zones if z['type'] == 'bearish']
            if not bearish_zones:
                return None
            above = [z for z in bearish_zones if z['low'] > current_price]
            if above:
                return min(above, key=lambda x: x['low'])
            return max(bearish_zones, key=lambda x: x['high'])

    # ========== ОСНОВНОЙ МЕТОД analyze ==========

    def analyze(self, h4_data: List[dict], trend_direction: str, current_price: float, symbol: str) -> Dict[str, Any]:
        # СОХРАНЯЕМ СВЕЧИ ДЛЯ ПРОВЕРКИ ПРОКОЛА
        self._last_candles = h4_data

        if not h4_data or len(h4_data) < 20:
            return {'success': False, 'score': 0, 'reason': 'Недостаточно данных H4'}

        try:
            smc_result = self._analyze_smc_zones(h4_data, trend_direction, current_price, symbol)
            if smc_result.get('success'):
                return smc_result

            # FALLBACK (уже не используется, но оставлено для совместимости)
            return {
                'success': False,
                'score': 0,
                'reason': 'SMC зоны не найдены'
            }

        except Exception as e:
            logger.error(f"❌ Ошибка анализа Screen2 для {symbol}: {e}")
            return {'success': False, 'score': 0, 'reason': str(e)}

    def _analyze_smc_zones(self, h4_data, trend_direction, current_price, symbol):
        fvg_detector = self._get_fvg_detector()
        liquidity_scanner = self._get_liquidity_scanner()

        if fvg_detector is None:
            return {'success': False, 'reason': 'FVGDetector не доступен'}

        converted_klines = self._convert_to_klines_dict(h4_data)
        all_fvg_zones = fvg_detector.find_fvg(converted_klines)

        if not all_fvg_zones:
            return {'success': False, 'reason': 'FVG зоны не найдены'}

        quality_fvg_zones = self._filter_fvg_by_quality(all_fvg_zones, converted_klines)

        liquidity_pools = []
        if liquidity_scanner is not None:
            liquidity_pools = liquidity_scanner.find_liquidity_pools(converted_klines)

        sniper_zone = self._find_sniper_zone(quality_fvg_zones, liquidity_pools, trend_direction, current_price)

        if sniper_zone:
            return {
                'success': True,
                'score': 8,
                'zone_low': sniper_zone['low'],
                'zone_high': sniper_zone['high'],
                'expected_pattern': 'PIN_BAR',
                'reason': 'Sniper entry: FVG + liquidity grab',
                'entry_type': 'SNIPER',
                'fvg_zones': quality_fvg_zones,
                'liquidity_pools': liquidity_pools,
                'selected_fvg': sniper_zone.get('fvg_zone'),
                'selected_liquidity_pool': sniper_zone.get('liquidity_pool'),
                'grab_price': sniper_zone.get('grab_price')
            }

        if quality_fvg_zones:
            trend_zone = self._find_nearest_fvg_zone(quality_fvg_zones, trend_direction, current_price)
            if trend_zone:
                return {
                    'success': True,
                    'score': 6,
                    'zone_low': trend_zone['low'],
                    'zone_high': trend_zone['high'],
                    'expected_pattern': 'ENGULFING',
                    'reason': 'Trend entry: nearest FVG',
                    'entry_type': 'TREND',
                    'fvg_zones': quality_fvg_zones,
                    'selected_fvg': trend_zone
                }

        return {'success': False, 'reason': 'SMC зоны не найдены'}

    def _convert_to_klines_dict(self, h4_data: List[dict]) -> List[Dict[str, Any]]:
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

    def _load_historical_levels(self, symbol: str) -> Tuple[List[Dict], List[Dict]]:
        return [], []


__all__ = ['Screen2Analyzer']