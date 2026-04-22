# analyzer/core/analyst/fvg_detector.py (НОВЫЙ - ПОЛНОСТЬЮ)
"""
🕳️ FVG DETECTOR - Обнаружение Fair Value Gaps (дисбалансов)

FVG (Fair Value Gap) — это ценовой разрыв между свечами,
который цена стремится закрыть.

Bullish FVG (поддержка):
    Low(C3) > High(C1)
    Зона: [High(C1), Low(C3)]

Bearish FVG (сопротивление):
    High(C3) < Low(C1)
    Зона: [Low(C1), High(C3)]
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime

logger = logging.getLogger('fvg_detector')


@dataclass
class FVGZone:
    """Зона Fair Value Gap"""
    type: str  # 'bullish' или 'bearish'
    low: float
    high: float
    age: int  # количество свечей с момента формирования
    strength: str  # 'STRONG', 'NORMAL', 'WEAK'
    formed_at: Optional[datetime] = None
    is_active: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class FVGDetector:
    """
    Детектор Fair Value Gaps на H4 таймфрейме
    """

    def __init__(self, lookback_candles: int = 100, min_gap_pct: float = 0.1):
        """
        Args:
            lookback_candles: количество свечей для анализа
            min_gap_pct: минимальный размер разрыва в процентах (0.1% по умолчанию)
        """
        self.lookback_candles = lookback_candles
        self.min_gap_pct = min_gap_pct / 100  # конвертируем в десятичную дробь

        logger.info(f"✅ FVGDetector инициализирован")
        logger.info(f"   Lookback: {lookback_candles} свечей")
        logger.info(f"   Min gap: {min_gap_pct}%")

    def find_fvg(self, klines_h4: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not klines_h4 or len(klines_h4) < 3:
            logger.warning("⚠️ Недостаточно данных для поиска FVG")
            return []

        candles = klines_h4[-self.lookback_candles:] if len(klines_h4) > self.lookback_candles else klines_h4
        fvg_zones = []

        for i in range(len(candles) - 2):
            c1 = candles[i]
            c2 = candles[i + 1]
            c3 = candles[i + 2]

            bullish_zone = self._detect_bullish_fvg(c1, c2, c3, i, candles)
            if bullish_zone:
                fvg_zones.append(bullish_zone)

            bearish_zone = self._detect_bearish_fvg(c1, c2, c3, i, candles)
            if bearish_zone:
                fvg_zones.append(bearish_zone)

        merged_zones = self._merge_overlapping_zones(fvg_zones)

        for zone in merged_zones:
            zone['age'] = self._calculate_age(zone, candles)
            zone['strength'] = self._calculate_strength(zone, candles)
            # Конвертируем formed_at в datetime
            if zone.get('formed_at'):
                if isinstance(zone['formed_at'], (int, float)):
                    zone['formed_at_dt'] = datetime.fromtimestamp(zone['formed_at'] / 1000)
                elif isinstance(zone['formed_at'], str):
                    try:
                        zone['formed_at_dt'] = datetime.fromisoformat(zone['formed_at'].replace('Z', '+00:00'))
                    except:
                        zone['formed_at_dt'] = None
                else:
                    zone['formed_at_dt'] = zone['formed_at']
            else:
                zone['formed_at_dt'] = None

        merged_zones.sort(key=lambda x: x['age'])

        if merged_zones:
            logger.info(f"🕳️ Найдено FVG зон: {len(merged_zones)}")
            for z in merged_zones[:3]:
                formed = z.get('formed_at_dt')
                formed_str = formed.strftime('%d.%m.%Y %H:%M') if formed else '?'
                logger.debug(
                    f"   {z['type'].upper()}: {z['low']:.4f}-{z['high']:.4f} (возраст: {z['age']}, образована: {formed_str})")

        return merged_zones

    def _detect_bullish_fvg(
        self,
        c1: Dict[str, Any],
        c2: Dict[str, Any],
        c3: Dict[str, Any],
        idx: int,
        all_candles: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Обнаружение бычьего FVG (поддержка)

        Условие: Low(C3) > High(C1)
        """
        try:
            c1_high = float(c1['high'])
            c3_low = float(c3['low'])

            # Проверяем условие разрыва
            if c3_low <= c1_high:
                return None

            # Проверяем минимальный размер разрыва
            gap_pct = (c3_low - c1_high) / c1_high
            if gap_pct < self.min_gap_pct:
                return None

            return {
                'type': 'bullish',
                'low': c1_high,
                'high': c3_low,
                'formed_at': c2.get('timestamp'),
                'age': 0,
                'strength': 'NORMAL'
            }

        except Exception as e:
            logger.error(f"❌ Ошибка обнаружения bullish FVG: {e}")
            return None

    def _detect_bearish_fvg(
        self,
        c1: Dict[str, Any],
        c2: Dict[str, Any],
        c3: Dict[str, Any],
        idx: int,
        all_candles: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Обнаружение медвежьего FVG (сопротивление)

        Условие: High(C3) < Low(C1)
        """
        try:
            c1_low = float(c1['low'])
            c3_high = float(c3['high'])

            # Проверяем условие разрыва
            if c3_high >= c1_low:
                return None

            # Проверяем минимальный размер разрыва
            gap_pct = (c1_low - c3_high) / c1_low
            if gap_pct < self.min_gap_pct:
                return None

            return {
                'type': 'bearish',
                'low': c1_low,
                'high': c3_high,
                'formed_at': c2.get('timestamp'),
                'age': 0,
                'strength': 'NORMAL'
            }

        except Exception as e:
            logger.error(f"❌ Ошибка обнаружения bearish FVG: {e}")
            return None

    def _merge_overlapping_zones(self, zones: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Объединяет пересекающиеся FVG зоны"""
        if not zones:
            return []

        # Сортируем по нижней границе
        sorted_zones = sorted(zones, key=lambda x: x['low'])
        merged = [sorted_zones[0].copy()]

        for zone in sorted_zones[1:]:
            last = merged[-1]

            # Проверяем пересечение
            if zone['low'] <= last['high']:
                # Объединяем зоны
                last['low'] = min(last['low'], zone['low'])
                last['high'] = max(last['high'], zone['high'])

                # Оставляем более старую дату формирования
                if zone.get('formed_at') and last.get('formed_at'):
                    if zone['formed_at'] < last['formed_at']:
                        last['formed_at'] = zone['formed_at']
            else:
                merged.append(zone.copy())

        return merged

    def _calculate_age(self, zone: Dict[str, Any], candles: List[Dict[str, Any]]) -> int:
        """Рассчитывает возраст FVG зоны (количество свечей с момента формирования)"""
        if not zone.get('formed_at') or not candles:
            return 0

        formed_at = zone['formed_at']

        # Находим позицию свечи формирования
        for i, candle in enumerate(candles):
            if candle.get('timestamp') == formed_at:
                return len(candles) - i - 1

        return 0

    def _calculate_strength(self, zone: Dict[str, Any], candles: List[Dict[str, Any]]) -> str:
        """Рассчитывает силу FVG зоны на основе возраста и количества касаний"""
        age = zone['age']

        # Свежие FVG (до 10 свечей) — сильные
        if age < 10:
            return 'STRONG'
        # Средние (10-30 свечей) — нормальные
        elif age < 30:
            return 'NORMAL'
        # Старые (более 30 свечей) — слабые
        else:
            return 'WEAK'

    def find_nearest_fvg(
        self,
        fvg_zones: List[Dict[str, Any]],
        current_price: float,
        direction: str = 'bullish'
    ) -> Optional[Dict[str, Any]]:
        """
        Находит ближайшую FVG зону к текущей цене

        Args:
            fvg_zones: список FVG зон
            current_price: текущая цена
            direction: 'bullish' или 'bearish'

        Returns:
            Ближайшая зона или None
        """
        if not fvg_zones:
            return None

        filtered = [z for z in fvg_zones if z['type'] == direction]

        if not filtered:
            return None

        if direction == 'bullish':
            # Для BUY ищем поддержку ниже цены
            below = [z for z in filtered if z['high'] < current_price]
            if below:
                # Ближайшая снизу
                return max(below, key=lambda x: x['high'])
            # Если нет ниже, берём самую близкую сверху
            return min(filtered, key=lambda x: x['low'])
        else:
            # Для SELL ищем сопротивление выше цены
            above = [z for z in filtered if z['low'] > current_price]
            if above:
                # Ближайшая сверху
                return min(above, key=lambda x: x['low'])
            # Если нет выше, берём самую близкую снизу
            return max(filtered, key=lambda x: x['high'])

    def is_price_in_fvg(self, price: float, fvg_zone: Dict[str, Any], tolerance_pct: float = 0.5) -> bool:
        """
        Проверяет, находится ли цена в FVG зоне

        Args:
            price: текущая цена
            fvg_zone: FVG зона
            tolerance_pct: допустимое отклонение в процентах

        Returns:
            True если цена в зоне (с учётом допуска)
        """
        if not fvg_zone:
            return False

        low = fvg_zone['low']
        high = fvg_zone['high']
        tolerance = (high - low) * tolerance_pct / 100

        return (low - tolerance) <= price <= (high + tolerance)