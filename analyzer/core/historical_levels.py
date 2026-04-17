# analyzer/core/historical_levels.py (ПОЛНОСТЬЮ - ФАЗА 1.5.1)
"""
📊 HISTORICAL LEVELS — Сбор исторических уровней поддержки/сопротивления
ФАЗА 1.5.1:
- Сбор уровней с W1 (2 года) и D1 (1 год)
- Определение силы уровня (VERY_STRONG/STRONG/WEAK)
- Сохранение в ОТДЕЛЬНУЮ БД historical.db
"""

import logging
import asyncio
import os
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

import aiosqlite

logger = logging.getLogger('historical_levels')


class LevelStrength(Enum):
    """Сила уровня"""
    VERY_STRONG = "VERY_STRONG"  # 5+ касаний или W1 уровень
    STRONG = "STRONG"  # 3-4 касания
    WEAK = "WEAK"  # 2 касания


class LevelType(Enum):
    """Тип уровня"""
    SUPPORT = "SUPPORT"
    RESISTANCE = "RESISTANCE"


@dataclass
class HistoricalLevel:
    """Исторический уровень поддержки/сопротивления"""
    symbol: str
    price: float
    level_type: LevelType
    strength: LevelStrength
    touches: int
    timeframe: str
    first_seen: datetime
    last_touch: datetime
    volume_at_level: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'symbol': self.symbol,
            'price': self.price,
            'level_type': self.level_type.value,
            'strength': self.strength.value,
            'touches': self.touches,
            'timeframe': self.timeframe,
            'first_seen': self.first_seen.isoformat() if self.first_seen else None,
            'last_touch': self.last_touch.isoformat() if self.last_touch else None,
            'volume_at_level': self.volume_at_level
        }


class HistoricalLevelsCollector:
    """
    Сборщик исторических уровней поддержки/сопротивления
    Использует ОТДЕЛЬНУЮ БД historical.db
    """

    def __init__(self, db_path: str = None, config: Dict = None):
        self.config = config or {}

        # ✅ БЕРЁМ ПУТЬ К ОТДЕЛЬНОЙ БД ИЗ КОНФИГА
        if db_path:
            self.db_path = db_path
        else:
            historical_config = self.config.get('historical_data', {})
            self.db_path = historical_config.get('db_path', 'data/historical.db')

        # Делаем путь абсолютным
        if not os.path.isabs(self.db_path):
            from pathlib import Path
            project_root = Path(__file__).parent.parent.parent
            self.db_path = str(project_root / self.db_path)

        # Настройки
        levels_config = self.config.get('historical_data', {}).get('levels', {})
        self.w1_years = levels_config.get('w1_years', 2)
        self.d1_years = levels_config.get('d1_years', 1)

        # Параметры поиска уровней
        self.touch_tolerance_pct = levels_config.get('touch_tolerance_pct', 0.5)
        self.min_touches_for_level = levels_config.get('min_touches', 2)
        self.very_strong_touches = levels_config.get('very_strong_touches', 5)
        self.strong_touches = levels_config.get('strong_touches', 3)

        # Кэш
        self._levels_cache: Dict[str, List[HistoricalLevel]] = {}

        logger.info(f"✅ HistoricalLevelsCollector инициализирован")
        logger.info(f"   БД: {self.db_path} (ОТДЕЛЬНАЯ ОТ ТОРГОВОЙ)")
        logger.info(f"   W1: {self.w1_years} лет, D1: {self.d1_years} год")
        logger.info(f"   Допуск касания: {self.touch_tolerance_pct}%")
        logger.info(f"   Мин. касаний: {self.min_touches_for_level}")

    async def initialize(self) -> bool:
        """Инициализация таблицы historical_levels"""
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS historical_levels (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        price REAL NOT NULL,
                        level_type TEXT NOT NULL,
                        strength TEXT NOT NULL,
                        touches INTEGER DEFAULT 0,
                        timeframe TEXT NOT NULL,
                        first_seen TIMESTAMP,
                        last_touch TIMESTAMP,
                        volume_at_level REAL DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(symbol, price, timeframe, level_type)
                    )
                """)

                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_historical_levels_symbol ON historical_levels(symbol)")
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_historical_levels_timeframe ON historical_levels(timeframe)")
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_historical_levels_strength ON historical_levels(strength)")

                await conn.commit()

            logger.info(f"✅ Таблица historical_levels создана в {self.db_path}")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации historical_levels: {e}")
            return False

    async def collect_levels(
            self,
            symbol: str,
            timeframe: str,
            years_back: float = None
    ) -> List[HistoricalLevel]:
        """
        Сбор исторических уровней для символа с отображением прогресса
        """
        if years_back is None:
            years_back = self.w1_years if timeframe == 'W1' else self.d1_years

        logger.info(f"🔍 [{symbol}] Этап 1/4: Начинаем сбор уровней на {timeframe} за {years_back} лет...")

        try:
            # Этап 1: Получение данных
            logger.info(f"📡 [{symbol}] Этап 2/4: Загружаем исторические свечи {timeframe}...")
            klines = await self._get_historical_klines(symbol, timeframe, years_back)

            if not klines or len(klines) < 50:
                logger.warning(
                    f"⚠️ [{symbol}] Недостаточно данных для {timeframe}: {len(klines) if klines else 0} свечей")
                return []

            logger.info(f"✅ [{symbol}] Этап 2/4: Получено {len(klines)} свечей (25%)")

            # Этап 2: Поиск уровней
            logger.info(f"🔎 [{symbol}] Этап 3/4: Ищем уровни поддержки и сопротивления...")
            support_levels = self._find_support_levels(klines, timeframe)
            resistance_levels = self._find_resistance_levels(klines, timeframe)

            all_levels = support_levels + resistance_levels
            logger.info(f"✅ [{symbol}] Этап 3/4: Найдено {len(all_levels)} сырых уровней (50%)")

            # Этап 3: Фильтрация и группировка
            logger.info(f"🔬 [{symbol}] Этап 3/4: Фильтруем и группируем уровни...")
            filtered_levels = self._filter_and_merge_levels(all_levels)

            for level in filtered_levels:
                level.touches = self._count_touches(level.price, klines, level.level_type)
                level.strength = self._determine_strength(level.touches, timeframe)

            valid_levels = [l for l in filtered_levels if l.touches >= self.min_touches_for_level]
            logger.info(f"✅ [{symbol}] Этап 3/4: Отфильтровано {len(valid_levels)} качественных уровней (75%)")

            # Этап 4: Сохранение
            logger.info(f"💾 [{symbol}] Этап 4/4: Сохраняем {len(valid_levels)} уровней в БД...")

            cache_key = f"{symbol}_{timeframe}"
            self._levels_cache[cache_key] = valid_levels

            logger.info(f"🎉 [{symbol}] ГОТОВО! {timeframe}: найдено {len(valid_levels)} уровней (100%)")

            return valid_levels

        except Exception as e:
            logger.error(f"❌ [{symbol}] Ошибка сбора уровней {timeframe}: {e}")
            import traceback
            traceback.print_exc()
            return []

    async def _get_historical_klines(
            self,
            symbol: str,
            timeframe: str,
            years_back: float
    ) -> List[Dict]:
        """Получение исторических свечей"""
        try:
            from analyzer.core.data_provider import data_provider

            interval_map = {
                'W1': '1w',
                'D1': '1d',
                '1w': '1w',
                '1d': '1d'
            }
            interval = interval_map.get(timeframe, timeframe)

            if timeframe in ('W1', '1w'):
                candles_needed = int(years_back * 52)
            else:
                candles_needed = int(years_back * 365)

            limit = min(candles_needed, 1000)

            klines = await data_provider.get_klines(symbol, interval, limit)

            if not klines:
                return []

            result = []
            for k in klines:
                try:
                    result.append({
                        'timestamp': k[0],
                        'open': float(k[1]),
                        'high': float(k[2]),
                        'low': float(k[3]),
                        'close': float(k[4]),
                        'volume': float(k[5]) if len(k) > 5 else 0
                    })
                except (ValueError, TypeError, IndexError):
                    continue

            return result

        except Exception as e:
            logger.error(f"❌ Ошибка получения исторических свечей {symbol} {timeframe}: {e}")
            return []

    def _find_support_levels(self, klines: List[Dict], timeframe: str) -> List[HistoricalLevel]:
        """Поиск уровней поддержки с отображением прогресса"""
        supports = []
        lows = [k['low'] for k in klines]
        total_candles = len(klines)

        logger.debug(f"   🔍 Поиск поддержек в {total_candles} свечах...")

        lookback = 5
        for i in range(lookback, len(klines) - lookback):
            current_low = lows[i]
            left_lows = lows[i - lookback:i]
            right_lows = lows[i + 1:i + lookback + 1]

            if current_low <= min(left_lows) and current_low <= min(right_lows):
                timestamp = klines[i]['timestamp']
                if isinstance(timestamp, (int, float)):
                    dt = datetime.fromtimestamp(timestamp / 1000)
                else:
                    dt = datetime.now()

                level = HistoricalLevel(
                    symbol="",
                    price=current_low,
                    level_type=LevelType.SUPPORT,
                    strength=LevelStrength.WEAK,
                    touches=1,
                    timeframe=timeframe,
                    first_seen=dt,
                    last_touch=dt,
                    volume_at_level=klines[i]['volume']
                )
                supports.append(level)

            # Логируем прогресс каждые 50 свечей
            if (i + 1) % 50 == 0:
                logger.debug(f"   📊 Просканировано {i + 1}/{total_candles} свечей, найдено {len(supports)} поддержек")

        logger.debug(f"   ✅ Найдено {len(supports)} поддержек")
        return supports

    def _find_resistance_levels(self, klines: List[Dict], timeframe: str) -> List[HistoricalLevel]:
        """Поиск уровней сопротивления с отображением прогресса"""
        resistances = []
        highs = [k['high'] for k in klines]
        total_candles = len(klines)

        logger.debug(f"   🔍 Поиск сопротивлений в {total_candles} свечах...")

        lookback = 5
        for i in range(lookback, len(klines) - lookback):
            current_high = highs[i]
            left_highs = highs[i - lookback:i]
            right_highs = highs[i + 1:i + lookback + 1]

            if current_high >= max(left_highs) and current_high >= max(right_highs):
                timestamp = klines[i]['timestamp']
                if isinstance(timestamp, (int, float)):
                    dt = datetime.fromtimestamp(timestamp / 1000)
                else:
                    dt = datetime.now()

                level = HistoricalLevel(
                    symbol="",
                    price=current_high,
                    level_type=LevelType.RESISTANCE,
                    strength=LevelStrength.WEAK,
                    touches=1,
                    timeframe=timeframe,
                    first_seen=dt,
                    last_touch=dt,
                    volume_at_level=klines[i]['volume']
                )
                resistances.append(level)

            # Логируем прогресс каждые 50 свечей
            if (i + 1) % 50 == 0:
                logger.debug(
                    f"   📊 Просканировано {i + 1}/{total_candles} свечей, найдено {len(resistances)} сопротивлений")

        logger.debug(f"   ✅ Найдено {len(resistances)} сопротивлений")
        return resistances

    def _filter_and_merge_levels(self, levels: List[HistoricalLevel]) -> List[HistoricalLevel]:
        """Фильтрация и объединение близких уровней"""
        if not levels:
            return []

        sorted_levels = sorted(levels, key=lambda x: x.price)

        merged = []
        current_group = [sorted_levels[0]]

        for level in sorted_levels[1:]:
            prev_price = current_group[-1].price
            diff_pct = abs(level.price - prev_price) / prev_price * 100

            if diff_pct < self.touch_tolerance_pct:
                current_group.append(level)
            else:
                merged_level = self._merge_level_group(current_group)
                merged.append(merged_level)
                current_group = [level]

        if current_group:
            merged_level = self._merge_level_group(current_group)
            merged.append(merged_level)

        return merged

    def _merge_level_group(self, group: List[HistoricalLevel]) -> HistoricalLevel:
        """Объединение группы уровней в один"""
        avg_price = sum(l.price for l in group) / len(group)
        total_volume = sum(l.volume_at_level for l in group)
        first_seen = min(l.first_seen for l in group)
        last_touch = max(l.last_touch for l in group)

        supports = sum(1 for l in group if l.level_type == LevelType.SUPPORT)
        resistances = len(group) - supports
        level_type = LevelType.SUPPORT if supports >= resistances else LevelType.RESISTANCE

        return HistoricalLevel(
            symbol=group[0].symbol,
            price=avg_price,
            level_type=level_type,
            strength=LevelStrength.WEAK,
            touches=len(group),
            timeframe=group[0].timeframe,
            first_seen=first_seen,
            last_touch=last_touch,
            volume_at_level=total_volume
        )

    def _count_touches(self, price: float, klines: List[Dict], level_type: LevelType) -> int:
        """Подсчёт количества касаний уровня"""
        touches = 0
        tolerance = price * self.touch_tolerance_pct / 100

        for k in klines:
            if level_type == LevelType.SUPPORT:
                if abs(k['low'] - price) <= tolerance:
                    touches += 1
            else:
                if abs(k['high'] - price) <= tolerance:
                    touches += 1

        return touches

    def _determine_strength(self, touches: int, timeframe: str) -> LevelStrength:
        """Определение силы уровня"""
        if timeframe == 'W1':
            if touches >= 3:
                return LevelStrength.VERY_STRONG
            elif touches >= 2:
                return LevelStrength.STRONG
            else:
                return LevelStrength.WEAK
        else:
            if touches >= self.very_strong_touches:
                return LevelStrength.VERY_STRONG
            elif touches >= self.strong_touches:
                return LevelStrength.STRONG
            else:
                return LevelStrength.WEAK

    async def save_levels_to_db(self, symbol: str, levels: List[HistoricalLevel]) -> int:
        """Сохранение уровней в БД"""
        if not levels:
            return 0

        saved = 0
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                for level in levels:
                    level.symbol = symbol

                    cursor = await conn.execute("""
                        SELECT id FROM historical_levels 
                        WHERE symbol = ? AND price = ? AND timeframe = ? AND level_type = ?
                    """, (symbol, level.price, level.timeframe, level.level_type.value))

                    existing = await cursor.fetchone()

                    if existing:
                        await conn.execute("""
                            UPDATE historical_levels 
                            SET strength = ?, touches = ?, last_touch = ?, 
                                volume_at_level = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        """, (
                            level.strength.value,
                            level.touches,
                            level.last_touch.isoformat() if level.last_touch else None,
                            level.volume_at_level,
                            existing[0]
                        ))
                    else:
                        await conn.execute("""
                            INSERT INTO historical_levels 
                            (symbol, price, level_type, strength, touches, timeframe, 
                             first_seen, last_touch, volume_at_level)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            symbol,
                            level.price,
                            level.level_type.value,
                            level.strength.value,
                            level.touches,
                            level.timeframe,
                            level.first_seen.isoformat() if level.first_seen else None,
                            level.last_touch.isoformat() if level.last_touch else None,
                            level.volume_at_level
                        ))

                    saved += 1

                await conn.commit()

            logger.info(f"✅ Сохранено {saved} уровней для {symbol}")
            return saved

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения уровней {symbol}: {e}")
            return 0

    async def get_historical_levels(
            self,
            symbol: str,
            min_strength: LevelStrength = LevelStrength.STRONG
    ) -> List[HistoricalLevel]:
        """Загрузка исторических уровней из БД"""
        try:
            cache_key = f"db_{symbol}_{min_strength.value}"
            if cache_key in self._levels_cache:
                return self._levels_cache[cache_key]

            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row

                if min_strength == LevelStrength.VERY_STRONG:
                    strength_filter = "('VERY_STRONG')"
                elif min_strength == LevelStrength.STRONG:
                    strength_filter = "('VERY_STRONG', 'STRONG')"
                else:
                    strength_filter = "('VERY_STRONG', 'STRONG', 'WEAK')"

                cursor = await conn.execute(f"""
                    SELECT * FROM historical_levels 
                    WHERE symbol = ? AND strength IN {strength_filter}
                    ORDER BY strength DESC, price ASC
                """, (symbol,))

                rows = await cursor.fetchall()

                levels = []
                for row in rows:
                    level = HistoricalLevel(
                        symbol=row['symbol'],
                        price=row['price'],
                        level_type=LevelType(row['level_type']),
                        strength=LevelStrength(row['strength']),
                        touches=row['touches'],
                        timeframe=row['timeframe'],
                        first_seen=datetime.fromisoformat(row['first_seen']) if row['first_seen'] else None,
                        last_touch=datetime.fromisoformat(row['last_touch']) if row['last_touch'] else None,
                        volume_at_level=row['volume_at_level'] or 0
                    )
                    levels.append(level)

                self._levels_cache[cache_key] = levels

                logger.info(f"📊 Загружено {len(levels)} исторических уровней для {symbol}")
                return levels

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки уровней {symbol}: {e}")
            return []

    async def collect_and_save_all(
            self,
            symbols: List[str],
            timeframes: List[str] = None
    ) -> Dict[str, int]:
        """
        Сбор и сохранение уровней для всех символов с отображением прогресса
        """
        if timeframes is None:
            timeframes = ['W1', 'D1']

        results = {}
        total_symbols = len(symbols)

        logger.info(f"📊 ЗАПУСК СБОРА УРОВНЕЙ ДЛЯ {total_symbols} МОНЕТ")
        logger.info(f"   Таймфреймы: {', '.join(timeframes)}")
        logger.info(f"   Это займёт ~30-60 секунд на монету...")
        logger.info(f"")

        for idx, symbol in enumerate(symbols, 1):
            logger.info(f"🔄 [{idx}/{total_symbols}] Обрабатываем {symbol}...")
            total_saved = 0

            for tf in timeframes:
                levels = await self.collect_levels(symbol, tf)
                saved = await self.save_levels_to_db(symbol, levels)
                total_saved += saved

                if saved > 0:
                    logger.info(f"   ✅ {tf}: сохранено {saved} уровней")

            results[symbol] = total_saved

            if total_saved > 0:
                logger.info(f"🎯 [{idx}/{total_symbols}] {symbol}: ВСЕГО {total_saved} уровней")
            else:
                logger.warning(f"⚠️ [{idx}/{total_symbols}] {symbol}: уровней не найдено")

            logger.info(f"")  # пустая строка для читаемости

        total = sum(results.values())
        logger.info(f"🏁 СБОР ЗАВЕРШЁН! Всего {total} уровней для {len([r for r in results.values() if r > 0])} монет")

        return results

    async def get_levels_near_price(
            self,
            symbol: str,
            current_price: float,
            range_pct: float = 5.0,
            min_strength: LevelStrength = LevelStrength.STRONG
    ) -> Tuple[List[HistoricalLevel], List[HistoricalLevel]]:
        """Получение уровней вблизи текущей цены"""
        all_levels = await self.get_historical_levels(symbol, min_strength)

        price_range = current_price * range_pct / 100
        min_price = current_price - price_range
        max_price = current_price + price_range

        supports = []
        resistances = []

        for level in all_levels:
            if min_price <= level.price <= max_price:
                if level.level_type == LevelType.SUPPORT:
                    supports.append(level)
                else:
                    resistances.append(level)

        supports.sort(key=lambda x: x.price, reverse=True)
        resistances.sort(key=lambda x: x.price)

        return supports, resistances

    def clear_cache(self) -> None:
        """Очистка кэша"""
        self._levels_cache.clear()
        logger.info("🧹 Кэш исторических уровней очищен")

    async def get_statistics(self) -> Dict[str, Any]:
        """Статистика по уровням в БД"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row

                cursor = await conn.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN timeframe = 'W1' THEN 1 ELSE 0 END) as w1_count,
                        SUM(CASE WHEN timeframe = 'D1' THEN 1 ELSE 0 END) as d1_count,
                        SUM(CASE WHEN strength = 'VERY_STRONG' THEN 1 ELSE 0 END) as very_strong,
                        SUM(CASE WHEN strength = 'STRONG' THEN 1 ELSE 0 END) as strong,
                        SUM(CASE WHEN strength = 'WEAK' THEN 1 ELSE 0 END) as weak,
                        COUNT(DISTINCT symbol) as symbols_count
                    FROM historical_levels
                """)
                stats = dict(await cursor.fetchone())

                return stats

        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}


historical_levels = HistoricalLevelsCollector()

__all__ = [
    'HistoricalLevel',
    'HistoricalLevelsCollector',
    'LevelStrength',
    'LevelType',
    'historical_levels'
]