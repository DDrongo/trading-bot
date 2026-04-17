# analyzer/core/historical_data_provider.py (ПОЛНОСТЬЮ - ФАЗА 1.5.1)
"""
📦 HISTORICAL DATA PROVIDER — Загрузка исторических свечей из БД
ФАЗА 1.5.1:
- Загрузка свечей за период из ОТДЕЛЬНОЙ БД historical.db
- Сохранение исторических данных
- Кэширование для быстрого доступа
"""

import logging
import asyncio
import os
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

import aiosqlite

from analyzer.core.data_provider import data_provider

logger = logging.getLogger('historical_data_provider')


@dataclass
class HistoricalCandle:
    """Историческая свеча"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp.isoformat(),
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume
        }

    def to_list(self) -> List:
        return [
            int(self.timestamp.timestamp() * 1000),
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume
        ]


class HistoricalDataProvider:
    """
    Провайдер исторических данных
    Использует ОТДЕЛЬНУЮ БД historical.db
    """

    def __init__(self, db_path: str = None, config: Dict = None):
        self.config = config or {}

        # ✅ БЕРЁМ ПУТЬ К ОТДЕЛЬНОЙ БД
        if db_path:
            self.db_path = db_path
        else:
            historical_config = self.config.get('historical_data', {})
            self.db_path = historical_config.get('db_path', 'data/historical.db')

        if not os.path.isabs(self.db_path):
            from pathlib import Path
            project_root = Path(__file__).parent.parent.parent
            self.db_path = str(project_root / self.db_path)

        self._klines_cache: Dict[str, List[HistoricalCandle]] = {}
        self._cache_ttl_hours = 24

        logger.info(f"✅ HistoricalDataProvider инициализирован")
        logger.info(f"   БД: {self.db_path} (ОТДЕЛЬНАЯ ОТ ТОРГОВОЙ)")

    async def initialize(self) -> bool:
        """Инициализация таблицы historical_klines"""
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS historical_klines (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        interval TEXT NOT NULL,
                        timestamp INTEGER NOT NULL,
                        open REAL NOT NULL,
                        high REAL NOT NULL,
                        low REAL NOT NULL,
                        close REAL NOT NULL,
                        volume REAL DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(symbol, interval, timestamp)
                    )
                """)

                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_historical_klines_symbol ON historical_klines(symbol)")
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_historical_klines_interval ON historical_klines(interval)")
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_historical_klines_timestamp ON historical_klines(timestamp)")

                await conn.commit()

            logger.info(f"✅ Таблица historical_klines создана в {self.db_path}")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации historical_klines: {e}")
            return False

    async def load_klines(
            self,
            symbol: str,
            interval: str,
            start_date: datetime,
            end_date: datetime,
            force_refresh: bool = False
    ) -> List[HistoricalCandle]:
        """Загрузка исторических свечей за период"""
        cache_key = f"{symbol}_{interval}_{start_date.date()}_{end_date.date()}"

        if not force_refresh and cache_key in self._klines_cache:
            logger.debug(f"♻️ Кэш попадание: {cache_key}")
            return self._klines_cache[cache_key]

        logger.info(f"📊 Загрузка {symbol} {interval} с {start_date.date()} по {end_date.date()}")

        try:
            db_klines = await self._load_from_db(symbol, interval, start_date, end_date)

            expected_count = self._calculate_expected_candles(interval, start_date, end_date)
            missing_threshold = expected_count * 0.1

            if len(db_klines) < expected_count - missing_threshold or force_refresh:
                logger.info(f"🔄 Недостаточно данных в БД ({len(db_klines)}/{expected_count}), загружаем из API...")

                api_klines = await self._load_from_api(symbol, interval, start_date, end_date)

                if api_klines:
                    await self._save_to_db(symbol, interval, api_klines)
                    db_klines = api_klines

            db_klines.sort(key=lambda x: x.timestamp)
            self._klines_cache[cache_key] = db_klines

            logger.info(f"✅ Загружено {len(db_klines)} свечей {symbol} {interval}")
            return db_klines

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки свечей {symbol} {interval}: {e}")
            return []

    async def _load_from_db(
            self,
            symbol: str,
            interval: str,
            start_date: datetime,
            end_date: datetime
    ) -> List[HistoricalCandle]:
        """Загрузка свечей из БД"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row

                start_ts = int(start_date.timestamp() * 1000)
                end_ts = int(end_date.timestamp() * 1000)

                cursor = await conn.execute("""
                    SELECT * FROM historical_klines 
                    WHERE symbol = ? AND interval = ? 
                    AND timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp ASC
                """, (symbol, interval, start_ts, end_ts))

                rows = await cursor.fetchall()

                candles = []
                for row in rows:
                    candle = HistoricalCandle(
                        timestamp=datetime.fromtimestamp(row['timestamp'] / 1000),
                        open=row['open'],
                        high=row['high'],
                        low=row['low'],
                        close=row['close'],
                        volume=row['volume'] or 0
                    )
                    candles.append(candle)

                return candles

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки из БД: {e}")
            return []

    async def _load_from_api(
            self,
            symbol: str,
            interval: str,
            start_date: datetime,
            end_date: datetime
    ) -> List[HistoricalCandle]:
        """Загрузка свечей из API Bybit"""
        try:
            interval_map = {
                '1w': 'W',
                '1d': 'D',
                '4h': '240',
                '1h': '60',
                '15m': '15',
                '5m': '5'
            }
            api_interval = interval_map.get(interval, interval)

            expected = self._calculate_expected_candles(interval, start_date, end_date)
            limit = min(expected, 1000)

            klines = await data_provider.get_klines(symbol, api_interval, limit)

            if not klines:
                return []

            candles = []
            for k in klines:
                try:
                    candle = HistoricalCandle(
                        timestamp=datetime.fromtimestamp(int(k[0]) / 1000),
                        open=float(k[1]),
                        high=float(k[2]),
                        low=float(k[3]),
                        close=float(k[4]),
                        volume=float(k[5]) if len(k) > 5 else 0
                    )
                    candles.append(candle)
                except (ValueError, TypeError, IndexError):
                    continue

            filtered = [
                c for c in candles
                if start_date <= c.timestamp <= end_date
            ]

            return filtered

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки из API: {e}")
            return []

    async def _save_to_db(
            self,
            symbol: str,
            interval: str,
            candles: List[HistoricalCandle]
    ) -> int:
        """Сохранение свечей в БД"""
        if not candles:
            return 0

        saved = 0
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                for candle in candles:
                    ts = int(candle.timestamp.timestamp() * 1000)

                    await conn.execute("""
                        INSERT INTO historical_klines 
                        (symbol, interval, timestamp, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(symbol, interval, timestamp) DO UPDATE SET
                            open = excluded.open,
                            high = excluded.high,
                            low = excluded.low,
                            close = excluded.close,
                            volume = excluded.volume
                    """, (
                        symbol, interval, ts,
                        candle.open, candle.high, candle.low, candle.close, candle.volume
                    ))
                    saved += 1

                await conn.commit()

            logger.info(f"💾 Сохранено {saved} свечей в БД")
            return saved

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в БД: {e}")
            return 0

    def _calculate_expected_candles(
            self,
            interval: str,
            start_date: datetime,
            end_date: datetime
    ) -> int:
        """Расчёт ожидаемого количества свечей"""
        delta = end_date - start_date
        total_seconds = delta.total_seconds()

        interval_seconds = {
            '1w': 7 * 24 * 3600,
            '1d': 24 * 3600,
            '4h': 4 * 3600,
            '1h': 3600,
            '15m': 15 * 60,
            '5m': 5 * 60
        }

        seconds_per_candle = interval_seconds.get(interval, 3600)
        return int(total_seconds / seconds_per_candle) + 1

    async def get_available_symbols(self) -> List[str]:
        """Получение списка символов с историческими данными"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    SELECT DISTINCT symbol FROM historical_klines
                    ORDER BY symbol
                """)
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

        except Exception as e:
            logger.error(f"❌ Ошибка получения символов: {e}")
            return []

    async def preload_data(
            self,
            symbols: List[str],
            intervals: List[str],
            years_back: float = 1.0
    ) -> Dict[str, int]:
        """Предзагрузка исторических данных для символов"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=int(years_back * 365))

        results = {}

        for symbol in symbols:
            total_loaded = 0

            for interval in intervals:
                candles = await self.load_klines(symbol, interval, start_date, end_date)
                total_loaded += len(candles)

            results[symbol] = total_loaded

        logger.info(f"✅ Предзагружены данные для {len(symbols)} символов")
        return results

    def clear_cache(self) -> None:
        """Очистка кэша"""
        self._klines_cache.clear()
        logger.info("🧹 Кэш исторических данных очищен")

    async def get_statistics(self) -> Dict[str, Any]:
        """Статистика по историческим данным"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row

                cursor = await conn.execute("""
                    SELECT 
                        COUNT(*) as total_candles,
                        COUNT(DISTINCT symbol) as symbols_count,
                        COUNT(DISTINCT interval) as intervals_count,
                        MIN(timestamp) as oldest_candle,
                        MAX(timestamp) as newest_candle
                    FROM historical_klines
                """)
                stats = dict(await cursor.fetchone())

                if stats['oldest_candle']:
                    stats['oldest_candle'] = datetime.fromtimestamp(stats['oldest_candle'] / 1000).isoformat()
                if stats['newest_candle']:
                    stats['newest_candle'] = datetime.fromtimestamp(stats['newest_candle'] / 1000).isoformat()

                return stats

        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}


historical_data_provider = HistoricalDataProvider()

__all__ = [
    'HistoricalCandle',
    'HistoricalDataProvider',
    'historical_data_provider'
]