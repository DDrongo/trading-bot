# analyzer/core/signal_repository.py (ПОЛНОСТЬЮ - ФАЗА 1.3.10)
"""
📊 SIGNAL REPOSITORY - Репозиторий для работы с сигналами
ФАЗА 1.3.9.2:
- Добавлен метод save_signal() для M15 сигналов
- Добавлены поля current_price_at_signal и position_vs_zone
- Сохранение цены на момент создания WATCH и M15 сигналов

ФАЗА 1.3.10:
- Добавлена таблица trend_analysis
- Добавлены методы save_trend_analysis(), get_trend_analysis(), get_latest_trends()
"""

import aiosqlite
import logging
import os
from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime, timedelta

from analyzer.core.event_bus import EventType, event_bus as global_event_bus
from analyzer.core.time_utils import utc_now, iso_utc

logger = logging.getLogger("signal_repository")


class SignalRepository:
    def __init__(self, config=None, db_path: Optional[str] = None):
        self.config = config or {}
        self.event_bus = global_event_bus

        if db_path:
            self.db_path = db_path
        else:
            project_root = Path(__file__).parent.parent.parent
            self.db_path = str(project_root / "data/trading_bot.db")

        logger.info(f"📊 SignalRepository использует: {self.db_path}")

    async def initialize(self) -> bool:
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signals'")
                table_exists = await cursor.fetchone()

                if not table_exists:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS signals (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            symbol TEXT NOT NULL,
                            strategy TEXT DEFAULT "three_screen",
                            direction TEXT NOT NULL,
                            signal_subtype TEXT DEFAULT "M15",
                            status TEXT DEFAULT "WATCH",
                            confidence REAL DEFAULT 0.0,
                            entry_price REAL DEFAULT 0.0,
                            stop_loss REAL DEFAULT 0.0,
                            take_profit REAL DEFAULT 0.0,
                            trend_direction TEXT,
                            trend_strength TEXT,
                            signal_strength TEXT,
                            trigger_pattern TEXT,
                            risk_reward_ratio REAL,
                            risk_pct REAL,
                            expiration_time TIMESTAMP,
                            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            screen TEXT DEFAULT "M15",
                            order_type TEXT DEFAULT "MARKET",
                            fill_price REAL,
                            position_size REAL,
                            rejection_reason TEXT,
                            zone_low REAL,
                            zone_high REAL,
                            expected_pattern TEXT,
                            screen2_score INTEGER,
                            leverage REAL DEFAULT 10,
                            margin REAL,
                            position_value REAL,
                            reserved_margin REAL,
                            current_price_at_signal REAL,
                            position_vs_zone TEXT
                        )
                    """)
                    logger.info("✅ Таблица signals создана")
                else:
                    cursor = await conn.execute("PRAGMA table_info(signals)")
                    columns = await cursor.fetchall()
                    column_names = [col[1] for col in columns]

                    fields_to_add = [
                        'signal_subtype', 'expiration_time', 'screen',
                        'order_type', 'fill_price', 'position_size', 'rejection_reason',
                        'zone_low', 'zone_high', 'expected_pattern', 'screen2_score',
                        'updated_time', 'leverage', 'margin', 'position_value', 'reserved_margin',
                        'current_price_at_signal', 'position_vs_zone'
                    ]

                    for field in fields_to_add:
                        if field not in column_names:
                            await conn.execute(f"ALTER TABLE signals ADD COLUMN {field} TEXT")
                            logger.info(f"✅ Добавлено поле {field}")

                cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades'")
                trades_exists = await cursor.fetchone()

                if not trades_exists:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS trades (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            signal_id INTEGER,
                            symbol TEXT,
                            direction TEXT,
                            entry_price REAL,
                            close_price REAL,
                            quantity REAL,
                            leverage REAL DEFAULT 10,
                            margin REAL,
                            position_value REAL,
                            pnl REAL,
                            pnl_percent REAL,
                            commission REAL,
                            close_reason TEXT,
                            opened_at TIMESTAMP,
                            closed_at TIMESTAMP,
                            status TEXT,
                            order_type TEXT DEFAULT 'LIMIT',
                            fill_price REAL,
                            FOREIGN KEY (signal_id) REFERENCES signals(id)
                        )
                    """)
                    logger.info("✅ Таблица trades создана")
                else:
                    cursor = await conn.execute("PRAGMA table_info(trades)")
                    columns = await cursor.fetchall()
                    column_names = [col[1] for col in columns]

                    trade_fields = ['leverage', 'margin', 'position_value']
                    for field in trade_fields:
                        if field not in column_names:
                            await conn.execute(f"ALTER TABLE trades ADD COLUMN {field} REAL")
                            logger.info(f"✅ Добавлено поле {field}")

                # Фаза 1.3.10: Создание таблицы trend_analysis
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='trend_analysis'")
                trend_table_exists = await cursor.fetchone()

                if not trend_table_exists:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS trend_analysis (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            symbol TEXT NOT NULL,
                            trend_direction TEXT NOT NULL,
                            adx REAL,
                            ema20 REAL,
                            ema50 REAL,
                            macd_line REAL,
                            macd_signal REAL,
                            structure TEXT,
                            confidence REAL,
                            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(symbol, created_time)
                        )
                    """)
                    logger.info("✅ Таблица trend_analysis создана (Фаза 1.3.10)")

                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_time)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_subtype ON signals(signal_subtype)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_signal_id ON trades(signal_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_opened ON trades(opened_at)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol_status ON signals(symbol, status)")
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_signals_subtype_status ON signals(signal_subtype, status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_watch_status ON signals(status, expiration_time)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_trend_symbol ON trend_analysis(symbol)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_trend_created ON trend_analysis(created_time)")

                await conn.commit()
                logger.info("✅ База данных инициализирована")
                return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
            return False

    async def check_duplicate_signal(self, symbol: str, signal_subtype: str, expiration_hours: int = 24) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("""
                    SELECT COUNT(*) as count FROM signals 
                    WHERE symbol = ? 
                    AND signal_subtype = ?
                    AND status IN ('WATCH', 'ACTIVE', 'PENDING')
                    AND created_time > datetime('now', ?)
                """, (symbol, signal_subtype, f'-{expiration_hours} hours'))
                row = await cursor.fetchone()
                return (row['count'] if row else 0) > 0
        except Exception as e:
            logger.error(f"❌ Ошибка проверки дубликата: {e}")
            return False

    async def has_active_m15(self, symbol: str) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    SELECT COUNT(*) as count FROM signals 
                    WHERE symbol = ? 
                    AND signal_subtype = 'M15'
                    AND status = 'ACTIVE'
                    AND expiration_time > datetime('now')
                """, (symbol,))
                row = await cursor.fetchone()
                return row[0] > 0
        except Exception as e:
            logger.error(f"❌ Ошибка проверки активного M15: {e}")
            return False

    async def get_watch_symbols(self) -> List[str]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    SELECT symbol FROM signals 
                    WHERE status = 'WATCH' 
                    AND expiration_time > datetime('now')
                """)
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка получения WATCH символов: {e}")
            return []

    async def save_watch_signal(
            self,
            symbol: str,
            direction: str,
            zone_low: float,
            zone_high: float,
            screen2_score: int,
            expected_pattern: str = None,
            expiration_hours: int = 3,
            position_size: float = None,
            entry_price: float = None,
            leverage: float = 10,
            current_price: float = None
    ) -> Optional[int]:
        try:
            expiration_time = utc_now() + timedelta(hours=expiration_hours)

            position_vs_zone = ""
            if current_price is not None and zone_low > 0 and zone_high > 0:
                if current_price > zone_high:
                    position_vs_zone = "ABOVE"
                elif current_price < zone_low:
                    position_vs_zone = "BELOW"
                else:
                    position_vs_zone = "INSIDE"
                logger.info(
                    f"📊 {symbol}: цена={current_price:.4f}, зона={zone_low:.4f}-{zone_high:.4f}, позиция={position_vs_zone}")

            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    SELECT id FROM signals 
                    WHERE symbol = ? AND status = 'WATCH' 
                    AND expiration_time > datetime('now')
                """, (symbol,))
                existing = await cursor.fetchone()

                if existing:
                    await conn.execute("""
                        UPDATE signals SET
                            zone_low = ?, zone_high = ?, screen2_score = ?,
                            expected_pattern = ?, expiration_time = ?,
                            direction = ?, position_size = ?, entry_price = ?,
                            leverage = ?, current_price_at_signal = ?,
                            position_vs_zone = ?, updated_time = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (zone_low, zone_high, screen2_score, expected_pattern,
                          expiration_time.isoformat(), direction, position_size,
                          entry_price, leverage, current_price or 0,
                          position_vs_zone, existing[0]))
                    await conn.commit()
                    logger.info(f"🔄 Обновлён WATCH сигнал для {symbol}")
                    return existing[0]
                else:
                    cursor = await conn.execute("""
                        INSERT INTO signals (
                            symbol, direction, signal_subtype, status,
                            zone_low, zone_high, screen2_score, expected_pattern,
                            expiration_time, position_size, entry_price, leverage,
                            current_price_at_signal, position_vs_zone,
                            created_time, updated_time
                        ) VALUES (?, ?, 'WATCH', 'WATCH', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """, (symbol, direction, zone_low, zone_high, screen2_score,
                          expected_pattern, expiration_time.isoformat(),
                          position_size, entry_price, leverage,
                          current_price or 0, position_vs_zone))
                    await conn.commit()
                    signal_id = cursor.lastrowid
                    logger.info(
                        f"✅ Создан WATCH сигнал для {symbol} (ID={signal_id}, цена={current_price:.4f}, позиция={position_vs_zone})")

                    await self.event_bus.publish(
                        EventType.WATCH_CREATED,
                        {
                            'signal_id': signal_id,
                            'symbol': symbol,
                            'direction': direction,
                            'position_size': position_size,
                            'entry_price': entry_price,
                            'leverage': leverage,
                            'expiration_time': expiration_time.isoformat(),
                            'current_price_at_signal': current_price,
                            'position_vs_zone': position_vs_zone
                        },
                        'signal_repository'
                    )

                    return signal_id

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения WATCH сигнала {symbol}: {e}")
            return None

    # ========== ФАЗА 1.3.9.2: МЕТОД ДЛЯ M15 СИГНАЛОВ ==========
    async def save_signal(self, analysis) -> Optional[int]:
        """
        Сохраняет M15 сигнал в БД и публикует событие для Position Manager

        Args:
            analysis: ThreeScreenAnalysis с результатами анализа

        Returns:
            ID сигнала или None при ошибке
        """
        try:
            from analyzer.core.data_classes import ThreeScreenAnalysis

            screen3 = analysis.screen3
            if not screen3 or not screen3.passed:
                logger.warning(f"⚠️ Попытка сохранить непрошедший сигнал {analysis.symbol}")
                return None

            current_price = screen3.entry_price

            # Рассчитываем позицию цены относительно зоны
            position_vs_zone = ""
            if analysis.zone_low > 0 and analysis.zone_high > 0:
                if current_price > analysis.zone_high:
                    position_vs_zone = "ABOVE"
                elif current_price < analysis.zone_low:
                    position_vs_zone = "BELOW"
                else:
                    position_vs_zone = "INSIDE"

            expiration_time = utc_now() + timedelta(hours=3)

            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    INSERT INTO signals (
                        symbol, direction, signal_subtype, status,
                        entry_price, stop_loss, take_profit,
                        trigger_pattern, confidence, risk_reward_ratio,
                        current_price_at_signal, position_vs_zone,
                        zone_low, zone_high, screen2_score, expected_pattern,
                        expiration_time, order_type,
                        created_time, updated_time
                    ) VALUES (?, ?, 'M15', 'ACTIVE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'MARKET', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, (
                    analysis.symbol,
                    "BUY" if screen3.signal_type == "BUY" else "SELL",
                    screen3.entry_price,
                    screen3.stop_loss,
                    screen3.take_profit,
                    screen3.trigger_pattern,
                    analysis.overall_confidence,
                    screen3.indicators.get('risk_reward_ratio', 0),
                    current_price,
                    position_vs_zone,
                    analysis.zone_low,
                    analysis.zone_high,
                    analysis.screen2_score,
                    analysis.expected_pattern,
                    expiration_time.isoformat()
                ))
                await conn.commit()
                signal_id = cursor.lastrowid
                logger.info(
                    f"✅ M15 сигнал {analysis.symbol} сохранён (ID={signal_id}, цена={current_price:.4f}, R/R={screen3.indicators.get('risk_reward_ratio', 0):.2f})")

                # Публикуем событие для Position Manager
                await self.event_bus.publish(
                    EventType.TRADING_SIGNAL_GENERATED,
                    {
                        'signal_id': signal_id,
                        'symbol': analysis.symbol,
                        'signal_type': screen3.signal_type,
                        'entry_price': screen3.entry_price,
                        'stop_loss': screen3.stop_loss,
                        'take_profit': screen3.take_profit,
                        'confidence': analysis.overall_confidence,
                        'risk_reward_ratio': screen3.indicators.get('risk_reward_ratio', 0),
                        'signal_subtype': 'M15',
                        'order_type': 'MARKET',
                        'expiration_time': expiration_time.isoformat(),
                        'leverage': self.config.get('paper_trading', {}).get('leverage', 10) if self.config else 10
                    },
                    'signal_repository'
                )

                return signal_id

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения M15 сигнала {analysis.symbol}: {e}")
            import traceback
            traceback.print_exc()
            return None

    # ========== ФАЗА 1.3.10: МЕТОД ДЛЯ СОХРАНЕНИЯ ТРЕНДА ==========
    async def save_trend_analysis(
            self,
            symbol: str,
            trend_direction: str,
            adx: float,
            ema20: float,
            ema50: float,
            macd_line: float,
            macd_signal: float,
            structure: str,
            confidence: float
    ) -> Optional[int]:
        """
        Сохраняет результат анализа тренда D1 в таблицу trend_analysis

        Args:
            symbol: Символ монеты (например, BTCUSDT)
            trend_direction: BULL / BEAR / SIDEWAYS
            adx: Значение ADX
            ema20: Значение EMA20
            ema50: Значение EMA50
            macd_line: Значение линии MACD
            macd_signal: Значение сигнальной линии MACD
            structure: HH/HL / LH/LL / NONE
            confidence: Уверенность (0.0 - 1.0)

        Returns:
            ID записи или None при ошибке
        """
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    INSERT INTO trend_analysis (
                        symbol, trend_direction, adx, ema20, ema50,
                        macd_line, macd_signal, structure, confidence
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol, trend_direction, adx, ema20, ema50,
                    macd_line, macd_signal, structure, confidence
                ))
                await conn.commit()
                trend_id = cursor.lastrowid
                logger.info(f"📊 Тренд {symbol} сохранён: {trend_direction} (ADX={adx:.1f}, структура={structure})")

                # Публикуем событие о новом анализе тренда
                await self.event_bus.publish(
                    EventType.TREND_ANALYZED,
                    {
                        'trend_id': trend_id,
                        'symbol': symbol,
                        'trend_direction': trend_direction,
                        'adx': adx,
                        'structure': structure,
                        'confidence': confidence
                    },
                    'signal_repository'
                )

                return trend_id

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения тренда {symbol}: {e}")
            return None

    async def get_trend_analysis(self, symbol: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Получает историю анализа тренда для символа

        Args:
            symbol: Символ монеты
            limit: Количество записей

        Returns:
            Список словарей с данными тренда
        """
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("""
                    SELECT * FROM trend_analysis 
                    WHERE symbol = ? 
                    ORDER BY created_time DESC 
                    LIMIT ?
                """, (symbol, limit))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка получения тренда {symbol}: {e}")
            return []

    async def get_latest_trends(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Получает последние анализы тренда для всех символов

        Args:
            limit: Количество записей

        Returns:
            Список словарей с данными тренда
        """
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("""
                    SELECT t.* FROM trend_analysis t
                    INNER JOIN (
                        SELECT symbol, MAX(created_time) as max_time
                        FROM trend_analysis
                        GROUP BY symbol
                    ) latest ON t.symbol = latest.symbol AND t.created_time = latest.max_time
                    ORDER BY t.created_time DESC
                    LIMIT ?
                """, (limit,))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка получения последних трендов: {e}")
            return []

    async def update_watch_to_active(
            self,
            symbol: str,
            entry_price: float,
            stop_loss: float,
            take_profit: float,
            trigger_pattern: str,
            expiration_hours: int = 3
    ) -> Optional[int]:
        try:
            expiration_time = utc_now() + timedelta(hours=expiration_hours)

            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    SELECT id FROM signals 
                    WHERE symbol = ? AND status = 'WATCH' 
                    AND expiration_time > datetime('now')
                    ORDER BY created_time DESC LIMIT 1
                """, (symbol,))
                row = await cursor.fetchone()

                if not row:
                    logger.warning(f"⚠️ Нет активного WATCH для {symbol}")
                    return None

                signal_id = row[0]

                await conn.execute("""
                    UPDATE signals SET
                        status = 'ACTIVE',
                        signal_subtype = 'M15',
                        entry_price = ?,
                        stop_loss = ?,
                        take_profit = ?,
                        trigger_pattern = ?,
                        order_type = 'MARKET',
                        expiration_time = ?,
                        updated_time = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (entry_price, stop_loss, take_profit, trigger_pattern,
                      expiration_time.isoformat(), signal_id))
                await conn.commit()

                logger.info(f"✅ WATCH → ACTIVE для {symbol} (ID={signal_id})")
                return signal_id

        except Exception as e:
            logger.error(f"❌ Ошибка обновления WATCH → ACTIVE: {e}")
            return None

    async def get_watch_signal(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("""
                    SELECT * FROM signals 
                    WHERE symbol = ? AND status = 'WATCH' 
                    AND expiration_time > datetime('now')
                    ORDER BY created_time DESC LIMIT 1
                """, (symbol,))
                row = await cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения WATCH сигнала: {e}")
            return None

    async def get_watch_reserve(self, signal_id: int) -> Optional[Dict[str, Any]]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("""
                    SELECT id, symbol, signal_subtype, status, 
                           position_size, entry_price, leverage, 
                           reserved_margin, expiration_time,
                           current_price_at_signal, position_vs_zone
                    FROM signals 
                    WHERE id = ? AND signal_subtype = 'WATCH'
                """, (signal_id,))
                row = await cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения резерва WATCH: {e}")
            return None

    async def get_watch_signals_with_reserve(self) -> List[Dict[str, Any]]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("""
                    SELECT id, symbol, signal_subtype, status, 
                           position_size, entry_price, leverage, 
                           reserved_margin, expiration_time,
                           created_time, updated_time,
                           zone_low, zone_high, current_price_at_signal, position_vs_zone
                    FROM signals 
                    WHERE signal_subtype = 'WATCH' 
                    AND status = 'WATCH'
                    AND expiration_time > datetime('now')
                    ORDER BY created_time DESC
                """)
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка получения WATCH с резервами: {e}")
            return []

    async def update_reserved_margin(self, signal_id: int, reserved_margin: float) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("""
                    UPDATE signals 
                    SET reserved_margin = ?, updated_time = CURRENT_TIMESTAMP 
                    WHERE id = ?
                """, (reserved_margin, signal_id))
                await conn.commit()
                logger.info(f"✅ Резерв для WATCH #{signal_id}: {reserved_margin:.2f} USDT")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления резерва: {e}")
            return False

    async def update_position_size(self, signal_id: int, position_size: float) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("""
                    UPDATE signals SET position_size = ?, updated_time = CURRENT_TIMESTAMP WHERE id = ?
                """, (position_size, signal_id))
                await conn.commit()
                logger.info(f"✅ Размер позиции #{signal_id}: {position_size:.4f}")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления размера позиции: {e}")
            return False

    async def update_leverage(self, signal_id: int, leverage: float) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("""
                    UPDATE signals SET leverage = ?, updated_time = CURRENT_TIMESTAMP WHERE id = ?
                """, (leverage, signal_id))
                await conn.commit()
                logger.info(f"✅ Плечо #{signal_id}: {leverage}x")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления плеча: {e}")
            return False

    async def update_margin(self, signal_id: int, margin: float, position_value: float) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("""
                    UPDATE signals SET margin = ?, position_value = ?, updated_time = CURRENT_TIMESTAMP WHERE id = ?
                """, (margin, position_value, signal_id))
                await conn.commit()
                logger.info(f"✅ Маржа #{signal_id}: {margin:.2f} USDT")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления маржи: {e}")
            return False

    async def update_fill_price(self, signal_id: int, fill_price: float) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("""
                    UPDATE signals SET fill_price = ?, updated_time = CURRENT_TIMESTAMP WHERE id = ?
                """, (fill_price, signal_id))
                await conn.commit()
                logger.info(f"✅ Цена исполнения #{signal_id}: {fill_price:.6f}")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления цены исполнения: {e}")
            return False

    async def update_signal_status(self, signal_id: int, status: str) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("""
                    UPDATE signals SET status = ?, updated_time = CURRENT_TIMESTAMP WHERE id = ?
                """, (status, signal_id))
                await conn.commit()
                logger.info(f"✅ Статус #{signal_id}: {status}")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления статуса: {e}")
            return False

    async def get_signal_by_id(self, signal_id: int) -> Optional[Dict]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("SELECT * FROM signals WHERE id = ?", (signal_id,))
                row = await cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения сигнала: {e}")
            return None

    async def get_signals(self, limit: int = 50, signal_subtype: Optional[str] = None,
                          status: Optional[str] = None, symbol: Optional[str] = None) -> List[Dict]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                query = "SELECT * FROM signals"
                params = []
                conditions = []

                if signal_subtype:
                    conditions.append("signal_subtype = ?")
                    params.append(signal_subtype)
                if status:
                    conditions.append("status = ?")
                    params.append(status)
                if symbol:
                    conditions.append("symbol = ?")
                    params.append(symbol)

                if conditions:
                    query += " WHERE " + " AND ".join(conditions)

                query += " ORDER BY created_time DESC LIMIT ?"
                params.append(limit)

                cursor = await conn.execute(query, params)
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка получения сигналов: {e}")
            return []

    async def get_signals_with_trades(self, limit: int = 20) -> List[Dict]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("""
                    SELECT 
                        s.*, 
                        t.id as trade_id, 
                        t.pnl as trade_pnl, 
                        t.status as trade_status,
                        t.close_price as trade_close_price,
                        t.closed_at as trade_closed_at,
                        t.leverage as trade_leverage,
                        t.margin as trade_margin
                    FROM signals s
                    LEFT JOIN trades t ON s.id = t.signal_id
                    ORDER BY s.created_time DESC 
                    LIMIT ?
                """, (limit,))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка получения сигналов со сделками: {e}")
            return []

    async def get_active_signals(self) -> List[Dict]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                current_time = utc_now().isoformat()
                cursor = await conn.execute("""
                    SELECT * FROM signals 
                    WHERE status IN ('WATCH', 'ACTIVE')
                    AND (expiration_time IS NULL OR expiration_time > ?)
                    ORDER BY created_time DESC
                """, (current_time,))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка получения активных сигналов: {e}")
            return []

    async def get_database_stats(self) -> Dict[str, Any]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row

                cursor = await conn.execute("SELECT COUNT(*) as total FROM signals")
                row = await cursor.fetchone()
                total_signals = row['total'] if row else 0

                cursor = await conn.execute("SELECT COUNT(*) as buy_count FROM signals WHERE direction = 'BUY'")
                row = await cursor.fetchone()
                buy_signals = row['buy_count'] if row else 0

                cursor = await conn.execute("SELECT COUNT(*) as sell_count FROM signals WHERE direction = 'SELL'")
                row = await cursor.fetchone()
                sell_signals = row['sell_count'] if row else 0

                current_time = utc_now().isoformat()
                cursor = await conn.execute("""
                    SELECT COUNT(*) as active_count 
                    FROM signals 
                    WHERE status IN ('WATCH', 'ACTIVE')
                    AND (expiration_time IS NULL OR expiration_time > ?)
                """, (current_time,))
                row = await cursor.fetchone()
                active_signals = row['active_count'] if row else 0

                cursor = await conn.execute("SELECT COUNT(*) as rejected_count FROM signals WHERE status = 'REJECTED'")
                row = await cursor.fetchone()
                rejected_signals = row['rejected_count'] if row else 0

                cursor = await conn.execute(
                    "SELECT signal_subtype, COUNT(*) as count FROM signals GROUP BY signal_subtype")
                rows = await cursor.fetchall()
                subtypes_stats = {row['signal_subtype']: row['count'] for row in rows}

                cursor = await conn.execute("SELECT status, COUNT(*) as count FROM signals GROUP BY status")
                rows = await cursor.fetchall()
                status_stats = {row['status']: row['count'] for row in rows}

                cursor = await conn.execute("""
                    SELECT 
                        COUNT(*) as total_trades,
                        COALESCE(SUM(pnl), 0) as total_pnl,
                        COUNT(CASE WHEN pnl > 0 THEN 1 END) as winning_trades,
                        COUNT(CASE WHEN pnl < 0 THEN 1 END) as losing_trades
                    FROM trades 
                    WHERE status = 'CLOSED'
                """)
                trade_stats = await cursor.fetchone()

                total_trades = trade_stats['total_trades'] if trade_stats else 0
                total_pnl = trade_stats['total_pnl'] if trade_stats else 0
                winning_trades = trade_stats['winning_trades'] if trade_stats else 0
                losing_trades = trade_stats['losing_trades'] if trade_stats else 0
                win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0

                # Фаза 1.3.10: Статистика по трендам
                cursor = await conn.execute("SELECT COUNT(*) as total FROM trend_analysis")
                row = await cursor.fetchone()
                total_trends = row['total'] if row else 0

                return {
                    'total_signals': total_signals,
                    'active_signals': active_signals,
                    'rejected_signals': rejected_signals,
                    'three_screen_signals': total_signals,
                    'buy_signals': buy_signals,
                    'sell_signals': sell_signals,
                    'subtypes_stats': subtypes_stats,
                    'status_stats': status_stats,
                    'closed_trades': total_trades,
                    'winning_trades': winning_trades,
                    'losing_trades': losing_trades,
                    'total_pnl': total_pnl,
                    'win_rate': win_rate,
                    'total_trends': total_trends  # Фаза 1.3.10
                }
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}

    async def delete_old_signals(self, days: int = 30) -> int:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    DELETE FROM signals 
                    WHERE created_time < datetime('now', ?)
                    AND status NOT IN ('WATCH', 'ACTIVE')
                """, (f'-{days} days',))
                await conn.commit()
                deleted_count = cursor.rowcount
                logger.info(f"🧹 Удалено {deleted_count} старых сигналов")
                return deleted_count
        except Exception as e:
            logger.error(f"❌ Ошибка удаления старых сигналов: {e}")
            return 0

    async def vacuum(self) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("VACUUM")
                logger.info("✅ База данных оптимизирована")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка оптимизации: {e}")
            return False

    async def close(self) -> None:
        logger.info("📊 SignalRepository закрыт")
        pass


signal_repository = SignalRepository()