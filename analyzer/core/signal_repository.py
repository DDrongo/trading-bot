# analyzer/core/signal_repository.py (ПОЛНОСТЬЮ - ИСПРАВЛЕННЫЙ)

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
                        position_vs_zone TEXT,
                        learning_comment TEXT,
                        entry_type TEXT DEFAULT "LEGACY",
                        fvg_present INTEGER DEFAULT 0,
                        liquidity_grabbed INTEGER DEFAULT 0,
                        grab_price REAL,
                        fvg_type TEXT,
                        fvg_formed_at TIMESTAMP,
                        fvg_age INTEGER,
                        fvg_strength TEXT,
                        liquidity_pools TEXT,
                        selected_pool_price REAL,
                        selected_pool_touches INTEGER,
                        grab_time TIMESTAMP,
                        grab_timeframe TEXT
                    )
                """)
                logger.info("✅ Таблица signals создана (с SMC полями)")

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
                logger.info("✅ Таблица trend_analysis создана")

                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_time)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_subtype ON signals(signal_subtype)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_entry_type ON signals(entry_type)")
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
                logger.info("✅ База данных инициализирована (Фаза 2.0 SMC)")
                return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
            return False

    async def was_traded_recently(self, symbol: str, minutes: int = 30) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    SELECT COUNT(*) FROM trades 
                    WHERE symbol = ? 
                    AND status = 'CLOSED'
                    AND closed_at > datetime('now', ?)
                """, (symbol, f'-{minutes} minutes'))
                row = await cursor.fetchone()
                return row[0] > 0 if row else False
        except Exception as e:
            logger.error(f"❌ Ошибка проверки недавних сделок: {e}")
            return False

    async def get_active_or_recent_signals(self, symbol: str, minutes: int = 30) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    SELECT COUNT(*) FROM signals 
                    WHERE symbol = ? 
                    AND (
                        status IN ('ACTIVE', 'WATCH')
                        OR (status = 'CLOSED' AND closed_time > datetime('now', ?))
                        OR (status = 'REJECTED' AND created_time > datetime('now', ?))
                    )
                """, (symbol, f'-{minutes} minutes', f'-{minutes} minutes'))
                row = await cursor.fetchone()
                return row[0] > 0 if row else False
        except Exception as e:
            logger.error(f"❌ Ошибка проверки сигналов: {e}")
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
            expiration_hours: int = 8,
            position_size: float = None,
            entry_price: float = None,
            leverage: float = 10,
            current_price: float = None,
            learning_comment: str = None,
            entry_type: str = "LEGACY",
            fvg_type: str = None,
            fvg_formed_at: str = None,
            fvg_age: int = 0,
            fvg_strength: str = None,
            liquidity_pools: str = None,
            selected_pool_price: float = 0,
            selected_pool_touches: int = 0
    ) -> Optional[int]:
        try:
            from analyzer.core.time_utils import utc_now
            expiration_time = (utc_now() + timedelta(hours=expiration_hours)).strftime('%Y-%m-%d %H:%M:%S')

            position_vs_zone = ""
            if current_price is not None and zone_low > 0 and zone_high > 0:
                if current_price > zone_high:
                    position_vs_zone = "ABOVE"
                elif current_price < zone_low:
                    position_vs_zone = "BELOW"
                else:
                    position_vs_zone = "INSIDE"

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
                            position_vs_zone = ?, learning_comment = ?,
                            entry_type = ?, fvg_type = ?, fvg_formed_at = ?,
                            fvg_age = ?, fvg_strength = ?, liquidity_pools = ?,
                            selected_pool_price = ?, selected_pool_touches = ?,
                            updated_time = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (zone_low, zone_high, screen2_score, expected_pattern,
                          expiration_time, direction, position_size,
                          entry_price, leverage, current_price or 0,
                          position_vs_zone, learning_comment, entry_type,
                          fvg_type, fvg_formed_at, fvg_age, fvg_strength,
                          liquidity_pools, selected_pool_price, selected_pool_touches,
                          existing[0]))
                    await conn.commit()
                    logger.info(f"🔄 Обновлён WATCH сигнал для {symbol} (тип: {entry_type})")
                    return existing[0]
                else:
                    cursor = await conn.execute("""
                        INSERT INTO signals (
                            symbol, direction, signal_subtype, status,
                            zone_low, zone_high, screen2_score, expected_pattern,
                            expiration_time, position_size, entry_price, leverage,
                            current_price_at_signal, position_vs_zone, learning_comment,
                            entry_type, fvg_type, fvg_formed_at, fvg_age, fvg_strength,
                            liquidity_pools, selected_pool_price, selected_pool_touches,
                            created_time, updated_time
                        ) VALUES (?, ?, 'WATCH', 'WATCH', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """, (symbol, direction, zone_low, zone_high, screen2_score,
                          expected_pattern, expiration_time,
                          position_size, entry_price, leverage,
                          current_price or 0, position_vs_zone, learning_comment,
                          entry_type, fvg_type, fvg_formed_at, fvg_age, fvg_strength,
                          liquidity_pools, selected_pool_price, selected_pool_touches))
                    await conn.commit()
                    signal_id = cursor.lastrowid
                    logger.info(f"✅ Создан WATCH сигнал для {symbol} (ID={signal_id}, тип={entry_type})")

                    await self.event_bus.publish(
                        EventType.WATCH_CREATED,
                        {
                            'signal_id': signal_id,
                            'symbol': symbol,
                            'direction': direction,
                            'position_size': position_size,
                            'entry_price': entry_price,
                            'leverage': leverage,
                            'expiration_time': expiration_time,
                            'current_price_at_signal': current_price,
                            'position_vs_zone': position_vs_zone,
                            'entry_type': entry_type
                        },
                        'signal_repository'
                    )

                    return signal_id

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения WATCH сигнала {symbol}: {e}")
            return None

    async def save_signal(self, analysis, learning_comment: str = None) -> Optional[int]:
        try:
            from analyzer.core.data_classes import ThreeScreenAnalysis

            screen3 = analysis.screen3
            if not screen3 or not screen3.passed:
                logger.warning(f"⚠️ Попытка сохранить непрошедший сигнал {analysis.symbol}")
                return None

            current_price = screen3.entry_price

            position_vs_zone = ""
            if analysis.zone_low > 0 and analysis.zone_high > 0:
                if current_price > analysis.zone_high:
                    position_vs_zone = "ABOVE"
                elif current_price < analysis.zone_low:
                    position_vs_zone = "BELOW"
                else:
                    position_vs_zone = "INSIDE"

            from analyzer.core.time_utils import utc_now
            expiration_time = (utc_now() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')

            entry_type = getattr(screen3, 'entry_type', 'LEGACY')
            fvg_present = 1 if getattr(screen3, 'fvg_present', False) else 0
            liquidity_grabbed = 1 if getattr(screen3, 'liquidity_grabbed', False) else 0
            grab_price = getattr(screen3, 'grab_price', None)
            grab_time = getattr(screen3, 'grab_time', None)
            if grab_time and hasattr(grab_time, 'isoformat'):
                grab_time = grab_time.isoformat()
            grab_timeframe = getattr(screen3, 'grab_timeframe', None)

            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    INSERT INTO signals (
                        symbol, direction, signal_subtype, status,
                        entry_price, stop_loss, take_profit,
                        trigger_pattern, confidence, risk_reward_ratio,
                        current_price_at_signal, position_vs_zone,
                        zone_low, zone_high, screen2_score, expected_pattern,
                        expiration_time, order_type, learning_comment,
                        entry_type, fvg_present, liquidity_grabbed, grab_price,
                        grab_time, grab_timeframe, created_time, updated_time
                    ) VALUES (?, ?, 'M15', 'ACTIVE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'MARKET', ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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
                    expiration_time,
                    learning_comment,
                    entry_type,
                    fvg_present,
                    liquidity_grabbed,
                    grab_price,
                    grab_time,
                    grab_timeframe
                ))
                await conn.commit()
                signal_id = cursor.lastrowid
                logger.info(f"✅ M15 сигнал {analysis.symbol} сохранён (ID={signal_id}, тип={entry_type})")

                return signal_id

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения M15 сигнала {analysis.symbol}: {e}")
            return None

    async def update_entry_type(self, signal_id: int, entry_type: str) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("""
                    UPDATE signals 
                    SET entry_type = ?, updated_time = CURRENT_TIMESTAMP 
                    WHERE id = ?
                """, (entry_type, signal_id))
                await conn.commit()
                logger.info(f"✅ Сигнал #{signal_id}: entry_type = {entry_type}")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления entry_type: {e}")
            return False

    async def get_signals_by_entry_type(self, entry_type: str, limit: int = 100) -> List[Dict[str, Any]]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("""
                    SELECT * FROM signals 
                    WHERE entry_type = ? 
                    ORDER BY created_time DESC 
                    LIMIT ?
                """, (entry_type, limit))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка получения сигналов по entry_type: {e}")
            return []

    async def get_smc_statistics(self) -> Dict[str, Any]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row

                cursor = await conn.execute("""
                    SELECT 
                        entry_type,
                        COUNT(*) as total,
                        SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) as active,
                        SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) as closed,
                        SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) as rejected
                    FROM signals 
                    WHERE signal_subtype = 'M15'
                    GROUP BY entry_type
                """)
                by_entry_type = [dict(row) for row in await cursor.fetchall()]

                cursor = await conn.execute("""
                    SELECT 
                        SUM(CASE WHEN fvg_present = 1 THEN 1 ELSE 0 END) as with_fvg,
                        SUM(CASE WHEN fvg_present = 0 THEN 1 ELSE 0 END) as without_fvg
                    FROM signals 
                    WHERE signal_subtype = 'M15'
                """)
                fvg_stats = dict(await cursor.fetchone())

                cursor = await conn.execute("""
                    SELECT 
                        SUM(CASE WHEN liquidity_grabbed = 1 THEN 1 ELSE 0 END) as with_grab,
                        SUM(CASE WHEN liquidity_grabbed = 0 THEN 1 ELSE 0 END) as without_grab
                    FROM signals 
                    WHERE signal_subtype = 'M15'
                """)
                grab_stats = dict(await cursor.fetchone())

                return {
                    'by_entry_type': by_entry_type,
                    'fvg_stats': fvg_stats,
                    'liquidity_grab_stats': grab_stats
                }

        except Exception as e:
            logger.error(f"❌ Ошибка получения SMC статистики: {e}")
            return {}

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
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row

                cursor = await conn.execute("""
                    SELECT trend_direction, adx, created_time 
                    FROM trend_analysis 
                    WHERE symbol = ? 
                    ORDER BY created_time DESC 
                    LIMIT 1
                """, (symbol,))

                last_trend = await cursor.fetchone()

                should_save = True

                if last_trend:
                    last_direction = last_trend['trend_direction']
                    last_adx = last_trend['adx']
                    last_time_str = last_trend['created_time']

                    if last_time_str:
                        try:
                            last_time = datetime.fromisoformat(last_time_str.replace('Z', '+00:00'))
                            hours_passed = (datetime.utcnow() - last_time).total_seconds() / 3600
                        except:
                            hours_passed = 999
                    else:
                        hours_passed = 999

                    direction_changed = last_direction != trend_direction
                    adx_changed = abs(last_adx - adx) > 5
                    time_passed = hours_passed >= 4

                    should_save = direction_changed or adx_changed or time_passed

                    if not should_save:
                        logger.debug(f"⏭️ {symbol}: тренд не изменился, пропускаем сохранение")
                        return None
                    else:
                        reason = []
                        if direction_changed:
                            reason.append("направление изменилось")
                        if adx_changed:
                            reason.append(f"ADX изменился ({last_adx:.1f} → {adx:.1f})")
                        if time_passed:
                            reason.append(f"прошло {hours_passed:.1f} ч")
                        logger.info(f"💾 {symbol}: сохраняем тренд ({', '.join(reason)})")

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
            expiration_hours: int = 8,
            grab_price: float = None,
            grab_time: str = None,
            grab_timeframe: str = None
    ) -> Optional[int]:
        try:
            from analyzer.core.time_utils import utc_now
            new_expiration = (utc_now() + timedelta(hours=expiration_hours)).strftime('%Y-%m-%d %H:%M:%S')

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
                        grab_price = COALESCE(?, grab_price),
                        grab_time = COALESCE(?, grab_time),
                        grab_timeframe = COALESCE(?, grab_timeframe),
                        updated_time = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (entry_price, stop_loss, take_profit, trigger_pattern,
                      new_expiration, grab_price, grab_time, grab_timeframe, signal_id))
                await conn.commit()

                logger.info(f"✅ WATCH → ACTIVE для {symbol} (ID={signal_id}), истекает {new_expiration}")
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
                           current_price_at_signal, position_vs_zone,
                           entry_type
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
                           zone_low, zone_high, current_price_at_signal, position_vs_zone,
                           entry_type
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
                    SELECT entry_type, COUNT(*) as count 
                    FROM signals 
                    WHERE signal_subtype = 'M15'
                    GROUP BY entry_type
                """)
                rows = await cursor.fetchall()
                entry_type_stats = {row['entry_type']: row['count'] for row in rows}

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
                    'entry_type_stats': entry_type_stats,
                    'closed_trades': total_trades,
                    'winning_trades': winning_trades,
                    'losing_trades': losing_trades,
                    'total_pnl': total_pnl,
                    'win_rate': win_rate,
                    'total_trends': total_trends
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

    async def get_watch_count(self) -> int:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    SELECT COUNT(*) FROM signals 
                    WHERE status = 'WATCH' 
                    AND expiration_time > datetime('now')
                """)
                row = await cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error(f"❌ Ошибка получения количества WATCH: {e}")
            return 0


signal_repository = SignalRepository()