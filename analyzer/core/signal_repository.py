# analyzer/core/signal_repository.py
import aiosqlite
import logging
import os
from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime, timedelta

from analyzer.core.event_bus import EventType, event_bus as global_event_bus

logger = logging.getLogger("signal_repository")


class SignalRepository:
    def __init__(self, config=None):
        self.config = config or {}
        self.event_bus = global_event_bus
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
                            signal_subtype TEXT DEFAULT "LIMIT",
                            status TEXT DEFAULT "PENDING",
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
                            screen TEXT DEFAULT "M15",
                            order_type TEXT DEFAULT "LIMIT",
                            fill_price REAL,
                            position_size REAL
                        )
                    """)
                else:
                    cursor = await conn.execute("PRAGMA table_info(signals)")
                    columns = await cursor.fetchall()
                    column_names = [col[1] for col in columns]

                    for field in ['signal_subtype', 'expiration_time', 'screen', 'order_type', 'fill_price',
                                  'position_size']:
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

                await conn.commit()
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка БД: {e}")
            return False

    async def save_signal(self, analysis: Any, screen: str = "M15") -> Optional[int]:
        try:
            if not hasattr(analysis, "screen3") or not analysis.screen3:
                return None

            signal_subtype = getattr(analysis.screen3, 'signal_subtype', 'LIMIT')
            expiration_time = getattr(analysis.screen3, 'expiration_time', None)
            order_type = "LIMIT" if signal_subtype == "LIMIT" else "MARKET"
            position_size = getattr(analysis.screen3, 'position_size', None)
            expiration_time_str = expiration_time.isoformat() if expiration_time else None

            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    INSERT INTO signals (
                        symbol, direction, signal_subtype, confidence, entry_price,
                        stop_loss, take_profit, trend_direction, trend_strength,
                        signal_strength, trigger_pattern, risk_reward_ratio,
                        risk_pct, expiration_time, screen, order_type, position_size
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    analysis.symbol, analysis.screen3.signal_type, signal_subtype,
                    analysis.overall_confidence, analysis.screen3.entry_price,
                    analysis.screen3.stop_loss, analysis.screen3.take_profit,
                    getattr(analysis.screen1, "trend_direction", ""),
                    getattr(analysis.screen1, "trend_strength", ""),
                    getattr(analysis.screen3, "signal_strength", ""),
                    getattr(analysis.screen3, "trigger_pattern", ""),
                    analysis.screen3.indicators.get("risk_reward_ratio", 0),
                    analysis.screen3.indicators.get("risk_pct", 0),
                    expiration_time_str, screen, order_type, position_size
                ))
                await conn.commit()
                return cursor.lastrowid
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения: {e}")
            return None

    async def get_signals_with_trades(self, limit: int = 20) -> List[Dict]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("""
                    SELECT s.*, t.id as trade_id, t.pnl as trade_pnl, t.status as trade_status
                    FROM signals s
                    LEFT JOIN trades t ON s.id = t.signal_id
                    ORDER BY s.created_time DESC LIMIT ?
                """, (limit,))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return []

    async def get_signal_by_id(self, signal_id: int) -> Optional[Dict]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("SELECT * FROM signals WHERE id = ?", (signal_id,))
                row = await cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return None

    async def get_signals(self, limit: int = 50, signal_subtype: Optional[str] = None) -> List[Dict]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                if signal_subtype:
                    cursor = await conn.execute(
                        "SELECT * FROM signals WHERE signal_subtype = ? ORDER BY created_time DESC LIMIT ?",
                        (signal_subtype, limit)
                    )
                else:
                    cursor = await conn.execute(
                        "SELECT * FROM signals ORDER BY created_time DESC LIMIT ?", (limit,)
                    )
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return []

    async def get_active_signals(self) -> List[Dict]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                current_time = datetime.now().isoformat()
                cursor = await conn.execute("""
                    SELECT * FROM signals 
                    WHERE status IN ('PENDING', 'ACTIVE')
                    AND (expiration_time IS NULL OR expiration_time > ?)
                    ORDER BY created_time DESC
                """, (current_time,))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return []

    async def get_pending_signals(self) -> List[Dict]:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                current_time = datetime.now().isoformat()
                cursor = await conn.execute("""
                    SELECT * FROM signals 
                    WHERE status = 'PENDING' AND signal_subtype = 'LIMIT'
                    AND (expiration_time IS NULL OR expiration_time > ?)
                    ORDER BY created_time ASC
                """, (current_time,))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
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

                cursor = await conn.execute(
                    "SELECT signal_subtype, COUNT(*) as count FROM signals GROUP BY signal_subtype")
                rows = await cursor.fetchall()
                subtypes_stats = {row['signal_subtype']: row['count'] for row in rows}

                cursor = await conn.execute("""
                    SELECT COUNT(*) as total_trades, COALESCE(SUM(pnl), 0) as total_pnl
                    FROM trades WHERE status = 'CLOSED'
                """)
                trade_stats = await cursor.fetchone()

                return {
                    'total_signals': total_signals,
                    'active_signals': 0,
                    'three_screen_signals': total_signals,
                    'buy_signals': buy_signals,
                    'sell_signals': sell_signals,
                    'subtypes_stats': subtypes_stats,
                    'active_trades': 0,
                    'closed_trades': trade_stats['total_trades'] if trade_stats else 0,
                    'total_pnl': trade_stats['total_pnl'] if trade_stats else 0,
                    'win_rate': 0
                }
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return {}

    async def update_signal_status(self, signal_id: int, status: str) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("UPDATE signals SET status = ? WHERE id = ?", (status, signal_id))
                await conn.commit()
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return False

    async def update_fill_price(self, signal_id: int, fill_price: float) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("UPDATE signals SET fill_price = ? WHERE id = ?", (fill_price, signal_id))
                await conn.commit()
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return False

    async def update_position_size(self, signal_id: int, position_size: float) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("UPDATE signals SET position_size = ? WHERE id = ?", (position_size, signal_id))
                await conn.commit()
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return False

    async def check_duplicate_signal(self, symbol: str, signal_subtype: str, expiration_hours: int = 24) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("""
                    SELECT COUNT(*) as count FROM signals 
                    WHERE symbol = ? AND signal_subtype = ?
                    AND status IN ('PENDING', 'ACTIVE')
                    AND created_time > datetime('now', ?)
                """, (symbol, signal_subtype, f'-{expiration_hours} hours'))
                row = await cursor.fetchone()
                return (row['count'] if row else 0) > 0
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return False


signal_repository = SignalRepository()