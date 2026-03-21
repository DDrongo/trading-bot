# analyzer/core/trade_repository.py
"""
📊 TRADE REPOSITORY - Репозиторий для истории сделок
"""

import aiosqlite
import logging
import os
from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime

logger = logging.getLogger('trade_repository')


class TradeRepository:
    """Репозиторий для хранения истории сделок"""

    def __init__(self, config=None):
        self.config = config or {}
        project_root = Path(__file__).parent.parent.parent
        self.db_path = str(project_root / "data/trading_bot.db")
        self._initialized = False

    async def initialize(self) -> bool:
        """Инициализация таблицы trades"""
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

            async with aiosqlite.connect(self.db_path) as conn:
                # Создаём таблицу trades
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        signal_id INTEGER NOT NULL,
                        symbol TEXT NOT NULL,
                        direction TEXT NOT NULL,
                        entry_price REAL NOT NULL,
                        close_price REAL,
                        quantity REAL NOT NULL,
                        stop_loss REAL,
                        take_profit REAL,
                        pnl REAL DEFAULT 0,
                        pnl_percent REAL DEFAULT 0,
                        commission REAL DEFAULT 0,
                        close_reason TEXT,
                        opened_at TIMESTAMP NOT NULL,
                        closed_at TIMESTAMP,
                        status TEXT DEFAULT 'OPEN',
                        FOREIGN KEY (signal_id) REFERENCES signals (id)
                    )
                """)

                # Создаём индексы для быстрого поиска
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trades_signal_id ON trades(signal_id)"
                )
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)"
                )
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)"
                )
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trades_opened_at ON trades(opened_at)"
                )

                await conn.commit()
                self._initialized = True
                logger.info("✅ TradeRepository инициализирован, таблица trades готова")
                return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации TradeRepository: {e}")
            return False

    async def save_trade(self, trade: Dict[str, Any]) -> Optional[int]:
        """
        Сохранить сделку в историю
        trade dict должен содержать:
            - signal_id
            - symbol
            - direction
            - entry_price
            - close_price (опционально)
            - quantity
            - stop_loss
            - take_profit
            - pnl (опционально)
            - pnl_percent (опционально)
            - commission (опционально)
            - close_reason (опционально)
            - opened_at
            - closed_at (опционально)
            - status (опционально)
        """
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    INSERT INTO trades (
                        signal_id, symbol, direction, entry_price, close_price,
                        quantity, stop_loss, take_profit, pnl, pnl_percent,
                        commission, close_reason, opened_at, closed_at, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    trade.get('signal_id'),
                    trade.get('symbol'),
                    trade.get('direction'),
                    trade.get('entry_price'),
                    trade.get('close_price'),
                    trade.get('quantity'),
                    trade.get('stop_loss'),
                    trade.get('take_profit'),
                    trade.get('pnl', 0),
                    trade.get('pnl_percent', 0),
                    trade.get('commission', 0),
                    trade.get('close_reason'),
                    trade.get('opened_at'),
                    trade.get('closed_at'),
                    trade.get('status', 'OPEN')
                ))

                await conn.commit()
                trade_id = cursor.lastrowid
                logger.info(f"💾 Сделка сохранена: ID={trade_id}, Signal={trade.get('signal_id')}")
                return trade_id

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения сделки: {e}")
            return None

    async def update_trade(
            self,
            trade_id: int,
            close_price: float,
            pnl: float,
            pnl_percent: float,
            close_reason: str,
            closed_at: datetime
    ) -> bool:
        """Обновить закрытую сделку"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("""
                    UPDATE trades
                    SET close_price = ?, pnl = ?, pnl_percent = ?,
                        close_reason = ?, closed_at = ?, status = 'CLOSED'
                    WHERE id = ?
                """, (close_price, pnl, pnl_percent, close_reason, closed_at, trade_id))

                await conn.commit()
                logger.info(f"✅ Сделка #{trade_id} обновлена (закрыта)")
                return True

        except Exception as e:
            logger.error(f"❌ Ошибка обновления сделки: {e}")
            return False

    async def get_trade_by_signal_id(self, signal_id: int) -> Optional[Dict[str, Any]]:
        """Получить сделку по ID сигнала"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute(
                    "SELECT * FROM trades WHERE signal_id = ? ORDER BY id DESC LIMIT 1",
                    (signal_id,)
                )
                row = await cursor.fetchone()
                return dict(row) if row else None

        except Exception as e:
            logger.error(f"❌ Ошибка получения сделки: {e}")
            return None

    async def get_open_trades(self) -> List[Dict[str, Any]]:
        """Получить все открытые сделки"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute(
                    "SELECT * FROM trades WHERE status = 'OPEN' ORDER BY opened_at DESC"
                )
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"❌ Ошибка получения открытых сделок: {e}")
            return []

    async def get_closed_trades(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Получить закрытые сделки"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute(
                    "SELECT * FROM trades WHERE status = 'CLOSED' ORDER BY closed_at DESC LIMIT ?",
                    (limit,)
                )
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"❌ Ошибка получения закрытых сделок: {e}")
            return []

    async def get_trades_statistics(self) -> Dict[str, Any]:
        """Получить статистику по сделкам"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row

                # Общая статистика
                cursor = await conn.execute("""
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                        SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                        SUM(pnl) as total_pnl,
                        AVG(pnl) as avg_pnl,
                        MAX(pnl) as max_win,
                        MIN(pnl) as max_loss
                    FROM trades
                    WHERE status = 'CLOSED'
                """)
                stats = dict(await cursor.fetchone())

                # Статистика по символам
                cursor = await conn.execute("""
                    SELECT 
                        symbol,
                        COUNT(*) as trades_count,
                        SUM(pnl) as total_pnl,
                        AVG(pnl) as avg_pnl
                    FROM trades
                    WHERE status = 'CLOSED'
                    GROUP BY symbol
                    ORDER BY total_pnl DESC
                    LIMIT 10
                """)
                rows = await cursor.fetchall()
                stats['by_symbol'] = [dict(row) for row in rows]

                # Статистика по причинам закрытия
                cursor = await conn.execute("""
                    SELECT 
                        close_reason,
                        COUNT(*) as count,
                        AVG(pnl) as avg_pnl
                    FROM trades
                    WHERE status = 'CLOSED' AND close_reason IS NOT NULL
                    GROUP BY close_reason
                """)
                rows = await cursor.fetchall()
                stats['by_close_reason'] = [dict(row) for row in rows]

                return stats

        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}

    async def cleanup_old_trades(self, days: int = 30) -> int:
        """Очистить старые закрытые сделки"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    DELETE FROM trades
                    WHERE status = 'CLOSED'
                    AND closed_at < datetime('now', ?)
                """, (f'-{days} days',))

                await conn.commit()
                deleted = cursor.rowcount
                if deleted > 0:
                    logger.info(f"🧹 Удалено {deleted} старых сделок")
                return deleted

        except Exception as e:
            logger.error(f"❌ Ошибка очистки старых сделок: {e}")
            return 0


# Глобальный экземпляр
trade_repository = TradeRepository()