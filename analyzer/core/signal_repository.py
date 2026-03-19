# analyzer/core/signal_repository.py - ДОПОЛНЕННАЯ ВЕРСИЯ
import aiosqlite
import logging
import os
from typing import List, Dict, Optional, Any
from pathlib import Path

logger = logging.getLogger("signal_repository")


class SignalRepository:
    """🎯 Репозиторий для сигналов"""

    def __init__(self, config=None):
        self.config = config or {}

        # ВСЕГДА используем одну БД!
        project_root = Path(__file__).parent.parent.parent
        self.db_path = str(project_root / "data/trading_bot.db")

        logger.info(f"📊 SignalRepository использует: {self.db_path}")

    async def initialize(self) -> bool:
        """Инициализация БД"""
        try:
            # Создаем директорию если нет
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

            async with aiosqlite.connect(self.db_path) as conn:
                # Таблица signals
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS signals (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        strategy TEXT DEFAULT "three_screen",
                        direction TEXT NOT NULL,
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
                        created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                await conn.commit()
                logger.info(f"✅ БД готова: {self.db_path}")
                return True

        except Exception as e:
            logger.error(f"❌ Ошибка БД: {e}")
            return False

    async def save_signal(self, analysis: Any) -> Optional[int]:
        """Сохраняет сигнал в БД"""
        try:
            if not hasattr(analysis, "screen3") or not analysis.screen3:
                return None

            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    INSERT INTO signals (
                        symbol, direction, confidence, entry_price,
                        stop_loss, take_profit, trend_direction,
                        trend_strength, signal_strength, trigger_pattern,
                        risk_reward_ratio, risk_pct
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    analysis.symbol,
                    analysis.screen3.signal_type,
                    analysis.overall_confidence,
                    analysis.screen3.entry_price,
                    analysis.screen3.stop_loss,
                    analysis.screen3.take_profit,
                    getattr(analysis.screen1, "trend_direction", ""),
                    getattr(analysis.screen1, "trend_strength", ""),
                    getattr(analysis.screen3, "signal_strength", ""),
                    getattr(analysis.screen3, "trigger_pattern", ""),
                    analysis.screen3.indicators.get("risk_reward_ratio", 0),
                    analysis.screen3.indicators.get("risk_pct", 0)
                ))

                await conn.commit()
                signal_id = cursor.lastrowid

                logger.info(f"💾 Сигнал сохранен: {analysis.symbol} (ID: {signal_id})")
                logger.info(f"   Entry: {analysis.screen3.entry_price:.6f}, SL: {analysis.screen3.stop_loss:.6f}")

                return signal_id

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения: {e}")
            return None

    async def get_all_signals(self, limit: int = 100) -> List[Dict]:
        """Получает все сигналы"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute(
                    "SELECT * FROM signals ORDER BY created_time DESC LIMIT ?",
                    (limit,)
                )
                return [dict(row) for row in await cursor.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения: {e}")
            return []

    # ДОБАВЛЕННЫЕ МЕТОДЫ ДЛЯ СОВМЕСТИМОСТИ С МОНИТОРОМ
    async def get_signals(self, limit: int = 50) -> List[Dict]:
        """Получение сигналов (аналогично get_all_signals)"""
        return await self.get_all_signals(limit)

    async def get_signals_with_trades(self, limit: int = 20) -> List[Dict]:
        """Получение сигналов с информацией о трейдах (упрощенная версия)"""
        try:
            signals = await self.get_all_signals(limit)
            # Добавляем информацию о трейдах (пока заглушка)
            for signal in signals:
                signal['active_trades'] = 0
                signal['trade_count'] = 0
                signal['current_price'] = signal.get('entry_price', 0)
                signal['leverage'] = 10  # По умолчанию
            return signals
        except Exception as e:
            logger.error(f"❌ Ошибка получения сигналов с трейдами: {e}")
            return []

    async def get_signal_by_id(self, signal_id: int) -> Optional[Dict]:
        """Получение сигнала по ID"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute(
                    "SELECT * FROM signals WHERE id = ?",
                    (signal_id,)
                )
                row = await cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения сигнала по ID: {e}")
            return None

    async def get_database_stats(self) -> Dict[str, Any]:
        """Получение статистики БД"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row

                # Общее количество сигналов
                cursor = await conn.execute("SELECT COUNT(*) as total FROM signals")
                total_result = await cursor.fetchone()
                total_signals = dict(total_result)['total'] if total_result else 0

                # Активные сигналы
                cursor = await conn.execute(
                    "SELECT COUNT(*) as active FROM signals WHERE status IN ('PENDING', 'ACTIVE')"
                )
                active_result = await cursor.fetchone()
                active_signals = dict(active_result)['active'] if active_result else 0

                # Three Screen сигналы
                cursor = await conn.execute(
                    "SELECT COUNT(*) as three_screen FROM signals WHERE strategy = 'three_screen'"
                )
                three_screen_result = await cursor.fetchone()
                three_screen_signals = dict(three_screen_result)['three_screen'] if three_screen_result else 0

                # BUY сигналы
                cursor = await conn.execute(
                    "SELECT COUNT(*) as buy_count FROM signals WHERE direction = 'BUY'"
                )
                buy_result = await cursor.fetchone()
                buy_signals = dict(buy_result)['buy_count'] if buy_result else 0

                # SELL сигналы
                cursor = await conn.execute(
                    "SELECT COUNT(*) as sell_count FROM signals WHERE direction = 'SELL'"
                )
                sell_result = await cursor.fetchone()
                sell_signals = dict(sell_result)['sell_count'] if sell_result else 0

                # Общий PnL (заглушка)
                total_pnl = 0

                return {
                    'total_signals': total_signals,
                    'active_signals': active_signals,
                    'three_screen_signals': three_screen_signals,
                    'buy_signals': buy_signals,
                    'sell_signals': sell_signals,
                    'active_trades': 0,
                    'closed_trades': 0,
                    'total_pnl': total_pnl
                }
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {
                'total_signals': 0,
                'active_signals': 0,
                'three_screen_signals': 0,
                'buy_signals': 0,
                'sell_signals': 0,
                'active_trades': 0,
                'closed_trades': 0,
                'total_pnl': 0
            }

    async def update_signal_status(self, signal_id: int, status: str) -> bool:
        """Обновление статуса сигнала"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute(
                    "UPDATE signals SET status = ? WHERE id = ?",
                    (status, signal_id)
                )
                await conn.commit()
                logger.info(f"✅ Статус сигнала #{signal_id} обновлен на '{status}'")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления статуса сигнала: {e}")
            return False

    async def get_recent_signals(self, hours: int = 24) -> List[Dict]:
        """Получение сигналов за последние N часов"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                query = """
                    SELECT * FROM signals 
                    WHERE datetime(created_time) >= datetime('now', ?)
                    ORDER BY created_time DESC
                """
                cursor = await conn.execute(query, (f'-{hours} hours',))
                return [dict(row) for row in await cursor.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения недавних сигналов: {e}")
            return []


# Глобальный экземпляр
signal_repository = SignalRepository()