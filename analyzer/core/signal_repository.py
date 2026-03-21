# analyzer/core/signal_repository.py
import aiosqlite
import logging
import os
from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime

# Импорт EventBus
from analyzer.core.event_bus import EventType, event_bus as global_event_bus

logger = logging.getLogger("signal_repository")


class SignalRepository:
    """🎯 Репозиторий для сигналов"""

    def __init__(self, config=None):
        self.config = config or {}
        self.event_bus = global_event_bus  # Сохраняем ссылку на event_bus

        # ВСЕГДА используем одну БД!
        project_root = Path(__file__).parent.parent.parent
        self.db_path = str(project_root / "data/trading_bot.db")

        logger.info(f"📊 SignalRepository использует: {self.db_path}")

    async def initialize(self) -> bool:
        """Инициализация БД с добавлением полей для сигналов (WATCH/LIMIT/INSTANT)"""
        try:
            # Создаем директорию если нет
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

            async with aiosqlite.connect(self.db_path) as conn:
                # Проверяем существование таблицы
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='signals'"
                )
                table_exists = await cursor.fetchone()

                if not table_exists:
                    # Создаем таблицу с новыми полями
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
                            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    logger.info("✅ Таблица signals создана с полями signal_subtype и expiration_time")
                else:
                    # Проверяем наличие поля signal_subtype
                    cursor = await conn.execute("PRAGMA table_info(signals)")
                    columns = await cursor.fetchall()
                    column_names = [col[1] for col in columns]

                    if 'signal_subtype' not in column_names:
                        await conn.execute(
                            "ALTER TABLE signals ADD COLUMN signal_subtype TEXT DEFAULT 'LIMIT'"
                        )
                        logger.info("✅ Добавлено поле signal_subtype в таблицу signals")

                    if 'expiration_time' not in column_names:
                        await conn.execute(
                            "ALTER TABLE signals ADD COLUMN expiration_time TIMESTAMP"
                        )
                        logger.info("✅ Добавлено поле expiration_time в таблицу signals")

                await conn.commit()
                logger.info(f"✅ БД готова: {self.db_path}")
                return True

        except Exception as e:
            logger.error(f"❌ Ошибка БД: {e}")
            return False

    async def save_signal(self, analysis: Any) -> Optional[int]:
        """Сохраняет сигнал в БД с указанием подтипа (WATCH/LIMIT/INSTANT)"""
        try:
            if not hasattr(analysis, "screen3") or not analysis.screen3:
                logger.warning("⚠️ Нет screen3 в анализе, сигнал не сохраняется")
                return None

            # Определяем подтип сигнала
            signal_subtype = getattr(analysis.screen3, 'signal_subtype', 'LIMIT')
            expiration_time = getattr(analysis.screen3, 'expiration_time', None)

            # Форматируем expiration_time для SQLite
            expiration_time_str = expiration_time.isoformat() if expiration_time else None

            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    INSERT INTO signals (
                        symbol, direction, signal_subtype, confidence, entry_price,
                        stop_loss, take_profit, trend_direction,
                        trend_strength, signal_strength, trigger_pattern,
                        risk_reward_ratio, risk_pct, expiration_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    analysis.symbol,
                    analysis.screen3.signal_type,
                    signal_subtype,
                    analysis.overall_confidence,
                    analysis.screen3.entry_price,
                    analysis.screen3.stop_loss,
                    analysis.screen3.take_profit,
                    getattr(analysis.screen1, "trend_direction", ""),
                    getattr(analysis.screen1, "trend_strength", ""),
                    getattr(analysis.screen3, "signal_strength", ""),
                    getattr(analysis.screen3, "trigger_pattern", ""),
                    analysis.screen3.indicators.get("risk_reward_ratio", 0),
                    analysis.screen3.indicators.get("risk_pct", 0),
                    expiration_time_str
                ))

                await conn.commit()
                db_signal_id = cursor.lastrowid

                logger.info(f"💾 Сигнал сохранен: {analysis.symbol} (ID: {db_signal_id}, тип: {signal_subtype})")
                logger.info(f"   Истекает: {expiration_time_str or 'никогда'}")

                # Публикация события
                if db_signal_id and self.event_bus and self.event_bus._is_running:
                    event_data = {
                        'signal_id': db_signal_id,
                        'symbol': analysis.symbol,
                        'signal_type': analysis.screen3.signal_type,
                        'signal_subtype': signal_subtype,
                        'entry_price': analysis.screen3.entry_price,
                        'stop_loss': analysis.screen3.stop_loss,
                        'take_profit': analysis.screen3.take_profit,
                        'confidence': analysis.overall_confidence,
                        'risk_reward_ratio': analysis.screen3.indicators.get("risk_reward_ratio", 0),
                        'expiration_time': expiration_time_str,
                        'trend_direction': getattr(analysis.screen1, "trend_direction", ""),
                        'trigger_pattern': getattr(analysis.screen3, "trigger_pattern", ""),
                        'strategy': 'three_screen'
                    }

                    await self.event_bus.publish(
                        event_type=EventType.TRADING_SIGNAL_GENERATED,
                        data=event_data,
                        source='signal_repository'
                    )
                    logger.info(f"📢 Событие опубликовано для сигнала #{db_signal_id} ({signal_subtype})")

                return db_signal_id

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
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
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка получения: {e}")
            return []

    async def get_signals(self, limit: int = 50, signal_subtype: Optional[str] = None) -> List[Dict]:
        """
        Получение сигналов с возможностью фильтрации по подтипу (WATCH/LIMIT/INSTANT)
        """
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
                        "SELECT * FROM signals ORDER BY created_time DESC LIMIT ?",
                        (limit,)
                    )

                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка получения: {e}")
            return []

    async def get_signals_with_trades(self, limit: int = 20) -> List[Dict]:
        """Получение сигналов с информацией о трейдах (упрощенная версия)"""
        try:
            signals = await self.get_all_signals(limit)
            for signal in signals:
                signal['active_trades'] = 0
                signal['trade_count'] = 0
                signal['current_price'] = signal.get('entry_price', 0)
                signal['leverage'] = 10
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

    async def get_active_signals(self) -> List[Dict]:
        """
        Получение активных (не истекших) сигналов
        """
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
            logger.error(f"❌ Ошибка получения активных сигналов: {e}")
            return []

    async def get_database_stats(self) -> Dict[str, Any]:
        """Получение статистики БД"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row

                # Общее количество сигналов
                cursor = await conn.execute("SELECT COUNT(*) as total FROM signals")
                row = await cursor.fetchone()
                total_signals = row['total'] if row else 0

                # Активные сигналы (с учетом expiration_time)
                current_time = datetime.now().isoformat()
                cursor = await conn.execute("""
                    SELECT COUNT(*) as active FROM signals 
                    WHERE status IN ('PENDING', 'ACTIVE')
                    AND (expiration_time IS NULL OR expiration_time > ?)
                """, (current_time,))
                row = await cursor.fetchone()
                active_signals = row['active'] if row else 0

                # Three Screen сигналы
                cursor = await conn.execute(
                    "SELECT COUNT(*) as three_screen FROM signals WHERE strategy = 'three_screen'"
                )
                row = await cursor.fetchone()
                three_screen_signals = row['three_screen'] if row else 0

                # BUY сигналы
                cursor = await conn.execute(
                    "SELECT COUNT(*) as buy_count FROM signals WHERE direction = 'BUY'"
                )
                row = await cursor.fetchone()
                buy_signals = row['buy_count'] if row else 0

                # SELL сигналы
                cursor = await conn.execute(
                    "SELECT COUNT(*) as sell_count FROM signals WHERE direction = 'SELL'"
                )
                row = await cursor.fetchone()
                sell_signals = row['sell_count'] if row else 0

                # Статистика по подтипам сигналов
                cursor = await conn.execute(
                    "SELECT signal_subtype, COUNT(*) as count FROM signals GROUP BY signal_subtype"
                )
                rows = await cursor.fetchall()
                subtypes_stats = {row['signal_subtype']: row['count'] for row in rows}

                return {
                    'total_signals': total_signals,
                    'active_signals': active_signals,
                    'three_screen_signals': three_screen_signals,
                    'buy_signals': buy_signals,
                    'sell_signals': sell_signals,
                    'subtypes_stats': subtypes_stats,
                    'active_trades': 0,
                    'closed_trades': 0,
                    'total_pnl': 0
                }

        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {
                'total_signals': 0,
                'active_signals': 0,
                'three_screen_signals': 0,
                'buy_signals': 0,
                'sell_signals': 0,
                'subtypes_stats': {},
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
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка получения недавних сигналов: {e}")
            return []

    async def expire_old_signals(self) -> int:
        """
        Помечает истекшие сигналы как EXPIRED
        Returns:
            Количество помеченных сигналов
        """
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                current_time = datetime.now().isoformat()

                cursor = await conn.execute("""
                    UPDATE signals 
                    SET status = 'EXPIRED'
                    WHERE status IN ('PENDING', 'ACTIVE')
                    AND expiration_time IS NOT NULL
                    AND expiration_time <= ?
                """, (current_time,))

                await conn.commit()
                expired_count = cursor.rowcount

                if expired_count > 0:
                    logger.info(f"⏰ Помечено {expired_count} истекших сигналов как EXPIRED")

                return expired_count

        except Exception as e:
            logger.error(f"❌ Ошибка обновления истекших сигналов: {e}")
            return 0


# Глобальный экземпляр
signal_repository = SignalRepository()