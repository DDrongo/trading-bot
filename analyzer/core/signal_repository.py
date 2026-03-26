# analyzer/core/signal_repository.py (ПОЛНОСТЬЮ - ИСПРАВЛЕННАЯ ВЕРСИЯ)
# Добавлены: updated_time, has_active_m15

import aiosqlite
import logging
import os
from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime, timedelta

from analyzer.core.event_bus import EventType, event_bus as global_event_bus

logger = logging.getLogger("signal_repository")


class SignalRepository:
    def __init__(self, config=None, db_path: Optional[str] = None):
        """
        Инициализация репозитория сигналов

        Args:
            config: Конфигурация (опционально)
            db_path: Путь к файлу БД (если None, используется путь по умолчанию)
        """
        self.config = config or {}
        self.event_bus = global_event_bus

        # Если передан явный путь к БД, используем его
        if db_path:
            self.db_path = db_path
        else:
            # Иначе используем путь по умолчанию
            project_root = Path(__file__).parent.parent.parent
            self.db_path = str(project_root / "data/trading_bot.db")

        logger.info(f"📊 SignalRepository использует: {self.db_path}")

    async def initialize(self) -> bool:
        """Инициализация базы данных и таблиц с добавлением полей zone_* и updated_time"""
        try:
            # Создаем директорию для БД если её нет
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

            async with aiosqlite.connect(self.db_path) as conn:
                # Проверяем существование таблицы signals
                cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signals'")
                table_exists = await cursor.fetchone()

                if not table_exists:
                    # Создаем таблицу signals с новыми полями (включая updated_time)
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
                            screen2_score INTEGER
                        )
                    """)
                    logger.info("✅ Таблица signals создана с полями zone_* и updated_time")
                else:
                    # Проверяем наличие всех необходимых полей и добавляем недостающие
                    cursor = await conn.execute("PRAGMA table_info(signals)")
                    columns = await cursor.fetchall()
                    column_names = [col[1] for col in columns]

                    # Список полей, которые нужно добавить если их нет
                    fields_to_add = [
                        'signal_subtype', 'expiration_time', 'screen',
                        'order_type', 'fill_price', 'position_size', 'rejection_reason',
                        'zone_low', 'zone_high', 'expected_pattern', 'screen2_score',
                        'updated_time'  # ✅ ДОБАВЛЕНО
                    ]

                    for field in fields_to_add:
                        if field not in column_names:
                            await conn.execute(f"ALTER TABLE signals ADD COLUMN {field} TEXT")
                            logger.info(f"✅ Добавлено поле {field} в таблицу signals")

                # Проверяем существование таблицы trades
                cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades'")
                trades_exists = await cursor.fetchone()

                if not trades_exists:
                    # Создаем таблицу trades
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
                    logger.info("✅ Таблица trades создана")

                # Создаем индексы для ускорения запросов
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_time)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_subtype ON signals(signal_subtype)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_signal_id ON trades(signal_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_opened ON trades(opened_at)")

                # Составные индексы для частых запросов
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol_status ON signals(symbol, status)")
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_signals_subtype_status ON signals(signal_subtype, status)")

                # ✅ НОВЫЙ ИНДЕКС: для быстрого поиска WATCH монет
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_watch_status ON signals(status, expiration_time)")

                await conn.commit()
                logger.info("✅ База данных успешно инициализирована с индексами")
                return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    async def check_duplicate_signal(
            self,
            symbol: str,
            signal_subtype: str,
            expiration_hours: int = 24
    ) -> bool:
        """
        Проверка на дубликат сигнала

        ✅ ФАЗА 1.3.6: Исключаем REJECTED из проверки, учитываем WATCH статус

        Args:
            symbol: Символ монеты
            signal_subtype: Тип сигнала (M15/WATCH)
            expiration_hours: Количество часов для проверки (время жизни активного сигнала)

        Returns:
            True если активный сигнал для этого символа уже существует
        """
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row

                # Исключаем REJECTED и EXPIRED из проверки дубликатов
                cursor = await conn.execute("""
                    SELECT COUNT(*) as count FROM signals 
                    WHERE symbol = ? 
                    AND signal_subtype = ?
                    AND status IN ('WATCH', 'ACTIVE', 'PENDING')
                    AND created_time > datetime('now', ?)
                """, (symbol, signal_subtype, f'-{expiration_hours} hours'))
                row = await cursor.fetchone()
                is_duplicate = (row['count'] if row else 0) > 0

                if is_duplicate:
                    logger.debug(f"⚠️ Найден дубликат для {symbol} ({signal_subtype})")
                return is_duplicate

        except Exception as e:
            logger.error(f"❌ Ошибка проверки дубликата: {e}")
            return False

    async def has_active_m15(self, symbol: str) -> bool:
        """
        Проверяет, есть ли активный M15 сигнал для символа

        ✅ НОВОЕ для Фазы 1.3.6.2 (HOTFIX)

        Args:
            symbol: Символ монеты

        Returns:
            True если есть активный M15 сигнал
        """
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
            logger.error(f"❌ Ошибка проверки активного M15 для {symbol}: {e}")
            return False

    async def get_watch_symbols(self) -> List[str]:
        """
        Получить список символов в статусе WATCH

        ✅ НОВОЕ для Фазы 1.3.6

        Returns:
            Список символов с активным статусом WATCH
        """
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
        expiration_hours: int = 3
    ) -> Optional[int]:
        """
        Сохранение WATCH сигнала (одна запись на монету)

        ✅ НОВОЕ для Фазы 1.3.6

        Args:
            symbol: Символ монеты
            direction: Направление (BUY/SELL)
            zone_low: Нижняя граница зоны
            zone_high: Верхняя граница зоны
            screen2_score: Количество условий Screen 2 (4 или 5)
            expected_pattern: Ожидаемый паттерн (опционально)
            expiration_hours: Время жизни WATCH в часах

        Returns:
            ID сохраненного сигнала или None
        """
        try:
            expiration_time = datetime.now() + timedelta(hours=expiration_hours)

            async with aiosqlite.connect(self.db_path) as conn:
                # Проверяем, есть ли уже активный WATCH для этого символа
                cursor = await conn.execute("""
                    SELECT id FROM signals 
                    WHERE symbol = ? AND status = 'WATCH' 
                    AND expiration_time > datetime('now')
                """, (symbol,))
                existing = await cursor.fetchone()

                if existing:
                    # Обновляем существующую запись
                    await conn.execute("""
                        UPDATE signals SET
                            zone_low = ?, zone_high = ?, screen2_score = ?,
                            expected_pattern = ?, expiration_time = ?,
                            direction = ?, updated_time = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (zone_low, zone_high, screen2_score, expected_pattern,
                          expiration_time.isoformat(), direction, existing[0]))
                    await conn.commit()
                    logger.info(f"🔄 Обновлён WATCH сигнал для {symbol} (score={screen2_score})")
                    return existing[0]
                else:
                    # Создаём новую запись
                    cursor = await conn.execute("""
                        INSERT INTO signals (
                            symbol, direction, signal_subtype, status,
                            zone_low, zone_high, screen2_score, expected_pattern,
                            expiration_time, created_time, updated_time
                        ) VALUES (?, ?, 'WATCH', 'WATCH', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """, (symbol, direction, zone_low, zone_high, screen2_score,
                          expected_pattern, expiration_time.isoformat()))
                    await conn.commit()
                    signal_id = cursor.lastrowid
                    logger.info(f"✅ Создан WATCH сигнал для {symbol} (ID={signal_id}, score={screen2_score})")
                    return signal_id

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения WATCH сигнала {symbol}: {e}")
            return None

    async def update_watch_to_active(
        self,
        symbol: str,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        trigger_pattern: str,
        expiration_hours: int = 3
    ) -> Optional[int]:
        """
        Обновление WATCH → ACTIVE при формировании паттерна

        ✅ НОВОЕ для Фазы 1.3.6

        Args:
            symbol: Символ монеты
            entry_price: Цена входа (текущая цена)
            stop_loss: Стоп-лосс
            take_profit: Тейк-профит
            trigger_pattern: Тип паттерна
            expiration_hours: Время жизни активного сигнала

        Returns:
            ID обновлённого сигнала или None
        """
        try:
            expiration_time = datetime.now() + timedelta(hours=expiration_hours)

            async with aiosqlite.connect(self.db_path) as conn:
                # Находим активный WATCH для символа
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

                # Обновляем на ACTIVE
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

                logger.info(f"✅ WATCH → ACTIVE для {symbol} (ID={signal_id}, паттерн={trigger_pattern})")
                return signal_id

        except Exception as e:
            logger.error(f"❌ Ошибка обновления WATCH → ACTIVE для {symbol}: {e}")
            return None

    async def get_watch_signal(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Получить активный WATCH сигнал для символа

        ✅ НОВОЕ для Фазы 1.3.6

        Args:
            symbol: Символ монеты

        Returns:
            Словарь с данными WATCH сигнала или None
        """
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
            logger.error(f"❌ Ошибка получения WATCH сигнала {symbol}: {e}")
            return None

    async def save_signal(self, analysis: Any, screen: str = "M15") -> Optional[int]:
        """
        Сохраняет сигнал в БД (для обратной совместимости)
        В v1.3.6 используется только для M15 сигналов

        Args:
            analysis: Объект ThreeScreenAnalysis с данными сигнала
            screen: Экран, на котором сгенерирован сигнал (M15 по умолчанию)

        Returns:
            ID сохраненного сигнала или None
        """
        try:
            # Проверяем наличие данных
            if not hasattr(analysis, "screen3") or not analysis.screen3:
                logger.warning("Нет данных screen3 для сохранения сигнала")
                return None

            signal_subtype = getattr(analysis.screen3, 'signal_subtype', 'M15')

            # Проверка на дубликат
            expiration_hours = 3  # M15 живёт 3 часа
            is_duplicate = await self.check_duplicate_signal(
                analysis.symbol,
                signal_subtype,
                expiration_hours
            )

            if is_duplicate:
                logger.info(f"⏭️ Пропускаем дубликат сигнала {analysis.symbol} ({signal_subtype})")
                return None

            # Получаем данные для сохранения
            expiration_time = getattr(analysis.screen3, 'expiration_time', None)
            order_type = "MARKET"  # M15 всегда MARKET
            position_size = getattr(analysis.screen3, 'position_size', None)
            rejection_reason = getattr(analysis.screen3, 'rejection_reason', None)

            # Преобразуем время в строку
            expiration_time_str = None
            if expiration_time:
                if isinstance(expiration_time, datetime):
                    expiration_time_str = expiration_time.isoformat()
                else:
                    expiration_time_str = str(expiration_time)

            # Получаем данные из анализа
            trend_direction = getattr(analysis.screen1, "trend_direction", "")
            trend_strength = getattr(analysis.screen1, "trend_strength", "")
            signal_strength = getattr(analysis.screen3, "signal_strength", "")
            trigger_pattern = getattr(analysis.screen3, "trigger_pattern", "")

            risk_reward_ratio = analysis.screen3.indicators.get("risk_reward_ratio", 0)
            risk_pct = analysis.screen3.indicators.get("risk_pct", 0)

            # Получаем zone_* из анализа
            zone_low = getattr(analysis, 'zone_low', 0.0)
            zone_high = getattr(analysis, 'zone_high', 0.0)
            expected_pattern = getattr(analysis, 'expected_pattern', '')
            screen2_score = getattr(analysis, 'screen2_score', 0)

            # Определяем статус
            status = 'REJECTED' if rejection_reason else 'ACTIVE'

            # Вставляем запись
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    INSERT INTO signals (
                        symbol, direction, signal_subtype, confidence, entry_price,
                        stop_loss, take_profit, trend_direction, trend_strength,
                        signal_strength, trigger_pattern, risk_reward_ratio,
                        risk_pct, expiration_time, screen, order_type, position_size,
                        status, rejection_reason, zone_low, zone_high,
                        expected_pattern, screen2_score, updated_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    analysis.symbol,
                    analysis.screen3.signal_type,
                    signal_subtype,
                    analysis.overall_confidence,
                    analysis.screen3.entry_price,
                    analysis.screen3.stop_loss,
                    analysis.screen3.take_profit,
                    trend_direction,
                    trend_strength,
                    signal_strength,
                    trigger_pattern,
                    risk_reward_ratio,
                    risk_pct,
                    expiration_time_str,
                    screen,
                    order_type,
                    position_size,
                    status,
                    rejection_reason,
                    zone_low,
                    zone_high,
                    expected_pattern,
                    screen2_score,
                    datetime.now()
                ))
                await conn.commit()
                signal_id = cursor.lastrowid

                if rejection_reason:
                    logger.info(f"⚠️ Сигнал сохранен как REJECTED: ID={signal_id}, {analysis.symbol}")
                else:
                    logger.info(f"✅ M15 сигнал сохранен: ID={signal_id}, {analysis.symbol}")
                return signal_id

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения сигнала: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    async def get_signals_with_trades(self, limit: int = 20) -> List[Dict]:
        """Получение сигналов с информацией о связанных сделках"""
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
                        t.closed_at as trade_closed_at
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
            logger.error(f"❌ Ошибка получения сигнала {signal_id}: {e}")
            return None

    async def get_signals(
            self,
            limit: int = 50,
            signal_subtype: Optional[str] = None,
            status: Optional[str] = None,
            symbol: Optional[str] = None
    ) -> List[Dict]:
        """Получение списка сигналов с фильтрацией"""
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

    async def get_active_signals(self) -> List[Dict]:
        """Получение активных сигналов (WATCH, ACTIVE)"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                current_time = datetime.now().isoformat()
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

    async def get_active_signals_by_subtype(self, signal_subtype: str) -> List[Dict]:
        """Получение активных сигналов по подтипу"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                current_time = datetime.now().isoformat()
                cursor = await conn.execute("""
                    SELECT * FROM signals 
                    WHERE signal_subtype = ?
                    AND status IN ('WATCH', 'ACTIVE')
                    AND (expiration_time IS NULL OR expiration_time > ?)
                    ORDER BY created_time DESC
                """, (signal_subtype, current_time))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка получения активных сигналов по подтипу: {e}")
            return []

    async def get_pending_signals(self) -> List[Dict]:
        """Получение ожидающих сигналов (для совместимости, больше не используется)"""
        return []

    async def get_rejected_signals(self, limit: int = 50) -> List[Dict]:
        """Получение отклонённых сигналов (для анализа)"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("""
                    SELECT * FROM signals 
                    WHERE status = 'REJECTED'
                    ORDER BY created_time DESC 
                    LIMIT ?
                """, (limit,))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Ошибка получения отклонённых сигналов: {e}")
            return []

    async def get_database_stats(self) -> Dict[str, Any]:
        """Получение статистики базы данных"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row

                # Общее количество сигналов
                cursor = await conn.execute("SELECT COUNT(*) as total FROM signals")
                row = await cursor.fetchone()
                total_signals = row['total'] if row else 0

                # Количество BUY/SELL
                cursor = await conn.execute(
                    "SELECT COUNT(*) as buy_count FROM signals WHERE direction = 'BUY'"
                )
                row = await cursor.fetchone()
                buy_signals = row['buy_count'] if row else 0

                cursor = await conn.execute(
                    "SELECT COUNT(*) as sell_count FROM signals WHERE direction = 'SELL'"
                )
                row = await cursor.fetchone()
                sell_signals = row['sell_count'] if row else 0

                # Количество активных сигналов (WATCH и ACTIVE)
                current_time = datetime.now().isoformat()
                cursor = await conn.execute("""
                    SELECT COUNT(*) as active_count 
                    FROM signals 
                    WHERE status IN ('WATCH', 'ACTIVE')
                    AND (expiration_time IS NULL OR expiration_time > ?)
                """, (current_time,))
                row = await cursor.fetchone()
                active_signals = row['active_count'] if row else 0

                # Количество отклонённых сигналов
                cursor = await conn.execute("""
                    SELECT COUNT(*) as rejected_count 
                    FROM signals 
                    WHERE status = 'REJECTED'
                """)
                row = await cursor.fetchone()
                rejected_signals = row['rejected_count'] if row else 0

                # Статистика по подтипам
                cursor = await conn.execute(
                    "SELECT signal_subtype, COUNT(*) as count FROM signals GROUP BY signal_subtype"
                )
                rows = await cursor.fetchall()
                subtypes_stats = {row['signal_subtype']: row['count'] for row in rows}

                # Статистика по статусам
                cursor = await conn.execute(
                    "SELECT status, COUNT(*) as count FROM signals GROUP BY status"
                )
                rows = await cursor.fetchall()
                status_stats = {row['status']: row['count'] for row in rows}

                # Статистика по сделкам
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

                return {
                    'total_signals': total_signals,
                    'active_signals': active_signals,
                    'rejected_signals': rejected_signals,
                    'three_screen_signals': total_signals,
                    'buy_signals': buy_signals,
                    'sell_signals': sell_signals,
                    'subtypes_stats': subtypes_stats,
                    'status_stats': status_stats,
                    'active_trades': 0,
                    'closed_trades': total_trades,
                    'winning_trades': winning_trades,
                    'losing_trades': losing_trades,
                    'total_pnl': total_pnl,
                    'win_rate': win_rate
                }
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}

    async def update_signal_status(self, signal_id: int, status: str) -> bool:
        """Обновление статуса сигнала"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute(
                    "UPDATE signals SET status = ?, updated_time = CURRENT_TIMESTAMP WHERE id = ?",
                    (status, signal_id)
                )
                await conn.commit()
                logger.info(f"✅ Статус сигнала #{signal_id} обновлен на {status}")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления статуса сигнала {signal_id}: {e}")
            return False

    async def update_rejection_reason(self, signal_id: int, rejection_reason: str) -> bool:
        """Обновление причины отклонения сигнала"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute(
                    "UPDATE signals SET rejection_reason = ?, status = 'REJECTED', updated_time = CURRENT_TIMESTAMP WHERE id = ?",
                    (rejection_reason, signal_id)
                )
                await conn.commit()
                logger.info(f"✅ Сигнал #{signal_id} отклонён: {rejection_reason}")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления причины отклонения сигнала {signal_id}: {e}")
            return False

    async def update_fill_price(self, signal_id: int, fill_price: float) -> bool:
        """Обновление цены исполнения ордера"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute(
                    "UPDATE signals SET fill_price = ?, updated_time = CURRENT_TIMESTAMP WHERE id = ?",
                    (fill_price, signal_id)
                )
                await conn.commit()
                logger.info(f"✅ Цена исполнения #{signal_id}: {fill_price:.6f}")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления цены исполнения {signal_id}: {e}")
            return False

    async def update_position_size(self, signal_id: int, position_size: float) -> bool:
        """Обновление размера позиции"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute(
                    "UPDATE signals SET position_size = ?, updated_time = CURRENT_TIMESTAMP WHERE id = ?",
                    (position_size, signal_id)
                )
                await conn.commit()
                logger.info(f"✅ Размер позиции #{signal_id}: {position_size:.4f}")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления размера позиции {signal_id}: {e}")
            return False

    async def delete_old_signals(self, days: int = 30) -> int:
        """Удаление старых сигналов"""
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
        """Оптимизация базы данных"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("VACUUM")
                logger.info("✅ База данных оптимизирована (VACUUM)")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка оптимизации БД: {e}")
            return False

    async def close(self) -> None:
        """Закрытие соединения с БД (для совместимости)"""
        logger.info("📊 SignalRepository закрыт")
        pass


# Глобальный экземпляр репозитория
signal_repository = SignalRepository()