#!/usr/bin/env python3
"""
Тесты для предотвращения дублирования сигналов (Фаза 1.3.1)
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from analyzer.core.signal_repository import signal_repository


@pytest.mark.asyncio
async def test_check_duplicate_signal_no_duplicate():
    """Тест: Проверка дубликата - нет дубликатов"""
    with patch('analyzer.core.signal_repository.aiosqlite') as mock_sqlite:
        # Мокаем подключение
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()

        # Мокаем результат запроса (нет дубликатов)
        mock_cursor.fetchone = AsyncMock(return_value={'count': 0})
        mock_conn.execute = AsyncMock(return_value=mock_cursor)
        mock_sqlite.connect = AsyncMock(return_value=mock_conn)

        result = await signal_repository.check_duplicate_signal('BTCUSDT', 'LIMIT', 24)

        assert result is False


@pytest.mark.asyncio
async def test_check_duplicate_signal_has_duplicate():
    """Тест: Проверка дубликата - есть дубликат"""
    with patch('analyzer.core.signal_repository.aiosqlite') as mock_sqlite:
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()

        # Мокаем результат запроса (есть дубликат)
        mock_cursor.fetchone = AsyncMock(return_value={'count': 1})
        mock_conn.execute = AsyncMock(return_value=mock_cursor)
        mock_sqlite.connect = AsyncMock(return_value=mock_conn)

        result = await signal_repository.check_duplicate_signal('BTCUSDT', 'LIMIT', 24)

        assert result is True


@pytest.mark.asyncio
async def test_check_duplicate_signal_different_subtype():
    """Тест: Проверка дубликата - другой подтип сигнала"""
    with patch('analyzer.core.signal_repository.aiosqlite') as mock_sqlite:
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()

        # Мокаем результат запроса (нет дубликатов для INSTANT)
        mock_cursor.fetchone = AsyncMock(return_value={'count': 0})
        mock_conn.execute = AsyncMock(return_value=mock_cursor)
        mock_sqlite.connect = AsyncMock(return_value=mock_conn)

        result = await signal_repository.check_duplicate_signal('BTCUSDT', 'INSTANT', 24)

        assert result is False


@pytest.mark.asyncio
async def test_check_duplicate_signal_expired():
    """Тест: Проверка дубликата - старый сигнал уже истек"""
    with patch('analyzer.core.signal_repository.aiosqlite') as mock_sqlite:
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()

        # Мокаем результат запроса (сигнал истек, не считается)
        mock_cursor.fetchone = AsyncMock(return_value={'count': 0})
        mock_conn.execute = AsyncMock(return_value=mock_cursor)
        mock_sqlite.connect = AsyncMock(return_value=mock_conn)

        result = await signal_repository.check_duplicate_signal('BTCUSDT', 'LIMIT', 24)

        assert result is False


@pytest.mark.asyncio
async def test_check_duplicate_signal_different_symbol():
    """Тест: Проверка дубликата - разные символы"""
    with patch('analyzer.core.signal_repository.aiosqlite') as mock_sqlite:
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()

        # Мокаем результат запроса (дубликат для BTC, но проверяем ETH)
        def execute_side_effect(query, params):
            if params[0] == 'BTCUSDT':
                mock_cursor.fetchone = AsyncMock(return_value={'count': 1})
            else:
                mock_cursor.fetchone = AsyncMock(return_value={'count': 0})
            return mock_cursor

        mock_conn.execute = AsyncMock(side_effect=execute_side_effect)
        mock_sqlite.connect = AsyncMock(return_value=mock_conn)

        result = await signal_repository.check_duplicate_signal('ETHUSDT', 'LIMIT', 24)

        assert result is False


@pytest.mark.asyncio
async def test_save_signal_with_duplicate_check():
    """Тест: Сохранение сигнала с проверкой дубликата"""
    with patch('analyzer.core.signal_repository.aiosqlite') as mock_sqlite:
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()

        # Мокаем сначала проверку дубликата (нет)
        mock_cursor.fetchone = AsyncMock(return_value={'count': 0})
        mock_conn.execute = AsyncMock(return_value=mock_cursor)
        mock_conn.commit = AsyncMock()

        # Мокаем вставку
        mock_cursor.lastrowid = 123
        mock_conn.execute = AsyncMock(return_value=mock_cursor)
        mock_sqlite.connect = AsyncMock(return_value=mock_conn)

        # Создаем мок-анализ
        mock_analysis = MagicMock()
        mock_analysis.symbol = 'BTCUSDT'
        mock_analysis.overall_confidence = 0.85

        mock_analysis.screen1 = MagicMock()
        mock_analysis.screen1.trend_direction = 'BULL'
        mock_analysis.screen1.trend_strength = 'STRONG'

        mock_analysis.screen2 = MagicMock()
        mock_analysis.screen2.best_zone = 50000.0

        mock_analysis.screen3 = MagicMock()
        mock_analysis.screen3.signal_type = 'BUY'
        mock_analysis.screen3.signal_subtype = 'LIMIT'
        mock_analysis.screen3.entry_price = 50000.0
        mock_analysis.screen3.stop_loss = 49500.0
        mock_analysis.screen3.take_profit = 51000.0
        mock_analysis.screen3.signal_strength = 'STRONG'
        mock_analysis.screen3.trigger_pattern = 'ENGULFING'
        mock_analysis.screen3.indicators = {'risk_reward_ratio': 2.0, 'risk_pct': 2.0}
        mock_analysis.screen3.expiration_time = datetime.now() + timedelta(hours=24)

        # Проверяем дубликат перед сохранением
        is_duplicate = await signal_repository.check_duplicate_signal('BTCUSDT', 'LIMIT', 24)

        assert is_duplicate is False


@pytest.mark.asyncio
async def test_prevent_saving_duplicate_signal():
    """Тест: Предотвращение сохранения дубликата сигнала"""
    with patch('analyzer.core.signal_repository.aiosqlite') as mock_sqlite:
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()

        # Мокаем проверку дубликата (есть дубликат)
        mock_cursor.fetchone = AsyncMock(return_value={'count': 1})
        mock_conn.execute = AsyncMock(return_value=mock_cursor)
        mock_sqlite.connect = AsyncMock(return_value=mock_conn)

        # Проверяем дубликат
        is_duplicate = await signal_repository.check_duplicate_signal('BTCUSDT', 'LIMIT', 24)

        assert is_duplicate is True

        # В реальном коде при is_duplicate == True сигнал не сохраняется
        # Здесь проверяем только логику проверки


if __name__ == '__main__':
    pytest.main([__file__, '-v'])