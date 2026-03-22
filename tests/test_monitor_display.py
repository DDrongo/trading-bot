#!/usr/bin/env python3
"""
Тесты для монитора отображения (Фаза 1.3.1)
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from monitor_three_screen import ThreeScreenMonitor


@pytest.fixture
def mock_config():
    """Mock конфигурации"""
    return {
        'display': {
            'timezone_offset': 3,
            'refresh_interval': 5
        },
        'database': {
            'path': 'data/trading_bot.db'
        }
    }


@pytest.fixture
def monitor(mock_config):
    """Создание монитора с мок-конфигом"""
    monitor = ThreeScreenMonitor(mock_config)
    monitor.signal_repo = AsyncMock()
    monitor.trade_repo = AsyncMock()
    return monitor


def test_utc_to_local_conversion(monitor):
    """Тест: Преобразование UTC в локальное время"""
    utc_str = "2026-03-22 10:30:00"

    # С мок-часовым поясом +3
    monitor.timezone_offset = 3
    local = monitor.utc_to_local(utc_str)

    expected = datetime(2026, 3, 22, 13, 30, 0)
    assert local.year == expected.year
    assert local.month == expected.month
    assert local.day == expected.day
    assert local.hour == expected.hour
    assert local.minute == expected.minute


def test_format_time(monitor):
    """Тест: Форматирование времени"""
    monitor.timezone_offset = 3

    # Тест с полной датой
    result = monitor.format_time("2026-03-22 10:30:00")
    assert result == "13:30"  # UTC+3

    # Тест с пустым значением
    result = monitor.format_time("")
    assert result == "-"


def test_format_date(monitor):
    """Тест: Форматирование даты"""
    monitor.timezone_offset = 3

    result = monitor.format_date("2026-03-22 10:30:00")
    assert result == "03-22"  # Месяц-день

    result = monitor.format_date("")
    assert result == "-"


def test_format_direction(monitor):
    """Тест: Форматирование направления"""
    result = monitor.format_direction("BUY")
    assert "BUY" in result

    result = monitor.format_direction("SELL")
    assert "SELL" in result

    result = monitor.format_direction("")
    assert result == "N/A"


def test_format_status(monitor):
    """Тест: Форматирование статуса"""
    result = monitor.format_status("PENDING")
    assert "PENDING" in result

    result = monitor.format_status("ACTIVE")
    assert "ACTIVE" in result

    result = monitor.format_status("CLOSED")
    assert "CLOSED" in result


def test_format_confidence(monitor):
    """Тест: Форматирование уверенности"""
    result = monitor.format_confidence(0.85)
    assert "85.0%" in result

    result = monitor.format_confidence(0.65)
    assert "65.0%" in result

    result = monitor.format_confidence(None)
    assert result == "-"


def test_format_price(monitor):
    """Тест: Форматирование цены"""
    result = monitor.format_price(50000.0)
    assert result == "50000.00"

    result = monitor.format_price(0.28)
    assert result == "0.280000"

    result = monitor.format_price(None)
    assert result == "-"


def test_format_rr_ratio(monitor):
    """Тест: Форматирование R/R"""
    result = monitor.format_rr_ratio(3.5)
    assert "3.50:1" in result

    result = monitor.format_rr_ratio(2.5)
    assert "2.50:1" in result

    result = monitor.format_rr_ratio(1.5)
    assert "1.50:1" in result

    result = monitor.format_rr_ratio(None)
    assert result == "-"


def test_format_pnl(monitor):
    """Тест: Форматирование PnL"""
    result = monitor.format_pnl(150.5)
    assert "+150.50" in result

    result = monitor.format_pnl(-50.25)
    assert "-50.25" in result

    result = monitor.format_pnl(0)
    assert "0.00" in result


def test_format_trend(monitor):
    """Тест: Форматирование тренда"""
    result = monitor.format_trend("BULL")
    assert "BULL" in result

    result = monitor.format_trend("BEAR")
    assert "BEAR" in result

    result = monitor.format_trend("")
    assert result == "-"


def test_format_screen(monitor):
    """Тест: Форматирование экрана"""
    result = monitor.format_screen("D1")
    assert "D1" in result

    result = monitor.format_screen("H4")
    assert "H4" in result

    result = monitor.format_screen("M15")
    assert "M15" in result

    result = monitor.format_screen("")
    assert result == "-"


def test_format_position_size(monitor):
    """Тест: Форматирование размера позиции"""
    result = monitor.format_position_size(0.1234)
    assert result == "0.1234"

    result = monitor.format_position_size(None)
    assert result == "-"


def test_strip_ansi(monitor):
    """Тест: Удаление ANSI кодов"""
    ansi_text = "\x1b[32mGreen Text\x1b[0m"
    result = monitor.strip_ansi(ansi_text)
    assert result == "Green Text"

    plain_text = "Plain Text"
    result = monitor.strip_ansi(plain_text)
    assert result == "Plain Text"


def test_get_visible_length(monitor):
    """Тест: Длина видимого текста без ANSI кодов"""
    ansi_text = "\x1b[32mHello\x1b[0m"
    result = monitor.get_visible_length(ansi_text)
    assert result == 5

    plain_text = "Hello"
    result = monitor.get_visible_length(plain_text)
    assert result == 5


def test_create_table(monitor):
    """Тест: Создание таблицы"""
    headers = ["ID", "Name", "Value"]
    data = [
        ["1", "Test", "100"],
        ["2", "Another", "200"]
    ]

    table = monitor.create_table(headers, data)

    assert "┌" in table
    assert "│" in table
    assert "ID" in table
    assert "Name" in table
    assert "Value" in table
    assert "1" in table
    assert "Test" in table
    assert "100" in table


def test_create_table_empty(monitor):
    """Тест: Создание таблицы с пустыми данными"""
    table = monitor.create_table(["Header"], [])
    assert table == "Нет данных"


@pytest.mark.asyncio
async def test_display_trades_table(monitor):
    """Тест: Отображение таблицы сделок"""
    # Мокаем закрытые сделки
    mock_trades = [
        {
            'id': 1,
            'symbol': 'BTCUSDT',
            'direction': 'BUY',
            'entry_price': 50000.0,
            'close_price': 51000.0,
            'pnl': 100.0,
            'close_reason': 'TP',
            'closed_at': '2026-03-22 10:30:00'
        },
        {
            'id': 2,
            'symbol': 'ETHUSDT',
            'direction': 'SELL',
            'entry_price': 3000.0,
            'close_price': 2950.0,
            'pnl': 50.0,
            'close_reason': 'TP',
            'closed_at': '2026-03-22 11:00:00'
        }
    ]

    monitor.trade_repo.get_closed_trades = AsyncMock(return_value=mock_trades)

    # Патчим input для избежания ожидания ввода
    with patch('builtins.input', return_value=''):
        # Функция должна выполниться без ошибок
        try:
            await monitor.display_trades_table()
        except Exception as e:
            pytest.fail(f"display_trades_table raised exception: {e}")


@pytest.mark.asyncio
async def test_display_signal_details(monitor):
    """Тест: Отображение деталей сигнала"""
    # Мокаем сигнал
    mock_signal = {
        'id': 1,
        'symbol': 'BTCUSDT',
        'direction': 'BUY',
        'screen': 'M15',
        'status': 'ACTIVE',
        'confidence': 0.85,
        'strategy': 'three_screen',
        'trend_direction': 'BULL',
        'trend_strength': 'STRONG',
        'signal_strength': 'STRONG',
        'trigger_pattern': 'ENGULFING',
        'signal_subtype': 'LIMIT',
        'order_type': 'LIMIT',
        'entry_price': 50000.0,
        'stop_loss': 49500.0,
        'take_profit': 51000.0,
        'risk_reward_ratio': 2.0,
        'risk_pct': 2.0,
        'position_size': 0.1,
        'created_time': '2026-03-22 10:30:00',
        'expiration_time': (datetime.now() + timedelta(hours=24)).isoformat()
    }

    monitor.signal_repo.get_signal_by_id = AsyncMock(return_value=mock_signal)
    monitor.get_current_price = AsyncMock(return_value=50500.0)

    with patch('builtins.input', return_value=''):
        try:
            await monitor.display_signal_details(1)
        except Exception as e:
            pytest.fail(f"display_signal_details raised exception: {e}")


def test_format_datetime(monitor):
    """Тест: Форматирование полной даты и времени"""
    monitor.timezone_offset = 3
    result = monitor.format_datetime("2026-03-22 10:30:00")
    assert "22.03.2026" in result
    assert "13:30:00" in result

    result = monitor.format_datetime("")
    assert result == "-"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])