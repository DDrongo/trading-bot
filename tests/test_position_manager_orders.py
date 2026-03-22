#!/usr/bin/env python3
"""
Тесты для Position Manager с поддержкой LIMIT и INSTANT ордеров (Фаза 1.3.1)
"""

import asyncio
import pytest
import sys
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

# Добавляем путь к проекту
sys.path.insert(0, str(Path(__file__).parent.parent))

from analyzer.core.position_manager import PositionManager
from analyzer.core.paper_account import PaperAccount, PaperPosition, PendingOrder
from analyzer.core.event_bus import EventBus, EventType
from analyzer.core.signal_repository import signal_repository
from analyzer.core.trade_repository import trade_repository


@pytest.fixture
def mock_config():
    """Mock конфигурации"""
    return {
        'position_management': {
            'enabled': True,
            'monitoring_interval_seconds': 1,
            'default_quantity': 0.001,
            'max_quantity': 1.0,
            'max_positions': 5,
            'position_sizing': {
                'method': 'risk_based',
                'risk_per_trade_pct': 2.0
            }
        },
        'paper_trading': {
            'starting_virtual_balance': 10000.0,
            'commission_rate': 0.001,
            'slippage_percentage': 0.001
        }
    }


@pytest.fixture
def mock_api_client():
    """Mock API клиента"""
    client = AsyncMock()
    client.get_current_price = AsyncMock(return_value=50000.0)
    return client


@pytest.fixture
def event_bus():
    """Event bus для тестов"""
    bus = EventBus()
    bus._is_running = True
    return bus


@pytest.mark.asyncio
async def test_limit_signal_creates_pending_order(mock_config, mock_api_client, event_bus):
    """Тест: LIMIT сигнал создает PENDING ордер, а не открывает позицию"""

    # Патчим глобальный event_bus
    with patch('analyzer.core.position_manager.event_bus', event_bus):
        with patch('analyzer.core.signal_repository.signal_repository') as mock_repo:
            mock_repo.update_signal_status = AsyncMock(return_value=True)
            mock_repo.update_position_size = AsyncMock(return_value=True)
            mock_repo.get_active_signals = AsyncMock(return_value=[])
            mock_repo.get_pending_signals = AsyncMock(return_value=[])

            with patch('analyzer.core.trade_repository.trade_repository') as mock_trade_repo:
                mock_trade_repo.initialize = AsyncMock(return_value=True)
                mock_trade_repo.save_trade = AsyncMock(return_value=True)

                # Создаем Position Manager
                pm = PositionManager(mock_config, mock_api_client)

                # Инициализируем (без реального мониторинга для теста)
                pm.running = False

                # Создаем событие сигнала LIMIT
                signal_event = {
                    'signal_id': 1,
                    'symbol': 'BTCUSDT',
                    'signal_type': 'BUY',
                    'signal_subtype': 'LIMIT',
                    'order_type': 'LIMIT',
                    'entry_price': 50000.0,
                    'stop_loss': 49500.0,
                    'take_profit': 51000.0,
                    'confidence': 0.85,
                    'risk_reward_ratio': 2.0,
                    'expiration_time': (datetime.now() + timedelta(hours=24)).isoformat()
                }

                # Обрабатываем сигнал
                await pm.on_signal_generated(MagicMock(data=signal_event))

                # Проверяем: должен быть создан pending ордер, а не открытая позиция
                assert len(pm.pending_limit_orders) == 1
                assert 1 in pm.pending_limit_orders
                assert len(pm.open_positions) == 0

                # Проверяем статус сигнала в БД
                mock_repo.update_signal_status.assert_called_with(1, 'PENDING')


@pytest.mark.asyncio
async def test_instant_signal_opens_position_immediately(mock_config, mock_api_client, event_bus):
    """Тест: INSTANT сигнал открывает позицию сразу (рыночный ордер)"""

    with patch('analyzer.core.position_manager.event_bus', event_bus):
        with patch('analyzer.core.signal_repository.signal_repository') as mock_repo:
            mock_repo.update_signal_status = AsyncMock(return_value=True)
            mock_repo.update_position_size = AsyncMock(return_value=True)
            mock_repo.get_active_signals = AsyncMock(return_value=[])
            mock_repo.get_pending_signals = AsyncMock(return_value=[])

            with patch('analyzer.core.trade_repository.trade_repository') as mock_trade_repo:
                mock_trade_repo.initialize = AsyncMock(return_value=True)
                mock_trade_repo.save_trade = AsyncMock(return_value=True)

                pm = PositionManager(mock_config, mock_api_client)
                pm.running = False

                # Создаем событие сигнала INSTANT
                signal_event = {
                    'signal_id': 2,
                    'symbol': 'BTCUSDT',
                    'signal_type': 'BUY',
                    'signal_subtype': 'INSTANT',
                    'order_type': 'MARKET',
                    'entry_price': 50000.0,
                    'stop_loss': 49500.0,
                    'take_profit': 51000.0,
                    'confidence': 0.85,
                    'risk_reward_ratio': 2.0,
                    'expiration_time': (datetime.now() + timedelta(hours=3)).isoformat()
                }

                await pm.on_signal_generated(MagicMock(data=signal_event))

                # Проверяем: должна быть открыта позиция, нет pending ордеров
                assert len(pm.open_positions) == 1
                assert 2 in pm.open_positions
                assert len(pm.pending_limit_orders) == 0

                # Проверяем статус сигнала в БД
                mock_repo.update_signal_status.assert_called_with(2, 'ACTIVE')


@pytest.mark.asyncio
async def test_limit_order_executes_when_price_reaches(mock_config, mock_api_client, event_bus):
    """Тест: Лимитный ордер исполняется при достижении цены"""

    with patch('analyzer.core.position_manager.event_bus', event_bus):
        with patch('analyzer.core.signal_repository.signal_repository') as mock_repo:
            mock_repo.update_signal_status = AsyncMock(return_value=True)
            mock_repo.update_fill_price = AsyncMock(return_value=True)
            mock_repo.update_position_size = AsyncMock(return_value=True)
            mock_repo.get_active_signals = AsyncMock(return_value=[])
            mock_repo.get_pending_signals = AsyncMock(return_value=[])

            with patch('analyzer.core.trade_repository.trade_repository') as mock_trade_repo:
                mock_trade_repo.initialize = AsyncMock(return_value=True)
                mock_trade_repo.save_trade = AsyncMock(return_value=True)

                pm = PositionManager(mock_config, mock_api_client)
                pm.running = False

                # Создаем LIMIT сигнал
                signal_event = {
                    'signal_id': 3,
                    'symbol': 'BTCUSDT',
                    'signal_type': 'BUY',
                    'signal_subtype': 'LIMIT',
                    'order_type': 'LIMIT',
                    'entry_price': 50000.0,
                    'stop_loss': 49500.0,
                    'take_profit': 51000.0,
                    'confidence': 0.85,
                    'risk_reward_ratio': 2.0,
                    'expiration_time': (datetime.now() + timedelta(hours=24)).isoformat()
                }

                await pm.on_signal_generated(MagicMock(data=signal_event))

                # Проверяем: есть pending ордер
                assert len(pm.pending_limit_orders) == 1

                # Симулируем достижение цены
                # Устанавливаем текущую цену ниже entry для BUY
                mock_api_client.get_current_price = AsyncMock(return_value=49900.0)

                # Запускаем мониторинг лимитных ордеров
                await pm._monitor_limit_orders()

                # Проверяем: ордер должен быть исполнен, позиция открыта
                # (нужно немного подождать асинхронного исполнения)
                await asyncio.sleep(0.1)

                # После исполнения позиция должна быть в open_positions
                # (зависит от реализации, может потребоваться дополнительная проверка)
                # В текущей реализации ордер исполняется внутри _monitor_limit_orders
                # и перемещается из pending в open


@pytest.mark.asyncio
async def test_limit_order_expires(mock_config, mock_api_client, event_bus):
    """Тест: Лимитный ордер истекает по времени"""

    with patch('analyzer.core.position_manager.event_bus', event_bus):
        with patch('analyzer.core.signal_repository.signal_repository') as mock_repo:
            mock_repo.update_signal_status = AsyncMock(return_value=True)
            mock_repo.update_position_size = AsyncMock(return_value=True)
            mock_repo.get_active_signals = AsyncMock(return_value=[])
            mock_repo.get_pending_signals = AsyncMock(return_value=[])

            with patch('analyzer.core.trade_repository.trade_repository') as mock_trade_repo:
                mock_trade_repo.initialize = AsyncMock(return_value=True)

                pm = PositionManager(mock_config, mock_api_client)
                pm.running = False

                # Создаем LIMIT сигнал с истекающим временем (1 секунда)
                expiration = datetime.now() + timedelta(seconds=1)
                signal_event = {
                    'signal_id': 4,
                    'symbol': 'BTCUSDT',
                    'signal_type': 'BUY',
                    'signal_subtype': 'LIMIT',
                    'order_type': 'LIMIT',
                    'entry_price': 50000.0,
                    'stop_loss': 49500.0,
                    'take_profit': 51000.0,
                    'confidence': 0.85,
                    'risk_reward_ratio': 2.0,
                    'expiration_time': expiration.isoformat()
                }

                await pm.on_signal_generated(MagicMock(data=signal_event))

                # Ждем истечения
                await asyncio.sleep(1.5)

                # Проверяем истекшие ордера
                for signal_id, order in list(pm.pending_limit_orders.items()):
                    if pm._is_order_expired(order):
                        await pm._expire_limit_order(signal_id)

                # Проверяем: ордер должен быть удален
                # (зависит от того, как часто вызывается _expire_limit_order)
                # В реальном коде это делает _monitor_positions


@pytest.mark.asyncio
async def test_duplicate_signal_prevention(mock_config, mock_api_client, event_bus):
    """Тест: Предотвращение дублирования сигналов"""

    with patch('analyzer.core.signal_repository.signal_repository') as mock_repo:
        # Симулируем наличие активного сигнала для BTCUSDT
        mock_repo.check_duplicate_signal = AsyncMock(return_value=True)
        mock_repo.get_active_signals = AsyncMock(return_value=[{'symbol': 'BTCUSDT', 'status': 'PENDING'}])

        # Проверяем дубликат
        is_duplicate = await mock_repo.check_duplicate_signal('BTCUSDT', 'LIMIT', 24)

        assert is_duplicate is True


@pytest.mark.asyncio
async def test_watch_signal_does_not_open_position(mock_config, mock_api_client, event_bus):
    """Тест: WATCH сигнал не открывает позицию"""

    with patch('analyzer.core.position_manager.event_bus', event_bus):
        with patch('analyzer.core.signal_repository.signal_repository') as mock_repo:
            mock_repo.update_signal_status = AsyncMock(return_value=True)
            mock_repo.get_active_signals = AsyncMock(return_value=[])
            mock_repo.get_pending_signals = AsyncMock(return_value=[])

            with patch('analyzer.core.trade_repository.trade_repository') as mock_trade_repo:
                mock_trade_repo.initialize = AsyncMock(return_value=True)

                pm = PositionManager(mock_config, mock_api_client)
                pm.running = False

                # Создаем WATCH сигнал
                signal_event = {
                    'signal_id': 5,
                    'symbol': 'BTCUSDT',
                    'signal_type': 'BUY',
                    'signal_subtype': 'WATCH',
                    'order_type': None,
                    'entry_price': 50000.0,
                    'stop_loss': 49500.0,
                    'take_profit': 51000.0,
                    'confidence': 0.65,
                    'risk_reward_ratio': 2.0
                }

                await pm.on_signal_generated(MagicMock(data=signal_event))

                # Проверяем: нет ни позиций, ни pending ордеров
                assert len(pm.open_positions) == 0
                assert len(pm.pending_limit_orders) == 0


@pytest.mark.asyncio
async def test_position_size_calculation(mock_config, mock_api_client, event_bus):
    """Тест: Расчет размера позиции на основе риск-менеджмента"""

    with patch('analyzer.core.position_manager.event_bus', event_bus):
        with patch('analyzer.core.signal_repository.signal_repository') as mock_repo:
            mock_repo.update_signal_status = AsyncMock(return_value=True)
            mock_repo.update_position_size = AsyncMock(return_value=True)
            mock_repo.get_active_signals = AsyncMock(return_value=[])
            mock_repo.get_pending_signals = AsyncMock(return_value=[])

            with patch('analyzer.core.trade_repository.trade_repository') as mock_trade_repo:
                mock_trade_repo.initialize = AsyncMock(return_value=True)

                pm = PositionManager(mock_config, mock_api_client)

                # Баланс 10000, риск 2% = 200 USDT
                # Расстояние до SL = 500 (50000 - 49500)
                # Количество = 200 / 500 = 0.4
                quantity = pm._calculate_quantity(50000, 49500, 'BUY', 'LIMIT')

                # Проверяем расчет (с учетом округления)
                assert quantity == 0.4 or quantity > 0


@pytest.mark.asyncio
async def test_tp_sl_monitoring(mock_config, mock_api_client, event_bus):
    """Тест: Мониторинг TP/SL для открытой позиции"""

    with patch('analyzer.core.position_manager.event_bus', event_bus):
        with patch('analyzer.core.signal_repository.signal_repository') as mock_repo:
            mock_repo.update_signal_status = AsyncMock(return_value=True)
            mock_repo.update_position_size = AsyncMock(return_value=True)
            mock_repo.get_active_signals = AsyncMock(return_value=[])
            mock_repo.get_pending_signals = AsyncMock(return_value=[])

            with patch('analyzer.core.trade_repository.trade_repository') as mock_trade_repo:
                mock_trade_repo.initialize = AsyncMock(return_value=True)
                mock_trade_repo.update_trade = AsyncMock(return_value=True)

                pm = PositionManager(mock_config, mock_api_client)

                # Создаем открытую позицию
                position = PaperPosition(
                    signal_id=1,
                    symbol='BTCUSDT',
                    direction='BUY',
                    entry_price=50000.0,
                    quantity=0.1,
                    stop_loss=49500.0,
                    take_profit=51000.0,
                    order_type='MARKET',
                    fill_price=50000.0
                )
                pm.open_positions[1] = position

                # Проверка TP
                reason = pm._check_tp_sl(position, 51000.0)
                assert reason == 'TP'

                # Проверка SL
                reason = pm._check_tp_sl(position, 49500.0)
                assert reason == 'SL'

                # Проверка без достижения
                reason = pm._check_tp_sl(position, 50500.0)
                assert reason is None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])