# tests/test_position_manager.py
"""
Тесты для Position Manager (Фаза 1.3)
"""

import asyncio
import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta
from typing import Dict, Any

from analyzer.core.position_manager import PositionManager
from analyzer.core.paper_account import PaperAccount, PaperPosition
from analyzer.core.event_bus import EventType, event_bus
from analyzer.core.trade_repository import trade_repository
from analyzer.core.signal_repository import signal_repository


class TestPaperAccount:
    """Тесты Paper Account"""

    @pytest.fixture
    def paper_account(self):
        config = {'paper_trading': {'starting_virtual_balance': 10000.0, 'commission_rate': 0.001,
                                    'slippage_percentage': 0.001}}
        return PaperAccount(config)

    @pytest.mark.asyncio
    async def test_open_position(self, paper_account):
        """Тест открытия позиции"""
        position = await paper_account.open_position(
            signal_id=1,
            symbol='BTCUSDT',
            direction='BUY',
            entry_price=50000,
            stop_loss=49500,
            take_profit=51000,
            quantity=0.01,
            expiration_time=datetime.now() + timedelta(hours=24)
        )

        assert position.signal_id == 1
        assert position.symbol == 'BTCUSDT'
        assert position.direction == 'BUY'
        assert position.entry_price > 0
        assert position.quantity == 0.01

        assert len(paper_account.open_positions) == 1
        assert paper_account.balance < 10000

    @pytest.mark.asyncio
    async def test_close_position(self, paper_account):
        """Тест закрытия позиции"""
        await paper_account.open_position(
            signal_id=1,
            symbol='BTCUSDT',
            direction='BUY',
            entry_price=50000,
            stop_loss=49500,
            take_profit=51000,
            quantity=0.01,
            expiration_time=datetime.now() + timedelta(hours=24)
        )

        closed = await paper_account.close_position(1, 51000, 100, 'TP')

        assert closed is not None
        assert closed['pnl'] > 0
        assert len(paper_account.open_positions) == 0
        assert len(paper_account.closed_positions) == 1

    @pytest.mark.asyncio
    async def test_insufficient_balance(self, paper_account):
        """Тест недостатка средств"""
        with pytest.raises(ValueError):
            await paper_account.open_position(
                signal_id=1,
                symbol='BTCUSDT',
                direction='BUY',
                entry_price=50000,
                stop_loss=49500,
                take_profit=51000,
                quantity=10.0,
                expiration_time=datetime.now() + timedelta(hours=24)
            )


class TestPositionManager:
    """Тесты Position Manager"""

    @pytest.fixture
    def config(self):
        return {
            'position_management': {
                'enabled': True,
                'monitoring_interval_seconds': 1,
                'default_quantity': 0.001,
                'max_positions': 5,
                'max_quantity': 1.0,
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
    def api_client(self):
        client = Mock()
        client.get_current_price = AsyncMock(return_value=50000)
        return client

    @pytest.fixture
    def position_manager(self, config, api_client):
        """Синхронная фикстура для Position Manager"""
        pm = PositionManager(config, api_client)
        return pm

    @pytest.mark.asyncio
    async def test_watch_signal_no_position(self, position_manager):
        """Тест: WATCH сигналы не открывают позиции"""
        event_data = {
            'signal_id': 1,
            'symbol': 'BTCUSDT',
            'signal_type': 'BUY',
            'signal_subtype': 'WATCH',
            'entry_price': 50000,
            'stop_loss': 49500,
            'take_profit': 51000,
            'confidence': 0.8,
            'risk_reward_ratio': 3.0
        }

        await position_manager.on_signal_generated(
            Mock(event_type=EventType.TRADING_SIGNAL_GENERATED, data=event_data)
        )

        assert len(position_manager.open_positions) == 0

    @pytest.mark.asyncio
    async def test_limit_signal_opens_position(self, position_manager):
        """Тест: LIMIT сигналы открывают позиции"""
        # Инициализируем trade_repository
        await trade_repository.initialize()

        event_data = {
            'signal_id': 1,
            'symbol': 'BTCUSDT',
            'signal_type': 'BUY',
            'signal_subtype': 'LIMIT',
            'entry_price': 50000,
            'stop_loss': 49500,
            'take_profit': 51000,
            'confidence': 0.8,
            'risk_reward_ratio': 3.0,
            'expiration_time': (datetime.now() + timedelta(hours=24)).isoformat()
        }

        await position_manager.on_signal_generated(
            Mock(event_type=EventType.TRADING_SIGNAL_GENERATED, data=event_data)
        )

        assert len(position_manager.open_positions) == 1
        assert 1 in position_manager.open_positions

    @pytest.mark.asyncio
    async def test_instant_signal_opens_position(self, position_manager):
        """Тест: INSTANT сигналы открывают позиции"""
        await trade_repository.initialize()

        event_data = {
            'signal_id': 1,
            'symbol': 'BTCUSDT',
            'signal_type': 'BUY',
            'signal_subtype': 'INSTANT',
            'entry_price': 50000,
            'stop_loss': 49500,
            'take_profit': 51000,
            'confidence': 0.8,
            'risk_reward_ratio': 2.0,
            'expiration_time': (datetime.now() + timedelta(hours=3)).isoformat()
        }

        await position_manager.on_signal_generated(
            Mock(event_type=EventType.TRADING_SIGNAL_GENERATED, data=event_data)
        )

        assert len(position_manager.open_positions) == 1
        assert 1 in position_manager.open_positions

    @pytest.mark.asyncio
    async def test_max_positions_limit(self, position_manager):
        """Тест: лимит открытых позиций"""
        await trade_repository.initialize()
        position_manager.pos_config['max_positions'] = 1

        event_data_1 = {
            'signal_id': 1,
            'symbol': 'BTCUSDT',
            'signal_type': 'BUY',
            'signal_subtype': 'LIMIT',
            'entry_price': 50000,
            'stop_loss': 49500,
            'take_profit': 51000,
            'confidence': 0.8,
            'risk_reward_ratio': 3.0,
            'expiration_time': (datetime.now() + timedelta(hours=24)).isoformat()
        }

        await position_manager.on_signal_generated(
            Mock(event_type=EventType.TRADING_SIGNAL_GENERATED, data=event_data_1)
        )

        event_data_2 = {
            'signal_id': 2,
            'symbol': 'ETHUSDT',
            'signal_type': 'BUY',
            'signal_subtype': 'LIMIT',
            'entry_price': 3000,
            'stop_loss': 2970,
            'take_profit': 3060,
            'confidence': 0.8,
            'risk_reward_ratio': 3.0,
            'expiration_time': (datetime.now() + timedelta(hours=24)).isoformat()
        }

        await position_manager.on_signal_generated(
            Mock(event_type=EventType.TRADING_SIGNAL_GENERATED, data=event_data_2)
        )

        assert len(position_manager.open_positions) == 1
        assert 1 in position_manager.open_positions
        assert 2 not in position_manager.open_positions

    def test_check_tp_hit(self, position_manager):
        """Тест: достижение TP"""
        position = PaperPosition(
            signal_id=1,
            symbol='BTCUSDT',
            direction='BUY',
            entry_price=50000,
            quantity=0.01,
            stop_loss=49500,
            take_profit=51000
        )

        reason = position_manager._check_tp_sl(position, 51000)
        assert reason == 'TP'

    def test_check_sl_hit(self, position_manager):
        """Тест: достижение SL"""
        position = PaperPosition(
            signal_id=1,
            symbol='BTCUSDT',
            direction='BUY',
            entry_price=50000,
            quantity=0.01,
            stop_loss=49500,
            take_profit=51000
        )

        reason = position_manager._check_tp_sl(position, 49500)
        assert reason == 'SL'

    def test_check_expired(self, position_manager):
        """Тест: истечение времени"""
        expired_position = PaperPosition(
            signal_id=1,
            symbol='BTCUSDT',
            direction='BUY',
            entry_price=50000,
            quantity=0.01,
            stop_loss=49500,
            take_profit=51000,
            expiration_time=datetime.now() - timedelta(hours=1)
        )

        assert position_manager._is_expired(expired_position) is True

        active_position = PaperPosition(
            signal_id=2,
            symbol='BTCUSDT',
            direction='BUY',
            entry_price=50000,
            quantity=0.01,
            stop_loss=49500,
            take_profit=51000,
            expiration_time=datetime.now() + timedelta(hours=24)
        )

        assert position_manager._is_expired(active_position) is False

    def test_calculate_quantity(self, position_manager):
        """Тест: расчёт количества на основе риска"""
        position_manager.paper_account.balance = 10000

        quantity = position_manager._calculate_quantity(
            entry_price=50000,
            stop_loss=49500,
            direction='BUY',
            signal_subtype='LIMIT'
        )

        assert quantity == 0.01

    @pytest.mark.asyncio
    async def test_close_position_by_tp(self, position_manager):
        """Тест: закрытие по TP"""
        await trade_repository.initialize()

        event_data = {
            'signal_id': 1,
            'symbol': 'BTCUSDT',
            'signal_type': 'BUY',
            'signal_subtype': 'LIMIT',
            'entry_price': 50000,
            'stop_loss': 49500,
            'take_profit': 51000,
            'confidence': 0.8,
            'risk_reward_ratio': 3.0,
            'expiration_time': (datetime.now() + timedelta(hours=24)).isoformat()
        }

        await position_manager.on_signal_generated(
            Mock(event_type=EventType.TRADING_SIGNAL_GENERATED, data=event_data)
        )

        assert len(position_manager.open_positions) == 1

        await position_manager._close_position(1, 'TP', 51000)

        assert len(position_manager.open_positions) == 0


class TestTradeRepository:
    """Тесты Trade Repository"""

    @pytest.mark.asyncio
    async def test_save_trade(self):
        """Тест сохранения сделки"""
        await trade_repository.initialize()

        trade_data = {
            'signal_id': 1,
            'symbol': 'BTCUSDT',
            'direction': 'BUY',
            'entry_price': 50000,
            'quantity': 0.01,
            'stop_loss': 49500,
            'take_profit': 51000,
            'opened_at': datetime.now(),
            'status': 'OPEN'
        }

        trade_id = await trade_repository.save_trade(trade_data)

        assert trade_id is not None

    @pytest.mark.asyncio
    async def test_get_open_trades(self):
        """Тест получения открытых сделок"""
        await trade_repository.initialize()
        trades = await trade_repository.get_open_trades()
        assert isinstance(trades, list)


if __name__ == '__main__':
    pytest.main(['-v', __file__])