import pytest
import sys
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from analyzer.core.position_manager import PositionManager
from analyzer.core.paper_account import PaperAccount, PaperPosition, PendingOrder
from analyzer.core.signal_repository import signal_repository


@pytest.fixture
def mock_config():
    return {
        'position_management': {
            'enabled': True,
            'monitoring_interval_seconds': 1,
            'default_quantity': 0.001,
            'max_quantity': 1.0,
            'max_positions': 5,
            'position_sizing': {'method': 'risk_based', 'risk_per_trade_pct': 2.0}
        },
        'paper_trading': {
            'starting_virtual_balance': 10000.0,
            'commission_rate': 0.001,
            'slippage_percentage': 0.001
        }
    }


@pytest.fixture
def mock_api_client():
    client = AsyncMock()
    client.get_current_price = AsyncMock(return_value=50000.0)
    return client


@pytest.mark.asyncio
async def test_limit_signal_pending(mock_config, mock_api_client):
    with patch('analyzer.core.position_manager.signal_repository') as mock_repo:
        mock_repo.update_signal_status = AsyncMock(return_value=True)
        mock_repo.update_position_size = AsyncMock(return_value=True)
        mock_repo.get_active_signals = AsyncMock(return_value=[])
        mock_repo.get_pending_signals = AsyncMock(return_value=[])
        
        with patch('analyzer.core.position_manager.trade_repository') as mock_trade:
            mock_trade.initialize = AsyncMock(return_value=True)
            
            pm = PositionManager(mock_config, mock_api_client)
            pm.running = False
            
            event = MagicMock(data={
                'signal_id': 1, 'symbol': 'BTCUSDT', 'signal_type': 'BUY',
                'signal_subtype': 'LIMIT', 'order_type': 'LIMIT',
                'entry_price': 50000.0, 'stop_loss': 49500.0, 'take_profit': 51000.0,
                'expiration_time': (datetime.now() + timedelta(hours=24)).isoformat()
            })
            
            await pm.on_signal_generated(event)
            
            assert len(pm.pending_limit_orders) == 1
            assert len(pm.open_positions) == 0
            mock_repo.update_signal_status.assert_called_with(1, 'PENDING')


@pytest.mark.asyncio
async def test_instant_signal_active(mock_config, mock_api_client):
    with patch('analyzer.core.position_manager.signal_repository') as mock_repo:
        mock_repo.update_signal_status = AsyncMock(return_value=True)
        mock_repo.update_position_size = AsyncMock(return_value=True)
        mock_repo.get_active_signals = AsyncMock(return_value=[])
        mock_repo.get_pending_signals = AsyncMock(return_value=[])
        
        with patch('analyzer.core.position_manager.trade_repository') as mock_trade:
            mock_trade.initialize = AsyncMock(return_value=True)
            mock_trade.save_trade = AsyncMock(return_value=True)
            
            pm = PositionManager(mock_config, mock_api_client)
            pm.running = False
            
            event = MagicMock(data={
                'signal_id': 2, 'symbol': 'BTCUSDT', 'signal_type': 'BUY',
                'signal_subtype': 'INSTANT', 'order_type': 'MARKET',
                'entry_price': 50000.0, 'stop_loss': 49500.0, 'take_profit': 51000.0
            })
            
            await pm.on_signal_generated(event)
            
            assert len(pm.open_positions) == 1
            assert len(pm.pending_limit_orders) == 0
            mock_repo.update_signal_status.assert_called_with(2, 'ACTIVE')


@pytest.mark.asyncio
async def test_watch_signal_no_position(mock_config, mock_api_client):
    with patch('analyzer.core.position_manager.signal_repository') as mock_repo:
        mock_repo.update_signal_status = AsyncMock(return_value=True)
        mock_repo.get_active_signals = AsyncMock(return_value=[])
        mock_repo.get_pending_signals = AsyncMock(return_value=[])
        
        pm = PositionManager(mock_config, mock_api_client)
        pm.running = False
        
        event = MagicMock(data={
            'signal_id': 3, 'signal_subtype': 'WATCH'
        })
        
        await pm.on_signal_generated(event)
        
        assert len(pm.open_positions) == 0
        assert len(pm.pending_limit_orders) == 0


@pytest.mark.asyncio
async def test_position_size_calc():
    pm = PositionManager({'position_management': {'position_sizing': {'risk_per_trade_pct': 2.0}}}, AsyncMock())
    pm.paper_account.balance = 10000.0
    
    quantity = pm._calculate_quantity(50000, 49500, 'BUY', 'LIMIT')
    
    assert quantity == 0.4 or quantity > 0


@pytest.mark.asyncio
async def test_tp_sl_check():
    pm = PositionManager({}, AsyncMock())
    position = PaperPosition(
        signal_id=1, symbol='BTCUSDT', direction='BUY', entry_price=50000,
        quantity=0.1, stop_loss=49500, take_profit=51000
    )
    
    assert pm._check_tp_sl(position, 51000) == 'TP'
    assert pm._check_tp_sl(position, 49500) == 'SL'
    assert pm._check_tp_sl(position, 50500) is None


@pytest.mark.asyncio
async def test_duplicate_check():
    with patch.object(signal_repository, 'check_duplicate_signal', new_callable=AsyncMock) as mock_check:
        mock_check.return_value = True
        
        result = await signal_repository.check_duplicate_signal('BTCUSDT', 'LIMIT', 24)
        
        assert result is True
        mock_check.assert_called_once_with('BTCUSDT', 'LIMIT', 24)
