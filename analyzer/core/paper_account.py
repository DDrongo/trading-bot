# analyzer/core/paper_account.py (ПОЛНОСТЬЮ - СИНГЛТОН)

import logging
from typing import Dict, Optional, Any, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta

logger = logging.getLogger('paper_account')


@dataclass
class PaperPosition:
    signal_id: int
    symbol: str
    direction: str
    entry_price: float
    quantity: float
    stop_loss: float
    take_profit: float
    leverage: float = 10.0
    margin: float = 0.0
    position_value: float = 0.0
    order_type: str = "MARKET"
    fill_price: float = 0.0
    expiration_time: Optional[datetime] = None
    opened_at: datetime = field(default_factory=datetime.now)
    closed_at: Optional[datetime] = None
    close_price: Optional[float] = None
    pnl: float = 0.0
    close_reason: str = ""


@dataclass
class WatchReservation:
    signal_id: int
    symbol: str
    reserved_margin: float
    position_size: float
    entry_price: float
    leverage: float
    created_at: datetime = field(default_factory=datetime.now)
    expires_at: datetime = field(default_factory=lambda: datetime.now() + timedelta(hours=3))


class PaperAccount:
    """СИНГЛТОН - единый экземпляр для всего приложения"""

    _instance = None
    _initialized = False

    def __new__(cls, config: Dict[str, Any] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config: Dict[str, Any] = None):
        if self._initialized:
            return

        if config is None:
            config = {}

        self.config = config.get('paper_trading', {})
        self.position_config = config.get('position_management', {})

        self.balance = self.config.get('starting_virtual_balance', 10000.0)
        self.commission_rate = self.config.get('commission_rate', 0.001)
        self.slippage_pct = self.config.get('slippage_percentage', 0.001)
        self.default_leverage = self.config.get('leverage', 10)

        self.open_positions: Dict[int, PaperPosition] = {}
        self.closed_positions: List[Dict] = []
        self.watch_reservations: Dict[int, WatchReservation] = {}

        self._initialized = True

        logger.info(f"✅ PaperAccount (СИНГЛТОН) баланс: {self.balance:.2f} USDT")

    def _round_price(self, price: float) -> float:
        try:
            if price < 0.001:
                return round(price, 6)
            elif price < 0.01:
                return round(price, 5)
            elif price < 0.1:
                return round(price, 4)
            elif price < 1:
                return round(price, 3)
            elif price < 10:
                return round(price, 2)
            else:
                return round(price, 2)
        except:
            return round(price, 2)

    def _round_quantity(self, quantity: float) -> float:
        try:
            if quantity < 0.001:
                return round(quantity, 6)
            elif quantity < 0.01:
                return round(quantity, 5)
            elif quantity < 0.1:
                return round(quantity, 4)
            elif quantity < 1:
                return round(quantity, 3)
            elif quantity < 1000:
                return round(quantity, 2)
            else:
                return round(quantity, 0)
        except:
            return round(quantity, 2)

    def calculate_margin(self, position_size: float, entry_price: float, leverage: float) -> float:
        if leverage <= 0:
            leverage = self.default_leverage
        return (position_size * entry_price) / leverage

    def calculate_total_risk_pct(self) -> float:
        if self.balance <= 0:
            return 0.0
        total_risk = 0.0
        for pos in self.open_positions.values():
            risk_amount = abs(pos.entry_price - pos.stop_loss) * pos.quantity
            total_risk += (risk_amount / self.balance) * 100
        return total_risk

    async def reserve_for_watch(
            self,
            signal_id: int,
            symbol: str,
            position_size: float,
            entry_price: float,
            leverage: float = None,
            expiration_hours: int = 3
    ) -> Tuple[bool, float]:
        if leverage is None:
            leverage = self.default_leverage

        reserved_margin = self.calculate_margin(position_size, entry_price, leverage)

        used_margin = sum(p.margin for p in self.open_positions.values())
        total_reserved = sum(
            r.reserved_margin for r in self.watch_reservations.values() if r.expires_at > datetime.now())
        available = self.balance - used_margin - total_reserved

        if reserved_margin > available:
            logger.warning(
                f"⚠️ Недостаточно средств для WATCH #{signal_id}: нужно {reserved_margin:.2f}, доступно {available:.2f}")
            return False, 0.0

        self.watch_reservations[signal_id] = WatchReservation(
            signal_id=signal_id,
            symbol=symbol,
            reserved_margin=reserved_margin,
            position_size=position_size,
            entry_price=entry_price,
            leverage=leverage,
            expires_at=datetime.now() + timedelta(hours=expiration_hours)
        )
        logger.info(f"🔒 WATCH #{signal_id}: зарезервировано {reserved_margin:.2f} USDT")
        return True, reserved_margin

    async def release_watch_reserve(self, signal_id: int) -> bool:
        if signal_id in self.watch_reservations:
            res = self.watch_reservations.pop(signal_id)
            logger.info(f"🔓 Освобождён WATCH #{signal_id}: {res.reserved_margin:.2f} USDT")
            return True
        return False

    async def open_position(
            self,
            signal_id: int,
            symbol: str,
            direction: str,
            entry_price: float,
            stop_loss: float,
            take_profit: float,
            quantity: float,
            leverage: float = None,
            expiration_time: Optional[datetime] = None,
            order_type: str = "MARKET"
    ) -> PaperPosition:
        if quantity <= 0 or entry_price <= 0:
            raise ValueError(f"Invalid params: quantity={quantity}, entry_price={entry_price}")

        if leverage is None:
            leverage = self.default_leverage

        entry_price = self._round_price(entry_price)
        quantity = self._round_quantity(quantity)

        slippage = entry_price * self.slippage_pct
        if direction == 'BUY':
            actual_entry = entry_price + slippage
        else:
            actual_entry = entry_price - slippage
        actual_entry = self._round_price(actual_entry)

        position_value = quantity * actual_entry
        margin = position_value / leverage
        commission = actual_entry * quantity * self.commission_rate
        total_required = margin + commission

        if total_required > self.balance:
            raise ValueError(f"Insufficient balance: need {total_required:.2f}, have {self.balance:.2f}")

        old_balance = self.balance
        self.balance -= total_required

        position = PaperPosition(
            signal_id=signal_id,
            symbol=symbol,
            direction=direction,
            entry_price=actual_entry,
            quantity=quantity,
            stop_loss=stop_loss,
            take_profit=take_profit,
            leverage=leverage,
            margin=margin,
            position_value=position_value,
            order_type="MARKET",
            fill_price=actual_entry,
            expiration_time=expiration_time
        )

        self.open_positions[signal_id] = position

        logger.info(f"⚡ MARKET #{signal_id}: {symbol} {direction} {quantity:.2f} @ {actual_entry:.6f}")
        logger.info(f"   Маржа: {margin:.2f}, Баланс: {old_balance:.2f} → {self.balance:.2f}")

        return position

    async def close_position(
            self,
            signal_id: int,
            close_price: float,
            pnl: float,
            close_reason: str
    ) -> Optional[Dict[str, Any]]:
        position = self.open_positions.pop(signal_id, None)
        if not position:
            return None

        slippage = close_price * self.slippage_pct
        if position.direction == 'BUY':
            actual_close = close_price - slippage
        else:
            actual_close = close_price + slippage
        actual_close = self._round_price(actual_close)

        commission = actual_close * position.quantity * self.commission_rate

        if position.direction == 'BUY':
            actual_pnl = (actual_close - position.entry_price) * position.quantity
        else:
            actual_pnl = (position.entry_price - actual_close) * position.quantity
        actual_pnl -= commission

        self.balance += position.margin + actual_pnl

        position.closed_at = datetime.now()
        position.close_price = actual_close
        position.pnl = actual_pnl
        position.close_reason = close_reason

        closed_info = {
            'signal_id': signal_id,
            'symbol': position.symbol,
            'direction': position.direction,
            'entry_price': position.entry_price,
            'close_price': actual_close,
            'quantity': position.quantity,
            'leverage': position.leverage,
            'margin': position.margin,
            'pnl': actual_pnl,
            'pnl_percent': (actual_pnl / position.margin * 100) if position.margin > 0 else 0,
            'close_reason': close_reason,
            'opened_at': position.opened_at,
            'closed_at': position.closed_at,
            'commission': commission,
            'order_type': position.order_type,
            'fill_price': position.fill_price
        }
        self.closed_positions.append(closed_info)

        logger.info(f"📉 ЗАКРЫТА #{signal_id}: {close_reason}, PnL: {actual_pnl:+.2f}")

        return closed_info

    async def get_balance(self) -> float:
        return self.balance

    async def get_open_positions(self) -> Dict[int, PaperPosition]:
        return self.open_positions

    async def get_available_balance(self) -> float:
        used_margin = sum(p.margin for p in self.open_positions.values())
        total_reserved = sum(
            r.reserved_margin for r in self.watch_reservations.values() if r.expires_at > datetime.now())
        return self.balance - used_margin - total_reserved

    async def get_statistics(self) -> Dict[str, Any]:
        total_trades = len(self.closed_positions)
        winning_trades = sum(1 for t in self.closed_positions if t['pnl'] > 0)
        losing_trades = sum(1 for t in self.closed_positions if t['pnl'] < 0)
        total_pnl = sum(t['pnl'] for t in self.closed_positions)

        return {
            'balance': self.balance,
            'available_balance': await self.get_available_balance(),
            'used_margin': sum(p.margin for p in self.open_positions.values()),
            'reserved_for_watch': sum(
                r.reserved_margin for r in self.watch_reservations.values() if r.expires_at > datetime.now()),
            'open_positions': len(self.open_positions),
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': (winning_trades / total_trades * 100) if total_trades > 0 else 0,
            'total_pnl': total_pnl,
            'total_risk_pct': self.calculate_total_risk_pct()
        }

    async def cleanup_expired_reservations(self) -> int:
        now = datetime.now()
        expired = [sid for sid, res in self.watch_reservations.items() if res.expires_at < now]
        for sid in expired:
            await self.release_watch_reserve(sid)
        if expired:
            logger.info(f"🧹 Очищено {len(expired)} истёкших WATCH")
        return len(expired)


__all__ = ['PaperAccount', 'PaperPosition', 'WatchReservation']