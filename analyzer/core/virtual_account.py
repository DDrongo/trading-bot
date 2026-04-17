# analyzer/core/virtual_account.py (ПОЛНОСТЬЮ)
"""
💰 VIRTUAL ACCOUNT — Виртуальный счёт для бэктестера
ФАЗА 1.5.1:
- Симуляция открытия/закрытия позиций
- Расчёт PnL с учётом комиссий
- Статистика сделок
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

logger = logging.getLogger('virtual_account')


class PositionStatus(Enum):
    """Статус позиции"""
    OPEN = "OPEN"
    CLOSED = "CLOSED"


@dataclass
class VirtualPosition:
    """Виртуальная позиция"""
    id: int
    symbol: str
    direction: str  # BUY / SELL
    entry_price: float
    quantity: float
    stop_loss: float
    take_profit: float
    leverage: float = 10.0
    margin: float = 0.0
    position_value: float = 0.0
    opened_at: datetime = field(default_factory=datetime.now)
    closed_at: Optional[datetime] = None
    close_price: Optional[float] = None
    pnl: float = 0.0
    pnl_percent: float = 0.0
    close_reason: str = ""
    status: PositionStatus = PositionStatus.OPEN

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'symbol': self.symbol,
            'direction': self.direction,
            'entry_price': self.entry_price,
            'quantity': self.quantity,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'leverage': self.leverage,
            'margin': self.margin,
            'position_value': self.position_value,
            'opened_at': self.opened_at.isoformat() if self.opened_at else None,
            'closed_at': self.closed_at.isoformat() if self.closed_at else None,
            'close_price': self.close_price,
            'pnl': self.pnl,
            'pnl_percent': self.pnl_percent,
            'close_reason': self.close_reason,
            'status': self.status.value
        }


@dataclass
class VirtualTrade:
    """Закрытая сделка"""
    position_id: int
    symbol: str
    direction: str
    entry_price: float
    exit_price: float
    quantity: float
    pnl: float
    pnl_percent: float
    close_reason: str
    opened_at: datetime
    closed_at: datetime
    commission: float = 0.0


class VirtualAccount:
    """
    Виртуальный счёт для бэктестинга
    """

    def __init__(self, config: Dict = None):
        self.config = config or {}

        # Настройки
        paper_config = self.config.get('paper_trading', {})
        self.initial_balance = paper_config.get('starting_virtual_balance', 10000.0)
        self.commission_rate = paper_config.get('commission_rate', 0.001)  # 0.1%
        self.default_leverage = paper_config.get('leverage', 10)

        # Состояние счёта
        self.balance = self.initial_balance
        self.used_margin = 0.0
        self.realized_pnl = 0.0

        # Позиции и сделки
        self.positions: Dict[int, VirtualPosition] = {}
        self.closed_trades: List[VirtualTrade] = []
        self._next_position_id = 1

        # История баланса
        self.balance_history: List[Dict] = []
        self.equity_history: List[Dict] = []

        # Текущая цена (обновляется из бэктестера)
        self._current_prices: Dict[str, float] = {}

        logger.info(f"✅ VirtualAccount создан, баланс: {self.balance:.2f} USDT")
        logger.info(f"   Комиссия: {self.commission_rate * 100:.2f}%")
        logger.info(f"   Плечо: {self.default_leverage}x")

    def update_price(self, symbol: str, price: float) -> None:
        """Обновление текущей цены символа"""
        self._current_prices[symbol] = price

    def get_current_price(self, symbol: str) -> float:
        """Получение текущей цены"""
        return self._current_prices.get(symbol, 0.0)

    def calculate_margin(self, position_value: float, leverage: float) -> float:
        """Расчёт маржи"""
        if leverage <= 0:
            leverage = self.default_leverage
        return position_value / leverage

    def open_position(
            self,
            symbol: str,
            direction: str,
            entry_price: float,
            stop_loss: float,
            take_profit: float,
            quantity: float,
            leverage: float = None,
            opened_at: datetime = None
    ) -> Optional[VirtualPosition]:
        """
        Открытие виртуальной позиции
        """
        if quantity <= 0 or entry_price <= 0:
            logger.error(f"❌ Некорректные параметры: quantity={quantity}, entry_price={entry_price}")
            return None

        if leverage is None:
            leverage = self.default_leverage

        # Расчёт маржи
        position_value = quantity * entry_price
        margin = self.calculate_margin(position_value, leverage)

        # Комиссия за открытие
        commission = position_value * self.commission_rate
        total_required = margin + commission

        # Проверка баланса
        available = self.balance - self.used_margin
        if total_required > available:
            logger.warning(f"⚠️ Недостаточно средств: нужно {total_required:.2f}, доступно {available:.2f}")
            return None

        # Списываем средства
        self.balance -= total_required
        self.used_margin += margin

        # Создаём позицию
        position = VirtualPosition(
            id=self._next_position_id,
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            quantity=quantity,
            stop_loss=stop_loss,
            take_profit=take_profit,
            leverage=leverage,
            margin=margin,
            position_value=position_value,
            opened_at=opened_at or datetime.now(),
            status=PositionStatus.OPEN
        )

        self.positions[self._next_position_id] = position
        self._next_position_id += 1

        logger.info(f"✅ Открыта позиция #{position.id}: {symbol} {direction} {quantity:.4f} @ {entry_price:.6f}")
        logger.info(f"   Маржа: {margin:.2f} USDT, Баланс: {self.balance:.2f}")

        return position

    def close_position(
            self,
            position_id: int,
            close_price: float,
            close_reason: str,
            closed_at: datetime = None
    ) -> Optional[VirtualTrade]:
        """
        Закрытие позиции
        """
        position = self.positions.get(position_id)
        if not position:
            logger.warning(f"⚠️ Позиция #{position_id} не найдена")
            return None

        if position.status == PositionStatus.CLOSED:
            logger.warning(f"⚠️ Позиция #{position_id} уже закрыта")
            return None

        # Расчёт PnL
        if position.direction == 'BUY':
            pnl = (close_price - position.entry_price) * position.quantity
        else:  # SELL
            pnl = (position.entry_price - close_price) * position.quantity

        # Комиссия за закрытие
        close_value = position.quantity * close_price
        commission = close_value * self.commission_rate
        pnl -= commission

        pnl_percent = (pnl / position.margin) * 100 if position.margin > 0 else 0

        # Возвращаем маржу и добавляем PnL
        self.balance += position.margin + pnl
        self.used_margin -= position.margin
        self.realized_pnl += pnl

        # Обновляем позицию
        position.status = PositionStatus.CLOSED
        position.closed_at = closed_at or datetime.now()
        position.close_price = close_price
        position.pnl = pnl
        position.pnl_percent = pnl_percent
        position.close_reason = close_reason

        # Создаём запись о сделке
        trade = VirtualTrade(
            position_id=position.id,
            symbol=position.symbol,
            direction=position.direction,
            entry_price=position.entry_price,
            exit_price=close_price,
            quantity=position.quantity,
            pnl=pnl,
            pnl_percent=pnl_percent,
            close_reason=close_reason,
            opened_at=position.opened_at,
            closed_at=position.closed_at,
            commission=commission
        )

        self.closed_trades.append(trade)

        logger.info(f"✅ Закрыта позиция #{position_id}: {close_reason}")
        logger.info(f"   PnL: {pnl:+.2f} USDT ({pnl_percent:+.2f}%)")
        logger.info(f"   Баланс: {self.balance:.2f} USDT")

        return trade

    def check_stop_loss_take_profit(self, symbol: str, current_price: float) -> List[int]:
        """
        Проверка SL/TP для открытых позиций

        Returns:
            Список ID позиций, которые нужно закрыть
        """
        to_close = []

        for pos_id, pos in self.positions.items():
            if pos.symbol != symbol:
                continue

            if pos.status != PositionStatus.OPEN:
                continue

            if pos.direction == 'BUY':
                if current_price <= pos.stop_loss:
                    to_close.append((pos_id, 'SL'))
                elif current_price >= pos.take_profit:
                    to_close.append((pos_id, 'TP'))
            else:  # SELL
                if current_price >= pos.stop_loss:
                    to_close.append((pos_id, 'SL'))
                elif current_price <= pos.take_profit:
                    to_close.append((pos_id, 'TP'))

        return to_close

    def get_open_positions(self) -> List[VirtualPosition]:
        """Получение открытых позиций"""
        return [p for p in self.positions.values() if p.status == PositionStatus.OPEN]

    def get_position(self, position_id: int) -> Optional[VirtualPosition]:
        """Получение позиции по ID"""
        return self.positions.get(position_id)

    def get_unrealized_pnl(self) -> float:
        """Расчёт нереализованного PnL"""
        total = 0.0

        for pos in self.get_open_positions():
            current_price = self.get_current_price(pos.symbol)
            if current_price <= 0:
                continue

            if pos.direction == 'BUY':
                pnl = (current_price - pos.entry_price) * pos.quantity
            else:
                pnl = (pos.entry_price - current_price) * pos.quantity

            total += pnl

        return total

    def get_equity(self) -> float:
        """Расчёт эквити (баланс + нереализованный PnL)"""
        return self.balance + self.get_unrealized_pnl()

    def get_statistics(self) -> Dict[str, Any]:
        """Получение статистики счёта"""
        total_trades = len(self.closed_trades)
        winning_trades = sum(1 for t in self.closed_trades if t.pnl > 0)
        losing_trades = sum(1 for t in self.closed_trades if t.pnl < 0)
        breakeven_trades = total_trades - winning_trades - losing_trades

        total_pnl = sum(t.pnl for t in self.closed_trades)

        # Расчёт максимальной просадки
        if self.balance_history:
            peak = self.balance_history[0]['balance']
            max_drawdown = 0.0
            max_drawdown_pct = 0.0

            for entry in self.balance_history:
                balance = entry['balance']
                if balance > peak:
                    peak = balance
                drawdown = peak - balance
                drawdown_pct = (drawdown / peak) * 100 if peak > 0 else 0

                if drawdown > max_drawdown:
                    max_drawdown = drawdown
                if drawdown_pct > max_drawdown_pct:
                    max_drawdown_pct = drawdown_pct
        else:
            max_drawdown = 0.0
            max_drawdown_pct = 0.0

        # Расчёт Sharpe ratio (упрощённо)
        if len(self.closed_trades) > 1:
            returns = [t.pnl_percent for t in self.closed_trades]
            avg_return = sum(returns) / len(returns)
            std_return = (sum((r - avg_return) ** 2 for r in returns) / len(returns)) ** 0.5
            sharpe_ratio = avg_return / std_return if std_return > 0 else 0
        else:
            sharpe_ratio = 0

        return {
            'initial_balance': self.initial_balance,
            'current_balance': self.balance,
            'equity': self.get_equity(),
            'used_margin': self.used_margin,
            'available': self.balance - self.used_margin,
            'realized_pnl': self.realized_pnl,
            'unrealized_pnl': self.get_unrealized_pnl(),
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'breakeven_trades': breakeven_trades,
            'win_rate': (winning_trades / total_trades * 100) if total_trades > 0 else 0,
            'total_pnl': total_pnl,
            'avg_win': sum(
                t.pnl for t in self.closed_trades if t.pnl > 0) / winning_trades if winning_trades > 0 else 0,
            'avg_loss': sum(t.pnl for t in self.closed_trades if t.pnl < 0) / losing_trades if losing_trades > 0 else 0,
            'profit_factor': self._calculate_profit_factor(),
            'max_drawdown': max_drawdown,
            'max_drawdown_pct': max_drawdown_pct,
            'sharpe_ratio': sharpe_ratio,
            'open_positions': len(self.get_open_positions())
        }

    def _calculate_profit_factor(self) -> float:
        """Расчёт Profit Factor"""
        gross_profit = sum(t.pnl for t in self.closed_trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in self.closed_trades if t.pnl < 0))

        if gross_loss == 0:
            return gross_profit if gross_profit > 0 else 0

        return gross_profit / gross_loss

    def snapshot_balance(self, timestamp: datetime = None) -> None:
        """Сохранение снимка баланса"""
        self.balance_history.append({
            'timestamp': timestamp or datetime.now(),
            'balance': self.balance,
            'equity': self.get_equity(),
            'used_margin': self.used_margin
        })

    def reset(self) -> None:
        """Сброс счёта"""
        self.balance = self.initial_balance
        self.used_margin = 0.0
        self.realized_pnl = 0.0
        self.positions.clear()
        self.closed_trades.clear()
        self.balance_history.clear()
        self.equity_history.clear()
        self._next_position_id = 1
        self._current_prices.clear()

        logger.info(f"🔄 VirtualAccount сброшен, баланс: {self.balance:.2f}")

    def get_trades_report(self) -> str:
        """Формирование отчёта по сделкам"""
        stats = self.get_statistics()

        report = f"""
═══════════════════════════════════════════════════════════════
📊 ОТЧЁТ ПО СДЕЛКАМ (VirtualAccount)
═══════════════════════════════════════════════════════════════

💰 БАЛАНС
───────────────────────────────────────────────────────────────
  Начальный баланс:      {stats['initial_balance']:.2f} USDT
  Текущий баланс:        {stats['current_balance']:.2f} USDT
  Эквити:                {stats['equity']:.2f} USDT
  Использовано маржи:    {stats['used_margin']:.2f} USDT
  Доступно:              {stats['available']:.2f} USDT

📈 PnL
───────────────────────────────────────────────────────────────
  Реализованный PnL:     {stats['realized_pnl']:+.2f} USDT
  Нереализованный PnL:   {stats['unrealized_pnl']:+.2f} USDT
  Общий PnL:             {(stats['realized_pnl'] + stats['unrealized_pnl']):+.2f} USDT

📊 СТАТИСТИКА СДЕЛОК
───────────────────────────────────────────────────────────────
  Всего сделок:          {stats['total_trades']}
  Прибыльных:            {stats['winning_trades']}
  Убыточных:             {stats['losing_trades']}
  Безубыточных:          {stats['breakeven_trades']}
  Win Rate:              {stats['win_rate']:.1f}%

  Средний выигрыш:       {stats['avg_win']:.2f} USDT
  Средний проигрыш:      {stats['avg_loss']:.2f} USDT
  Profit Factor:         {stats['profit_factor']:.2f}

📉 РИСКИ
───────────────────────────────────────────────────────────────
  Макс. просадка:        {stats['max_drawdown']:.2f} USDT ({stats['max_drawdown_pct']:.1f}%)
  Sharpe Ratio:          {stats['sharpe_ratio']:.2f}

  Открытых позиций:      {stats['open_positions']}
═══════════════════════════════════════════════════════════════
"""
        return report


__all__ = [
    'VirtualPosition',
    'VirtualTrade',
    'VirtualAccount',
    'PositionStatus'
]