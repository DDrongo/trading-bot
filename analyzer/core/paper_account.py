# analyzer/core/paper_account.py
"""
📊 PAPER ACCOUNT - Виртуальный торговый счёт
Минимальная реализация для Фазы 1.3
"""

import logging
import asyncio
from typing import Dict, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import random

logger = logging.getLogger('paper_account')


@dataclass
class PaperPosition:
    """Виртуальная позиция"""
    signal_id: int
    symbol: str
    direction: str  # BUY/SELL
    entry_price: float
    quantity: float
    stop_loss: float
    take_profit: float
    expiration_time: Optional[datetime] = None
    opened_at: datetime = field(default_factory=datetime.now)
    closed_at: Optional[datetime] = None
    close_price: Optional[float] = None
    pnl: float = 0.0
    close_reason: str = ""


class PaperAccount:
    """
    Виртуальный торговый счёт для Paper Trading
    В Фазе 1.3 - минимальная реализация с базовым функционалом
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config.get('paper_trading', {})
        self.balance = self.config.get('starting_virtual_balance', 10000.0)
        self.commission_rate = self.config.get('commission_rate', 0.001)
        self.slippage_pct = self.config.get('slippage_percentage', 0.001)

        self.open_positions: Dict[int, PaperPosition] = {}
        self.closed_positions: list = []

        logger.info(f"✅ PaperAccount инициализирован. Баланс: {self.balance:.2f} USDT")
        logger.info(f"   Комиссия: {self.commission_rate * 100:.2f}%, Проскальзывание: {self.slippage_pct * 100:.2f}%")

    async def open_position(
            self,
            signal_id: int,
            symbol: str,
            direction: str,
            entry_price: float,
            stop_loss: float,
            take_profit: float,
            quantity: float,
            expiration_time: Optional[datetime] = None
    ) -> PaperPosition:
        """
        Открыть виртуальную позицию
        """
        try:
            # Симуляция проскальзывания при входе
            slippage = entry_price * self.slippage_pct
            if direction == 'BUY':
                actual_entry = entry_price + slippage
            else:
                actual_entry = entry_price - slippage

            # Расчёт комиссии
            commission = actual_entry * quantity * self.commission_rate

            # Проверка достаточности средств
            required_margin = actual_entry * quantity + commission
            if required_margin > self.balance:
                logger.warning(f"⚠️ Недостаточно средств для открытия позиции {signal_id}")
                logger.warning(f"   Нужно: {required_margin:.2f}, Есть: {self.balance:.2f}")
                raise ValueError(f"Insufficient balance: need {required_margin:.2f}, have {self.balance:.2f}")

            # Создаём позицию
            position = PaperPosition(
                signal_id=signal_id,
                symbol=symbol,
                direction=direction,
                entry_price=actual_entry,
                quantity=quantity,
                stop_loss=stop_loss,
                take_profit=take_profit,
                expiration_time=expiration_time
            )

            # Уменьшаем баланс
            self.balance -= (actual_entry * quantity + commission)

            # Сохраняем позицию
            self.open_positions[signal_id] = position

            logger.info(f"📈 ОТКРЫТА ПОЗИЦИЯ #{signal_id}")
            logger.info(f"   {symbol} {direction} @ {actual_entry:.4f}")
            logger.info(f"   Quantity: {quantity:.4f}, Комиссия: {commission:.2f}")
            logger.info(f"   SL: {stop_loss:.4f}, TP: {take_profit:.4f}")
            logger.info(f"   Новый баланс: {self.balance:.2f}")

            return position

        except Exception as e:
            logger.error(f"❌ Ошибка открытия позиции #{signal_id}: {e}")
            raise

    async def close_position(
            self,
            signal_id: int,
            close_price: float,
            pnl: float,
            close_reason: str
    ) -> Optional[Dict[str, Any]]:
        """
        Закрыть виртуальную позицию
        """
        try:
            position = self.open_positions.pop(signal_id, None)
            if not position:
                logger.warning(f"⚠️ Позиция #{signal_id} не найдена для закрытия")
                return None

            # Симуляция проскальзывания при выходе
            slippage = close_price * self.slippage_pct
            if position.direction == 'BUY':
                actual_close = close_price - slippage
            else:
                actual_close = close_price + slippage

            # Расчёт комиссии на закрытие
            commission = actual_close * position.quantity * self.commission_rate

            # Расчёт реального PnL с учётом проскальзывания
            if position.direction == 'BUY':
                actual_pnl = (actual_close - position.entry_price) * position.quantity
            else:
                actual_pnl = (position.entry_price - actual_close) * position.quantity

            actual_pnl -= commission

            # Обновляем баланс
            self.balance += (actual_close * position.quantity + actual_pnl)

            # Сохраняем информацию о закрытии
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
                'pnl': actual_pnl,
                'pnl_percent': (actual_pnl / (position.entry_price * position.quantity)) * 100,
                'close_reason': close_reason,
                'opened_at': position.opened_at,
                'closed_at': position.closed_at,
                'commission': commission
            }

            self.closed_positions.append(closed_info)

            logger.info(f"📉 ЗАКРЫТА ПОЗИЦИЯ #{signal_id}")
            logger.info(f"   {position.symbol} {position.direction}")
            logger.info(f"   Entry: {position.entry_price:.4f} → Close: {actual_close:.4f}")
            logger.info(f"   PnL: {actual_pnl:+.2f} ({closed_info['pnl_percent']:+.2f}%)")
            logger.info(f"   Причина: {close_reason}")
            logger.info(f"   Новый баланс: {self.balance:.2f}")

            return closed_info

        except Exception as e:
            logger.error(f"❌ Ошибка закрытия позиции #{signal_id}: {e}")
            return None

    async def get_position(self, signal_id: int) -> Optional[PaperPosition]:
        """Получить позицию по ID сигнала"""
        return self.open_positions.get(signal_id)

    async def get_open_positions(self) -> Dict[int, PaperPosition]:
        """Получить все открытые позиции"""
        return self.open_positions

    async def get_balance(self) -> float:
        """Получить текущий баланс"""
        return self.balance

    async def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику счёта"""
        total_trades = len(self.closed_positions)
        winning_trades = sum(1 for t in self.closed_positions if t['pnl'] > 0)
        losing_trades = sum(1 for t in self.closed_positions if t['pnl'] < 0)

        total_pnl = sum(t['pnl'] for t in self.closed_positions)

        return {
            'balance': self.balance,
            'open_positions': len(self.open_positions),
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': (winning_trades / total_trades * 100) if total_trades > 0 else 0,
            'total_pnl': total_pnl,
            'average_pnl': total_pnl / total_trades if total_trades > 0 else 0,
            'max_win': max([t['pnl'] for t in self.closed_positions]) if self.closed_positions else 0,
            'max_loss': min([t['pnl'] for t in self.closed_positions]) if self.closed_positions else 0
        }