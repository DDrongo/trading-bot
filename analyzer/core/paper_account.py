# analyzer/core/paper_account.py
"""
📊 PAPER ACCOUNT - Виртуальный торговый счёт
Полная реализация с поддержкой LIMIT/MARKET ордеров (Фаза 1.3.1)
"""

import logging
import asyncio
from typing import Dict, Optional, Any, List
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
    order_type: str = "MARKET"  # LIMIT или MARKET
    fill_price: float = 0.0  # Цена исполнения (для LIMIT)
    expiration_time: Optional[datetime] = None
    opened_at: datetime = field(default_factory=datetime.now)
    closed_at: Optional[datetime] = None
    close_price: Optional[float] = None
    pnl: float = 0.0
    close_reason: str = ""


@dataclass
class PendingOrder:
    """Лимитный ордер, ожидающий исполнения"""
    signal_id: int
    symbol: str
    direction: str
    entry_price: float
    quantity: float
    stop_loss: float
    take_profit: float
    expiration_time: datetime
    created_at: datetime = field(default_factory=datetime.now)


class PaperAccount:
    """
    Виртуальный торговый счёт для Paper Trading
    Поддерживает:
    - MARKET ордера (INSTANT сигналы) → открываются сразу
    - LIMIT ордера (LIMIT сигналы) → ожидают достижения цены
    - Комиссии и проскальзывания
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config.get('paper_trading', {})
        self.balance = self.config.get('starting_virtual_balance', 10000.0)
        self.commission_rate = self.config.get('commission_rate', 0.001)
        self.slippage_pct = self.config.get('slippage_percentage', 0.001)

        self.open_positions: Dict[int, PaperPosition] = {}
        self.pending_orders: Dict[int, PendingOrder] = {}
        self.closed_positions: List[Dict] = []

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
            expiration_time: Optional[datetime] = None,
            order_type: str = "MARKET"
    ) -> PaperPosition:
        """
        Открыть позицию (MARKET) или создать лимитный ордер (LIMIT)
        """
        try:
            if order_type == "LIMIT":
                # Создаем лимитный ордер (pending)
                pending = PendingOrder(
                    signal_id=signal_id,
                    symbol=symbol,
                    direction=direction,
                    entry_price=entry_price,
                    quantity=quantity,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    expiration_time=expiration_time
                )
                self.pending_orders[signal_id] = pending

                # Резервируем средства под лимитный ордер
                required_margin = entry_price * quantity * (1 + self.commission_rate)

                logger.info(f"📊 LIMIT ОРДЕР #{signal_id} создан")
                logger.info(f"   {symbol} {direction} @ {entry_price:.6f}")
                logger.info(f"   Quantity: {quantity:.4f}, Резерв: {required_margin:.2f}")
                logger.info(
                    f"   Истекает: {expiration_time.strftime('%Y-%m-%d %H:%M') if expiration_time else 'никогда'}")

                # Возвращаем фиктивную позицию с pending статусом
                position = PaperPosition(
                    signal_id=signal_id,
                    symbol=symbol,
                    direction=direction,
                    entry_price=entry_price,
                    quantity=quantity,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    order_type="LIMIT",
                    fill_price=0.0,
                    expiration_time=expiration_time
                )
                return position

            else:
                # MARKET ордер - открываем сразу
                return await self._execute_market_order(
                    signal_id, symbol, direction, entry_price,
                    stop_loss, take_profit, quantity, expiration_time
                )

        except Exception as e:
            logger.error(f"❌ Ошибка открытия позиции/ордера #{signal_id}: {e}")
            raise

    async def _execute_market_order(
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
        """Исполнение рыночного ордера"""
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
            order_type="MARKET",
            fill_price=actual_entry,
            expiration_time=expiration_time
        )

        # Уменьшаем баланс
        self.balance -= (actual_entry * quantity + commission)

        # Сохраняем позицию
        self.open_positions[signal_id] = position

        logger.info(f"⚡ MARKET ОРДЕР #{signal_id} ИСПОЛНЕН")
        logger.info(f"   {symbol} {direction} @ {actual_entry:.6f} (запланировано: {entry_price:.6f})")
        logger.info(f"   Quantity: {quantity:.4f}, Комиссия: {commission:.2f}")
        logger.info(f"   SL: {stop_loss:.6f}, TP: {take_profit:.6f}")
        logger.info(f"   Новый баланс: {self.balance:.2f}")

        return position

    async def execute_limit_order(self, signal_id: int, current_price: float) -> Optional[PaperPosition]:
        """
        Исполнить лимитный ордер при достижении цены
        """
        try:
            pending = self.pending_orders.pop(signal_id, None)
            if not pending:
                logger.warning(f"⚠️ Лимитный ордер #{signal_id} не найден")
                return None

            # Проверяем достижение цены
            if pending.direction == 'BUY':
                if current_price > pending.entry_price:
                    logger.info(
                        f"⏳ LIMIT ордер #{signal_id}: цена {current_price:.6f} > {pending.entry_price:.6f}, ожидаем")
                    # Возвращаем обратно в pending
                    self.pending_orders[signal_id] = pending
                    return None
            else:  # SELL
                if current_price < pending.entry_price:
                    logger.info(
                        f"⏳ LIMIT ордер #{signal_id}: цена {current_price:.6f} < {pending.entry_price:.6f}, ожидаем")
                    self.pending_orders[signal_id] = pending
                    return None

            # Цена достигнута - исполняем
            fill_price = current_price

            # Симуляция проскальзывания при входе
            slippage = fill_price * self.slippage_pct
            if pending.direction == 'BUY':
                actual_entry = fill_price + slippage
            else:
                actual_entry = fill_price - slippage

            # Расчёт комиссии
            commission = actual_entry * pending.quantity * self.commission_rate

            # Проверка достаточности средств
            required_margin = actual_entry * pending.quantity + commission
            if required_margin > self.balance:
                logger.warning(f"⚠️ Недостаточно средств для исполнения лимитного ордера {signal_id}")
                return None

            # Создаём позицию
            position = PaperPosition(
                signal_id=pending.signal_id,
                symbol=pending.symbol,
                direction=pending.direction,
                entry_price=actual_entry,
                quantity=pending.quantity,
                stop_loss=pending.stop_loss,
                take_profit=pending.take_profit,
                order_type="LIMIT",
                fill_price=actual_entry,
                expiration_time=pending.expiration_time
            )

            # Уменьшаем баланс
            self.balance -= (actual_entry * pending.quantity + commission)

            # Сохраняем позицию
            self.open_positions[signal_id] = position

            logger.info(f"✅ LIMIT ОРДЕР #{signal_id} ИСПОЛНЕН")
            logger.info(
                f"   {pending.symbol} {pending.direction} @ {actual_entry:.6f} (лимит: {pending.entry_price:.6f})")
            logger.info(f"   Quantity: {pending.quantity:.4f}, Комиссия: {commission:.2f}")
            logger.info(f"   Новый баланс: {self.balance:.2f}")

            return position

        except Exception as e:
            logger.error(f"❌ Ошибка исполнения лимитного ордера #{signal_id}: {e}")
            return None

    async def expire_limit_order(self, signal_id: int) -> bool:
        """
        Отменить истекший лимитный ордер
        """
        try:
            pending = self.pending_orders.pop(signal_id, None)
            if pending:
                logger.info(f"⏰ Лимитный ордер #{signal_id} истек и отменен")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка отмены лимитного ордера #{signal_id}: {e}")
            return False

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
                'pnl_percent': (actual_pnl / (
                            position.entry_price * position.quantity)) * 100 if position.entry_price * position.quantity > 0 else 0,
                'close_reason': close_reason,
                'opened_at': position.opened_at,
                'closed_at': position.closed_at,
                'commission': commission,
                'order_type': position.order_type,
                'fill_price': position.fill_price
            }

            self.closed_positions.append(closed_info)

            logger.info(f"📉 ЗАКРЫТА ПОЗИЦИЯ #{signal_id}")
            logger.info(f"   {position.symbol} {position.direction} ({position.order_type})")
            logger.info(f"   Entry: {position.entry_price:.6f} → Close: {actual_close:.6f}")
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

    async def get_pending_order(self, signal_id: int) -> Optional[PendingOrder]:
        """Получить ожидающий лимитный ордер"""
        return self.pending_orders.get(signal_id)

    async def get_open_positions(self) -> Dict[int, PaperPosition]:
        """Получить все открытые позиции"""
        return self.open_positions

    async def get_pending_orders(self) -> Dict[int, PendingOrder]:
        """Получить все ожидающие лимитные ордера"""
        return self.pending_orders

    async def get_balance(self) -> float:
        """Получить текущий баланс"""
        return self.balance

    async def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику счёта"""
        total_trades = len(self.closed_positions)
        winning_trades = sum(1 for t in self.closed_positions if t['pnl'] > 0)
        losing_trades = sum(1 for t in self.closed_positions if t['pnl'] < 0)

        total_pnl = sum(t['pnl'] for t in self.closed_positions)

        # Статистика по типам ордеров
        limit_trades = sum(1 for t in self.closed_positions if t.get('order_type') == 'LIMIT')
        market_trades = sum(1 for t in self.closed_positions if t.get('order_type') == 'MARKET')
        limit_pnl = sum(t['pnl'] for t in self.closed_positions if t.get('order_type') == 'LIMIT')
        market_pnl = sum(t['pnl'] for t in self.closed_positions if t.get('order_type') == 'MARKET')

        return {
            'balance': self.balance,
            'open_positions': len(self.open_positions),
            'pending_orders': len(self.pending_orders),
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': (winning_trades / total_trades * 100) if total_trades > 0 else 0,
            'total_pnl': total_pnl,
            'average_pnl': total_pnl / total_trades if total_trades > 0 else 0,
            'max_win': max([t['pnl'] for t in self.closed_positions]) if self.closed_positions else 0,
            'max_loss': min([t['pnl'] for t in self.closed_positions]) if self.closed_positions else 0,
            'limit_trades': limit_trades,
            'limit_pnl': limit_pnl,
            'market_trades': market_trades,
            'market_pnl': market_pnl
        }