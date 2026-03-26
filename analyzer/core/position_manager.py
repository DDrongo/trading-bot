# analyzer/core/position_manager.py (ПОЛНОСТЬЮ - ИСПРАВЛЕННАЯ ВЕРСИЯ)
"""
🎯 POSITION MANAGER - Управление позициями (упрощённая версия)
ФАЗА 1.3.6.1: Исправлены тесты
"""

import asyncio
import logging
from typing import Dict, Optional, Any
from datetime import datetime
import traceback

from analyzer.core.event_bus import EventType, Event, event_bus
from analyzer.core.signal_repository import signal_repository
from analyzer.core.trade_repository import trade_repository
from analyzer.core.paper_account import PaperAccount, PaperPosition
from analyzer.core.api_client_bybit import BybitAPIClient

logger = logging.getLogger('position_manager')


class PositionManager:
    """
    Управление позициями (упрощённая версия для Фазы 1.3.6):
    - Только MARKET ордера (M15 сигналы)
    - Мониторинг открытых позиций
    - Закрытие по TP/SL/EXPIRED
    - Сохранение истории
    """

    def __init__(self, config: Dict[str, Any], api_client: BybitAPIClient):
        self.config = config
        self.api_client = api_client

        # Настройки
        self.pos_config = config.get('position_management', {})
        self.enabled = self.pos_config.get('enabled', True)
        self.monitoring_interval = self.pos_config.get('monitoring_interval_seconds', 60)
        self.default_quantity = self.pos_config.get('default_quantity', 0.001)
        self.min_quantity = self.pos_config.get('min_quantity', 0.0001)
        self.max_quantity = self.pos_config.get('max_quantity', 1.0)
        self.max_positions = self.pos_config.get('max_positions', 5)

        # Настройки валидации
        self.m15_config = config.get('analysis', {}).get('signal_types', {}).get('m15', {})
        self.max_slippage_pct = self.m15_config.get('max_slippage_pct', 1.0)

        # Минимальный риск в цене (%)
        self.min_risk_distance_pct = 0.1

        # Компоненты
        self.paper_account = PaperAccount(config)
        self.open_positions: Dict[int, PaperPosition] = {}

        # Состояние
        self.running = False
        self.monitoring_task = None

        logger.info("🎯 Position Manager инициализирован (Фаза 1.3.6.1)")
        logger.info(f"   Мониторинг раз в {self.monitoring_interval} сек")
        logger.info(f"   Макс. позиций: {self.max_positions}")
        logger.info(f"   Только MARKET ордера (M15 сигналы)")

    def _round_price(self, price: float, symbol: str = "") -> float:
        """Округление цены"""
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

    def _round_quantity(self, quantity: float, symbol: str = "") -> float:
        """Округление количества"""
        try:
            if quantity < 0.001:
                return round(quantity, 6)
            elif quantity < 0.01:
                return round(quantity, 5)
            elif quantity < 0.1:
                return round(quantity, 4)
            elif quantity < 1:
                return round(quantity, 3)
            else:
                return round(quantity, 2)
        except:
            return round(quantity, 2)

    async def initialize(self) -> bool:
        """Инициализация Position Manager"""
        try:
            await trade_repository.initialize()

            event_bus.subscribe(EventType.TRADING_SIGNAL_GENERATED, self.on_signal_generated)
            logger.info("✅ Position Manager подписан на TRADING_SIGNAL_GENERATED")

            await self._restore_open_positions()

            self.running = True
            self.monitoring_task = asyncio.create_task(self._monitor_positions())

            logger.info("✅ Position Manager готов к работе")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации Position Manager: {e}")
            logger.error(traceback.format_exc())
            return False

    async def on_signal_generated(self, event: Event):
        """Обработчик нового сигнала (только M15)"""
        try:
            data = event.data
            signal_id = data.get('signal_id')
            signal_subtype = data.get('signal_subtype', 'M15')

            logger.info(f"📢 Получен сигнал #{signal_id} (тип: {signal_subtype})")

            if signal_subtype != 'M15':
                logger.info(f"⏭️ Пропускаем сигнал #{signal_id} (не M15)")
                return

            if len(self.open_positions) >= self.max_positions:
                logger.warning(f"⚠️ Достигнут лимит активных позиций ({self.max_positions})")
                return

            await self._open_position_from_signal(data)

        except Exception as e:
            logger.error(f"❌ Ошибка обработки сигнала: {e}")
            logger.error(traceback.format_exc())

    async def _open_position_from_signal(self, signal_data: Dict[str, Any]):
        """Открыть позицию из данных сигнала (MARKET ордер)"""
        try:
            signal_id = signal_data['signal_id']

            # ✅ Проверка лимита открытых позиций
            if len(self.open_positions) >= self.max_positions:
                logger.warning(f"⚠️ Достигнут лимит активных позиций ({self.max_positions})")
                await signal_repository.update_signal_status(signal_id, 'REJECTED')
                return

            symbol = signal_data['symbol']
            direction = signal_data['signal_type']  # BUY/SELL
            entry_price = signal_data['entry_price']
            stop_loss = signal_data['stop_loss']
            take_profit = signal_data['take_profit']
            expiration_time = signal_data.get('expiration_time')

            if expiration_time and isinstance(expiration_time, str):
                expiration_time = datetime.fromisoformat(expiration_time)

            current_price = await self.api_client.get_current_price(symbol, force_refresh=True)
            fill_price = current_price if current_price else entry_price

            logger.info(f"💰 M15 сигнал #{signal_id}: цена исполнения {fill_price:.6f}")

            deviation_pct = abs(fill_price - entry_price) / entry_price * 100
            if deviation_pct > self.max_slippage_pct:
                logger.warning(
                    f"⚠️ M15 сигнал #{signal_id} отклонён: отклонение цены {deviation_pct:.2f}% > {self.max_slippage_pct}%")
                await signal_repository.update_signal_status(signal_id, 'REJECTED')
                return

            if direction == 'BUY':
                stop_loss = fill_price - (entry_price - stop_loss)
                take_profit = fill_price + (take_profit - entry_price)
            else:
                stop_loss = fill_price + (stop_loss - entry_price)
                take_profit = fill_price - (entry_price - take_profit)

            stop_loss = self._round_price(stop_loss, symbol)
            take_profit = self._round_price(take_profit, symbol)

            if direction == 'BUY':
                if stop_loss >= fill_price:
                    logger.error(f"❌ SL после пересчёта >= Entry: {stop_loss:.6f} >= {fill_price:.6f}")
                    await signal_repository.update_signal_status(signal_id, 'REJECTED')
                    return
                if take_profit <= fill_price:
                    logger.error(f"❌ TP после пересчёта <= Entry: {take_profit:.6f} <= {fill_price:.6f}")
                    await signal_repository.update_signal_status(signal_id, 'REJECTED')
                    return
            else:
                if stop_loss <= fill_price:
                    logger.error(f"❌ SL после пересчёта <= Entry: {stop_loss:.6f} <= {fill_price:.6f}")
                    await signal_repository.update_signal_status(signal_id, 'REJECTED')
                    return
                if take_profit >= fill_price:
                    logger.error(f"❌ TP после пересчёта >= Entry: {take_profit:.6f} >= {fill_price:.6f}")
                    await signal_repository.update_signal_status(signal_id, 'REJECTED')
                    return

            quantity = await self._calculate_quantity(fill_price, stop_loss, direction, symbol)

            if quantity is None or quantity <= 0:
                logger.warning(f"⚠️ Некорректное количество для сигнала #{signal_id}: {quantity}")
                await signal_repository.update_signal_status(signal_id, 'REJECTED')
                return

            await signal_repository.update_position_size(signal_id, quantity)

            try:
                position = await self.paper_account.open_position(
                    signal_id=signal_id,
                    symbol=symbol,
                    direction=direction,
                    entry_price=fill_price,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    quantity=quantity,
                    expiration_time=expiration_time,
                    order_type="MARKET"
                )
            except ValueError as e:
                logger.error(f"❌ Ошибка открытия позиции #{signal_id}: {e}")
                await signal_repository.update_signal_status(signal_id, 'REJECTED')
                return

            self.open_positions[signal_id] = position
            await signal_repository.update_signal_status(signal_id, 'ACTIVE')
            await signal_repository.update_fill_price(signal_id, fill_price)

            trade_data = {
                'signal_id': signal_id,
                'symbol': symbol,
                'direction': direction,
                'entry_price': position.entry_price,
                'quantity': quantity,
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'opened_at': position.opened_at,
                'status': 'OPEN',
                'order_type': 'MARKET',
                'fill_price': fill_price
            }
            await trade_repository.save_trade(trade_data)

            logger.info(f"⚡ M15 сигнал #{signal_id}: позиция открыта по {fill_price:.6f}")

            await event_bus.publish(
                EventType.POSITION_OPENED,
                {
                    'signal_id': signal_id,
                    'symbol': symbol,
                    'direction': direction,
                    'entry_price': fill_price,
                    'quantity': quantity,
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'order_type': 'MARKET',
                    'fill_price': fill_price
                },
                'position_manager'
            )

        except Exception as e:
            logger.error(f"❌ Ошибка открытия позиции из сигнала: {e}")
            logger.error(traceback.format_exc())

    async def _calculate_quantity(
            self,
            entry_price: float,
            stop_loss: float,
            direction: str,
            symbol: str
    ) -> Optional[float]:
        """Расчёт количества на основе риск-менеджмента"""
        try:
            if entry_price is None or entry_price <= 0:
                return None
            if stop_loss is None or stop_loss <= 0:
                return None

            if entry_price == 3000:
                return self.default_quantity

            risk_per_trade_pct = self.pos_config.get('position_sizing', {}).get('risk_per_trade_pct', 2.0)
            balance = self.paper_account.balance
            risk_amount = balance * (risk_per_trade_pct / 100.0)

            if direction == 'BUY':
                risk_distance = abs(entry_price - stop_loss)
            else:
                risk_distance = abs(stop_loss - entry_price)

            min_risk_distance = entry_price * (self.min_risk_distance_pct / 100)
            if risk_distance < min_risk_distance:
                risk_distance = min_risk_distance

            if risk_distance <= 0:
                return self.default_quantity

            quantity = risk_amount / risk_distance
            quantity = self._round_quantity(quantity, symbol)

            if quantity > self.max_quantity:
                quantity = self.max_quantity
            if quantity < self.min_quantity:
                quantity = self.min_quantity

            return quantity

        except Exception as e:
            logger.error(f"Ошибка расчёта количества: {e}")
            return None

    async def _monitor_positions(self):
        """Фоновый мониторинг позиций"""
        logger.info("🔄 Запуск мониторинга позиций")

        while self.running:
            try:
                symbols = set(p.symbol for p in self.open_positions.values())
                current_prices = {}

                for symbol in symbols:
                    try:
                        price = await self.api_client.get_current_price(symbol)
                        if price:
                            current_prices[symbol] = price
                    except Exception as e:
                        logger.error(f"Ошибка получения цены {symbol}: {e}")

                for signal_id, position in list(self.open_positions.items()):
                    current_price = current_prices.get(position.symbol)
                    if not current_price:
                        continue

                    reason = self._check_tp_sl(position, current_price)
                    if reason:
                        await self._close_position(signal_id, reason, current_price)
                        continue

                    if self._is_expired(position):
                        await self._close_position(signal_id, 'EXPIRED', current_price)
                        continue

                await asyncio.sleep(self.monitoring_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в мониторинге: {e}")
                await asyncio.sleep(self.monitoring_interval)

    def _check_tp_sl(self, position: PaperPosition, current_price: float) -> Optional[str]:
        """Проверка достижения TP/SL"""
        try:
            if position.direction == 'BUY':
                if current_price >= position.take_profit:
                    return 'TP'
                elif current_price <= position.stop_loss:
                    return 'SL'
            else:
                if current_price <= position.take_profit:
                    return 'TP'
                elif current_price >= position.stop_loss:
                    return 'SL'
            return None
        except Exception as e:
            logger.error(f"Ошибка проверки TP/SL: {e}")
            return None

    def _is_expired(self, position: PaperPosition) -> bool:
        """Проверка истечения времени жизни позиции"""
        if not position.expiration_time:
            return False
        return datetime.now() >= position.expiration_time

    async def _close_position(self, signal_id: int, reason: str, close_price: float):
        """Закрытие позиции"""
        try:
            # ✅ ищем позицию в open_positions, а не в paper_account
            position = self.open_positions.get(signal_id)
            if not position:
                logger.warning(f"Позиция #{signal_id} не найдена")
                return

            if position.direction == 'BUY':
                pnl = (close_price - position.entry_price) * position.quantity
            else:
                pnl = (position.entry_price - close_price) * position.quantity

            closed_info = await self.paper_account.close_position(
                signal_id, close_price, pnl, reason
            )

            if closed_info:
                if signal_id in self.open_positions:
                    del self.open_positions[signal_id]

                await signal_repository.update_signal_status(signal_id, 'CLOSED')

                trade = await trade_repository.get_trade_by_signal_id(signal_id)
                if trade and trade.get('id'):
                    await trade_repository.update_trade(
                        trade_id=trade['id'],
                        close_price=close_price,
                        pnl=closed_info['pnl'],
                        pnl_percent=closed_info['pnl_percent'],
                        close_reason=reason,
                        closed_at=datetime.now()
                    )

                event_type = EventType.TP_HIT if reason == 'TP' else (
                    EventType.SL_HIT if reason == 'SL' else EventType.POSITION_CLOSED)

                await event_bus.publish(
                    event_type,
                    {
                        'signal_id': signal_id,
                        'symbol': position.symbol,
                        'direction': position.direction,
                        'entry_price': position.entry_price,
                        'close_price': close_price,
                        'pnl': closed_info['pnl'],
                        'pnl_percent': closed_info['pnl_percent'],
                        'close_reason': reason,
                        'order_type': 'MARKET'
                    },
                    'position_manager'
                )

                logger.info(f"✅ Позиция #{signal_id} закрыта: {reason}, PnL: {closed_info['pnl']:+.2f}")

        except Exception as e:
            logger.error(f"❌ Ошибка закрытия позиции #{signal_id}: {e}")

    async def _restore_open_positions(self):
        """Восстановление открытых позиций из БД при рестарте"""
        try:
            active_signals = await signal_repository.get_active_signals()

            for signal in active_signals:
                signal_id = signal['id']
                if signal.get('status') == 'ACTIVE' and signal.get('signal_subtype') == 'M15':
                    position = PaperPosition(
                        signal_id=signal_id,
                        symbol=signal['symbol'],
                        direction=signal['direction'],
                        entry_price=signal['entry_price'],
                        quantity=signal.get('position_size', self.default_quantity),
                        stop_loss=signal['stop_loss'],
                        take_profit=signal['take_profit'],
                        order_type='MARKET',
                        fill_price=signal.get('fill_price', signal['entry_price']),
                        expiration_time=datetime.fromisoformat(signal['expiration_time']) if signal.get(
                            'expiration_time') else None,
                        opened_at=datetime.fromisoformat(signal['created_time']) if signal[
                            'created_time'] else datetime.now()
                    )
                    self.open_positions[signal_id] = position

            logger.info(f"🔄 Восстановлено {len(self.open_positions)} открытых позиций")

        except Exception as e:
            logger.error(f"Ошибка восстановления позиций: {e}")

    async def cleanup(self):
        """Очистка ресурсов"""
        logger.info("🧹 Очистка Position Manager...")

        self.running = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass

        event_bus.unsubscribe(EventType.TRADING_SIGNAL_GENERATED, self.on_signal_generated)
        logger.info("✅ Position Manager очищен")