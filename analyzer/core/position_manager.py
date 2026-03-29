# analyzer/core/position_manager.py (ПОЛНОСТЬЮ - ФАЗА 1.3.8)
"""
🎯 POSITION MANAGER - Управление позициями (с риск-менеджментом)
ФАЗА 1.3.8:
- Резервирование средств под WATCH сигналы
- Контроль суммарного риска
- Учёт плеча в расчётах маржи
- ИСПРАВЛЕНО: формула расчёта для фьючерсов (залог = риск% от депозита)
- ИСПРАВЛЕНО: единое время через time_utils
"""

import asyncio
import logging
from typing import Dict, Optional, Any, List, Tuple
from datetime import datetime, timedelta
import traceback

from analyzer.core.event_bus import EventType, Event, event_bus
from analyzer.core.signal_repository import signal_repository
from analyzer.core.trade_repository import trade_repository
from analyzer.core.paper_account import PaperAccount, PaperPosition
from analyzer.core.data_provider import data_provider
from analyzer.core.time_utils import now, utc_now, to_local, format_local

logger = logging.getLogger('position_manager')


class PositionManager:

    def __init__(self, config: Dict[str, Any]):
        self.config = config

        self.pos_config = config.get('position_management', {})
        self.enabled = self.pos_config.get('enabled', True)
        self.monitoring_interval = self.pos_config.get('monitoring_interval_seconds', 60)
        self.default_quantity = self.pos_config.get('default_quantity', 0.001)
        self.min_quantity = self.pos_config.get('min_quantity', 0.0001)
        self.max_quantity = self.pos_config.get('max_quantity', 1000000.0)
        self.max_positions = self.pos_config.get('max_positions', 5)
        self.reserve_for_watch = self.pos_config.get('reserve_for_watch', True)
        self.max_total_risk_pct = self.pos_config.get('max_total_risk_pct', 20.0)

        analysis_config = config.get('analysis', {})
        self.m15_config = analysis_config.get('signal_types', {}).get('m15', {})
        self.watch_config = analysis_config.get('signal_types', {}).get('watch', {})
        self.max_slippage_pct = self.m15_config.get('max_slippage_pct', 1.0)

        self.min_risk_distance_pct = 0.1
        self.risk_per_trade_pct = self.pos_config.get('position_sizing', {}).get('risk_per_trade_pct', 2.0)

        self.paper_account = PaperAccount(config)
        self.open_positions: Dict[int, PaperPosition] = {}

        self.running = False
        self.monitoring_task = None

        logger.info("🎯 Position Manager инициализирован")
        logger.info(f"   Мониторинг раз в {self.monitoring_interval} сек")
        logger.info(f"   Макс. позиций: {self.max_positions}")
        logger.info(f"   Макс. суммарный риск: {self.max_total_risk_pct}%")
        logger.info(f"   Риск на сделку: {self.risk_per_trade_pct}%")
        logger.info(f"   Формула: залог = риск% от депозита")

    def _round_price(self, price: float, symbol: str = "") -> float:
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

    async def initialize(self) -> bool:
        try:
            await trade_repository.initialize()
            event_bus.subscribe(EventType.TRADING_SIGNAL_GENERATED, self.on_signal_generated)
            event_bus.subscribe(EventType.WATCH_CREATED, self.on_watch_created)
            event_bus.subscribe(EventType.WATCH_EXPIRED, self.on_watch_expired)
            await self._restore_open_positions()
            self.running = True
            self.monitoring_task = asyncio.create_task(self._monitor_positions())
            logger.info("✅ Position Manager готов")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            return False

    async def on_watch_created(self, event: Event):
        try:
            data = event.data
            signal_id = data.get('signal_id')
            symbol = data.get('symbol')
            position_size = data.get('position_size')
            entry_price = data.get('entry_price')
            leverage = data.get('leverage', 10)

            if not self.reserve_for_watch:
                return

            if position_size is None or position_size <= 0:
                balance = await self.paper_account.get_balance()
                margin_target = balance * (self.risk_per_trade_pct / 100.0)
                position_value = margin_target * leverage
                position_size = position_value / entry_price if entry_price > 0 else self.default_quantity
                position_size = self._round_quantity(position_size, symbol)

            success, reserved_margin = await self.paper_account.reserve_for_watch(
                signal_id=signal_id, symbol=symbol, position_size=position_size,
                entry_price=entry_price, leverage=leverage,
                expiration_hours=self.watch_config.get('expiration_hours', 3)
            )

            if success:
                await signal_repository.update_reserved_margin(signal_id, reserved_margin)
        except Exception as e:
            logger.error(f"❌ Ошибка WATCH_CREATED: {e}")

    async def on_watch_expired(self, event: Event):
        try:
            signal_id = event.data.get('signal_id')
            await self.paper_account.release_watch_reserve(signal_id)
        except Exception as e:
            logger.error(f"❌ Ошибка WATCH_EXPIRED: {e}")

    async def on_signal_generated(self, event: Event):
        try:
            data = event.data
            signal_id = data.get('signal_id')
            signal_subtype = data.get('signal_subtype', 'M15')

            if signal_subtype == 'WATCH':
                return
            if signal_subtype != 'M15':
                return
            if len(self.open_positions) >= self.max_positions:
                await signal_repository.update_signal_status(signal_id, 'REJECTED')
                return

            await self._open_position_from_signal(data)
        except Exception as e:
            logger.error(f"❌ Ошибка обработки сигнала: {e}")

    async def _open_position_from_signal(self, signal_data: Dict[str, Any]):
        try:
            signal_id = signal_data['signal_id']
            symbol = signal_data['symbol']
            direction = signal_data['signal_type']
            planned_entry = signal_data['entry_price']
            planned_sl = signal_data['stop_loss']
            planned_tp = signal_data['take_profit']
            expiration_time = signal_data.get('expiration_time')
            leverage = signal_data.get('leverage', 10)

            if expiration_time and isinstance(expiration_time, str):
                expiration_time = datetime.fromisoformat(expiration_time)

            current_price = await data_provider.get_current_price(symbol, force_refresh=True)
            fill_price = current_price if current_price else planned_entry

            logger.info(f"{'=' * 60}")
            logger.info(f"💰 ОТКРЫТИЕ ПОЗИЦИИ #{signal_id} ({symbol})")
            logger.info(f"{'=' * 60}")
            logger.info(f"   Направление: {direction}")
            logger.info(f"   Планируемый вход: {planned_entry:.6f}")
            logger.info(f"   Фактическая цена: {fill_price:.6f}")
            logger.info(f"   Время: {format_local(now())}")

            deviation_pct = abs(fill_price - planned_entry) / planned_entry * 100
            if deviation_pct > self.max_slippage_pct:
                logger.warning(f"⚠️ Отклонение {deviation_pct:.2f}% > {self.max_slippage_pct}%")
                await signal_repository.update_signal_status(signal_id, 'REJECTED')
                return

            if direction == 'BUY':
                stop_loss = fill_price - (planned_entry - planned_sl)
                take_profit = fill_price + (planned_tp - planned_entry)
            else:
                stop_loss = fill_price + (planned_sl - planned_entry)
                take_profit = fill_price - (planned_entry - planned_tp)

            stop_loss = self._round_price(stop_loss, symbol)
            take_profit = self._round_price(take_profit, symbol)

            logger.info(f"   SL (пересчитан): {stop_loss:.6f}")
            logger.info(f"   TP (пересчитан): {take_profit:.6f}")

            # ========== РАСЧЁТ РАЗМЕРА ПОЗИЦИИ (ФЬЮЧЕРСНАЯ ФОРМУЛА) ==========
            balance = await self.paper_account.get_balance()

            margin_target = balance * (self.risk_per_trade_pct / 100.0)
            logger.info(f"\n📊 РАСЧЁТ РАЗМЕРА ПОЗИЦИИ:")
            logger.info(f"   Баланс: {balance:.2f} USDT")
            logger.info(f"   Целевой залог: {margin_target:.2f} USDT ({self.risk_per_trade_pct}% от депозита)")

            position_value = margin_target * leverage
            logger.info(f"   Стоимость позиции: {position_value:.2f} USDT")

            quantity = position_value / fill_price
            logger.info(f"   Количество монет (расчётное): {quantity:.2f}")

            quantity = self._round_quantity(quantity, symbol)
            logger.info(f"   Количество монет (округлённое): {quantity:.4f}")

            if quantity > self.max_quantity:
                quantity = self.max_quantity
                logger.warning(f"   ⚠️ Ограничено максимумом: {quantity:.2f}")
            if quantity < self.min_quantity:
                quantity = self.min_quantity
                logger.warning(f"   ⚠️ Увеличено до минимума: {quantity:.4f}")

            actual_position_value = quantity * fill_price
            actual_margin = actual_position_value / leverage
            risk_distance = abs(fill_price - stop_loss)
            actual_risk = quantity * risk_distance
            risk_pct = (actual_risk / balance) * 100

            logger.info(f"\n📊 ИТОГОВЫЙ РАСЧЁТ:")
            logger.info(f"   Количество: {quantity:.4f} {symbol}")
            logger.info(f"   Стоимость позиции: {actual_position_value:.2f} USDT")
            logger.info(f"   Залог: {actual_margin:.2f} USDT")
            logger.info(f"   Риск: {actual_risk:.2f} USDT ({risk_pct:.2f}%)")

            if actual_margin > balance:
                logger.error(f"❌ Залог {actual_margin:.2f} > баланс {balance:.2f}")
                await signal_repository.update_signal_status(signal_id, 'REJECTED')
                return

            await signal_repository.update_position_size(signal_id, quantity)
            await signal_repository.update_leverage(signal_id, leverage)

            try:
                position = await self.paper_account.open_position(
                    signal_id=signal_id, symbol=symbol, direction=direction,
                    entry_price=fill_price, stop_loss=stop_loss, take_profit=take_profit,
                    quantity=quantity, leverage=leverage, expiration_time=expiration_time,
                    order_type="MARKET"
                )
            except ValueError as e:
                logger.error(f"❌ Ошибка открытия позиции: {e}")
                await signal_repository.update_signal_status(signal_id, 'REJECTED')
                return

            self.open_positions[signal_id] = position
            await signal_repository.update_signal_status(signal_id, 'ACTIVE')
            await signal_repository.update_fill_price(signal_id, fill_price)
            await signal_repository.update_margin(signal_id, position.margin, position.position_value)

            trade_data = {
                'signal_id': signal_id, 'symbol': symbol, 'direction': direction,
                'entry_price': position.entry_price, 'quantity': quantity,
                'leverage': leverage, 'margin': position.margin,
                'position_value': position.position_value, 'stop_loss': stop_loss,
                'take_profit': take_profit, 'opened_at': utc_now().isoformat(),
                'status': 'OPEN', 'order_type': 'MARKET', 'fill_price': fill_price
            }
            await trade_repository.save_trade(trade_data)

            logger.info(f"\n✅ ПОЗИЦИЯ #{signal_id} ОТКРЫТА!")
            logger.info(f"   {symbol} {direction} {quantity:.4f} монет @ {fill_price:.6f}")
            logger.info(f"   Залог: {position.margin:.2f} USDT")
            logger.info(f"   Новый баланс: {await self.paper_account.get_balance():.2f} USDT")
            logger.info(f"{'=' * 60}")

            await event_bus.publish(
                EventType.POSITION_OPENED,
                {
                    'signal_id': signal_id, 'symbol': symbol, 'direction': direction,
                    'entry_price': fill_price, 'quantity': quantity, 'leverage': leverage,
                    'margin': position.margin, 'stop_loss': stop_loss,
                    'take_profit': take_profit, 'order_type': 'MARKET', 'fill_price': fill_price
                },
                'position_manager'
            )

        except Exception as e:
            logger.error(f"❌ Ошибка открытия позиции: {e}")
            logger.error(traceback.format_exc())

    async def _monitor_positions(self):
        logger.info("🔄 Запуск мониторинга позиций")
        while self.running:
            try:
                symbols = set(p.symbol for p in self.open_positions.values())
                current_prices = {}
                for symbol in symbols:
                    try:
                        price = await data_provider.get_current_price(symbol)
                        if price:
                            current_prices[symbol] = price
                    except Exception as e:
                        logger.error(f"Ошибка цены {symbol}: {e}")

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
                logger.error(f"Ошибка мониторинга: {e}")
                await asyncio.sleep(self.monitoring_interval)

    def _check_tp_sl(self, position: PaperPosition, current_price: float) -> Optional[str]:
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
            logger.error(f"Ошибка TP/SL: {e}")
            return None

    def _is_expired(self, position: PaperPosition) -> bool:
        if not position.expiration_time:
            return False
        return now() >= position.expiration_time

    async def _close_position(self, signal_id: int, reason: str, close_price: float):
        try:
            position = self.open_positions.get(signal_id)
            if not position:
                return

            if position.direction == 'BUY':
                pnl = (close_price - position.entry_price) * position.quantity
            else:
                pnl = (position.entry_price - close_price) * position.quantity

            closed_info = await self.paper_account.close_position(signal_id, close_price, pnl, reason)

            if closed_info:
                del self.open_positions[signal_id]
                await signal_repository.update_signal_status(signal_id, 'CLOSED')
                trade = await trade_repository.get_trade_by_signal_id(signal_id)
                if trade and trade.get('id'):
                    await trade_repository.update_trade(
                        trade_id=trade['id'], close_price=close_price, pnl=closed_info['pnl'],
                        pnl_percent=closed_info['pnl_percent'], close_reason=reason, closed_at=utc_now()
                    )
                await event_bus.publish(
                    EventType.POSITION_CLOSED,
                    {'signal_id': signal_id, 'symbol': position.symbol, 'pnl': closed_info['pnl']},
                    'position_manager'
                )
                logger.info(f"✅ Позиция #{signal_id} закрыта: {reason}, PnL: {closed_info['pnl']:+.2f}")
        except Exception as e:
            logger.error(f"❌ Ошибка закрытия: {e}")

    async def _restore_open_positions(self):
        try:
            active_signals = await signal_repository.get_active_signals()
            for signal in active_signals:
                signal_id = signal['id']
                if signal.get('status') == 'ACTIVE' and signal.get('signal_subtype') == 'M15':
                    trade = await trade_repository.get_trade_by_signal_id(signal_id)
                    position = PaperPosition(
                        signal_id=signal_id, symbol=signal['symbol'], direction=signal['direction'],
                        entry_price=signal['entry_price'], quantity=signal.get('position_size', self.default_quantity),
                        stop_loss=signal['stop_loss'], take_profit=signal['take_profit'],
                        leverage=signal.get('leverage', 10), margin=trade.get('margin', 0) if trade else 0,
                        position_value=trade.get('position_value', 0) if trade else 0,
                        order_type='MARKET', fill_price=signal.get('fill_price', signal['entry_price']),
                        expiration_time=datetime.fromisoformat(signal['expiration_time']) if signal.get(
                            'expiration_time') else None,
                        opened_at=datetime.fromisoformat(trade['opened_at']) if trade and trade.get(
                            'opened_at') else now()
                    )
                    self.open_positions[signal_id] = position
            logger.info(f"🔄 Восстановлено {len(self.open_positions)} позиций")
        except Exception as e:
            logger.error(f"Ошибка восстановления: {e}")

    async def cleanup(self):
        logger.info("🧹 Очистка Position Manager...")
        self.running = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        await self.paper_account.cleanup_expired_reservations()
        event_bus.unsubscribe(EventType.TRADING_SIGNAL_GENERATED, self.on_signal_generated)
        event_bus.unsubscribe(EventType.WATCH_CREATED, self.on_watch_created)
        event_bus.unsubscribe(EventType.WATCH_EXPIRED, self.on_watch_expired)
        logger.info("✅ Position Manager очищен")