#!/usr/bin/env python3
# analyzer/utils/monitoring/tables.py
"""
📊 TABLES - Таблицы сигналов (реалтайм, все сигналы, сделки)
Запуск: python -m analyzer.utils.monitoring.tables
"""

import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from analyzer.utils.monitoring.lib import MonitorBase, TableBuilder
from analyzer.core.signal_repository import signal_repository
from analyzer.core.trade_repository import trade_repository
from analyzer.core.paper_account import PaperAccount
from analyzer.core.data_provider import data_provider
from analyzer.core.websocket_client import BybitWebSocketClient
from analyzer.core.time_utils import now, utc_now, to_local, TIMEZONE_OFFSET

logger = logging.getLogger('tables')


class MonitorTables(MonitorBase):
    """Таблицы сигналов и сделок"""

    def __init__(self):
        super().__init__(color_enabled=True)
        self.signal_repo = signal_repository
        self.trade_repo = trade_repository
        self.paper_account = PaperAccount({})
        self.data_provider = data_provider
        self.websocket = BybitWebSocketClient()

        import yaml
        config_path = Path(__file__).parent.parent.parent / 'config/config.yaml'
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.timezone_offset = self.config.get('display', {}).get('timezone_offset', TIMEZONE_OFFSET)
        self.refresh_interval = self.config.get('display', {}).get('refresh_interval', 5)
        self.table_builder = TableBuilder()
        self._shutdown = False
        self._price_cache: Dict[str, float] = {}
        self._price_cache_ttl = 5

    def utc_to_local(self, utc_str: str) -> datetime:
        if not utc_str:
            return now()
        try:
            if '+' in utc_str or 'Z' in utc_str:
                dt = datetime.fromisoformat(utc_str.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(utc_str)
            return dt + timedelta(hours=self.timezone_offset)
        except Exception:
            return now()

    def format_time(self, timestamp_str: str) -> str:
        if not timestamp_str:
            return "-"
        try:
            local_dt = self.utc_to_local(timestamp_str)
            return local_dt.strftime("%H:%M")
        except:
            return timestamp_str[11:16] if ':' in timestamp_str else '-'

    def format_date(self, timestamp_str: str) -> str:
        if not timestamp_str:
            return "-"
        try:
            local_dt = self.utc_to_local(timestamp_str)
            return local_dt.strftime("%m-%d")
        except:
            return timestamp_str[:10] if '-' in timestamp_str else '-'

    def format_datetime(self, timestamp_str: str) -> str:
        if not timestamp_str:
            return "-"
        try:
            local_dt = self.utc_to_local(timestamp_str)
            return local_dt.strftime("%d.%m.%Y %H:%M:%S")
        except:
            return timestamp_str

    async def get_current_price(self, symbol: str) -> Optional[float]:
        if self.websocket:
            try:
                price = self.websocket.get_latest_price(symbol)
                if price and price > 0:
                    return price
            except Exception:
                pass

        cache_key = f"price_{symbol}"
        if cache_key in self._price_cache:
            cached_price, cached_time = self._price_cache[cache_key]
            if (datetime.now() - cached_time).total_seconds() < self._price_cache_ttl:
                return cached_price

        if self.data_provider:
            try:
                price = await self.data_provider.get_current_price(symbol)
                if price:
                    self._price_cache[cache_key] = (price, datetime.now())
                return price
            except Exception:
                pass
        return None

    async def display_all_signals(self):
        """Таблица всех сигналов"""
        try:
            signals = await self.signal_repo.get_signals_with_trades(limit=200)

            if not signals:
                print("📭 Нет сигналов")
                return

            table_data = []
            symbols_to_subscribe = []

            for signal in signals:
                signal_id = signal.get('id', '')
                symbol = signal.get('symbol', '')
                status = signal.get('status', '')
                entry_type = signal.get('entry_type', 'LEGACY')
                direction = signal.get('direction', '')
                entry_price = signal.get('entry_price', 0)
                stop_loss = signal.get('stop_loss', 0)
                take_profit = signal.get('take_profit', 0)
                zone_low = signal.get('zone_low', 0)
                zone_high = signal.get('zone_high', 0)
                score = signal.get('screen2_score', 0)
                rr_ratio = signal.get('risk_reward_ratio', 0)
                trade_pnl = signal.get('trade_pnl')
                position_size = signal.get('position_size', 0)

                symbols_to_subscribe.append(symbol)
                current_price = await self.get_current_price(symbol)

                if status == 'ACTIVE' and current_price and entry_price > 0:
                    if direction == 'BUY':
                        pnl = (current_price - entry_price) * position_size
                    else:
                        pnl = (entry_price - current_price) * position_size
                    pnl_display = self.format_pnl(pnl)
                else:
                    pnl_display = self.format_pnl(trade_pnl) if trade_pnl is not None else "-"

                if status in ['ACTIVE', 'CLOSED']:
                    entry_display = self.format_price(entry_price) if entry_price > 0 else "-"
                    sl_display = self.format_price(stop_loss) if stop_loss > 0 else "-"
                    tp_display = self.format_price(take_profit) if take_profit > 0 else "-"
                else:
                    entry_display = "-"
                    sl_display = "-"
                    tp_display = "-"

                zone_display = self.format_zone(zone_low, zone_high) if zone_low > 0 else "-"
                price_display = self.format_price(current_price) if current_price else "-"
                dir_display = "BUY" if direction == 'BUY' else "SELL" if direction == 'SELL' else "-"

                time_str = self.format_time(signal.get('created_time', ''))
                date_str = self.format_date(signal.get('created_time', ''))

                table_data.append([
                    str(signal_id), symbol, dir_display,
                    self.format_status(status), self.format_entry_type(entry_type),
                    entry_display, sl_display, tp_display, zone_display,
                    self.format_score(score), price_display, pnl_display,
                    self.format_rr_ratio(rr_ratio) if rr_ratio else "-",
                    f"{date_str} {time_str}"
                ])

            headers = ["ID", "Монета", "Напр", "Статус", "Тип", "Entry", "SL", "TP", "Зона", "Score", "Цена", "PnL", "R/R", "Время"]
            print(self.table_builder.create_table(headers, table_data))

        except Exception as e:
            logger.error(f"Ошибка: {e}")
            print(f"❌ Ошибка: {e}")

    async def display_realtime_monitor(self):
        """Реалтайм монитор с автообновлением"""
        self._shutdown = False

        if self.websocket:
            try:
                if not self.websocket.running:
                    asyncio.create_task(self.websocket.connect())
                    await asyncio.sleep(1)
            except Exception:
                pass

        try:
            while not self._shutdown:
                self.clear_screen()
                self.print_header("РЕАЛТАЙМ МОНИТОР")

                signals = await self.signal_repo.get_signals_with_trades(limit=50)

                if not signals:
                    print("📭 Нет сигналов")
                else:
                    table_data = []
                    symbols_to_subscribe = []

                    for signal in signals:
                        signal_id = signal.get('id', '')
                        symbol = signal.get('symbol', '')
                        status = signal.get('status', '')
                        entry_type = signal.get('entry_type', 'LEGACY')
                        direction = signal.get('direction', '')
                        entry_price = signal.get('entry_price', 0)
                        stop_loss = signal.get('stop_loss', 0)
                        take_profit = signal.get('take_profit', 0)
                        zone_low = signal.get('zone_low', 0)
                        zone_high = signal.get('zone_high', 0)
                        score = signal.get('screen2_score', 0)
                        rr_ratio = signal.get('risk_reward_ratio', 0)
                        trade_pnl = signal.get('trade_pnl')
                        position_size = signal.get('position_size', 0)

                        symbols_to_subscribe.append(symbol)
                        current_price = await self.get_current_price(symbol)

                        if status == 'ACTIVE' and current_price and entry_price > 0:
                            if direction == 'BUY':
                                pnl = (current_price - entry_price) * position_size
                            else:
                                pnl = (entry_price - current_price) * position_size
                            pnl_display = self.format_pnl(pnl)
                        else:
                            pnl_display = self.format_pnl(trade_pnl) if trade_pnl is not None else "-"

                        if status in ['ACTIVE', 'CLOSED']:
                            entry_display = self.format_price(entry_price) if entry_price > 0 else "-"
                            sl_display = self.format_price(stop_loss) if stop_loss > 0 else "-"
                            tp_display = self.format_price(take_profit) if take_profit > 0 else "-"
                        else:
                            entry_display = "-"
                            sl_display = "-"
                            tp_display = "-"

                        zone_display = self.format_zone(zone_low, zone_high) if zone_low > 0 else "-"
                        price_display = self.format_price(current_price) if current_price else "-"
                        dir_display = "BUY" if direction == 'BUY' else "SELL" if direction == 'SELL' else "-"

                        time_str = self.format_time(signal.get('created_time', ''))
                        date_str = self.format_date(signal.get('created_time', ''))

                        table_data.append([
                            str(signal_id), symbol, dir_display,
                            self.format_status(status), self.format_entry_type(entry_type),
                            entry_display, sl_display, tp_display, zone_display,
                            self.format_score(score), price_display, pnl_display,
                            self.format_rr_ratio(rr_ratio) if rr_ratio else "-",
                            f"{date_str} {time_str}"
                        ])

                    headers = ["ID", "Монета", "Напр", "Статус", "Тип", "Entry", "SL", "TP", "Зона", "Score", "Цена", "PnL", "R/R", "Время"]
                    print(self.table_builder.create_table(headers, table_data))

                print(f"\n🔄 Автообновление через {self.refresh_interval} сек...")
                await asyncio.sleep(self.refresh_interval)

        except asyncio.CancelledError:
            pass
        finally:
            self._shutdown = False

    async def display_trades_table(self):
        """Таблица сделок"""
        try:
            trades = await self.trade_repo.get_closed_trades(limit=50)

            if not trades:
                print("📭 Нет закрытых сделок")
                return

            table_data = []
            total_pnl = 0
            winning = 0
            losing = 0

            for trade in trades:
                pnl = trade.get('pnl', 0)
                total_pnl += pnl
                if pnl > 0:
                    winning += 1
                elif pnl < 0:
                    losing += 1

                commission_open = trade.get('commission_open', 0)
                commission_close = trade.get('commission_close', 0)
                total_commission = commission_open + commission_close

                table_data.append([
                    str(trade.get('id', '')),
                    trade.get('symbol', ''),
                    self.format_direction(trade.get('direction', '')),
                    self.format_price(trade.get('entry_price', 0)),
                    self.format_price(trade.get('close_price', 0)),
                    self.format_pnl(pnl),
                    f"{trade.get('pnl_percent', 0):+.2f}%" if trade.get('pnl_percent') else "-",
                    f"{total_commission:.4f}",
                    trade.get('close_reason', '-'),
                    self.format_datetime(trade.get('closed_at', ''))
                ])

            headers = ["ID", "Монета", "Напр", "Entry", "Exit", "PnL", "PnL%", "Комисс", "Причина", "Закрыта"]
            print(self.table_builder.create_table(headers, table_data))

            print(f"\n📊 ИТОГИ:")
            print(f"   Всего сделок: {len(trades)}")
            print(f"   Прибыльных: {winning}")
            print(f"   Убыточных: {losing}")
            print(f"   Общий PnL: {self.format_pnl(total_pnl)}")

        except Exception as e:
            logger.error(f"Ошибка: {e}")

    async def run(self):
        await self.signal_repo.initialize()
        await self.trade_repo.initialize()
        data_provider.configure(self.config)

        while True:
            self.clear_screen()
            self.print_header("MONITOR TABLES")
            print("1. Все сигналы")
            print("2. Реалтайм монитор")
            print("3. Таблица сделок")
            print("0. Выход")

            choice = input("\nВыбор: ").strip()

            if choice == '1':
                await self.display_all_signals()
                input("\nНажмите Enter...")
            elif choice == '2':
                await self.display_realtime_monitor()
            elif choice == '3':
                await self.display_trades_table()
                input("\nНажмите Enter...")
            elif choice == '0':
                break


async def main():
    tables = MonitorTables()
    await tables.run()


if __name__ == "__main__":
    asyncio.run(main())