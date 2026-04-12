#!/usr/bin/env python3
# monitor_three_screen.py (ПОЛНАЯ ВЕРСИЯ - ФАЗА 1.3.10)
"""
🎯 МОНИТОР ДЛЯ THREE SCREEN ANALYZER - ВЕРСИЯ 1.3.10
Особенности:
- Отображение WATCH и M15 сигналов
- Показ зон входа (zone_low/zone_high)
- Показ score Screen2
- ЭКСПОРТ ДАННЫХ в CSV/JSON/TXT/ZIP
- Папка экспорта: logs/exports/YYYY-MM-DD/
- DataProvider для получения текущей цены
- РАЗДЕЛЕНИЕ планируемых и фактических данных
- ВРЕМЕННАЯ ШКАЛА (создан, истекает, открыт, закрыт)
- СОСТОЯНИЕ СЧЁТА (депозит, маржа, резерв, доступно, PnL)
- ИСПРАВЛЕНО: единое время через time_utils
- ИСПРАВЛЕНО: отображение состояния счёта с учётом позиций

ФАЗА 1.3.10:
- Добавлен пункт меню "5. 📊 Анализ тренда (D1)"
- Добавлен метод display_trend_analysis() для отображения таблицы трендов
- Добавлен метод display_trend_details() для детального просмотра с пояснениями
- Обновлена нумерация пунктов меню (1-8 вместо 1-7)
"""

import asyncio
import logging
import os
import re
import sys
import yaml
import csv
import json
import zipfile
import aiosqlite
from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent))

try:
    from colorama import init, Fore, Style

    init()
except ImportError:
    class Fore:
        RED = '';
        GREEN = '';
        YELLOW = '';
        CYAN = '';
        MAGENTA = '';
        WHITE = '';
        RESET = ''


    class Style:
        BRIGHT = '';
        RESET_ALL = ''


    def init():
        pass

from analyzer.core.time_utils import now, utc_now, to_local, format_local, parse_iso_to_local, TIMEZONE_OFFSET

logger = logging.getLogger('three_screen_monitor')


class ThreeScreenMonitor:

    def __init__(self, config: Optional[Dict] = None):
        if config is None:
            config_path = Path(__file__).parent / 'analyzer/config/config.yaml'
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
            except Exception:
                config = {}

        self.config = config

        self.signal_repo = None
        self.trade_repo = None
        self.paper_account = None

        try:
            from analyzer.core.signal_repository import signal_repository
            self.signal_repo = signal_repository
        except ImportError:
            pass

        try:
            from analyzer.core.trade_repository import trade_repository
            self.trade_repo = trade_repository
        except ImportError:
            pass

        try:
            from analyzer.core.paper_account import PaperAccount
            self.paper_account = PaperAccount(config) if config else None
        except ImportError:
            pass

        self.data_provider = None
        try:
            from analyzer.core.data_provider import data_provider
            self.data_provider = data_provider
            self.data_provider.configure(self.config)
        except Exception:
            pass

        display_config = self.config.get('display', {})
        self.timezone_offset = display_config.get('timezone_offset', TIMEZONE_OFFSET)
        self.refresh_interval = display_config.get('refresh_interval', 5)

        paper_config = self.config.get('paper_trading', {})
        self.starting_balance = paper_config.get('starting_virtual_balance', 10000.0)

        db_config = self.config.get('database', {})
        db_signals_config = self.config.get('database_signals', {})
        db_path = None
        if isinstance(db_signals_config, dict) and db_signals_config.get('enabled', False):
            db_path = db_signals_config.get('path')
        if not db_path and isinstance(db_config, dict):
            db_path = db_config.get('path')
        if not db_path:
            db_path = 'data/trading_bot.db'

        if not os.path.isabs(db_path):
            project_root = Path(__file__).parent
            self.db_path = str(project_root / db_path)
        else:
            self.db_path = db_path

        self._shutdown = False
        self.settings = {
            'refresh_interval': self.refresh_interval,
            'signals_limit': 20,
            'show_confidence': True,
            'show_rr_ratio': True
        }

        print(f"📊 Монитор использует БД: {self.db_path}")
        print(f"🕐 Часовой пояс: UTC+{self.timezone_offset}")

    @staticmethod
    def strip_ansi(text: str) -> str:
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        return ansi_escape.sub('', str(text))

    def get_visible_length(self, text: str) -> int:
        return len(self.strip_ansi(str(text)))

    @staticmethod
    def clear_screen() -> None:
        os.system('cls' if os.name == 'nt' else 'clear')

    @staticmethod
    def print_header(title: str) -> None:
        print(f"\n{Fore.CYAN}{Style.BRIGHT}{'=' * 80}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{Style.BRIGHT}{title}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 80}{Style.RESET_ALL}")

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

    @staticmethod
    def format_direction(direction: str) -> str:
        direction_lower = direction.lower() if direction else ''
        if direction_lower in ['buy', 'long']:
            return f"{Fore.GREEN}▲ BUY{Style.RESET_ALL}"
        elif direction_lower in ['sell', 'short']:
            return f"{Fore.RED}▼ SELL{Style.RESET_ALL}"
        return direction or 'N/A'

    @staticmethod
    def format_status(status: str) -> str:
        status_lower = status.lower() if status else ''
        if status_lower == 'watch':
            return f"{Fore.YELLOW}WATCH{Style.RESET_ALL}"
        elif status_lower == 'active':
            return f"{Fore.CYAN}ACTIVE{Style.RESET_ALL}"
        elif status_lower in ['closed', 'completed']:
            return f"{Fore.MAGENTA}CLOSED{Style.RESET_ALL}"
        elif status_lower == 'cancelled':
            return f"{Fore.RED}CANCELLED{Style.RESET_ALL}"
        elif status_lower == 'expired':
            return f"{Fore.WHITE}EXPIRED{Style.RESET_ALL}"
        elif status_lower == 'rejected':
            return f"{Fore.RED}REJECTED{Style.RESET_ALL}"
        else:
            return status or 'N/A'

    @staticmethod
    def format_confidence(confidence: float) -> str:
        if confidence is None:
            return "-"
        if confidence > 0.8:
            color = Fore.GREEN
        elif confidence > 0.6:
            color = Fore.YELLOW
        else:
            color = Fore.RED
        return f"{color}{confidence * 100:.1f}%{Style.RESET_ALL}"

    @staticmethod
    def format_price(price: float) -> str:
        if price is None or price == 0:
            return "-"
        if price < 0.001:
            return f"{price:.8f}"
        elif price < 0.01:
            return f"{price:.6f}"
        elif price < 0.1:
            return f"{price:.5f}"
        elif price < 1:
            return f"{price:.4f}"
        elif price < 10:
            return f"{price:.3f}"
        elif price < 100:
            return f"{price:.2f}"
        else:
            return f"{price:.2f}"

    @staticmethod
    def format_pnl(pnl: float) -> str:
        if pnl is None:
            return "-"
        if abs(pnl) < 0.01 and pnl != 0:
            return f"{Fore.YELLOW}{pnl:.6f}{Style.RESET_ALL}"
        if pnl > 0:
            return f"{Fore.GREEN}+{pnl:.2f}{Style.RESET_ALL}"
        elif pnl < 0:
            return f"{Fore.RED}{pnl:.2f}{Style.RESET_ALL}"
        else:
            return f"{Fore.YELLOW}0.00{Style.RESET_ALL}"

    @staticmethod
    def format_rr_ratio(rr: float) -> str:
        if rr is None or rr == 0:
            return "-"
        if rr >= 3.0:
            color = Fore.GREEN
        elif rr >= 2.0:
            color = Fore.YELLOW
        else:
            color = Fore.RED
        return f"{color}{rr:.2f}:1{Style.RESET_ALL}"

    @staticmethod
    def format_screen(screen: str) -> str:
        if not screen:
            return "-"
        if screen.upper() == 'WATCH':
            return f"{Fore.YELLOW}WATCH{Style.RESET_ALL}"
        elif screen.upper() == 'M15':
            return f"{Fore.GREEN}M15{Style.RESET_ALL}"
        return screen

    def format_zone(self, zone_low: float, zone_high: float) -> str:
        if zone_low is None or zone_high is None or zone_low == 0 or zone_high == 0:
            return "-"
        return f"{self.format_price(zone_low)}-{self.format_price(zone_high)}"

    @staticmethod
    def format_score(score: int) -> str:
        if score is None or score == 0:
            return "-"
        if score >= 4:
            return f"{Fore.GREEN}{score}/5{Style.RESET_ALL}"
        elif score >= 3:
            return f"{Fore.YELLOW}{score}/5{Style.RESET_ALL}"
        else:
            return f"{Fore.RED}{score}/5{Style.RESET_ALL}"

    @staticmethod
    def format_position_size(size: float) -> str:
        if size is None or size == 0:
            return "-"
        return f"{size:.4f}"

    @staticmethod
    def format_leverage(leverage: float) -> str:
        if leverage is None or leverage == 0:
            return "10x"
        return f"{leverage:.1f}x"

    def create_table(self, headers: List[str], data: List[List[str]]) -> str:
        if not data:
            return "Нет данных"

        col_widths = []
        for i, header in enumerate(headers):
            max_width = len(header)
            for row in data:
                if i < len(row):
                    visible_text = self.strip_ansi(str(row[i]))
                    max_width = max(max_width, len(visible_text))
            col_widths.append(max_width + 2)

        result = []
        top_border = "┌" + "┬".join("─" * width for width in col_widths) + "┐"
        result.append(top_border)

        header_line = "│"
        for i, header in enumerate(headers):
            padding = col_widths[i] - len(header)
            left_padding = padding // 2
            right_padding = padding - left_padding
            centered_header = " " * left_padding + header + " " * right_padding
            header_line += centered_header + "│"
        result.append(header_line)

        separator = "├" + "┼".join("─" * width for width in col_widths) + "┤"
        result.append(separator)

        for row in data:
            row_line = "│"
            for i in range(len(headers)):
                cell = row[i] if i < len(row) else ""
                visible_cell = str(cell)
                padding = col_widths[i] - self.get_visible_length(visible_cell)
                aligned_cell = visible_cell + " " * padding
                row_line += aligned_cell + "│"
            result.append(row_line)

        bottom_border = "└" + "┴".join("─" * width for width in col_widths) + "┘"
        result.append(bottom_border)
        return "\n".join(result)

    def _get_export_dir(self) -> Path:
        project_root = Path(__file__).parent
        date_folder = now().strftime("%Y-%m-%d")
        export_dir = project_root / 'logs' / 'exports' / date_folder
        export_dir.mkdir(parents=True, exist_ok=True)
        return export_dir

    @staticmethod
    def _get_file_size(file_path: str) -> str:
        try:
            size = os.path.getsize(file_path)
            if size < 1024:
                return f"{size} B"
            elif size < 1024 * 1024:
                return f"{size / 1024:.2f} KB"
            else:
                return f"{size / (1024 * 1024):.2f} MB"
        except Exception:
            return "?"

    def _open_export_folder(self, file_path: str) -> None:
        folder = os.path.dirname(file_path)
        try:
            if os.name == 'nt':
                os.startfile(folder)
            elif os.name == 'posix':
                if os.system(f'open "{folder}"') != 0:
                    os.system(f'xdg-open "{folder}"')
            print(f"{Fore.GREEN}📂 Папка открыта: {folder}{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.YELLOW}⚠️ Не удалось открыть папку: {e}{Style.RESET_ALL}")

    async def export_signals(self, fmt: str = 'csv') -> Optional[str]:
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                return None

            signals = await self.signal_repo.get_signals(limit=1000)
            if not signals:
                print(f"{Fore.YELLOW}⚠️ Нет данных для экспорта{Style.RESET_ALL}")
                return None

            export_dir = self._get_export_dir()
            timestamp = now().strftime("%Y%m%d_%H%M%S")
            fields = ['id', 'symbol', 'direction', 'signal_subtype', 'status', 'entry_price', 'stop_loss',
                      'take_profit', 'risk_reward_ratio', 'zone_low', 'zone_high', 'screen2_score', 'expected_pattern',
                      'trigger_pattern', 'confidence', 'created_time', 'expiration_time', 'updated_time', 'fill_price',
                      'position_size', 'leverage']

            if fmt == 'csv':
                filename = export_dir / f"signals_{timestamp}.csv"
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fields)
                    writer.writeheader()
                    for signal in signals:
                        row = {field: signal.get(field, '') for field in fields}
                        writer.writerow(row)
                print(f"{Fore.GREEN}✅ Файл сохранён: {filename}{Style.RESET_ALL}")
                return str(filename)
            elif fmt == 'json':
                filename = export_dir / f"signals_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(signals, f, ensure_ascii=False, indent=2, default=str)
                print(f"{Fore.GREEN}✅ Файл сохранён: {filename}{Style.RESET_ALL}")
                return str(filename)
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка экспорта: {e}")
            print(f"{Fore.RED}❌ Ошибка экспорта: {e}{Style.RESET_ALL}")
            return None

    async def export_trades(self, fmt: str = 'csv') -> Optional[str]:
        try:
            if not self.trade_repo:
                print(f"{Fore.RED}❌ TradeRepository не инициализирован{Style.RESET_ALL}")
                return None

            trades = await self.trade_repo.get_closed_trades(limit=1000)
            if not trades:
                print(f"{Fore.YELLOW}⚠️ Нет данных для экспорта{Style.RESET_ALL}")
                return None

            export_dir = self._get_export_dir()
            timestamp = now().strftime("%Y%m%d_%H%M%S")
            fields = ['id', 'signal_id', 'symbol', 'direction', 'entry_price', 'close_price', 'quantity', 'leverage',
                      'margin', 'position_value', 'pnl', 'pnl_percent', 'commission', 'close_reason', 'opened_at',
                      'closed_at', 'status']

            if fmt == 'csv':
                filename = export_dir / f"trades_{timestamp}.csv"
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fields)
                    writer.writeheader()
                    for trade in trades:
                        row = {field: trade.get(field, '') for field in fields}
                        writer.writerow(row)
                print(f"{Fore.GREEN}✅ Файл сохранён: {filename}{Style.RESET_ALL}")
                return str(filename)
            elif fmt == 'json':
                filename = export_dir / f"trades_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(trades, f, ensure_ascii=False, indent=2, default=str)
                print(f"{Fore.GREEN}✅ Файл сохранён: {filename}{Style.RESET_ALL}")
                return str(filename)
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка экспорта: {e}")
            return None

    async def export_stats(self) -> str:
        try:
            stats = await self.signal_repo.get_database_stats() if self.signal_repo else {}
            export_dir = self._get_export_dir()
            timestamp = now().strftime("%Y%m%d_%H%M%S")
            filename = export_dir / f"stats_{timestamp}.txt"

            content = f"""
========================================
СТАТИСТИКА ТОРГОВОГО БОТА
Дата: {format_local(now())}
========================================

СИГНАЛЫ:
  Всего сигналов:      {stats.get('total_signals', 0)}
  WATCH:               {stats.get('subtypes_stats', {}).get('WATCH', 0)}
  M15:                 {stats.get('subtypes_stats', {}).get('M15', 0)}
  Активных:            {stats.get('active_signals', 0)}
  Отклонённых:         {stats.get('rejected_signals', 0)}
  BUY:                 {stats.get('buy_signals', 0)}
  SELL:                {stats.get('sell_signals', 0)}

СДЕЛКИ:
  Закрытых сделок:     {stats.get('closed_trades', 0)}
  Прибыльных:          {stats.get('winning_trades', 0)}
  Убыточных:           {stats.get('losing_trades', 0)}
  Общий PnL:           {stats.get('total_pnl', 0):.2f}
  Win Rate:            {stats.get('win_rate', 0):.1f}%

ТРЕНДЫ (Фаза 1.3.10):
  Всего анализов:      {stats.get('total_trends', 0)}
"""
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"{Fore.GREEN}✅ Файл сохранён: {filename}{Style.RESET_ALL}")
            return str(filename)
        except Exception as e:
            logger.error(f"❌ Ошибка экспорта: {e}")
            return ""

    async def export_all(self) -> str:
        try:
            export_dir = self._get_export_dir()
            timestamp = now().strftime("%Y%m%d_%H%M%S")
            zip_path = export_dir / f"full_export_{timestamp}.zip"

            temp_files = []
            signals_file = await self.export_signals('csv')
            if signals_file:
                temp_files.append(signals_file)
            trades_file = await self.export_trades('csv')
            if trades_file:
                temp_files.append(trades_file)
            stats_file = await self.export_stats()
            if stats_file:
                temp_files.append(stats_file)

            if not temp_files:
                print(f"{Fore.YELLOW}⚠️ Нет данных для архивации{Style.RESET_ALL}")
                return ""

            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for file_path in temp_files:
                    zipf.write(file_path, os.path.basename(file_path))

            print(f"{Fore.GREEN}✅ ZIP архив создан: {zip_path}{Style.RESET_ALL}")
            return str(zip_path)
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return ""

    async def export_menu(self):
        while True:
            self.clear_screen()
            self.print_header("📁 ЭКСПОРТ ДАННЫХ")
            export_dir = self._get_export_dir()
            print(f"{Fore.YELLOW}Выберите формат экспорта:{Style.RESET_ALL}")
            print("1. 📊 Экспорт сигналов (CSV)")
            print("2. 📊 Экспорт сигналов (JSON)")
            print("3. 📈 Экспорт сделок (CSV)")
            print("4. 📈 Экспорт сделок (JSON)")
            print("5. 📋 Экспорт статистики (TXT)")
            print("6. 📦 Экспорт всех данных (ZIP)")
            print("0. 🔙 Назад")
            print(f"{Fore.CYAN}📂 Папка экспорта: {export_dir}{Style.RESET_ALL}")

            try:
                choice = input(f"\n{Fore.CYAN}🎯 Выбор (0-6): {Style.RESET_ALL}").strip()
                if choice == '1':
                    await self.export_signals('csv')
                    await asyncio.sleep(2)
                elif choice == '2':
                    await self.export_signals('json')
                    await asyncio.sleep(2)
                elif choice == '3':
                    await self.export_trades('csv')
                    await asyncio.sleep(2)
                elif choice == '4':
                    await self.export_trades('json')
                    await asyncio.sleep(2)
                elif choice == '5':
                    await self.export_stats()
                    await asyncio.sleep(2)
                elif choice == '6':
                    await self.export_all()
                    await asyncio.sleep(2)
                elif choice == '0':
                    break
            except KeyboardInterrupt:
                break

    async def get_current_price(self, symbol: str) -> Optional[float]:
        if not self.data_provider:
            return None
        try:
            return await self.data_provider.get_current_price(symbol)
        except Exception as e:
            logger.error(f"❌ Ошибка получения цены {symbol}: {e}")
            return None

    async def get_account_state(self) -> Dict[str, Any]:
        state = {
            'initial_balance': self.starting_balance,
            'current_balance': self.starting_balance,
            'used_margin': 0.0,
            'reserved_for_watch': 0.0,
            'available': self.starting_balance,
            'total_pnl': 0.0
        }

        try:
            if self.paper_account:
                await self.paper_account.cleanup_expired_reservations()

                state['current_balance'] = await self.paper_account.get_balance()
                positions = await self.paper_account.get_open_positions()
                for pos in positions.values():
                    state['used_margin'] += pos.margin
                stats = await self.paper_account.get_statistics()
                state['total_pnl'] = stats.get('total_pnl', 0)

            if self.signal_repo:
                watch_signals = await self.signal_repo.get_watch_signals_with_reserve()
                for watch in watch_signals:
                    reserved = watch.get('reserved_margin')
                    if reserved is not None:
                        state['reserved_for_watch'] += float(reserved)

            state['available'] = state['current_balance'] - state['used_margin'] - state['reserved_for_watch']

        except Exception as e:
            logger.error(f"❌ Ошибка получения состояния счёта: {e}")

        return state

    async def display_trades_table(self):
        """Отображение таблицы сделок"""
        try:
            if not self.trade_repo:
                print(f"{Fore.RED}❌ TradeRepository не инициализирован{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
                return

            trades = await self.trade_repo.get_closed_trades(limit=50)

            self.clear_screen()
            self.print_header("📈 ТАБЛИЦА СДЕЛОК")

            if not trades:
                print(f"{Fore.YELLOW}📭 Нет закрытых сделок{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
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

                table_data.append([
                    str(trade.get('id', '')),
                    trade.get('symbol', ''),
                    self.format_direction(trade.get('direction', '')),
                    self.format_price(trade.get('entry_price', 0)),
                    self.format_price(trade.get('close_price', 0)),
                    self.format_pnl(pnl),
                    f"{trade.get('pnl_percent', 0):+.2f}%" if trade.get('pnl_percent') else "-",
                    self.format_datetime(trade.get('closed_at', ''))
                ])

            headers = ["ID", "Монета", "Напр", "Entry", "Exit", "PnL", "PnL%", "Закрыта"]
            table = self.create_table(headers, table_data)
            print(table)

            print(f"\n{Fore.CYAN}📊 ИТОГИ:{Style.RESET_ALL}")
            print(f"   Всего сделок: {len(trades)}")
            print(f"   Прибыльных: {Fore.GREEN}{winning}{Style.RESET_ALL}")
            print(f"   Убыточных: {Fore.RED}{losing}{Style.RESET_ALL}")
            print(f"   Общий PnL: {self.format_pnl(total_pnl)}")

            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")

        except Exception as e:
            logger.error(f"❌ Ошибка отображения сделок: {e}")
            print(f"{Fore.RED}❌ Ошибка: {e}{Style.RESET_ALL}")
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")

    async def display_realtime_monitor(self):
        self._shutdown = False
        try:
            while not self._shutdown:
                self.clear_screen()
                self.print_header("THREE SCREEN ANALYZER - РЕАЛТАЙМ МОНИТОР (v1.3.10)")

                # ========== ФАЗА 1.5.0: ОТОБРАЖЕНИЕ РЕЖИМА ТОРГОВЛИ ==========
                trading_mode = self.config.get('trading_mode', 'pro')
                mode_color = Fore.GREEN if trading_mode == 'light' else Fore.CYAN
                print(f"{mode_color}🎯 Режим торговли: {trading_mode.upper()}{Style.RESET_ALL}")
                print()

                if not self.signal_repo:
                    print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                    break

                signals = await self.signal_repo.get_signals_with_trades(self.settings['signals_limit'])
                stats = await self.signal_repo.get_database_stats()
                account_state = await self.get_account_state()

                print(f"{Fore.YELLOW}💰 СОСТОЯНИЕ СЧЁТА:{Style.RESET_ALL}")
                print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
                print(
                    f"  Депозит (начальный):    {Fore.WHITE}{account_state['initial_balance']:.2f} USDT{Style.RESET_ALL}")
                print(
                    f"  Текущий баланс:         {Fore.WHITE}{account_state['current_balance']:.2f} USDT{Style.RESET_ALL}")
                print(
                    f"  Использовано маржи:     {Fore.YELLOW}{account_state['used_margin']:.2f} USDT{Style.RESET_ALL}")
                print(
                    f"  Зарезервировано (WATCH):{Fore.YELLOW}{account_state['reserved_for_watch']:.2f} USDT{Style.RESET_ALL}")
                print(
                    f"  Доступно средств:       {Fore.GREEN if account_state['available'] > 0 else Fore.RED}{account_state['available']:.2f} USDT{Style.RESET_ALL}")
                print(f"  Общий PnL:              {self.format_pnl(account_state['total_pnl'])}")
                print()

                print(f"{Fore.YELLOW}📊 СТАТИСТИКА СИСТЕМЫ:{Style.RESET_ALL}")
                print(f"   Всего сигналов: {stats.get('total_signals', 0)}")
                print(f"   WATCH: {stats.get('subtypes_stats', {}).get('WATCH', 0)}")
                print(f"   M15: {stats.get('subtypes_stats', {}).get('M15', 0)}")
                print(f"   Активных: {stats.get('active_signals', 0)}")
                print(f"   BUY: {stats.get('buy_signals', 0)}  |  SELL: {stats.get('sell_signals', 0)}")
                print(f"   Закрыто сделок: {stats.get('closed_trades', 0)}")
                print(f"   Общий PnL: {self.format_pnl(stats.get('total_pnl', 0))}")
                print(f"   Win Rate: {stats.get('win_rate', 0):.1f}%")
                print(f"   Анализов тренда: {stats.get('total_trends', 0)}")
                print()

                if not signals:
                    print(f"{Fore.YELLOW}📭 Нет сигналов в базе данных{Style.RESET_ALL}")
                else:
                    table_data = []
                    for signal in signals:
                        time_str = self.format_time(signal.get('created_time', ''))
                        date_str = self.format_date(signal.get('created_time', ''))
                        signal_subtype = signal.get('signal_subtype', '')
                        pnl_str = self.format_pnl(signal.get('trade_pnl', 0)) if signal.get(
                            'trade_pnl') is not None else "-"
                        zone_str = self.format_zone(signal.get('zone_low', 0), signal.get('zone_high', 0))
                        score_str = self.format_score(signal.get('screen2_score', 0))

                        current_price = signal.get('current_price_at_signal', 0)
                        if current_price == 0:
                            current_price = await self.get_current_price(signal.get('symbol', ''))

                        zone_low = signal.get('zone_low', 0)
                        zone_high = signal.get('zone_high', 0)

                        if current_price > 0 and zone_low > 0 and zone_high > 0:
                            if current_price > zone_high:
                                position_display = f"{Fore.GREEN}▲ ВЫШЕ{Style.RESET_ALL}"
                            elif current_price < zone_low:
                                position_display = f"{Fore.RED}▼ НИЖЕ{Style.RESET_ALL}"
                            else:
                                position_display = f"{Fore.YELLOW}● ВНУТРИ{Style.RESET_ALL}"
                            price_display = self.format_price(current_price)
                        else:
                            price_display = "-"
                            position_display = "-"

                        table_data.append([
                            str(signal.get('id', '')),
                            signal.get('symbol', ''),
                            self.format_screen(signal_subtype),
                            self.format_direction(signal.get('direction', '')),
                            self.format_status(signal.get('status', '')),
                            self.format_price(signal.get('entry_price', 0)),
                            zone_str,
                            score_str,
                            self.format_price(signal.get('stop_loss', 0)),
                            self.format_price(signal.get('take_profit', 0)),
                            self.format_rr_ratio(signal.get('risk_reward_ratio', 0)),
                            pnl_str,
                            price_display,
                            position_display,
                            f"{date_str} {time_str}"
                        ])

                    headers = ["ID", "Монета", "Тип", "Напр", "Статус", "Entry", "Зона", "Score", "SL", "TP", "R/R",
                               "PnL", "Цена", "Поз.", "Время"]
                    table = self.create_table(headers, table_data)
                    print(table)

                print(f"\n{Fore.CYAN}🔄 Автообновление через {self.settings['refresh_interval']} сек...")
                print(f"{Fore.YELLOW}Нажмите Enter для возврата в меню{Style.RESET_ALL}")

                try:
                    await asyncio.wait_for(self._wait_for_enter(), timeout=self.settings['refresh_interval'])
                    break
                except asyncio.TimeoutError:
                    continue

        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}🛑 Монитор остановлен{Style.RESET_ALL}")
        finally:
            self._shutdown = False

    async def display_signal_details(self, signal_id: int):
        """Детальная информация о сигнале с разделением планируемых/фактических данных"""
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                return

            signal = await self.signal_repo.get_signal_by_id(signal_id)
            if not signal:
                print(f"{Fore.RED}❌ Сигнал #{signal_id} не найден{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
                return

            current_price = None
            if self.data_provider:
                current_price = await self.get_current_price(signal.get('symbol', ''))
            if not current_price:
                current_price = signal.get('entry_price', 0)

            trade = None
            if self.trade_repo:
                trade = await self.trade_repo.get_trade_by_signal_id(signal_id)

            account_state = await self.get_account_state()

            self.clear_screen()
            self.print_header(f"ДЕТАЛИ СИГНАЛА #{signal_id}")

            signal_subtype = signal.get('signal_subtype', '')
            status = signal.get('status', '')

            print(f"{Fore.CYAN}🎯 ОСНОВНАЯ ИНФОРМАЦИЯ{Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            print(f"  Монета:        {Fore.WHITE}{signal.get('symbol', 'N/A')}{Style.RESET_ALL}")
            print(f"  Направление:   {self.format_direction(signal.get('direction', ''))}")
            print(f"  Тип сигнала:   {self.format_screen(signal_subtype)}")
            print(f"  Статус:        {self.format_status(status)}")
            print(f"  Уверенность:   {self.format_confidence(signal.get('confidence', 0))}")

            price_at_signal = signal.get('current_price_at_signal', 0)
            if price_at_signal > 0:
                print(f"  Цена при создании: {Fore.WHITE}{self.format_price(price_at_signal)}{Style.RESET_ALL}")

            position_vs_zone = signal.get('position_vs_zone', '')
            if position_vs_zone:
                if position_vs_zone == "ABOVE":
                    pos_color = Fore.GREEN
                    pos_text = "ВЫШЕ зоны"
                elif position_vs_zone == "BELOW":
                    pos_color = Fore.RED
                    pos_text = "НИЖЕ зоны"
                else:
                    pos_color = Fore.YELLOW
                    pos_text = "ВНУТРИ зоны"
                print(f"  Позиция цены:   {pos_color}{pos_text}{Style.RESET_ALL}")

            print(f"\n{Fore.CYAN}📋 ПЛАНИРУЕМЫЕ ПАРАМЕТРЫ (из сигнала){Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            print(
                f"  Планируемая цена входа: {Fore.WHITE}{self.format_price(signal.get('entry_price', 0))}{Style.RESET_ALL}")
            print(
                f"  Планируемый SL:         {Fore.RED}{self.format_price(signal.get('stop_loss', 0))}{Style.RESET_ALL}")
            print(
                f"  Планируемый TP:         {Fore.GREEN}{self.format_price(signal.get('take_profit', 0))}{Style.RESET_ALL}")

            if signal_subtype == 'WATCH':
                print(
                    f"  Зона входа (WATCH):     {self.format_zone(signal.get('zone_low', 0), signal.get('zone_high', 0))}")
                print(f"  Score Screen2:          {self.format_score(signal.get('screen2_score', 0))}")
                print(f"  Ожидаемый паттерн:      {Fore.WHITE}{signal.get('expected_pattern', '—')}{Style.RESET_ALL}")
            elif signal.get('trigger_pattern'):
                print(f"  Паттерн-триггер:        {Fore.WHITE}{signal.get('trigger_pattern', '—')}{Style.RESET_ALL}")

            print(f"\n{Fore.CYAN}⚡ ФАКТИЧЕСКОЕ ИСПОЛНЕНИЕ (из trades){Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")

            if trade:
                actual_entry = trade.get('entry_price', signal.get('entry_price', 0))
                print(f"  Реальная цена входа:   {Fore.WHITE}{self.format_price(actual_entry)}{Style.RESET_ALL}")

                position_size = trade.get('quantity', signal.get('position_size', 0))
                print(f"  Размер позиции:        {self.format_position_size(position_size)}")

                leverage = trade.get('leverage', signal.get('leverage', 10))
                if leverage is None:
                    leverage = 10
                if actual_entry > 0 and position_size > 0:
                    position_value = position_size * actual_entry
                    margin = position_value / leverage
                    print(f"  Плечо:                 {self.format_leverage(leverage)}")
                    print(f"  Стоимость позиции:     {Fore.YELLOW}{self.format_price(position_value)}{Style.RESET_ALL}")
                    print(f"  Маржа (залог):         {Fore.YELLOW}{self.format_price(margin)}{Style.RESET_ALL}")

                commission = trade.get('commission', 0)
                if commission is not None and commission > 0:
                    print(f"  Комиссия:              {Fore.YELLOW}{commission:.4f}{Style.RESET_ALL}")

                actual_sl = trade.get('stop_loss')
                actual_tp = trade.get('take_profit')
                if actual_sl and actual_sl != signal.get('stop_loss', 0):
                    print(f"  Реальный SL:           {Fore.RED}{self.format_price(actual_sl)}{Style.RESET_ALL}")
                if actual_tp and actual_tp != signal.get('take_profit', 0):
                    print(f"  Реальный TP:           {Fore.GREEN}{self.format_price(actual_tp)}{Style.RESET_ALL}")

                pnl = trade.get('pnl', 0)
                pnl_pct = trade.get('pnl_percent', 0)
                if pnl != 0:
                    pnl_pct_str = f"{pnl_pct:+.2f}%" if pnl_pct is not None else "0.00%"
                    print(f"  PnL:                   {self.format_pnl(pnl)} ({pnl_pct_str})")

                opened_at = trade.get('opened_at')
                closed_at = trade.get('closed_at')
                if opened_at:
                    print(f"  Открыта:               {self.format_datetime(opened_at)}")
                if closed_at:
                    print(f"  Закрыта:               {self.format_datetime(closed_at)}")

            else:
                print(f"  {Fore.YELLOW}Нет данных об исполнении (сигнал не активирован){Style.RESET_ALL}")

            print(f"\n{Fore.CYAN}⏱ ВРЕМЕННАЯ ШКАЛА{Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")

            timeline_entries = []

            created = signal.get('created_time')
            if created:
                timeline_entries.append(("📝 Сигнал создан", self.format_datetime(created)))

            expiration = signal.get('expiration_time')
            if expiration:
                exp_utc = datetime.fromisoformat(expiration)
                now_utc = datetime.utcnow()

                if exp_utc > now_utc:
                    remaining = exp_utc - now_utc
                    hours = remaining.seconds // 3600
                    minutes = (remaining.seconds % 3600) // 60
                    timeline_entries.append(
                        ("⏰ Сигнал истекает", f"{self.format_datetime(expiration)} (через {hours}ч {minutes}м)"))
                else:
                    timeline_entries.append(("⏰ Сигнал истёк", self.format_datetime(expiration)))

            if trade and trade.get('opened_at'):
                timeline_entries.append(("⚡ Позиция открыта", self.format_datetime(trade.get('opened_at'))))

            if trade and trade.get('closed_at'):
                timeline_entries.append(("🔒 Позиция закрыта", self.format_datetime(trade.get('closed_at'))))

            updated = signal.get('updated_time')
            if updated and updated != created:
                timeline_entries.append(("🔄 Сигнал обновлён", self.format_datetime(updated)))

            for label, time_str in timeline_entries:
                print(f"  {label:<20} {time_str}")

            print(f"\n{Fore.CYAN}💰 СОСТОЯНИЕ СЧЁТА{Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            print(f"  Депозит (начальный):    {Fore.WHITE}{account_state['initial_balance']:.2f} USDT{Style.RESET_ALL}")
            print(f"  Текущий баланс:         {Fore.WHITE}{account_state['current_balance']:.2f} USDT{Style.RESET_ALL}")
            print(f"  Использовано маржи:     {Fore.YELLOW}{account_state['used_margin']:.2f} USDT{Style.RESET_ALL}")
            print(
                f"  Зарезервировано (WATCH):{Fore.YELLOW}{account_state['reserved_for_watch']:.2f} USDT{Style.RESET_ALL}")
            print(
                f"  Доступно средств:       {Fore.GREEN if account_state['available'] > 0 else Fore.RED}{account_state['available']:.2f} USDT{Style.RESET_ALL}")
            print(f"  Общий PnL:              {self.format_pnl(account_state['total_pnl'])}")

            print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата в меню...{Style.RESET_ALL}")

        except Exception as e:
            print(f"{Fore.RED}❌ Ошибка при отображении деталей: {e}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")

    async def display_all_signals(self):
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                return

            monitor_config = self.config.get('monitor', {})
            signals_limit = monitor_config.get('signals_limit', 200)

            signals = await self.signal_repo.get_signals(limit=signals_limit)
            self.clear_screen()
            self.print_header("ВСЕ СИГНАЛЫ")

            if not signals:
                print(f"{Fore.YELLOW}📭 Нет сигналов{Style.RESET_ALL}")
            else:
                table_data = []
                for signal in signals:
                    time_str = self.format_time(signal.get('created_time', ''))
                    date_str = self.format_date(signal.get('created_time', ''))
                    signal_subtype = signal.get('signal_subtype', '')
                    zone_str = self.format_zone(signal.get('zone_low', 0), signal.get('zone_high', 0))
                    score_str = self.format_score(signal.get('screen2_score', 0))

                    current_price = signal.get('current_price_at_signal', 0)
                    price_display = self.format_price(current_price) if current_price > 0 else "-"

                    trade_pnl = signal.get('trade_pnl')
                    pnl_display = self.format_pnl(trade_pnl) if trade_pnl is not None else "-"

                    table_data.append([
                        str(signal.get('id', '')),
                        signal.get('symbol', ''),
                        self.format_screen(signal_subtype),
                        self.format_direction(signal.get('direction', '')),
                        self.format_status(signal.get('status', '')),
                        self.format_price(signal.get('entry_price', 0)),
                        zone_str,
                        score_str,
                        price_display,
                        pnl_display,
                        self.format_rr_ratio(signal.get('risk_reward_ratio', 0)),
                        f"{date_str} {time_str}"
                    ])

                headers = ["ID", "Монета", "Тип", "Напр", "Статус", "Entry", "Зона", "Score", "Цена при сигнале", "PnL",
                           "R/R", "Время"]
                table = self.create_table(headers, table_data)
                print(table)

                active_count = sum(1 for s in signals if s.get('status', '').lower() in ['watch', 'active'])
                watch_count = sum(1 for s in signals if s.get('signal_subtype', '') == 'WATCH')
                m15_count = sum(1 for s in signals if s.get('signal_subtype', '') == 'M15')
                buy_count = sum(1 for s in signals if s.get('direction', '').lower() == 'buy')
                sell_count = sum(1 for s in signals if s.get('direction', '').lower() == 'sell')

                print(f"\n{Fore.CYAN}📈 СТАТИСТИКА:{Style.RESET_ALL}")
                print(f"   Всего сигналов: {len(signals)}")
                print(f"   Активных: {active_count}")
                print(f"   WATCH: {watch_count} | M15: {m15_count}")
                print(f"   BUY: {buy_count} | SELL: {sell_count}")

        except Exception as e:
            print(f"{Fore.RED}❌ Ошибка: {e}{Style.RESET_ALL}")
        input(f"\n{Fore.GREEN}Нажмите Enter для продолжения...{Style.RESET_ALL}")

    @staticmethod
    async def _wait_for_enter():
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, input)

    async def display_database_stats(self):
        if not self.signal_repo:
            print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
            return

        stats = await self.signal_repo.get_database_stats()
        self.clear_screen()
        self.print_header("СТАТИСТИКА БАЗЫ ДАННЫХ")

        print(f"{Fore.YELLOW}📊 СТАТИСТИКА СИГНАЛОВ:{Style.RESET_ALL}")
        print(f"  Всего сигналов:      {stats.get('total_signals', 0)}")
        print(f"  WATCH:               {stats.get('subtypes_stats', {}).get('WATCH', 0)}")
        print(f"  M15:                 {stats.get('subtypes_stats', {}).get('M15', 0)}")
        print(f"  Активных:            {stats.get('active_signals', 0)}")
        print(f"  BUY:                 {stats.get('buy_signals', 0)}")
        print(f"  SELL:                {stats.get('sell_signals', 0)}")

        print(f"\n{Fore.YELLOW}📈 СТАТИСТИКА СДЕЛОК:{Style.RESET_ALL}")
        print(f"  Закрытых сделок:      {stats.get('closed_trades', 0)}")
        print(f"  Общий PnL:            {self.format_pnl(stats.get('total_pnl', 0))}")
        print(f"  Win Rate:             {stats.get('win_rate', 0):.1f}%")

        print(f"\n{Fore.YELLOW}📊 СТАТИСТИКА ТРЕНДОВ (Фаза 1.3.10):{Style.RESET_ALL}")
        print(f"  Всего анализов:       {stats.get('total_trends', 0)}")

        print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
        input(f"\n{Fore.GREEN}Нажмите Enter для продолжения...{Style.RESET_ALL}")

    # ========== ФАЗА 1.3.10: ОТОБРАЖЕНИЕ ТРЕНДОВ ==========

    async def display_trend_analysis(self):
        """Отображение анализа тренда D1 (Фаза 1.3.10)"""
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
                return

            trends = await self.signal_repo.get_latest_trends(limit=50)
            trends = sorted(trends, key=lambda x: x.get('symbol', ''))

            self.clear_screen()
            self.print_header("📊 АНАЛИЗ ТРЕНДА (D1)")

            if not trends:
                print(f"{Fore.YELLOW}📭 Нет данных о трендах{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
                return

            bull_count = sum(1 for t in trends if t.get('trend_direction') == 'BULL')
            bear_count = sum(1 for t in trends if t.get('trend_direction') == 'BEAR')
            sideways_count = sum(1 for t in trends if t.get('trend_direction') == 'SIDEWAYS')
            strong_trends = sum(1 for t in trends if t.get('adx', 0) > 25)

            print(f"{Fore.YELLOW}📈 СТАТИСТИКА ТРЕНДОВ:{Style.RESET_ALL}")
            print(
                f"   BULL: {Fore.GREEN}{bull_count}{Style.RESET_ALL} | BEAR: {Fore.RED}{bear_count}{Style.RESET_ALL} | SIDEWAYS: {Fore.YELLOW}{sideways_count}{Style.RESET_ALL}")
            print(f"   Сильных трендов (ADX > 25): {Fore.CYAN}{strong_trends}{Style.RESET_ALL} из {len(trends)}")
            print()

            table_data = []
            for idx, trend in enumerate(trends, 1):
                symbol = trend.get('symbol', '')
                db_id = trend.get('id', '')
                direction = trend.get('trend_direction', '')
                adx = trend.get('adx', 0)
                ema20 = trend.get('ema20', 0)
                ema50 = trend.get('ema50', 0)
                macd = trend.get('macd_line', 0)
                created = trend.get('created_time', '')

                if direction == 'BULL':
                    dir_display = f"{Fore.GREEN}▲ BULL{Style.RESET_ALL}"
                elif direction == 'BEAR':
                    dir_display = f"{Fore.RED}▼ BEAR{Style.RESET_ALL}"
                else:
                    dir_display = f"{Fore.YELLOW}● SIDEWAYS{Style.RESET_ALL}"

                if adx > 25:
                    adx_display = f"{Fore.GREEN}{adx:.1f}{Style.RESET_ALL}"
                elif adx > 20:
                    adx_display = f"{Fore.YELLOW}{adx:.1f}{Style.RESET_ALL}"
                else:
                    adx_display = f"{Fore.RED}{adx:.1f}{Style.RESET_ALL}"

                if macd > 0:
                    macd_display = f"{Fore.GREEN}+{macd:.2f}{Style.RESET_ALL}"
                elif macd < 0:
                    macd_display = f"{Fore.RED}{macd:.2f}{Style.RESET_ALL}"
                else:
                    macd_display = "0.00"

                table_data.append([
                    str(idx),
                    symbol,
                    dir_display,
                    adx_display,
                    self.format_price(ema20),
                    self.format_price(ema50),
                    macd_display,
                    self.format_time(created),
                    str(db_id)
                ])

            headers = ["#", "Монета", "Тренд", "ADX", "EMA20", "EMA50", "MACD", "Время", "DB_ID"]
            table = self.create_table(headers, table_data)
            print(table)

            print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}💡 Для просмотра деталей введите НОМЕР строки (#) или 0 для возврата{Style.RESET_ALL}")
            print(f"{Fore.CYAN}📊 Всего монет в анализе: {len(trends)}{Style.RESET_ALL}")

            try:
                choice = input(f"\n{Fore.CYAN}🔢 Номер строки (0 - назад): {Style.RESET_ALL}").strip()
                if choice and choice != '0':
                    row_num = int(choice)
                    if 1 <= row_num <= len(table_data):
                        db_id = int(table_data[row_num - 1][8])
                        await self.display_trend_details(db_id)
                        return
                    else:
                        print(f"{Fore.RED}❌ Неверный номер строки!{Style.RESET_ALL}")
                        await asyncio.sleep(1)
            except ValueError:
                pass

            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")

        except Exception as e:
            logger.error(f"❌ Ошибка отображения трендов: {e}")
            print(f"{Fore.RED}❌ Ошибка: {e}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")

    async def display_trend_details(self, trend_id: int):
        """Отображение детальной информации о тренде с пояснениями (Фаза 1.3.10)"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute("SELECT * FROM trend_analysis WHERE id = ?", (trend_id,))
                row = await cursor.fetchone()
                trend = dict(row) if row else None

            if not trend:
                print(f"{Fore.RED}❌ Тренд #{trend_id} не найден{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
                return

            self.clear_screen()
            symbol = trend.get('symbol', '')
            direction = trend.get('trend_direction', '')

            self.print_header(f"📊 ТРЕНД (D1) — {symbol} (ID: {trend_id})")

            if direction == 'BULL':
                dir_display = f"{Fore.GREEN}BULL (бычий){Style.RESET_ALL}"
                dir_comment = "восходящий"
                trade_hint = "Торгуем только LONG"
            elif direction == 'BEAR':
                dir_display = f"{Fore.RED}BEAR (медвежий){Style.RESET_ALL}"
                dir_comment = "нисходящий"
                trade_hint = "Торгуем только SHORT"
            else:
                dir_display = f"{Fore.YELLOW}SIDEWAYS (боковик){Style.RESET_ALL}"
                dir_comment = "без тренда"
                trade_hint = "Лучше воздержаться от торговли"

            print(f"  Направление:   {dir_display}")

            adx = trend.get('adx', 0)
            if adx > 25:
                adx_comment = ">25 = сильный тренд"
                adx_color = Fore.GREEN
            elif adx > 20:
                adx_comment = "20-25 = умеренный тренд"
                adx_color = Fore.YELLOW
            else:
                adx_comment = "<20 = слабый тренд или флэт"
                adx_color = Fore.RED

            print(f"  ADX:           {adx_color}{adx:.1f}{Style.RESET_ALL} ({adx_comment})")

            ema20 = trend.get('ema20', 0)
            ema50 = trend.get('ema50', 0)
            print(f"  EMA20:         {self.format_price(ema20)}")
            print(f"  EMA50:         {self.format_price(ema50)}")

            if ema20 > ema50:
                ema_comment = "EMA20 выше EMA50 → бычий сигнал"
                ema_color = Fore.GREEN
            elif ema20 < ema50:
                ema_comment = "EMA20 ниже EMA50 → медвежий сигнал"
                ema_color = Fore.RED
            else:
                ema_comment = "EMA20 и EMA50 близки → неопределённость"
                ema_color = Fore.YELLOW
            print(f"                  💡 {ema_color}{ema_comment}{Style.RESET_ALL}")

            macd_line = trend.get('macd_line', 0)
            macd_signal = trend.get('macd_signal', 0)

            if macd_line > 0:
                macd_line_display = f"{Fore.GREEN}{macd_line:.2f}{Style.RESET_ALL}"
            elif macd_line < 0:
                macd_line_display = f"{Fore.RED}{macd_line:.2f}{Style.RESET_ALL}"
            else:
                macd_line_display = f"{Fore.YELLOW}0.00{Style.RESET_ALL}"

            if macd_signal > 0:
                macd_signal_display = f"{Fore.GREEN}{macd_signal:.2f}{Style.RESET_ALL}"
            elif macd_signal < 0:
                macd_signal_display = f"{Fore.RED}{macd_signal:.2f}{Style.RESET_ALL}"
            else:
                macd_signal_display = f"{Fore.YELLOW}0.00{Style.RESET_ALL}"

            print(f"  MACD линия:    {macd_line_display}")
            print(f"  Сигнальная:    {macd_signal_display}")

            if macd_line > macd_signal:
                macd_comment = "MACD выше сигнальной → бычий сигнал"
                macd_color = Fore.GREEN
            elif macd_line < macd_signal:
                macd_comment = "MACD ниже сигнальной → медвежий сигнал"
                macd_color = Fore.RED
            else:
                macd_comment = "MACD на уровне сигнальной → ждём"
                macd_color = Fore.YELLOW
            print(f"                  💡 {macd_color}{macd_comment}{Style.RESET_ALL}")

            confidence = trend.get('confidence', 0)
            print(f"  Уверенность:   {self.format_confidence(confidence)}")

            created = trend.get('created_time', '')
            print(f"  Время анализа: {self.format_datetime(created)}")

            print(f"\n{Fore.CYAN}{'─' * 60}{Style.RESET_ALL}")
            print(f"  💡 Комментарий: Тренд {dir_comment}, ADX {adx:.1f}.")

            if direction == 'BULL':
                print(f"     EMA и MACD подтверждают рост.")
            elif direction == 'BEAR':
                print(f"     EMA и MACD подтверждают падение.")
            else:
                print(f"     Индикаторы не дают чёткого направления.")

            print(f"     {trade_hint}.")

            print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")

        except Exception as e:
            logger.error(f"❌ Ошибка отображения деталей тренда: {e}")
            print(f"{Fore.RED}❌ Ошибка: {e}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")

    async def main_menu(self):
        while True:
            self.clear_screen()
            self.print_header("THREE SCREEN ANALYZER - МОНИТОР СИГНАЛОВ (v1.3.10)")

            # ========== ФАЗА 1.5.0: ОТОБРАЖЕНИЕ РЕЖИМА ТОРГОВЛИ ==========
            trading_mode = self.config.get('trading_mode', 'pro')
            mode_color = Fore.GREEN if trading_mode == 'light' else Fore.CYAN
            print(f"{mode_color}🎯 Режим торговли: {trading_mode.upper()}{Style.RESET_ALL}")

            account_state = await self.get_account_state()

            print(f"{Fore.YELLOW}💰 СОСТОЯНИЕ СЧЁТА:{Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            print(f"  Депозит (начальный):    {Fore.WHITE}{account_state['initial_balance']:.2f} USDT{Style.RESET_ALL}")
            print(f"  Текущий баланс:         {Fore.WHITE}{account_state['current_balance']:.2f} USDT{Style.RESET_ALL}")
            print(f"  Использовано маржи:     {Fore.YELLOW}{account_state['used_margin']:.2f} USDT{Style.RESET_ALL}")
            print(
                f"  Зарезервировано (WATCH):{Fore.YELLOW}{account_state['reserved_for_watch']:.2f} USDT{Style.RESET_ALL}")
            print(
                f"  Доступно средств:       {Fore.GREEN if account_state['available'] > 0 else Fore.RED}{account_state['available']:.2f} USDT{Style.RESET_ALL}")
            print(f"  Общий PnL:              {self.format_pnl(account_state['total_pnl'])}")
            print()

            print(f"{Fore.YELLOW}📋 ВЫБЕРИТЕ РЕЖИМ РАБОТЫ:{Style.RESET_ALL}")
            print("1. 📊 Реалтайм монитор (WATCH + M15)")
            print("2. 📋 Таблица всех сигналов")
            print("3. 📈 Таблица сделок (trades)")
            print("4. 🔍 Детали сигнала по ID")
            print("5. 📊 Анализ тренда (D1)")
            print("6. 📊 Статистика БД")
            print("7. 💾 Экспорт данных")
            print("8. 🚪 Выход")
            print()
            print(f"{Fore.CYAN}🕐 Локальное время: UTC+{self.timezone_offset}{Style.RESET_ALL}")
            export_dir = self._get_export_dir()
            print(f"{Fore.CYAN}📂 Папка экспорта: {export_dir}{Style.RESET_ALL}")

            try:
                choice = input(f"\n{Fore.CYAN}🎯 Выбор (1-8): {Style.RESET_ALL}").strip()

                if choice == '1':
                    await self.display_realtime_monitor()
                elif choice == '2':
                    await self.display_all_signals()
                elif choice == '3':
                    await self.display_trades_table()
                elif choice == '4':
                    try:
                        signal_id = int(input(f"{Fore.CYAN}🔢 Введите ID сигнала: {Style.RESET_ALL}"))
                        await self.display_signal_details(signal_id)
                    except ValueError:
                        print(f"{Fore.RED}❌ Введите корректный ID{Style.RESET_ALL}")
                        await asyncio.sleep(1)
                elif choice == '5':
                    await self.display_trend_analysis()
                elif choice == '6':
                    await self.display_database_stats()
                elif choice == '7':
                    await self.export_menu()
                elif choice == '8':
                    print(f"{Fore.GREEN}👋 До свидания!{Style.RESET_ALL}")
                    break
                else:
                    print(f"{Fore.RED}❌ Неверный выбор!{Style.RESET_ALL}")
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                print(f"\n{Fore.YELLOW}🛑 Выход...{Style.RESET_ALL}")
                break

    async def run(self):
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не найден{Style.RESET_ALL}")
                return

            if not await self.signal_repo.initialize():
                print(f"{Fore.RED}❌ Не удалось инициализировать БД{Style.RESET_ALL}")
                return

            if self.trade_repo:
                await self.trade_repo.initialize()

            print(f"{Fore.GREEN}✅ База данных подключена: {self.db_path}{Style.RESET_ALL}")
            await self.main_menu()
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}🛑 Монитор остановлен{Style.RESET_ALL}")


async def main():
    print(f"{Fore.GREEN}🚀 Запуск Three Screen Analyzer Monitor (v1.3.10)...{Style.RESET_ALL}")
    monitor = ThreeScreenMonitor()
    await monitor.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}👋 Программа завершена{Style.RESET_ALL}")