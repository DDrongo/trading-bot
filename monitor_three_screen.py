#!/usr/bin/env python3
# monitor_three_screen.py (ПОЛНАЯ ИСПРАВЛЕННАЯ ВЕРСИЯ)
"""
🎯 МОНИТОР ДЛЯ THREE SCREEN ANALYZER - ВЕРСИЯ 1.3.6
Особенности:
- Отображение WATCH и M15 сигналов
- Показ зон входа (zone_low/zone_high)
- Показ score Screen2
- ✅ ЭКСПОРТ ДАННЫХ в CSV/JSON/TXT/ZIP
- ✅ Единая папка экспорта: logs/exports/
"""

import asyncio
import logging
import os
import re
import yaml
import csv
import json
import zipfile
from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime, timedelta
from colorama import init, Fore, Style

init()
logger = logging.getLogger('three_screen_monitor')


class ThreeScreenMonitor:
    def __init__(self, config=None):
        if config is None:
            config_path = Path(__file__).parent / 'analyzer/config/config.yaml'
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
            except Exception as e:
                print(f"❌ Ошибка загрузки конфига: {e}")
                config = {}

        self.config = config

        try:
            from analyzer.core.signal_repository import signal_repository
            self.signal_repo = signal_repository
        except ImportError as e:
            print(f"❌ Ошибка импорта signal_repository: {e}")
            self.signal_repo = None

        try:
            from analyzer.core.trade_repository import trade_repository
            self.trade_repo = trade_repository
        except ImportError as e:
            print(f"❌ Ошибка импорта trade_repository: {e}")
            self.trade_repo = None

        display_config = self.config.get('display', {})
        self.timezone_offset = display_config.get('timezone_offset', 3)
        self.refresh_interval = display_config.get('refresh_interval', 5)

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

    def strip_ansi(self, text: str) -> str:
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        return ansi_escape.sub('', str(text))

    def get_visible_length(self, text: str) -> int:
        return len(self.strip_ansi(str(text)))

    def clear_screen(self):
        os.system('cls' if os.name == 'nt' else 'clear')

    def print_header(self, title: str):
        print(f"\n{Fore.CYAN}{Style.BRIGHT}{'=' * 80}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{Style.BRIGHT}{title}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 80}{Style.RESET_ALL}")

    def utc_to_local(self, utc_str: str) -> datetime:
        if not utc_str:
            return datetime.now()
        try:
            utc_time = datetime.fromisoformat(utc_str.replace('Z', '+00:00'))
            local_time = utc_time + timedelta(hours=self.timezone_offset)
            return local_time
        except Exception:
            return datetime.now()

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

    def format_direction(self, direction: str) -> str:
        direction_lower = direction.lower() if direction else ''
        if direction_lower in ['buy', 'long']:
            return f"{Fore.GREEN}▲ BUY{Style.RESET_ALL}"
        elif direction_lower in ['sell', 'short']:
            return f"{Fore.RED}▼ SELL{Style.RESET_ALL}"
        return direction or 'N/A'

    def format_status(self, status: str) -> str:
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

    def format_confidence(self, confidence: float) -> str:
        if confidence is None:
            return "-"

        if confidence > 0.8:
            color = Fore.GREEN
        elif confidence > 0.6:
            color = Fore.YELLOW
        else:
            color = Fore.RED

        return f"{color}{confidence * 100:.1f}%{Style.RESET_ALL}"

    def format_price(self, price: float) -> str:
        if price is None or price == 0:
            return "-"
        if price >= 1:
            return f"{price:.2f}"
        return f"{price:.6f}"

    def format_rr_ratio(self, rr: float) -> str:
        if rr is None or rr == 0:
            return "-"

        if rr >= 3.0:
            color = Fore.GREEN
        elif rr >= 2.0:
            color = Fore.YELLOW
        else:
            color = Fore.RED

        return f"{color}{rr:.2f}:1{Style.RESET_ALL}"

    def format_pnl(self, pnl: float) -> str:
        if pnl is None:
            return "-"
        if pnl > 0:
            return f"{Fore.GREEN}+{pnl:.2f}{Style.RESET_ALL}"
        elif pnl < 0:
            return f"{Fore.RED}{pnl:.2f}{Style.RESET_ALL}"
        else:
            return f"{Fore.YELLOW}0.00{Style.RESET_ALL}"

    def format_trend(self, trend: str) -> str:
        if not trend:
            return "-"
        trend_upper = trend.upper()
        if trend_upper == 'BULL':
            return f"{Fore.GREEN}BULL{Style.RESET_ALL}"
        elif trend_upper == 'BEAR':
            return f"{Fore.RED}BEAR{Style.RESET_ALL}"
        return trend

    def format_screen(self, screen: str) -> str:
        if not screen:
            return "-"

        if screen.upper() == 'WATCH':
            return f"{Fore.YELLOW}WATCH{Style.RESET_ALL}"
        elif screen.upper() == 'M15':
            return f"{Fore.GREEN}M15{Style.RESET_ALL}"
        elif screen.upper() == 'D1':
            return f"{Fore.CYAN}D1{Style.RESET_ALL}"
        elif screen.upper() == 'H4':
            return f"{Fore.YELLOW}H4{Style.RESET_ALL}"
        return screen

    def format_zone(self, zone_low: float, zone_high: float) -> str:
        if zone_low is None or zone_high is None or zone_low == 0 or zone_high == 0:
            return "-"
        return f"{self.format_price(zone_low)}-{self.format_price(zone_high)}"

    def format_score(self, score: int) -> str:
        if score is None or score == 0:
            return "-"
        if score >= 4:
            return f"{Fore.GREEN}{score}/5{Style.RESET_ALL}"
        elif score >= 3:
            return f"{Fore.YELLOW}{score}/5{Style.RESET_ALL}"
        else:
            return f"{Fore.RED}{score}/5{Style.RESET_ALL}"

    def format_position_size(self, size: float) -> str:
        if size is None or size == 0:
            return "-"
        return f"{size:.4f}"

    def create_table(self, headers: List[str], data: List[List[str]]) -> str:
        if not data:
            return "Нет данных"

        col_widths = []
        for i in range(len(headers)):
            max_width = len(headers[i])
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

    async def get_current_price(self, symbol: str) -> Optional[float]:
        try:
            from analyzer.core.api_client_bybit import BybitAPIClient
            api_config = self.config.get('api', {})
            client = BybitAPIClient(api_config)
            return await client.get_current_price(symbol)
        except Exception as e:
            return None

    # ========== МЕТОДЫ ЭКСПОРТА ==========

    def _get_export_dir(self) -> Path:
        """Создаёт и возвращает папку для экспорта (единая папка logs/exports)"""
        project_root = Path(__file__).parent
        export_dir = project_root / 'logs' / 'exports'
        export_dir.mkdir(parents=True, exist_ok=True)
        return export_dir

    def _get_file_size(self, file_path: str) -> str:
        """Возвращает человеко-читаемый размер файла"""
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
        """Открывает папку с файлом в проводнике/файндере"""
        folder = os.path.dirname(file_path)
        try:
            if os.name == 'nt':  # Windows
                os.startfile(folder)
            elif os.name == 'posix':  # macOS/Linux
                if os.system(f'open "{folder}"') != 0:
                    os.system(f'xdg-open "{folder}"')
            print(f"{Fore.GREEN}📂 Папка открыта: {folder}{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.YELLOW}⚠️ Не удалось открыть папку: {e}{Style.RESET_ALL}")
            print(f"📁 Путь: {folder}")

    async def export_signals(self, format: str = 'csv') -> Optional[str]:
        """
        Экспорт таблицы сигналов в файл.
        Форматы: csv, json
        """
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                return None

            signals = await self.signal_repo.get_signals(limit=1000)

            if not signals:
                print(f"{Fore.YELLOW}⚠️ Нет данных для экспорта{Style.RESET_ALL}")
                return None

            export_dir = self._get_export_dir()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            fields = [
                'id', 'symbol', 'direction', 'signal_subtype', 'status',
                'entry_price', 'stop_loss', 'take_profit', 'risk_reward_ratio',
                'zone_low', 'zone_high', 'screen2_score', 'expected_pattern',
                'trigger_pattern', 'confidence', 'created_time', 'expiration_time'
            ]

            if format == 'csv':
                filename = export_dir / f"signals_{timestamp}.csv"
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fields)
                    writer.writeheader()
                    for signal in signals:
                        row = {field: signal.get(field, '') for field in fields}
                        writer.writerow(row)

                print(f"{Fore.GREEN}✅ Файл сохранён: {filename}{Style.RESET_ALL}")
                print(f"📊 Размер: {self._get_file_size(str(filename))}")
                print(f"📈 Всего записей: {len(signals)}")

                return str(filename)

            elif format == 'json':
                filename = export_dir / f"signals_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(signals, f, ensure_ascii=False, indent=2, default=str)

                print(f"{Fore.GREEN}✅ Файл сохранён: {filename}{Style.RESET_ALL}")
                print(f"📊 Размер: {self._get_file_size(str(filename))}")
                print(f"📈 Всего записей: {len(signals)}")

                return str(filename)

            else:
                print(f"{Fore.RED}❌ Неподдерживаемый формат: {format}{Style.RESET_ALL}")
                return None

        except Exception as e:
            logger.error(f"❌ Ошибка экспорта сигналов: {e}")
            print(f"{Fore.RED}❌ Ошибка экспорта: {e}{Style.RESET_ALL}")
            return None

    async def export_trades(self, format: str = 'csv') -> Optional[str]:
        """Экспорт таблицы сделок в файл"""
        try:
            if not self.trade_repo:
                print(f"{Fore.RED}❌ TradeRepository не инициализирован{Style.RESET_ALL}")
                return None

            trades = await self.trade_repo.get_closed_trades(limit=1000)

            if not trades:
                print(f"{Fore.YELLOW}⚠️ Нет данных для экспорта{Style.RESET_ALL}")
                return None

            export_dir = self._get_export_dir()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            fields = [
                'id', 'signal_id', 'symbol', 'direction', 'entry_price',
                'close_price', 'quantity', 'pnl', 'pnl_percent', 'commission',
                'close_reason', 'opened_at', 'closed_at', 'status'
            ]

            if format == 'csv':
                filename = export_dir / f"trades_{timestamp}.csv"
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fields)
                    writer.writeheader()
                    for trade in trades:
                        row = {field: trade.get(field, '') for field in fields}
                        writer.writerow(row)

                print(f"{Fore.GREEN}✅ Файл сохранён: {filename}{Style.RESET_ALL}")
                print(f"📊 Размер: {self._get_file_size(str(filename))}")
                print(f"📈 Всего записей: {len(trades)}")

                return str(filename)

            elif format == 'json':
                filename = export_dir / f"trades_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(trades, f, ensure_ascii=False, indent=2, default=str)

                print(f"{Fore.GREEN}✅ Файл сохранён: {filename}{Style.RESET_ALL}")
                print(f"📊 Размер: {self._get_file_size(str(filename))}")
                print(f"📈 Всего записей: {len(trades)}")

                return str(filename)

            else:
                print(f"{Fore.RED}❌ Неподдерживаемый формат: {format}{Style.RESET_ALL}")
                return None

        except Exception as e:
            logger.error(f"❌ Ошибка экспорта сделок: {e}")
            print(f"{Fore.RED}❌ Ошибка экспорта: {e}{Style.RESET_ALL}")
            return None

    async def export_stats(self) -> str:
        """Экспорт статистики в текстовый файл"""
        try:
            stats = await self.signal_repo.get_database_stats() if self.signal_repo else {}

            export_dir = self._get_export_dir()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = export_dir / f"stats_{timestamp}.txt"

            content = f"""
========================================
СТАТИСТИКА ТОРГОВОГО БОТА
Дата: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
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

ДЕТАЛИ ПО СТАТУСАМ:
"""
            for status, count in stats.get('status_stats', {}).items():
                content += f"  {status}: {count}\n"

            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)

            print(f"{Fore.GREEN}✅ Файл сохранён: {filename}{Style.RESET_ALL}")
            print(f"📊 Размер: {self._get_file_size(str(filename))}")

            return str(filename)

        except Exception as e:
            logger.error(f"❌ Ошибка экспорта статистики: {e}")
            print(f"{Fore.RED}❌ Ошибка экспорта: {e}{Style.RESET_ALL}")
            return ""

    async def export_all(self) -> str:
        """Экспорт всех данных в ZIP архив"""
        try:
            export_dir = self._get_export_dir()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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
            print(f"📊 Размер: {self._get_file_size(str(zip_path))}")
            print(f"📁 В архиве: {len(temp_files)} файлов")

            return str(zip_path)

        except Exception as e:
            logger.error(f"❌ Ошибка создания ZIP архива: {e}")
            print(f"{Fore.RED}❌ Ошибка: {e}{Style.RESET_ALL}")
            return ""

    async def export_menu(self):
        """Подменю экспорта данных"""
        while True:
            self.clear_screen()
            self.print_header("📁 ЭКСПОРТ ДАННЫХ")

            print(f"{Fore.YELLOW}Выберите формат экспорта:{Style.RESET_ALL}")
            print("1. 📊 Экспорт сигналов (CSV)")
            print("2. 📊 Экспорт сигналов (JSON)")
            print("3. 📈 Экспорт сделок (CSV)")
            print("4. 📈 Экспорт сделок (JSON)")
            print("5. 📋 Экспорт статистики (TXT)")
            print("6. 📦 Экспорт всех данных (ZIP)")
            print("0. 🔙 Назад")
            print()
            print(f"{Fore.CYAN}📂 Папка экспорта: {self._get_export_dir()}{Style.RESET_ALL}")

            try:
                choice = input(f"\n{Fore.CYAN}🎯 Выбор (0-6): {Style.RESET_ALL}").strip()

                if choice == '1':
                    await self.export_signals('csv')
                    print(f"\n{Fore.GREEN}📂 Открыть папку с файлом? (y/n): {Style.RESET_ALL}", end='')
                    if input().lower() == 'y':
                        self._open_export_folder(str(self._get_export_dir()))
                    await asyncio.sleep(2)

                elif choice == '2':
                    await self.export_signals('json')
                    print(f"\n{Fore.GREEN}📂 Открыть папку с файлом? (y/n): {Style.RESET_ALL}", end='')
                    if input().lower() == 'y':
                        self._open_export_folder(str(self._get_export_dir()))
                    await asyncio.sleep(2)

                elif choice == '3':
                    await self.export_trades('csv')
                    print(f"\n{Fore.GREEN}📂 Открыть папку с файлом? (y/n): {Style.RESET_ALL}", end='')
                    if input().lower() == 'y':
                        self._open_export_folder(str(self._get_export_dir()))
                    await asyncio.sleep(2)

                elif choice == '4':
                    await self.export_trades('json')
                    print(f"\n{Fore.GREEN}📂 Открыть папку с файлом? (y/n): {Style.RESET_ALL}", end='')
                    if input().lower() == 'y':
                        self._open_export_folder(str(self._get_export_dir()))
                    await asyncio.sleep(2)

                elif choice == '5':
                    await self.export_stats()
                    print(f"\n{Fore.GREEN}📂 Открыть папку с файлом? (y/n): {Style.RESET_ALL}", end='')
                    if input().lower() == 'y':
                        self._open_export_folder(str(self._get_export_dir()))
                    await asyncio.sleep(2)

                elif choice == '6':
                    zip_file = await self.export_all()
                    if zip_file:
                        print(f"\n{Fore.GREEN}📂 Открыть папку с файлом? (y/n): {Style.RESET_ALL}", end='')
                        if input().lower() == 'y':
                            self._open_export_folder(str(self._get_export_dir()))
                    await asyncio.sleep(2)

                elif choice == '0':
                    break
                else:
                    print(f"{Fore.RED}❌ Неверный выбор!{Style.RESET_ALL}")
                    await asyncio.sleep(1)

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в меню экспорта: {e}")
                await asyncio.sleep(1)

    # ========== МЕТОДЫ МОНИТОРА ==========

    async def display_realtime_monitor(self):
        """Реалтайм мониторинг сигналов"""
        self._shutdown = False

        try:
            while not self._shutdown:
                self.clear_screen()
                self.print_header("THREE SCREEN ANALYZER - РЕАЛТАЙМ МОНИТОР (v1.3.6)")

                if not self.signal_repo:
                    print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                    break

                signals = await self.signal_repo.get_signals_with_trades(self.settings['signals_limit'])
                stats = await self.signal_repo.get_database_stats()

                print(f"{Fore.YELLOW}📊 СТАТИСТИКА СИСТЕМЫ:{Style.RESET_ALL}")
                print(f"   Всего сигналов: {stats.get('total_signals', 0)}")
                print(f"   WATCH: {stats.get('subtypes_stats', {}).get('WATCH', 0)}")
                print(f"   M15: {stats.get('subtypes_stats', {}).get('M15', 0)}")
                print(f"   Активных: {stats.get('active_signals', 0)}")
                print(f"   BUY: {stats.get('buy_signals', 0)}  |  SELL: {stats.get('sell_signals', 0)}")
                print(f"   Закрыто сделок: {stats.get('closed_trades', 0)}")
                print(f"   Общий PnL: {self.format_pnl(stats.get('total_pnl', 0))}")
                print(f"   Win Rate: {stats.get('win_rate', 0):.1f}%")
                print()

                if not signals:
                    print(f"{Fore.YELLOW}📭 Нет сигналов в базе данных{Style.RESET_ALL}")
                else:
                    table_data = []
                    for signal in signals:
                        time_str = self.format_time(signal.get('created_time', ''))
                        date_str = self.format_date(signal.get('created_time', ''))

                        signal_subtype = signal.get('signal_subtype', '')
                        status = signal.get('status', '')

                        pnl_str = self.format_pnl(signal.get('trade_pnl', 0)) if signal.get('trade_pnl') is not None else "-"

                        zone_str = self.format_zone(signal.get('zone_low', 0), signal.get('zone_high', 0))
                        score_str = self.format_score(signal.get('screen2_score', 0))

                        table_data.append([
                            str(signal.get('id', '')),
                            signal.get('symbol', ''),
                            self.format_screen(signal_subtype),
                            self.format_direction(signal.get('direction', '')),
                            self.format_status(status),
                            self.format_price(signal.get('entry_price', 0)),
                            zone_str,
                            score_str,
                            self.format_price(signal.get('stop_loss', 0)),
                            self.format_price(signal.get('take_profit', 0)),
                            self.format_rr_ratio(signal.get('risk_reward_ratio', 0)),
                            pnl_str,
                            f"{date_str} {time_str}"
                        ])

                    headers = ["ID", "Монета", "Тип", "Напр", "Статус", "Entry", "Зона", "Score", "SL", "TP", "R/R", "PnL", "Время"]
                    table = self.create_table(headers, table_data)
                    print(table)

                print(f"\n{Fore.CYAN}🔄 Автообновление через {self.settings['refresh_interval']} сек...")
                print(f"{Fore.YELLOW}Нажмите Enter для возврата в меню{Style.RESET_ALL}")

                try:
                    await asyncio.wait_for(self.wait_for_enter(), timeout=self.settings['refresh_interval'])
                    break
                except asyncio.TimeoutError:
                    continue

        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}🛑 Монитор остановлен{Style.RESET_ALL}")
        except Exception as e:
            logger.error(f"❌ Ошибка в реалтайм мониторе: {e}")
        finally:
            self._shutdown = False

    async def display_trades_table(self):
        """Таблица сделок (trades)"""
        try:
            if not self.trade_repo:
                print(f"{Fore.RED}❌ TradeRepository не инициализирован{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
                return

            trades = await self.trade_repo.get_closed_trades(limit=50)

            self.clear_screen()
            self.print_header("ИСТОРИЯ СДЕЛОК (TRADES)")

            if not trades:
                print(f"{Fore.YELLOW}📭 Нет закрытых сделок в базе данных{Style.RESET_ALL}")
            else:
                table_data = []
                total_pnl = 0
                winning_trades = 0

                for trade in trades:
                    pnl = trade.get('pnl', 0)
                    total_pnl += pnl
                    if pnl > 0:
                        winning_trades += 1

                    time_str = self.format_time(trade.get('closed_at', ''))
                    date_str = self.format_date(trade.get('closed_at', ''))

                    table_data.append([
                        str(trade.get('id', '')),
                        trade.get('symbol', ''),
                        self.format_direction(trade.get('direction', '')),
                        self.format_price(trade.get('entry_price', 0)),
                        self.format_price(trade.get('close_price', 0)),
                        self.format_pnl(pnl),
                        f"{(pnl / trade.get('entry_price', 1) * 100) if trade.get('entry_price', 0) > 0 else 0:.2f}%",
                        trade.get('close_reason', '-'),
                        f"{date_str} {time_str}"
                    ])

                headers = ["ID", "Монета", "Тип", "Entry", "Close", "PnL", "PnL %", "Причина", "Время"]
                table = self.create_table(headers, table_data)
                print(table)

                total_trades = len(trades)
                win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
                avg_pnl = total_pnl / total_trades if total_trades > 0 else 0

                print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}📊 СТАТИСТИКА ПО СДЕЛКАМ:{Style.RESET_ALL}")
                print(f"   Всего сделок:  {total_trades}")
                print(f"   Прибыльных:    {winning_trades} ({win_rate:.1f}%)")
                print(f"   Убыточных:     {total_trades - winning_trades} ({100 - win_rate:.1f}%)")
                print(f"   Общий PnL:     {self.format_pnl(total_pnl)}")
                print(f"   Средняя PnL:   {self.format_pnl(avg_pnl)}")

        except Exception as e:
            print(f"{Fore.RED}❌ Ошибка: {e}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()

        input(f"\n{Fore.GREEN}Нажмите Enter для продолжения...{Style.RESET_ALL}")

    async def display_signal_details(self, signal_id: int):
        """Детальная информация о сигнале"""
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                return

            signal = await self.signal_repo.get_signal_by_id(signal_id)
            if not signal:
                print(f"{Fore.RED}❌ Сигнал #{signal_id} не найден{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
                return

            current_price = await self.get_current_price(signal.get('symbol', ''))
            if not current_price:
                current_price = signal.get('entry_price', 0)

            trade = None
            if self.trade_repo:
                trade = await self.trade_repo.get_trade_by_signal_id(signal_id)

            self.clear_screen()
            self.print_header(f"ДЕТАЛИ СИГНАЛА #{signal_id}")

            signal_subtype = signal.get('signal_subtype', '')
            status = signal.get('status', '')

            print(f"{Fore.CYAN}🎯 ОСНОВНАЯ ИНФОРМАЦИЯ{Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            print(f"  Монета:        {Fore.WHITE}{signal.get('symbol', 'N/A')}{Style.RESET_ALL}")
            print(f"  Тип сигнала:   {self.format_direction(signal.get('direction', ''))}")
            print(f"  Экран:         {self.format_screen(signal_subtype)}")
            print(f"  Статус:        {self.format_status(status)}")
            print(f"  Уверенность:   {self.format_confidence(signal.get('confidence', 0))}")

            if signal_subtype == 'WATCH':
                print(f"\n{Fore.CYAN}👀 WATCH ПАРАМЕТРЫ{Style.RESET_ALL}")
                print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
                print(f"  Зона входа:    {self.format_zone(signal.get('zone_low', 0), signal.get('zone_high', 0))}")
                print(f"  Score Screen2: {self.format_score(signal.get('screen2_score', 0))}")
                print(f"  Ожидаемый паттерн: {Fore.WHITE}{signal.get('expected_pattern', '—')}{Style.RESET_ALL}")

            else:
                print(f"\n{Fore.CYAN}💸 ТОРГОВЫЕ ПАРАМЕТРЫ{Style.RESET_ALL}")
                print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
                print(f"  Цена входа:    {Fore.WHITE}{self.format_price(signal.get('entry_price', 0))}{Style.RESET_ALL}")
                print(f"  Stop Loss:     {Fore.RED}{self.format_price(signal.get('stop_loss', 0))}{Style.RESET_ALL}")
                print(f"  Take Profit:   {Fore.GREEN}{self.format_price(signal.get('take_profit', 0))}{Style.RESET_ALL}")
                print(f"  Risk/Reward:   {self.format_rr_ratio(signal.get('risk_reward_ratio', 0))}")
                print(f"  Паттерн:       {Fore.WHITE}{signal.get('trigger_pattern', '—')}{Style.RESET_ALL}")

                if signal.get('zone_low', 0) > 0:
                    print(f"  Зона входа:    {self.format_zone(signal.get('zone_low', 0), signal.get('zone_high', 0))}")
                if signal.get('screen2_score', 0) > 0:
                    print(f"  Score Screen2: {self.format_score(signal.get('screen2_score', 0))}")

            print(f"\n{Fore.CYAN}📈 ТЕКУЩАЯ СИТУАЦИЯ{Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            print(f"  Текущая цена:  {Fore.WHITE}{self.format_price(current_price)}{Style.RESET_ALL}")

            entry = signal.get('entry_price', 0)
            if entry > 0 and current_price > 0:
                distance_to_entry = current_price - entry
                distance_pct = (distance_to_entry / entry) * 100

                if signal.get('direction', '').upper() == 'BUY':
                    if distance_to_entry > 0:
                        status_emoji = "📈"
                        status_text = "цена выше входа"
                    else:
                        status_emoji = "📉"
                        status_text = "цена ниже входа"
                else:
                    if distance_to_entry < 0:
                        status_emoji = "📈"
                        status_text = "цена ниже входа (SELL)"
                    else:
                        status_emoji = "📉"
                        status_text = "цена выше входа (SELL)"

                print(f"  Расстояние до входа: {self.format_price(distance_to_entry)} ({distance_pct:+.2f}%)")
                print(f"    → {status_emoji} {status_text}")

            print(f"\n{Fore.CYAN}⏱ ВРЕМЯ{Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            created_utc = signal.get('created_time', '')
            if created_utc:
                local_dt = self.utc_to_local(created_utc)
                print(f"  Создан:        {Fore.WHITE}{local_dt.strftime('%d.%m.%Y %H:%M:%S')} (UTC+{self.timezone_offset}){Style.RESET_ALL}")

            expiration = signal.get('expiration_time')
            if expiration:
                exp_local = self.utc_to_local(expiration)
                now_local = datetime.now()
                if exp_local > now_local:
                    remaining = exp_local - now_local
                    hours = remaining.seconds // 3600
                    minutes = (remaining.seconds % 3600) // 60
                    print(f"  Истекает:      {Fore.WHITE}{exp_local.strftime('%d.%m.%Y %H:%M:%S')} (через {hours}ч {minutes}м){Style.RESET_ALL}")
                else:
                    print(f"  Истекает:      {Fore.RED}{exp_local.strftime('%d.%m.%Y %H:%M:%S')} (истек){Style.RESET_ALL}")

            if trade:
                print(f"\n{Fore.CYAN}📝 ИНФОРМАЦИЯ О СДЕЛКЕ{Style.RESET_ALL}")
                print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
                print(f"  Цена закрытия: {self.format_price(trade.get('close_price', 0))}")
                print(f"  PnL:           {self.format_pnl(trade.get('pnl', 0))}")
                print(f"  PnL %:         {trade.get('pnl_percent', 0):+.2f}%")
                print(f"  Причина:       {Fore.WHITE}{trade.get('close_reason', '—')}{Style.RESET_ALL}")

            print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата в меню...{Style.RESET_ALL}")

        except Exception as e:
            print(f"{Fore.RED}❌ Ошибка при отображении деталей: {e}{Style.RESET_ALL}")
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")

    async def display_all_signals(self):
        """Таблица всех сигналов"""
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                return

            signals = await self.signal_repo.get_signals(limit=50)

            self.clear_screen()
            self.print_header("ВСЕ СИГНАЛЫ")

            if not signals:
                print(f"{Fore.YELLOW}📭 Нет сигналов в базе данных{Style.RESET_ALL}")
            else:
                table_data = []
                for signal in signals:
                    time_str = self.format_time(signal.get('created_time', ''))
                    date_str = self.format_date(signal.get('created_time', ''))

                    signal_subtype = signal.get('signal_subtype', '')
                    zone_str = self.format_zone(signal.get('zone_low', 0), signal.get('zone_high', 0))
                    score_str = self.format_score(signal.get('screen2_score', 0))

                    table_data.append([
                        str(signal.get('id', '')),
                        signal.get('symbol', ''),
                        self.format_screen(signal_subtype),
                        self.format_direction(signal.get('direction', '')),
                        self.format_status(signal.get('status', '')),
                        self.format_price(signal.get('entry_price', 0)),
                        zone_str,
                        score_str,
                        self.format_rr_ratio(signal.get('risk_reward_ratio', 0)),
                        f"{date_str} {time_str}"
                    ])

                headers = ["ID", "Монета", "Тип", "Напр", "Статус", "Entry", "Зона", "Score", "R/R", "Время"]
                table = self.create_table(headers, table_data)
                print(table)

                print(f"\n{Fore.CYAN}📈 СТАТИСТИКА:{Style.RESET_ALL}")
                active_count = sum(1 for s in signals if s.get('status', '').lower() in ['watch', 'active'])
                watch_count = sum(1 for s in signals if s.get('signal_subtype', '') == 'WATCH')
                m15_count = sum(1 for s in signals if s.get('signal_subtype', '') == 'M15')
                buy_count = sum(1 for s in signals if s.get('direction', '').lower() == 'buy')
                sell_count = sum(1 for s in signals if s.get('direction', '').lower() == 'sell')

                print(f"   Всего сигналов: {len(signals)}")
                print(f"   Активных: {active_count}")
                print(f"   WATCH: {watch_count} | M15: {m15_count}")
                print(f"   BUY: {buy_count} | SELL: {sell_count}")

        except Exception as e:
            print(f"{Fore.RED}❌ Ошибка: {e}{Style.RESET_ALL}")

        input(f"\n{Fore.GREEN}Нажмите Enter для продолжения...{Style.RESET_ALL}")

    async def wait_for_enter(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, input)

    async def display_database_stats(self):
        """Отображение статистики БД"""
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

        print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
        input(f"\n{Fore.GREEN}Нажмите Enter для продолжения...{Style.RESET_ALL}")

    async def main_menu(self):
        """Главное меню монитора"""
        while True:
            self.clear_screen()
            self.print_header("THREE SCREEN ANALYZER - МОНИТОР СИГНАЛОВ (v1.3.6)")

            print(f"{Fore.YELLOW}📋 ВЫБЕРИТЕ РЕЖИМ РАБОТЫ:{Style.RESET_ALL}")
            print("1. 📊 Реалтайм монитор (WATCH + M15)")
            print("2. 📋 Таблица всех сигналов")
            print("3. 📈 Таблица сделок (trades)")
            print("4. 🔍 Детали сигнала по ID")
            print("5. 📊 Статистика БД")
            print("6. 💾 Экспорт данных")
            print("7. 🚪 Выход")
            print()
            print(f"{Fore.CYAN}🕐 Локальное время: UTC+{self.timezone_offset}{Style.RESET_ALL}")
            print(f"{Fore.CYAN}📌 Типы сигналов: WATCH (зона интереса) и M15 (рыночный ордер){Style.RESET_ALL}")
            print(f"{Fore.CYAN}📂 Папка экспорта: {self._get_export_dir()}{Style.RESET_ALL}")

            try:
                choice = input(f"\n{Fore.CYAN}🎯 Выбор (1-7): {Style.RESET_ALL}").strip()

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
                    await self.display_database_stats()
                elif choice == '6':
                    await self.export_menu()
                elif choice == '7':
                    print(f"{Fore.GREEN}👋 До свидания!{Style.RESET_ALL}")
                    break
                else:
                    print(f"{Fore.RED}❌ Неверный выбор!{Style.RESET_ALL}")
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                print(f"\n{Fore.YELLOW}🛑 Выход...{Style.RESET_ALL}")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в главном меню: {e}")
                await asyncio.sleep(1)

    async def run(self):
        """Запуск монитора"""
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
            print(f"{Fore.GREEN}✅ Папка экспорта: {self._get_export_dir()}{Style.RESET_ALL}")
            await self.main_menu()
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}🛑 Монитор остановлен{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}❌ Критическая ошибка: {e}{Style.RESET_ALL}")


async def main():
    print(f"{Fore.GREEN}🚀 Запуск Three Screen Analyzer Monitor (v1.3.6)...{Style.RESET_ALL}")
    os.environ['TERM'] = os.environ.get('TERM', 'xterm-256color')

    monitor = ThreeScreenMonitor()
    await monitor.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}👋 Программа завершена{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}💥 Фатальная ошибка: {e}{Style.RESET_ALL}")