# tools/log_analyzer.py
#!/usr/bin/env python3

import re
import sys
import os
import argparse
import csv
import subprocess
import platform
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))


class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


@dataclass
class SignalEntry:
    timestamp: datetime
    signal_id: Optional[int]
    symbol: str
    signal_type: str
    signal_subtype: str
    entry_price: float
    stop_loss: float
    take_profit: float
    confidence: float
    risk_reward: float


@dataclass
class PositionEntry:
    timestamp: datetime
    signal_id: int
    symbol: str
    direction: str
    entry_price: float
    quantity: float
    stop_loss: float
    take_profit: float
    order_type: str
    fill_price: float
    action: str
    pnl: Optional[float] = None
    close_reason: Optional[str] = None


@dataclass
class ErrorEntry:
    timestamp: datetime
    level: str
    message: str
    module: str


class LogAnalyzer:
    LOG_FILE_NAMES = ["signal_generator.log", "trading_bot.log", "bot.log"]

    def __init__(self, log_path: Optional[str] = None, output_dir: Optional[str] = None):
        self.project_root = Path(__file__).parent.parent

        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            # Папка экспорта: logs/exports/YYYY-MM-DD/
            date_folder = datetime.now().strftime("%Y-%m-%d")
            self.output_dir = self.project_root / "logs" / "exports" / date_folder

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self._find_log_file(log_path)

        self.signals: List[SignalEntry] = []
        self.positions: List[PositionEntry] = []
        self.errors: List[ErrorEntry] = []

        self.patterns = {
            'timestamp': re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'),
            'signal_found': re.compile(r'✅ (.*?): СИГНАЛ НАЙДЕН! (BUY|SELL) @ ([\d\.]+) \(R/R: ([\d\.]+):1\)'),
            'signal_saved': re.compile(r'✅ Сигнал сохранен: ID=(\d+), (.*?) \((LIMIT|INSTANT|WATCH)\)'),
            'position_opened_instant': re.compile(
                r'⚡ INSTANT сигнал #(\d+): позиция открыта по рыночной цене ([\d\.]+)'),
            'position_opened_limit': re.compile(r'✅ Лимитный ордер #(\d+) исполнен по цене ([\d\.]+)'),
            'position_closed': re.compile(r'✅ Позиция #(\d+) закрыта: (TP|SL|EXPIRED), PnL: ([+\-][\d\.]+)'),
            'error': re.compile(r'❌ (.*?): (.*)'),
            'critical': re.compile(r'🔥 CRITICAL: (.*)'),
            'instant_rejected': re.compile(
                r'⚠️ INSTANT сигнал #(\d+) отклонён: текущая цена ([\d\.]+) отличается от entry ([\d\.]+) на ([\d\.]+)%'),
        }

    def _find_log_file(self, provided_path: Optional[str]) -> Optional[Path]:
        if provided_path:
            path = Path(provided_path)
            if path.exists():
                return path
            print(f"{Colors.WARNING}⚠️ Указанный путь не существует: {provided_path}{Colors.ENDC}")

        possible_paths = []
        for log_name in self.LOG_FILE_NAMES:
            possible_paths.extend([
                self.project_root / "logs" / "bot" / log_name,
                self.project_root / "logs" / log_name,
                self.project_root / "analyzer" / "logs" / log_name,
                self.project_root / "logs" / "monitor" / log_name,
                self.project_root / log_name,
            ])

        seen = set()
        unique_paths = []
        for path in possible_paths:
            if path not in seen:
                seen.add(path)
                unique_paths.append(path)

        for path in unique_paths:
            if path.exists():
                return path

        return None

    def parse_log(self) -> bool:
        if not self.log_path:
            print(f"{Colors.FAIL}❌ Лог-файл не найден!{Colors.ENDC}")
            print(f"\n{Colors.CYAN}Искал в следующих местах:{Colors.ENDC}")
            for log_name in self.LOG_FILE_NAMES:
                print(f"  - logs/bot/{log_name}")
                print(f"  - logs/{log_name}")
                print(f"  - analyzer/logs/{log_name}")
                print(f"  - logs/monitor/{log_name}")
                print(f"  - {log_name}")
            print(f"\n{Colors.CYAN}Попробуйте указать путь вручную:{Colors.ENDC}")
            print(f"  python tools/log_analyzer.py --log-path /путь/к/логу.log")
            return False

        file_stat = self.log_path.stat()
        file_size_mb = file_stat.st_size / (1024 * 1024)
        last_modified = datetime.fromtimestamp(file_stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")

        print(f"{Colors.GREEN}📖 Найден лог-файл:{Colors.ENDC}")
        print(f"   Путь: {self.log_path}")
        print(f"   Размер: {file_size_mb:.2f} MB")
        print(f"   Последнее изменение: {last_modified}")
        print()

        with open(self.log_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        for line in lines:
            line = line.strip()
            if not line:
                continue

            timestamp_match = self.patterns['timestamp'].search(line)
            if not timestamp_match:
                continue

            try:
                timestamp = datetime.strptime(timestamp_match.group(1), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue

            parts = line.split(' - ')
            module = parts[1] if len(parts) > 1 else "unknown"

            signal_match = self.patterns['signal_found'].search(line)
            if signal_match:
                symbol, signal_type, entry_price, rr = signal_match.groups()
                self.signals.append(SignalEntry(
                    timestamp=timestamp, signal_id=None, symbol=symbol,
                    signal_type=signal_type, signal_subtype="UNKNOWN",
                    entry_price=float(entry_price), stop_loss=0.0, take_profit=0.0,
                    confidence=0.0, risk_reward=float(rr)
                ))
                continue

            saved_match = self.patterns['signal_saved'].search(line)
            if saved_match:
                signal_id, symbol, subtype = saved_match.groups()
                for s in reversed(self.signals):
                    if s.symbol == symbol and s.signal_id is None:
                        s.signal_id = int(signal_id)
                        s.signal_subtype = subtype
                        break
                continue

            instant_match = self.patterns['position_opened_instant'].search(line)
            if instant_match:
                signal_id, price = instant_match.groups()
                self.positions.append(PositionEntry(
                    timestamp=timestamp, signal_id=int(signal_id), symbol="", direction="",
                    entry_price=float(price), quantity=0.0, stop_loss=0.0, take_profit=0.0,
                    order_type="MARKET", fill_price=float(price), action="OPENED"
                ))
                continue

            limit_match = self.patterns['position_opened_limit'].search(line)
            if limit_match:
                signal_id, price = limit_match.groups()
                self.positions.append(PositionEntry(
                    timestamp=timestamp, signal_id=int(signal_id), symbol="", direction="",
                    entry_price=float(price), quantity=0.0, stop_loss=0.0, take_profit=0.0,
                    order_type="LIMIT", fill_price=float(price), action="OPENED"
                ))
                continue

            close_match = self.patterns['position_closed'].search(line)
            if close_match:
                signal_id, reason, pnl = close_match.groups()
                for p in self.positions:
                    if p.signal_id == int(signal_id) and p.action == "OPENED":
                        p.action = "CLOSED"
                        p.pnl = float(pnl)
                        p.close_reason = reason
                        break
                continue

            error_match = self.patterns['error'].search(line)
            if error_match and '❌' in line:
                _, msg = error_match.groups()
                self.errors.append(ErrorEntry(
                    timestamp=timestamp, level="ERROR", message=msg, module=module
                ))
                continue

            critical_match = self.patterns['critical'].search(line)
            if critical_match:
                msg = critical_match.group(1)
                self.errors.append(ErrorEntry(
                    timestamp=timestamp, level="CRITICAL", message=msg, module=module
                ))
                continue

        print(f"✅ Парсинг завершён:")
        print(f"   Сигналов: {len(self.signals)}")
        print(f"   Позиций: {len(self.positions)}")
        print(f"   Ошибок: {len(self.errors)}")

        return True

    def show_signals(self, filter_symbol: str = None, filter_subtype: str = None, limit: int = 50):
        signals = self.signals

        if filter_symbol:
            signals = [s for s in signals if filter_symbol.upper() in s.symbol.upper()]
        if filter_subtype:
            signals = [s for s in signals if s.signal_subtype == filter_subtype.upper()]

        signals = signals[-limit:] if limit else signals

        if not signals:
            print(f"{Colors.WARNING}⚠️ Сигналы не найдены{Colors.ENDC}")
            return

        print(f"\n{Colors.HEADER}{'=' * 100}{Colors.ENDC}")
        print(f"{Colors.BOLD}📊 СИГНАЛЫ ({len(signals)} записей){Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 100}{Colors.ENDC}")

        print(f"{'Время':<20} {'ID':<6} {'Символ':<12} {'Тип':<8} {'Подтип':<8} {'Entry':<12} {'R/R':<8}")
        print(f"{'-' * 100}")

        for s in signals:
            time_str = s.timestamp.strftime("%H:%M:%S")
            rr_color = Colors.GREEN if s.risk_reward >= 3 else (Colors.WARNING if s.risk_reward >= 2 else Colors.FAIL)

            print(f"{time_str:<20} "
                  f"{str(s.signal_id or '-'):<6} "
                  f"{s.symbol:<12} "
                  f"{Colors.GREEN if s.signal_type == 'BUY' else Colors.FAIL}{s.signal_type:<8}{Colors.ENDC} "
                  f"{s.signal_subtype:<8} "
                  f"{s.entry_price:<12.4f} "
                  f"{rr_color}{s.risk_reward:<8.2f}{Colors.ENDC}")

    def show_positions(self, filter_symbol: str = None, filter_action: str = None, limit: int = 50):
        positions = self.positions

        if filter_symbol:
            positions = [p for p in positions if filter_symbol.upper() in p.symbol.upper()]
        if filter_action:
            positions = [p for p in positions if p.action == filter_action.upper()]

        positions = positions[-limit:] if limit else positions

        if not positions:
            print(f"{Colors.WARNING}⚠️ Позиции не найдены{Colors.ENDC}")
            return

        print(f"\n{Colors.HEADER}{'=' * 120}{Colors.ENDC}")
        print(f"{Colors.BOLD}💰 ПОЗИЦИИ ({len(positions)} записей){Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 120}{Colors.ENDC}")

        print(
            f"{'Время':<20} {'ID':<6} {'Действие':<10} {'Тип':<8} {'Entry':<12} {'Fill':<12} {'PnL':<12} {'Причина':<10}")
        print(f"{'-' * 120}")

        for p in positions:
            time_str = p.timestamp.strftime("%H:%M:%S")

            action_color = Colors.GREEN if p.action == "OPENED" else (
                Colors.FAIL if "CLOSED" in p.action else Colors.WARNING)
            pnl_str = f"{p.pnl:+.2f}" if p.pnl else "-"
            pnl_color = Colors.GREEN if p.pnl and p.pnl > 0 else (
                Colors.FAIL if p.pnl and p.pnl < 0 else Colors.WARNING)

            print(f"{time_str:<20} "
                  f"{p.signal_id:<6} "
                  f"{action_color}{p.action:<10}{Colors.ENDC} "
                  f"{p.order_type:<8} "
                  f"{p.entry_price:<12.4f} "
                  f"{p.fill_price:<12.4f} "
                  f"{pnl_color}{pnl_str:<12}{Colors.ENDC} "
                  f"{p.close_reason or '-':<10}")

    def show_errors(self, filter_level: str = None, filter_module: str = None, limit: int = 50):
        errors = self.errors

        if filter_level:
            errors = [e for e in errors if e.level == filter_level.upper()]
        if filter_module:
            errors = [e for e in errors if filter_module.lower() in e.module.lower()]

        errors = errors[-limit:] if limit else errors

        if not errors:
            print(f"{Colors.GREEN}✅ Ошибки не найдены{Colors.ENDC}")
            return

        print(f"\n{Colors.HEADER}{'=' * 100}{Colors.ENDC}")
        print(f"{Colors.BOLD}❌ ОШИБКИ ({len(errors)} записей){Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 100}{Colors.ENDC}")

        print(f"{'Время':<20} {'Уровень':<10} {'Модуль':<20} {'Сообщение'}")
        print(f"{'-' * 100}")

        for e in errors:
            time_str = e.timestamp.strftime("%H:%M:%S")
            level_color = Colors.FAIL if e.level == "CRITICAL" else Colors.WARNING

            print(f"{time_str:<20} "
                  f"{level_color}{e.level:<10}{Colors.ENDC} "
                  f"{e.module:<20} "
                  f"{e.message[:50]}")

    def show_stats(self):
        print(f"\n{Colors.HEADER}{'=' * 80}{Colors.ENDC}")
        print(f"{Colors.BOLD}📈 СТАТИСТИКА АНАЛИЗА ЛОГОВ{Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 80}{Colors.ENDC}")

        signals_by_type = defaultdict(int)
        signals_by_subtype = defaultdict(int)
        for s in self.signals:
            signals_by_type[s.signal_type] += 1
            signals_by_subtype[s.signal_subtype] += 1

        print(f"\n{Colors.CYAN}Сигналы:{Colors.ENDC}")
        print(f"   Всего: {len(self.signals)}")
        print(f"   BUY: {signals_by_type.get('BUY', 0)}")
        print(f"   SELL: {signals_by_type.get('SELL', 0)}")
        print(f"   LIMIT: {signals_by_subtype.get('LIMIT', 0)}")
        print(f"   INSTANT: {signals_by_subtype.get('INSTANT', 0)}")
        print(f"   WATCH: {signals_by_subtype.get('WATCH', 0)}")

        positions_opened = sum(1 for p in self.positions if p.action == "OPENED")
        positions_closed = sum(1 for p in self.positions if "CLOSED" in p.action)

        pnl_values = [p.pnl for p in self.positions if p.pnl is not None]
        total_pnl = sum(pnl_values) if pnl_values else 0
        winning_trades = sum(1 for p in pnl_values if p > 0)
        losing_trades = sum(1 for p in pnl_values if p < 0)
        win_rate = (winning_trades / len(pnl_values) * 100) if pnl_values else 0

        print(f"\n{Colors.CYAN}Позиции:{Colors.ENDC}")
        print(f"   Открыто: {positions_opened}")
        print(f"   Закрыто: {positions_closed}")
        print(f"   Всего сделок: {len(pnl_values)}")
        print(f"   Прибыльных: {winning_trades}")
        print(f"   Убыточных: {losing_trades}")
        print(f"   Win Rate: {win_rate:.1f}%")
        print(f"   Общий PnL: {Colors.GREEN if total_pnl > 0 else Colors.FAIL}{total_pnl:+.2f}{Colors.ENDC}")

        errors_by_level = defaultdict(int)
        for e in self.errors:
            errors_by_level[e.level] += 1

        print(f"\n{Colors.CYAN}Ошибки:{Colors.ENDC}")
        print(f"   Всего: {len(self.errors)}")
        print(f"   CRITICAL: {errors_by_level.get('CRITICAL', 0)}")
        print(f"   ERROR: {errors_by_level.get('ERROR', 0)}")
        print(f"   WARNING: {errors_by_level.get('WARNING', 0)}")

    def export_to_csv(self, filename: str, data_type: str = "all"):
        """Экспорт данных в CSV с созданием папки по дате"""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Если filename содержит путь, используем его, иначе сохраняем в output_dir
        export_path = Path(filename)
        if not export_path.parent.exists() or str(export_path.parent) == '.':
            export_path = self.output_dir / export_path

        print(f"\n{Colors.CYAN}📁 Сохранение в: {export_path.absolute()}{Colors.ENDC}")

        try:
            with open(export_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)

                writer.writerow([f'# Экспорт логов от {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'])
                writer.writerow([f'# Лог-файл: {self.log_path}'])
                writer.writerow([f'# Тип данных: {data_type}'])
                writer.writerow([])

                if data_type in ['signals', 'all']:
                    writer.writerow(['=== СИГНАЛЫ ==='])
                    writer.writerow(['Время', 'ID', 'Символ', 'Тип', 'Подтип', 'Entry', 'R/R'])
                    for s in self.signals:
                        writer.writerow([
                            s.timestamp.isoformat(),
                            s.signal_id or '',
                            s.symbol,
                            s.signal_type,
                            s.signal_subtype,
                            s.entry_price,
                            s.risk_reward
                        ])
                    writer.writerow([])

                if data_type in ['positions', 'all']:
                    writer.writerow(['=== ПОЗИЦИИ ==='])
                    writer.writerow(['Время', 'ID', 'Действие', 'Тип', 'Entry', 'Fill', 'PnL', 'Причина'])
                    for p in self.positions:
                        writer.writerow([
                            p.timestamp.isoformat(),
                            p.signal_id,
                            p.action,
                            p.order_type,
                            p.entry_price,
                            p.fill_price,
                            p.pnl or '',
                            p.close_reason or ''
                        ])
                    writer.writerow([])

                if data_type in ['errors', 'all']:
                    writer.writerow(['=== ОШИБКИ ==='])
                    writer.writerow(['Время', 'Уровень', 'Модуль', 'Сообщение'])
                    for e in self.errors:
                        writer.writerow([
                            e.timestamp.isoformat(),
                            e.level,
                            e.module,
                            e.message
                        ])
                    writer.writerow([])

                if data_type in ['stats', 'all']:
                    writer.writerow(['=== СТАТИСТИКА ==='])

                    signals_by_type = defaultdict(int)
                    signals_by_subtype = defaultdict(int)
                    for s in self.signals:
                        signals_by_type[s.signal_type] += 1
                        signals_by_subtype[s.signal_subtype] += 1

                    writer.writerow(['Сигналы:'])
                    writer.writerow(['Всего', len(self.signals)])
                    writer.writerow(['BUY', signals_by_type.get('BUY', 0)])
                    writer.writerow(['SELL', signals_by_type.get('SELL', 0)])
                    writer.writerow(['LIMIT', signals_by_subtype.get('LIMIT', 0)])
                    writer.writerow(['INSTANT', signals_by_subtype.get('INSTANT', 0)])
                    writer.writerow(['WATCH', signals_by_subtype.get('WATCH', 0)])

                    writer.writerow([])
                    writer.writerow(['Позиции:'])
                    positions_opened = sum(1 for p in self.positions if p.action == "OPENED")
                    positions_closed = sum(1 for p in self.positions if "CLOSED" in p.action)
                    writer.writerow(['Открыто', positions_opened])
                    writer.writerow(['Закрыто', positions_closed])

                    pnl_values = [p.pnl for p in self.positions if p.pnl is not None]
                    total_pnl = sum(pnl_values) if pnl_values else 0
                    winning_trades = sum(1 for p in pnl_values if p > 0)
                    losing_trades = sum(1 for p in pnl_values if p < 0)
                    writer.writerow(['Всего сделок', len(pnl_values)])
                    writer.writerow(['Прибыльных', winning_trades])
                    writer.writerow(['Убыточных', losing_trades])
                    writer.writerow(
                        ['Win Rate', f"{winning_trades / len(pnl_values) * 100:.1f}%" if pnl_values else "0%"])
                    writer.writerow(['Общий PnL', f"{total_pnl:+.2f}"])

                    writer.writerow([])
                    writer.writerow(['Ошибки:'])
                    errors_by_level = defaultdict(int)
                    for e in self.errors:
                        errors_by_level[e.level] += 1
                    writer.writerow(['Всего', len(self.errors)])
                    writer.writerow(['CRITICAL', errors_by_level.get('CRITICAL', 0)])
                    writer.writerow(['ERROR', errors_by_level.get('ERROR', 0)])
                    writer.writerow(['WARNING', errors_by_level.get('WARNING', 0)])

                f.flush()
                file_size = export_path.stat().st_size

            print(f"\n{Colors.GREEN}{'=' * 60}{Colors.ENDC}")
            print(f"{Colors.GREEN}✅ ЭКСПОРТ УСПЕШНО ВЫПОЛНЕН!{Colors.ENDC}")
            print(f"{Colors.GREEN}{'=' * 60}{Colors.ENDC}")
            print(f"{Colors.BOLD}📄 Полный путь к файлу:{Colors.ENDC}")
            print(f"   {export_path.absolute()}")
            print(f"{Colors.BOLD}📊 Размер файла:{Colors.ENDC}")
            print(f"   {file_size:,} байт ({file_size / 1024:.2f} KB)")

            print(
                f"\n{Colors.WARNING}💡 В PyCharm нажмите {Colors.BOLD}Cmd+R{Colors.ENDC} на папке logs/exports, чтобы увидеть файл")

            print(f"\n{Colors.CYAN}📂 Открыть папку с файлом? (y/n): {Colors.ENDC}", end=' ')
            if input().lower() == 'y':
                if platform.system() == 'Darwin':
                    subprocess.run(['open', str(export_path.parent)])
                elif platform.system() == 'Linux':
                    subprocess.run(['xdg-open', str(export_path.parent)])
                elif platform.system() == 'Windows':
                    subprocess.run(['explorer', str(export_path.parent)])

        except Exception as e:
            print(f"\n{Colors.FAIL}❌ ОШИБКА ПРИ СОХРАНЕНИИ:{Colors.ENDC}")
            print(f"   {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()

    def ask_export(self, data_type: str, default_filename: str):
        print(f"\n{Colors.CYAN}📤 Экспортировать {data_type} в CSV? (y/n): {Colors.ENDC}", end=' ')
        if input().lower() == 'y':
            filename = input(f"{Colors.CYAN}Имя файла (Enter={default_filename}): {Colors.ENDC}").strip()
            if not filename:
                filename = default_filename
            self.export_to_csv(filename, data_type)
            return True
        return False

    def interactive_menu(self):
        while True:
            print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
            print(f"{Colors.BOLD}📊 LOG ANALYZER - ИНТЕРАКТИВНОЕ МЕНЮ{Colors.ENDC}")
            print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
            print(f"1. Показать сигналы")
            print(f"2. Показать позиции")
            print(f"3. Показать ошибки")
            print(f"4. Показать статистику")
            print(f"5. Экспорт всех данных в CSV")
            print(f"0. Выход")
            print(f"\n{Colors.CYAN}📂 Папка экспорта: {self.output_dir}{Colors.ENDC}")

            choice = input(f"\n{Colors.CYAN}Выберите действие: {Colors.ENDC}").strip()

            if choice == '1':
                symbol = input("Фильтр по символу (Enter для всех): ").strip()
                subtype = input("Фильтр по подтипу (1-LIMIT/2-INSTANT/3-WATCH, Enter для всех): ").strip()
                limit = input("Количество записей (Enter=50): ").strip()
                limit = int(limit) if limit else 50

                subtype_map = {'1': 'LIMIT', '2': 'INSTANT', '3': 'WATCH'}
                subtype_filter = subtype_map.get(subtype) if subtype else None

                self.show_signals(filter_symbol=symbol or None, filter_subtype=subtype_filter, limit=limit)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.ask_export("сигналы", f"signals_{timestamp}.csv")

            elif choice == '2':
                symbol = input("Фильтр по символу (Enter для всех): ").strip()
                action = input("Фильтр по действию (1-OPENED/2-CLOSED, Enter для всех): ").strip()
                limit = input("Количество записей (Enter=50): ").strip()
                limit = int(limit) if limit else 50

                action_map = {'1': 'OPENED', '2': 'CLOSED'}
                action_filter = action_map.get(action) if action else None

                self.show_positions(filter_symbol=symbol or None, filter_action=action_filter, limit=limit)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.ask_export("позиции", f"positions_{timestamp}.csv")

            elif choice == '3':
                level = input("Фильтр по уровню (1-ERROR/2-CRITICAL/3-WARNING, Enter для всех): ").strip()
                module = input("Фильтр по модулю (Enter для всех): ").strip()
                limit = input("Количество записей (Enter=50): ").strip()
                limit = int(limit) if limit else 50

                level_map = {'1': 'ERROR', '2': 'CRITICAL', '3': 'WARNING'}
                level_filter = level_map.get(level) if level else None

                self.show_errors(filter_level=level_filter, filter_module=module or None, limit=limit)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.ask_export("ошибки", f"errors_{timestamp}.csv")

            elif choice == '4':
                self.show_stats()
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.ask_export("статистику", f"stats_{timestamp}.csv")

            elif choice == '5':
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.export_to_csv(f"full_export_{timestamp}.csv", "all")

            elif choice == '0':
                print(f"{Colors.GREEN}До свидания!{Colors.ENDC}")
                break

            else:
                print(f"{Colors.WARNING}⚠️ Неверный выбор. Попробуйте снова.{Colors.ENDC}")


def main():
    parser = argparse.ArgumentParser(description='Анализатор логов торгового бота')
    parser.add_argument('--log-path', '-l', help='Путь к лог-файлу')
    parser.add_argument('--output-dir', '-o', help='Папка для сохранения результатов (по умолчанию: logs/exports/YYYY-MM-DD/)')
    parser.add_argument('--export', '-e', help='Экспорт всех данных в CSV (указать имя файла)')
    parser.add_argument('--signals', '-s', action='store_true', help='Показать сигналы')
    parser.add_argument('--stats', '-st', action='store_true', help='Показать статистику')

    args = parser.parse_args()

    analyzer = LogAnalyzer(log_path=args.log_path, output_dir=args.output_dir)

    if not analyzer.parse_log():
        sys.exit(1)

    if args.export:
        analyzer.export_to_csv(args.export, "all")
    elif args.signals:
        analyzer.show_signals()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        analyzer.ask_export("сигналы", f"signals_{timestamp}.csv")
    elif args.stats:
        analyzer.show_stats()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        analyzer.ask_export("статистику", f"stats_{timestamp}.csv")
    else:
        analyzer.interactive_menu()


if __name__ == "__main__":
    main()