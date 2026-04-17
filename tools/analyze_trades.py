#!/usr/bin/env python3
# tools/analyze_trades.py
"""
Анализатор сделок и сигналов
Использование: python tools/analyze_trades.py [--db PATH] [--output PATH]
"""

import sqlite3
import argparse
import os
from datetime import datetime
from pathlib import Path


def analyze_trades(db_path: str, output_path: str = None):
    """Основная функция анализа"""

    if not os.path.exists(db_path):
        print(f"❌ База данных не найдена: {db_path}")
        return

    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"analysis_report_{timestamp}.txt"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    with open(output_path, 'w', encoding='utf-8') as f:

        def p(text=""):
            print(text, file=f)

        p('=' * 80)
        p('                        ОТЧЁТ ПО ТОРГОВЛЕ (PRO РЕЖИМ)')
        p('=' * 80)
        p()
        p(f'Дата: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        p(f'База данных: {db_path}')
        p()

        # 1. Статистика по монетам
        p('=' * 80)
        p('1. СТАТИСТИКА ПО МОНЕТАМ')
        p('=' * 80)
        cursor.execute("""
            SELECT 
                symbol,
                COUNT(*) as trades,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses,
                ROUND(AVG(pnl), 2) as avg_pnl,
                ROUND(SUM(pnl), 2) as total_pnl,
                ROUND(AVG(pnl_percent), 2) as avg_pnl_pct,
                ROUND(AVG((julianday(closed_at) - julianday(opened_at)) * 86400), 1) as avg_seconds
            FROM trades 
            WHERE status='CLOSED'
            GROUP BY symbol 
            ORDER BY trades DESC
        """)
        rows = cursor.fetchall()
        if rows:
            p(f"{'Символ':<12} {'Сделок':>8} {'Wins':>6} {'Losses':>8} {'Avg PnL':>10} {'Total PnL':>12} {'Avg PnL%':>10} {'Avg сек':>8}")
            p('-' * 80)
            for row in rows:
                p(f"{row[0]:<12} {row[1]:>8} {row[2]:>6} {row[3]:>8} {row[4]:>10} {row[5]:>12} {row[6]:>10} {row[7]:>8}")
        p()

        # 2. Распределение по часам
        p('=' * 80)
        p('2. РАСПРЕДЕЛЕНИЕ ПО ЧАСАМ')
        p('=' * 80)
        cursor.execute("""
            SELECT 
                strftime('%H', closed_at) as hour,
                COUNT(*) as trades,
                ROUND(SUM(pnl), 2) as total_pnl
            FROM trades 
            WHERE status='CLOSED'
            GROUP BY hour 
            ORDER BY hour
        """)
        rows = cursor.fetchall()
        if rows:
            p(f"{'Час':<8} {'Сделок':>10} {'Total PnL':>12}")
            p('-' * 30)
            for row in rows:
                p(f"{row[0]:<8} {row[1]:>10} {row[2]:>12}")
        p()

        # 3. Причины закрытия
        p('=' * 80)
        p('3. ПРИЧИНЫ ЗАКРЫТИЯ')
        p('=' * 80)
        cursor.execute("""
            SELECT 
                close_reason,
                COUNT(*) as count,
                ROUND(AVG(pnl), 2) as avg_pnl,
                ROUND(SUM(pnl), 2) as total_pnl
            FROM trades 
            WHERE status='CLOSED' AND close_reason IS NOT NULL
            GROUP BY close_reason
        """)
        rows = cursor.fetchall()
        if rows:
            p(f"{'Причина':<15} {'Кол-во':>10} {'Avg PnL':>12} {'Total PnL':>12}")
            p('-' * 50)
            for row in rows:
                p(f"{row[0]:<15} {row[1]:>10} {row[2]:>12} {row[3]:>12}")
        p()

        # 4. Топ-10 убыточных
        p('=' * 80)
        p('4. ТОП-10 УБЫТОЧНЫХ СДЕЛОК')
        p('=' * 80)
        cursor.execute("""
            SELECT symbol, entry_price, close_price, ROUND(pnl, 2) as pnl, 
                   ROUND(pnl_percent, 2) as pnl_pct, close_reason, closed_at
            FROM trades 
            WHERE status='CLOSED' AND pnl < 0
            ORDER BY pnl ASC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        if rows:
            p(f"{'Символ':<12} {'Entry':>10} {'Exit':>10} {'PnL':>10} {'PnL%':>8} {'Причина':<12} {'Закрыта'}")
            p('-' * 80)
            for row in rows:
                p(f"{row[0]:<12} {row[1]:>10.4f} {row[2]:>10.4f} {row[3]:>10} {row[4]:>8} {row[5]:<12} {row[6]}")
        p()

        # 5. Топ-10 прибыльных
        p('=' * 80)
        p('5. ТОП-10 ПРИБЫЛЬНЫХ СДЕЛОК')
        p('=' * 80)
        cursor.execute("""
            SELECT symbol, entry_price, close_price, ROUND(pnl, 2) as pnl, 
                   ROUND(pnl_percent, 2) as pnl_pct, close_reason, closed_at
            FROM trades 
            WHERE status='CLOSED' AND pnl > 0
            ORDER BY pnl DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        if rows:
            p(f"{'Символ':<12} {'Entry':>10} {'Exit':>10} {'PnL':>10} {'PnL%':>8} {'Причина':<12} {'Закрыта'}")
            p('-' * 80)
            for row in rows:
                p(f"{row[0]:<12} {row[1]:>10.4f} {row[2]:>10.4f} {row[3]:>10} {row[4]:>8} {row[5]:<12} {row[6]}")
        p()

        # 6. Статистика WATCH сигналов
        p('=' * 80)
        p('6. СТАТИСТИКА WATCH СИГНАЛОВ')
        p('=' * 80)
        cursor.execute("""
            SELECT 
                symbol,
                COUNT(*) as watch_count,
                ROUND(AVG(screen2_score), 1) as avg_score
            FROM signals 
            WHERE signal_subtype='WATCH'
            GROUP BY symbol
            ORDER BY watch_count DESC
        """)
        rows = cursor.fetchall()
        if rows:
            p(f"{'Символ':<12} {'WATCH':>10} {'Avg Score':>12}")
            p('-' * 35)
            for row in rows:
                p(f"{row[0]:<12} {row[1]:>10} {row[2]:>12}")
        p()

        # 7. Score Screen2 vs PnL
        p('=' * 80)
        p('7. SCORE SCREEN2 vs PNL')
        p('=' * 80)
        cursor.execute("""
            SELECT 
                s.screen2_score,
                COUNT(*) as trades,
                ROUND(AVG(t.pnl), 2) as avg_pnl,
                SUM(CASE WHEN t.pnl > 0 THEN 1 ELSE 0 END) as wins
            FROM signals s
            JOIN trades t ON s.id = t.signal_id
            WHERE s.signal_subtype='M15' AND t.status='CLOSED'
            GROUP BY s.screen2_score
            ORDER BY s.screen2_score
        """)
        rows = cursor.fetchall()
        if rows:
            p(f"{'Score':<8} {'Сделок':>10} {'Avg PnL':>12} {'Wins':>8}")
            p('-' * 40)
            for row in rows:
                p(f"{row[0]:<8} {row[1]:>10} {row[2]:>12} {row[3]:>8}")
        p()

        # 8. Анализ трендов D1
        p('=' * 80)
        p('8. АНАЛИЗ ТРЕНДОВ D1')
        p('=' * 80)
        cursor.execute("""
            SELECT 
                symbol,
                trend_direction,
                COUNT(*) as analyses,
                ROUND(AVG(adx), 1) as avg_adx,
                ROUND(AVG(confidence), 2) as avg_confidence
            FROM trend_analysis 
            GROUP BY symbol, trend_direction
            ORDER BY symbol, analyses DESC
            LIMIT 30
        """)
        rows = cursor.fetchall()
        if rows:
            p(f"{'Символ':<12} {'Тренд':<8} {'Анализов':>10} {'Avg ADX':>10} {'Avg Conf':>10}")
            p('-' * 55)
            for row in rows:
                p(f"{row[0]:<12} {row[1]:<8} {row[2]:>10} {row[3]:>10} {row[4]:>10}")
        p()

        # 9. Общая сводка
        p('=' * 80)
        p('9. ОБЩАЯ СВОДКА')
        p('=' * 80)
        cursor.execute("SELECT COUNT(*) FROM trades WHERE status='CLOSED'")
        total = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM trades WHERE status='CLOSED' AND pnl > 0")
        wins = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM trades WHERE status='CLOSED' AND pnl < 0")
        losses = cursor.fetchone()[0]
        cursor.execute("SELECT ROUND(SUM(pnl), 2) FROM trades WHERE status='CLOSED'")
        total_pnl = cursor.fetchone()[0] or 0
        cursor.execute("SELECT COUNT(*) FROM signals WHERE signal_subtype='M15'")
        m15 = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM signals WHERE signal_subtype='WATCH'")
        watch = cursor.fetchone()[0]

        win_rate = (wins / total * 100) if total > 0 else 0

        p(f"  ИТОГО СДЕЛОК:      {total}")
        p(f"  ПРИБЫЛЬНЫХ:        {wins}")
        p(f"  УБЫТОЧНЫХ:         {losses}")
        p(f"  ОБЩИЙ PnL:         {total_pnl} USDT")
        p(f"  WIN RATE:          {win_rate:.2f}%")
        p(f"  ВСЕГО M15:         {m15}")
        p(f"  ВСЕГО WATCH:       {watch}")
        p()

        p('=' * 80)
        p(f'                        ОТЧЁТ СОХРАНЁН: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        p('=' * 80)

    conn.close()
    print(f"✅ Отчёт сохранён в: {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description='Анализатор сделок и сигналов')
    parser.add_argument('--db', type=str, default='data/trading_bot.db',
                        help='Путь к базе данных')
    parser.add_argument('--output', type=str, default=None,
                        help='Путь для сохранения отчёта')

    args = parser.parse_args()

    # Определяем путь к БД относительно корня проекта
    project_root = Path(__file__).parent.parent
    db_path = project_root / args.db

    if not db_path.exists():
        # Пробуем альтернативные пути
        alt_paths = [
            Path('data/trading_bot.db'),
            project_root / 'analyzer' / 'data' / 'trading_bot.db',
        ]
        for alt in alt_paths:
            if alt.exists():
                db_path = alt
                break

    output_path = analyze_trades(str(db_path), args.output)

    # Открываем файл
    if output_path:
        os.system(f"open {output_path}" if os.name == 'posix' else f"start {output_path}")


if __name__ == "__main__":
    main()