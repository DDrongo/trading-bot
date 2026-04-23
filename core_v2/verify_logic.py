#!/usr/bin/env python3
"""
🔍 VERIFY_LOGIC — Скрипт верификации компонентов ядра
ФАЗА 2.2: Пошаговая проверка каждого модуля
"""

import asyncio
import argparse
import sqlite3
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# Добавляем корень проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from analyzer.core.data_provider import data_provider
from analyzer.core.time_utils import utc_now


class VerificationRunner:
    """Запуск верификации компонентов"""

    def __init__(self):
        # Результаты сохраняем в core_v2/results/
        self.db_path = Path(__file__).parent / "results" / "verification_results.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

        # Импортируем компоненты из core_v2
        from core_v2.w1_trend_analyzer import W1TrendAnalyzer
        from core_v2.screen1_trend_analyzer import Screen1TrendAnalyzer

        self.w1_analyzer = W1TrendAnalyzer()
        self.d1_analyzer = Screen1TrendAnalyzer()

        self.data_provider = data_provider
        # TODO: загрузить конфиг
        self.data_provider.configure({})

    def _init_db(self):
        """Инициализация БД результатов"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS verification (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    step INTEGER NOT NULL,
                    component TEXT NOT NULL,
                    input_data TEXT,
                    output_data TEXT,
                    is_correct TEXT,
                    comment TEXT,
                    verified_at TIMESTAMP
                )
            """)

    def _save_result(self, symbol: str, step: int, component: str,
                     input_data: Dict, output_data: Dict,
                     is_correct: str, comment: str):
        """Сохранение результата верификации"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO verification 
                (symbol, step, component, input_data, output_data, is_correct, comment, verified_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol, step, component,
                json.dumps(input_data, default=str),
                json.dumps(output_data, default=str),
                is_correct, comment, utc_now().isoformat()
            ))

    async def step1_w1_analyzer(self, symbol: str) -> Dict:
        """Шаг 1: W1 Analyzer"""
        print(f"\n{'=' * 60}")
        print(f"🔍 ШАГ 1: W1 Analyzer — Глобальный тренд")
        print(f"{'=' * 60}")

        # Загружаем W1 данные
        w1_klines = await self.data_provider.get_klines(symbol, "1w", 52)

        if not w1_klines or len(w1_klines) < 20:
            print(f"❌ Недостаточно данных W1 для {symbol}")
            return {'error': 'no_data'}

        # Анализируем
        result = self.w1_analyzer.analyze(symbol, w1_klines)

        # Получаем текущую цену (может быть строкой)
        try:
            current_price = float(w1_klines[-1][4])
        except (ValueError, TypeError, IndexError):
            current_price = 0.0

        # Выводим результат
        print(f"\n📊 Входные данные:")
        print(f"   Символ: {symbol}")
        print(f"   Свечей W1: {len(w1_klines)}")
        print(f"   Текущая цена: {current_price:.2f}")

        print(f"\n📈 Результат:")
        print(f"   Тренд: {result.trend}")
        print(f"   Сила: {result.strength:.1f}%")
        print(f"   Структура: {result.structure}")
        print(f"   ADX: {result.adx:.1f}")
        print(f"   EMA20: {result.ema20:.2f}")
        print(f"   EMA50: {result.ema50:.2f}")
        print(f"   Уверенность: {result.confidence:.1%}")
        print(f"   Пройден: {result.passed}")

        print(f"\n❓ Правильно ли определён тренд?")
        is_correct = input("   (да/нет/частично): ").strip().lower()
        comment = input("   Комментарий (Enter для пропуска): ").strip()

        self._save_result(
            symbol=symbol, step=1, component='W1TrendAnalyzer',
            input_data={'symbol': symbol, 'candles_count': len(w1_klines)},
            output_data=result.to_dict(),
            is_correct=is_correct, comment=comment
        )

        return result.to_dict()

    async def step3_d1_analyzer(self, symbol: str) -> Dict:
        """Шаг 3: D1 Analyzer"""
        print(f"\n{'=' * 60}")
        print(f"🔍 ШАГ 3: D1 Analyzer — Дневной тренд")
        print(f"{'=' * 60}")

        # Загружаем D1 данные
        d1_klines = await self.data_provider.get_klines(symbol, "1d", 100)

        if not d1_klines or len(d1_klines) < 50:
            print(f"❌ Недостаточно данных D1 для {symbol}")
            return {'error': 'no_data'}

        # Анализируем
        result = self.d1_analyzer.analyze_daily_trend(symbol, d1_klines)

        print(f"\n📊 Входные данные:")
        print(f"   Символ: {symbol}")
        print(f"   Свечей D1: {len(d1_klines)}")

        print(f"\n📈 Результат:")
        print(f"   Тренд: {result.trend_direction}")
        print(f"   Сила: {result.trend_strength}")
        print(f"   ADX: {result.indicators.get('adx', 0):.1f}")
        print(f"   Уверенность: {result.confidence_score:.1%}")
        print(f"   Пройден: {result.passed}")

        print(f"\n❓ Правильно ли определён D1 тренд?")
        is_correct = input("   (да/нет/частично): ").strip().lower()
        comment = input("   Комментарий (Enter для пропуска): ").strip()

        self._save_result(
            symbol=symbol, step=3, component='Screen1TrendAnalyzer',
            input_data={'symbol': symbol, 'candles_count': len(d1_klines)},
            output_data={'trend': result.trend_direction, 'strength': result.trend_strength,
                         'adx': result.indicators.get('adx', 0), 'confidence': result.confidence_score},
            is_correct=is_correct, comment=comment
        )

        return {'trend': result.trend_direction, 'passed': result.passed}

    async def verify_all(self, symbol: str):
        """Запуск всех шагов верификации"""
        print(f"\n{'#' * 60}")
        print(f"# ВЕРИФИКАЦИЯ СИМВОЛА: {symbol}")
        print(f"{'#' * 60}")

        # Шаг 1: W1 Analyzer
        w1_result = await self.step1_w1_analyzer(symbol)
        if 'error' in w1_result:
            print("❌ Остановка верификации из-за отсутствия данных")
            return

        # Шаг 3: D1 Analyzer
        await self.step3_d1_analyzer(symbol)

        print(f"\n✅ Верификация {symbol} завершена!")

    async def step2_market_stage_manual(self):
        """Шаг 2: Market Stage — ручной ввод (отдельно)"""
        print(f"\n{'=' * 60}")
        print(f"🔍 ШАГ 2: Market Stage — Стадия рынка")
        print(f"{'=' * 60}")

        print("\n📊 Введите данные для определения стадии рынка:")
        w1_trend = input("   W1 тренд (BULL/BEAR/SIDEWAYS): ").strip().upper()
        d1_trend = input("   D1 тренд (BULL/BEAR/SIDEWAYS): ").strip().upper()
        symbol = input("   Символ: ").strip().upper()

        # Логика определения стадии
        if w1_trend == 'BEAR' and d1_trend == 'BULL':
            stage = 'BULL_CORRECTION'
            bias = 'CAUTIOUS_BUY'
        elif w1_trend == 'BULL' and d1_trend == 'BULL':
            stage = 'TREND_CONTINUATION'
            bias = 'AGGRESSIVE_BUY'
        elif w1_trend == 'BEAR' and d1_trend == 'BEAR':
            stage = 'TREND_CONTINUATION'
            bias = 'AGGRESSIVE_SELL'
        elif w1_trend == 'BULL' and d1_trend == 'BEAR':
            stage = 'BEAR_CORRECTION'
            bias = 'CAUTIOUS_SELL'
        else:
            stage = 'UNDEFINED'
            bias = 'NEUTRAL'

        result = {'stage': stage, 'bias': bias, 'w1_trend': w1_trend, 'd1_trend': d1_trend}

        print(f"\n📈 Результат:")
        print(f"   Стадия рынка: {stage}")
        print(f"   Bias: {bias}")

        print(f"\n❓ Правильно ли определена стадия?")
        is_correct = input("   (да/нет/частично): ").strip().lower()
        comment = input("   Комментарий (Enter для пропуска): ").strip()

        self._save_result(
            symbol=symbol, step=2, component='MarketStage',
            input_data={'w1_trend': w1_trend, 'd1_trend': d1_trend},
            output_data=result,
            is_correct=is_correct, comment=comment
        )

        return result


async def main():
    parser = argparse.ArgumentParser(description='Верификация компонентов ядра')
    parser.add_argument('--symbol', help='Символ (например, BTCUSDT)')
    parser.add_argument('--step', type=int, choices=[1, 2, 3], help='Номер шага (1-3)')

    args = parser.parse_args()

    runner = VerificationRunner()

    if args.step == 2:
        await runner.step2_market_stage_manual()
    elif args.symbol:
        await runner.verify_all(args.symbol)
    else:
        print("Использование:")
        print("  python core_v2/verify_logic.py --symbol BTCUSDT")
        print("  python core_v2/verify_logic.py --step 2")


if __name__ == '__main__':
    asyncio.run(main())