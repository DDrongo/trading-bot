# analyzer/core/light_trader.py
"""
🎯 LIGHT TRADER — упрощённый режим торговли (Фаза 1.5.0)

Логика:
1. D1: Определение тренда по EMA20/EMA50
2. H4: Ожидание отката к EMA20 (zone_proximity_pct)
3. M15: Вход при появлении паттерна (Engulfing, Pin Bar, Morning Star)

ИСПРАВЛЕНИЯ (12.04.2026):
- ✅ Реальная цена из DataProvider (не из H4 свечи)
- ✅ Проверка дубликатов перед генерацией
- ✅ Проверка, что локальный минимум НИЖЕ Entry для BUY
- ✅ Проверка, что локальный максимум ВЫШЕ Entry для SELL
"""

import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta

from .screen1_trend_analyzer import Screen1TrendAnalyzer
from .screen3_signal_generator import Screen3SignalGenerator
from .data_classes import ThreeScreenAnalysis, Screen1Result, Screen2Result, Screen3Result
from .event_bus import EventType, event_bus

logger = logging.getLogger('light_trader')


class LightTrader:
    """
    Упрощённый трейдер на EMA и откатах
    """

    def __init__(self, config: Dict[str, Any], data_provider):
        self.config = config
        self.data_provider = data_provider

        # Загружаем настройки Light режима
        light_config = config.get('light_trader', {})
        self.enabled = light_config.get('enabled', True)
        self.d1_ema_fast = light_config.get('d1_ema_fast', 20)
        self.d1_ema_slow = light_config.get('d1_ema_slow', 50)
        self.h4_ema = light_config.get('h4_ema', 20)
        self.zone_proximity_pct = light_config.get('zone_proximity_pct', 0.5)
        self.risk_reward_ratio = light_config.get('risk_reward_ratio', 2.0)
        self.min_patterns = light_config.get('min_patterns', ['ENGULFING', 'PIN_BAR', 'MORNING_STAR'])
        self.expiration_hours = light_config.get('expiration_hours', 3)

        # Используем существующие анализаторы
        self.screen1_analyzer = Screen1TrendAnalyzer(config)
        self.pattern_analyzer = Screen3SignalGenerator(config)

        logger.info(f"✅ LightTrader инициализирован")
        logger.info(f"   EMA D1: {self.d1_ema_fast}/{self.d1_ema_slow}")
        logger.info(f"   EMA H4: {self.h4_ema}")
        logger.info(f"   Зона отката: {self.zone_proximity_pct}%")
        logger.info(f"   R/R: {self.risk_reward_ratio}:1")
        logger.info(f"   Паттерны: {self.min_patterns}")

    async def analyze_symbol(self, symbol: str) -> Optional[ThreeScreenAnalysis]:
        """
        Основной метод анализа символа в Light режиме

        Returns:
            ThreeScreenAnalysis с заполненными screen1 и screen3,
            или None если сигнал не сгенерирован
        """
        logger.info(f"🔍 Light анализ {symbol}")

        try:
            # ========== ПРОВЕРКА ДУБЛИКАТОВ ==========
            from .signal_repository import signal_repository

            # Проверяем, есть ли уже активный M15 сигнал
            if await signal_repository.has_active_m15(symbol):
                logger.info(f"⏭️ {symbol}: уже есть активный M15 сигнал")
                return None

            # Проверяем, был ли REJECTED в последние 15 минут
            try:
                import aiosqlite
                db_path = signal_repository.db_path
                async with aiosqlite.connect(db_path) as conn:
                    cursor = await conn.execute("""
                        SELECT COUNT(*) FROM signals 
                        WHERE symbol = ? 
                        AND signal_subtype = 'M15'
                        AND status = 'REJECTED'
                        AND created_time > datetime('now', '-15 minutes')
                    """, (symbol,))
                    row = await cursor.fetchone()
                    if row and row[0] > 0:
                        logger.info(f"⏭️ {symbol}: был REJECTED в последние 15 минут")
                        return None
            except Exception as e:
                logger.warning(f"⚠️ {symbol}: ошибка проверки REJECTED: {e}")

            # ========== ШАГ 1: ПОЛУЧАЕМ ДАННЫЕ ==========
            klines_data = await self._get_klines(symbol)
            if not klines_data:
                logger.warning(f"❌ {symbol}: не удалось получить данные")
                return None

            d1_klines = klines_data.get('1d', [])
            h4_klines = klines_data.get('4h', [])
            m15_klines = klines_data.get('15m', [])

            if not d1_klines or not h4_klines or not m15_klines:
                logger.warning(f"❌ {symbol}: недостаточно данных")
                return None

            # ========== ШАГ 2: РЕАЛЬНАЯ ЦЕНА ==========
            current_price = await self.data_provider.get_current_price(symbol, force_refresh=True)
            if not current_price:
                logger.warning(f"❌ {symbol}: не удалось получить текущую цену")
                return None

            logger.info(f"📊 {symbol}: реальная цена = {current_price:.6f}")

            # ========== ШАГ 3: ОПРЕДЕЛЯЕМ ТРЕНД НА D1 ==========
            d1_closes = self._extract_closes(d1_klines)
            d1_ema20 = self._calculate_ema(d1_closes, self.d1_ema_fast)
            d1_ema50 = self._calculate_ema(d1_closes, self.d1_ema_slow)

            if not d1_ema20 or not d1_ema50:
                logger.warning(f"❌ {symbol}: не удалось рассчитать EMA D1")
                return None

            current_ema20 = d1_ema20[-1]
            current_ema50 = d1_ema50[-1]

            # Определяем направление
            if current_ema20 > current_ema50:
                trend_direction = "BULL"
                allowed_direction = "BUY"
            elif current_ema20 < current_ema50:
                trend_direction = "BEAR"
                allowed_direction = "SELL"
            else:
                logger.info(f"⏭️ {symbol}: SIDEWAYS (EMA20 ≈ EMA50)")
                return None

            logger.info(
                f"📊 {symbol}: D1 тренд = {trend_direction} (EMA20={current_ema20:.2f}, EMA50={current_ema50:.2f})")

            # ========== ШАГ 4: ПРОВЕРЯЕМ ОТКАТ К EMA20 НА H4 ==========
            h4_closes = self._extract_closes(h4_klines)
            h4_ema20 = self._calculate_ema(h4_closes, self.h4_ema)

            if not h4_ema20:
                logger.warning(f"❌ {symbol}: не удалось рассчитать EMA H4")
                return None

            current_h4_ema20 = h4_ema20[-1]

            # Проверяем близость РЕАЛЬНОЙ цены к EMA20
            distance_pct = abs(current_price - current_h4_ema20) / current_h4_ema20 * 100

            if distance_pct > self.zone_proximity_pct:
                logger.info(
                    f"⏭️ {symbol}: цена {current_price:.6f} далеко от EMA20 H4 ({current_h4_ema20:.6f}), расстояние {distance_pct:.2f}% > {self.zone_proximity_pct}%")
                return None

            logger.info(
                f"✅ {symbol}: цена {current_price:.6f} в зоне EMA20 H4 ({current_h4_ema20:.6f}), расстояние {distance_pct:.2f}%")

            # ========== ШАГ 5: ИЩЕМ ПАТТЕРН НА M15 ==========
            m15_converted = self._convert_klines_for_pattern(m15_klines)

            # Создаём фейковый Screen1Result для паттерн-анализатора
            fake_screen1 = Screen1Result()
            fake_screen1.trend_direction = trend_direction
            fake_screen1.passed = True
            fake_screen1.confidence_score = 0.8

            fake_screen2 = Screen2Result()
            fake_screen2.passed = True
            fake_screen2.confidence = 0.8
            fake_screen2.best_zone = current_price

            # Ищем паттерны
            patterns = self.pattern_analyzer._find_chart_patterns_m15(m15_converted, trend_direction)

            if not patterns:
                logger.info(f"⏭️ {symbol}: нет подтверждающего паттерна на M15")
                return None

            # Фильтруем по разрешённым паттернам
            valid_patterns = [p for p in patterns if p.get('type') in self.min_patterns]
            if not valid_patterns:
                logger.info(
                    f"⏭️ {symbol}: паттерны {[p.get('type') for p in patterns]} не в списке разрешённых {self.min_patterns}")
                return None

            best_pattern = max(valid_patterns, key=lambda x: x.get('confidence', 0))
            logger.info(
                f"✅ {symbol}: найден паттерн {best_pattern.get('type')} (уверенность: {best_pattern.get('confidence', 0):.1%})")

            # ========== ШАГ 6: РАССЧИТЫВАЕМ SL И TP ==========
            entry_price = current_price

            if trend_direction == "BULL":
                # BUY: SL должен быть НИЖЕ Entry
                local_min = self._find_local_minimum(m15_converted)

                if local_min is None:
                    logger.warning(f"❌ {symbol}: не удалось найти локальный минимум")
                    return None

                if local_min >= entry_price:
                    logger.info(
                        f"⏭️ {symbol}: локальный минимум {local_min:.6f} >= Entry {entry_price:.6f}, разворота нет")
                    return None

                stop_loss = local_min
                logger.info(f"📏 {symbol}: Entry={entry_price:.6f}, Local Min={local_min:.6f}, SL={stop_loss:.6f}")

            else:  # BEAR
                # SELL: SL должен быть ВЫШЕ Entry
                local_max = self._find_local_maximum(m15_converted)

                if local_max is None:
                    logger.warning(f"❌ {symbol}: не удалось найти локальный максимум")
                    return None

                if local_max <= entry_price:
                    logger.info(
                        f"⏭️ {symbol}: локальный максимум {local_max:.6f} <= Entry {entry_price:.6f}, разворота нет")
                    return None

                stop_loss = local_max
                logger.info(f"📏 {symbol}: Entry={entry_price:.6f}, Local Max={local_max:.6f}, SL={stop_loss:.6f}")

            # TP на основе R/R
            risk = abs(entry_price - stop_loss)
            reward = risk * self.risk_reward_ratio

            if trend_direction == "BULL":
                take_profit = entry_price + reward
            else:
                take_profit = entry_price - reward

            logger.info(
                f"📏 {symbol}: риск={risk:.6f} ({risk / entry_price * 100:.2f}%), TP={take_profit:.6f}, R/R={self.risk_reward_ratio}:1")

            # ========== ШАГ 7: СОЗДАЁМ РЕЗУЛЬТАТ ==========
            screen1 = Screen1Result()
            screen1.trend_direction = trend_direction
            screen1.passed = True
            screen1.confidence_score = 0.8
            screen1.indicators = {
                'ema_20': current_ema20,
                'ema_50': current_ema50,
                'adx': 0  # Не используется в Light
            }

            screen3 = Screen3Result()
            screen3.signal_type = allowed_direction
            screen3.signal_subtype = "M15"
            screen3.entry_price = self._round_price(entry_price, symbol)
            screen3.stop_loss = self._round_price(stop_loss, symbol)
            screen3.take_profit = self._round_price(take_profit, symbol)
            screen3.trigger_pattern = best_pattern.get('type', 'UNKNOWN')
            screen3.confidence = best_pattern.get('confidence', 0.7)
            screen3.passed = True
            screen3.expiration_time = datetime.now() + timedelta(hours=self.expiration_hours)
            screen3.order_type = "MARKET"
            screen3.indicators = {
                'risk_reward_ratio': self.risk_reward_ratio,
                'risk_pct': risk / entry_price * 100,
                'atr': risk / 1.5  # Примерный ATR
            }

            analysis = ThreeScreenAnalysis(
                symbol=symbol,
                screen1=screen1,
                screen2=Screen2Result(),
                screen3=screen3,
                overall_confidence=screen3.confidence,
                risk_reward_ratio=self.risk_reward_ratio,
                should_trade=True
            )

            logger.info(
                f"✅ {symbol}: LIGHT СИГНАЛ! {allowed_direction} @ {entry_price:.6f}, SL={stop_loss:.6f}, TP={take_profit:.6f}, R/R={self.risk_reward_ratio}:1")

            # ========== ШАГ 8: СОХРАНЯЕМ СИГНАЛ ==========
            await self._save_signal(analysis)

            return analysis

        except Exception as e:
            logger.error(f"❌ Ошибка Light анализа {symbol}: {e}")
            import traceback
            traceback.print_exc()
            return None

    async def _get_klines(self, symbol: str) -> Dict[str, List]:
        """Получение свечей для всех таймфреймов"""
        try:
            timeframes = {
                '1d': 100,
                '4h': 50,
                '15m': 50
            }

            klines_data = {}
            for tf, limit in timeframes.items():
                klines = await self.data_provider.get_klines(symbol, tf, limit)
                if klines:
                    klines_data[tf] = klines

            return klines_data

        except Exception as e:
            logger.error(f"❌ Ошибка получения данных {symbol}: {e}")
            return {}

    def _extract_closes(self, klines: List) -> List[float]:
        """Извлечение цен закрытия из свечей"""
        closes = []
        for k in klines:
            try:
                closes.append(float(k[4]))
            except (ValueError, TypeError, IndexError):
                continue
        return closes

    def _calculate_ema(self, prices: List[float], period: int) -> List[float]:
        """Расчёт EMA"""
        if len(prices) < period:
            return []

        multiplier = 2 / (period + 1)
        ema = [sum(prices[:period]) / period]

        for price in prices[period:]:
            ema.append((price * multiplier) + (ema[-1] * (1 - multiplier)))

        return ema

    def _convert_klines_for_pattern(self, klines: List) -> List:
        """Конвертация свечей в формат для паттерн-анализатора"""
        converted = []
        for k in klines:
            try:
                converted.append([
                    k[0],  # timestamp
                    float(k[1]),  # open
                    float(k[2]),  # high
                    float(k[3]),  # low
                    float(k[4]),  # close
                    float(k[5]) if len(k) > 5 else 0  # volume
                ])
            except (ValueError, TypeError, IndexError):
                continue
        return converted

    def _find_local_minimum(self, m15_klines: List) -> Optional[float]:
        """Поиск локального минимума на M15"""
        if len(m15_klines) < 5:
            return None

        # Ищем минимум за последние 10 свечей
        lows = []
        for k in m15_klines[-10:]:
            try:
                lows.append(float(k[3]))  # low
            except (ValueError, TypeError, IndexError):
                continue

        return min(lows) if lows else None

    def _find_local_maximum(self, m15_klines: List) -> Optional[float]:
        """Поиск локального максимума на M15"""
        if len(m15_klines) < 5:
            return None

        # Ищем максимум за последние 10 свечей
        highs = []
        for k in m15_klines[-10:]:
            try:
                highs.append(float(k[2]))  # high
            except (ValueError, TypeError, IndexError):
                continue

        return max(highs) if highs else None

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

    async def _save_signal(self, analysis: ThreeScreenAnalysis) -> None:
        """Сохранение сигнала через SignalRepository"""
        try:
            from .signal_repository import signal_repository

            signal_id = await signal_repository.save_signal(analysis)
            if signal_id:
                logger.info(f"💾 Light сигнал {analysis.symbol} сохранён (ID={signal_id})")

                # Публикуем событие для Position Manager
                await event_bus.publish(
                    EventType.TRADING_SIGNAL_GENERATED,
                    {
                        'signal_id': signal_id,
                        'symbol': analysis.symbol,
                        'signal_type': analysis.screen3.signal_type,
                        'entry_price': analysis.screen3.entry_price,
                        'stop_loss': analysis.screen3.stop_loss,
                        'take_profit': analysis.screen3.take_profit,
                        'confidence': analysis.overall_confidence,
                        'risk_reward_ratio': analysis.risk_reward_ratio,
                        'signal_subtype': 'M15',
                        'order_type': 'MARKET',
                        'expiration_time': analysis.screen3.expiration_time.isoformat() if analysis.screen3.expiration_time else None,
                        'leverage': self.config.get('paper_trading', {}).get('leverage', 10)
                    },
                    'light_trader'
                )
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения Light сигнала: {e}")


__all__ = ['LightTrader']