# analyzer/core/data_classes.py (ПОЛНОСТЬЮ - ОБНОВЛЁННАЯ ВЕРСИЯ)
"""
🏷️ КЛАССЫ ДАННЫХ ДЛЯ СИГНАЛОВ И СДЕЛОК
ФАЗА 1.3.6:
- Добавлен SignalType.M15 (вместо LIMIT/INSTANT)
- Добавлены поля zone_* в ThreeScreenAnalysis
- Обновлён Screen2Result для zone_*
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum
import json


class Direction(Enum):
    """Направление торговли"""
    LONG = "LONG"
    SHORT = "SHORT"


class SignalStatus(Enum):
    """Статус сигнала"""
    WATCH = "WATCH"
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


class TradeStatus(Enum):
    """Статус сделки"""
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    CANCELLED = "CANCELLED"


class SignalType(Enum):
    """Типы сигналов (Фаза 1.3.6)"""
    WATCH = "WATCH"  # Пре-сигнал (монета в зоне интереса)
    M15 = "M15"  # Торговый сигнал (рыночный ордер, 3ч, R/R ≥ 3:1)


# ========== SCREEN RESULTS ==========

@dataclass
class Screen1Result:
    """Результат анализа 1-го экрана (тренд)"""
    trend_direction: str = ""  # BULL, BEAR, SIDEWAYS
    trend_strength: float = 0.0  # 0-100%
    trend_age: int = 0  # В свечах
    key_levels: Dict[str, float] = field(default_factory=dict)  # support/resistance
    indicators: Dict[str, Any] = field(default_factory=dict)
    confidence_score: float = 0.0
    passed: bool = False
    rejection_reason: str = ""


@dataclass
class Screen2Result:
    """Результат анализа 2-го экрана (зоны входа)"""
    entry_zones: List[Dict] = field(default_factory=list)
    best_zone: Optional[float] = None
    invalidated_zones: List[float] = field(default_factory=list)
    fib_levels: Dict[str, float] = field(default_factory=dict)
    volume_confirmation: bool = False
    passed: bool = False
    confidence: float = 0.0
    rejection_reason: str = ""

    # ✅ НОВЫЕ ПОЛЯ для Фазы 1.3.6
    zone_low: float = 0.0
    zone_high: float = 0.0
    screen2_score: int = 0
    expected_pattern: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry_zones": self.entry_zones,
            "best_zone": self.best_zone,
            "invalidated_zones": self.invalidated_zones,
            "fib_levels": self.fib_levels,
            "volume_confirmation": self.volume_confirmation,
            "passed": self.passed,
            "confidence": self.confidence,
            "zone_low": self.zone_low,
            "zone_high": self.zone_high,
            "screen2_score": self.screen2_score,
            "expected_pattern": self.expected_pattern
        }


@dataclass
class Screen3Result:
    """Результат анализа 3-го экрана (сигналы)"""
    signal_type: str = ""  # BUY/SELL
    signal_subtype: str = "M15"  # M15 (только один тип)
    entry_price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    signal_strength: str = "WEAK"
    trigger_pattern: str = ""
    confidence: float = 0.0
    expiration_time: Optional[datetime] = None
    passed: bool = False
    indicators: Dict[str, Any] = field(default_factory=dict)
    rejection_reason: str = ""
    order_type: str = "MARKET"  # Всегда MARKET для M15

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signal_type": self.signal_type,
            "signal_subtype": self.signal_subtype,
            "entry_price": self.entry_price,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "signal_strength": self.signal_strength,
            "trigger_pattern": self.trigger_pattern,
            "confidence": self.confidence,
            "expiration_time": self.expiration_time.isoformat() if self.expiration_time else None,
            "passed": self.passed,
            "indicators": self.indicators,
            "order_type": self.order_type
        }


@dataclass
class ThreeScreenAnalysis:
    """Результат трехэкранного анализа"""
    screen1: Screen1Result = field(default_factory=Screen1Result)
    screen2: Screen2Result = field(default_factory=Screen2Result)
    screen3: Screen3Result = field(default_factory=Screen3Result)
    overall_confidence: float = 0.0
    risk_reward_ratio: float = 0.0
    symbol: str = ""
    signal_type: SignalType = SignalType.M15
    timestamp: datetime = field(default_factory=datetime.now)
    should_trade: bool = False

    # ✅ НОВЫЕ ПОЛЯ для Фазы 1.3.6 (из Screen2 для сохранения)
    zone_low: float = 0.0
    zone_high: float = 0.0
    expected_pattern: str = ""
    screen2_score: int = 0


# ========== SIGNALS AND TRADES ==========

@dataclass
class Signal:
    """
    Торговый сигнал
    """
    symbol: str
    strategy: str = 'three_screen'
    direction: Direction = Direction.LONG
    status: SignalStatus = SignalStatus.WATCH
    confidence: float = 0.0
    three_screen_analysis: Optional[ThreeScreenAnalysis] = None
    entry_prices: List[float] = field(default_factory=list)
    stop_loss: float = 0.0
    stop_loss_levels: List[float] = field(default_factory=list)
    take_profit_levels: List[float] = field(default_factory=list)
    position_size: float = 0.0
    margin_mode: str = 'cross'
    leverage: int = 10
    total_capital: float = 1000.0
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """
        Преобразование Signal в словарь для валидатора и сохранения

        Returns:
            Dict с данными сигнала
        """
        return {
            'symbol': self.symbol,
            'signal_type': 'BUY' if self.direction == Direction.LONG else 'SELL',
            'entry_price': self.entry_prices[0] if self.entry_prices else 0,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit_levels[0] if self.take_profit_levels else 0,
            'direction': self.direction.value,
            'confidence': self.confidence,
            'strategy': self.strategy,
            'status': self.status.value,
            'entry_prices': self.entry_prices,
            'take_profit_levels': self.take_profit_levels,
            'stop_loss_levels': self.stop_loss_levels,
            'position_size': self.position_size,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'three_screen_analysis': self.three_screen_analysis.to_dict() if self.three_screen_analysis else None
        }


@dataclass
class Trade:
    """Базовая сделка"""
    symbol: str
    direction: Direction
    entry_price: float
    quantity: float
    stop_loss: float
    take_profit: float
    commission: float = 0.0
    pnl: float = 0.0
    pnl_percent: float = 0.0
    status: TradeStatus = TradeStatus.OPEN
    opened_at: datetime = field(default_factory=datetime.now)
    closed_at: Optional[datetime] = None
    id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'symbol': self.symbol,
            'direction': self.direction.value,
            'entry_price': self.entry_price,
            'quantity': self.quantity,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'commission': self.commission,
            'pnl': self.pnl,
            'pnl_percent': self.pnl_percent,
            'status': self.status.value,
            'opened_at': self.opened_at.isoformat() if self.opened_at else None,
            'closed_at': self.closed_at.isoformat() if self.closed_at else None
        }


@dataclass
class PaperTrade(Trade):
    """Виртуальная сделка для Paper режима"""
    virtual_balance_before: float = 0.0
    virtual_balance_after: float = 0.0
    simulated_slippage: float = 0.0
    simulated_commission: float = 0.0
    fill_rate: float = 1.0  # Процент исполнения ордера (0.0-1.0)

    def to_dict(self) -> Dict[str, Any]:
        base_dict = super().to_dict()
        base_dict.update({
            "virtual_balance_before": self.virtual_balance_before,
            "virtual_balance_after": self.virtual_balance_after,
            "simulated_slippage": self.simulated_slippage,
            "simulated_commission": self.simulated_commission,
            "fill_rate": self.fill_rate
        })
        return base_dict


# ========== DATABASE MODELS ==========

@dataclass
class SignalModel:
    """Модель сигнала для БД"""
    id: Optional[int] = None
    symbol: str = ""
    signal_type: str = ""  # BUY/SELL
    signal_subtype: str = "M15"
    entry_price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    direction: str = ""
    confidence: float = 0.0
    status: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    zone_low: float = 0.0
    zone_high: float = 0.0
    expected_pattern: str = ""
    screen2_score: int = 0


@dataclass
class PaperTradeModel:
    """Модель Paper сделки для БД"""
    id: Optional[int] = None
    symbol: str = ""
    entry_price: float = 0.0
    exit_price: Optional[float] = None
    quantity: float = 0.0
    pnl: float = 0.0
    pnl_percent: float = 0.0
    status: str = ""
    opened_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None


# ========== HELPER METHODS ==========

def three_screen_analysis_to_dict(self) -> Dict[str, Any]:
    """Метод to_dict для ThreeScreenAnalysis"""
    return {
        'screen1': {
            'trend_direction': self.screen1.trend_direction,
            'trend_strength': self.screen1.trend_strength,
            'trend_age': self.screen1.trend_age,
            'key_levels': self.screen1.key_levels,
            'confidence_score': self.screen1.confidence_score,
            'passed': self.screen1.passed
        },
        'screen2': {
            'entry_zones': self.screen2.entry_zones,
            'best_zone': self.screen2.best_zone,
            'invalidated_zones': self.screen2.invalidated_zones,
            'fib_levels': self.screen2.fib_levels,
            'volume_confirmation': self.screen2.volume_confirmation,
            'confidence': self.screen2.confidence,
            'passed': self.screen2.passed,
            'zone_low': self.screen2.zone_low,
            'zone_high': self.screen2.zone_high,
            'screen2_score': self.screen2.screen2_score,
            'expected_pattern': self.screen2.expected_pattern
        },
        'screen3': {
            'signal_type': self.screen3.signal_type,
            'signal_subtype': self.screen3.signal_subtype,
            'entry_price': self.screen3.entry_price,
            'stop_loss': self.screen3.stop_loss,
            'take_profit': self.screen3.take_profit,
            'signal_strength': self.screen3.signal_strength,
            'trigger_pattern': self.screen3.trigger_pattern,
            'confidence': self.screen3.confidence,
            'expiration_time': self.screen3.expiration_time.isoformat() if self.screen3.expiration_time else None,
            'passed': self.screen3.passed,
            'order_type': self.screen3.order_type
        },
        'overall_confidence': self.overall_confidence,
        'risk_reward_ratio': self.risk_reward_ratio,
        'symbol': self.symbol,
        'signal_type': self.signal_type.value,
        'timestamp': self.timestamp.isoformat() if self.timestamp else None,
        'zone_low': self.zone_low,
        'zone_high': self.zone_high,
        'expected_pattern': self.expected_pattern,
        'screen2_score': self.screen2_score
    }


def screen1_result_to_dict(self) -> Dict[str, Any]:
    """Метод to_dict для Screen1Result"""
    return {
        'trend_direction': self.trend_direction,
        'trend_strength': self.trend_strength,
        'trend_age': self.trend_age,
        'key_levels': self.key_levels,
        'confidence_score': self.confidence_score,
        'passed': self.passed
    }


def screen2_result_to_dict(self) -> Dict[str, Any]:
    """Метод to_dict для Screen2Result"""
    return {
        'entry_zones': self.entry_zones,
        'best_zone': self.best_zone,
        'invalidated_zones': self.invalidated_zones,
        'fib_levels': self.fib_levels,
        'volume_confirmation': self.volume_confirmation,
        'confidence': self.confidence,
        'passed': self.passed,
        'zone_low': self.zone_low,
        'zone_high': self.zone_high,
        'screen2_score': self.screen2_score,
        'expected_pattern': self.expected_pattern
    }


def screen3_result_to_dict(self) -> Dict[str, Any]:
    """Метод to_dict для Screen3Result"""
    return {
        'signal_type': self.signal_type,
        'signal_subtype': self.signal_subtype,
        'entry_price': self.entry_price,
        'stop_loss': self.stop_loss,
        'take_profit': self.take_profit,
        'signal_strength': self.signal_strength,
        'trigger_pattern': self.trigger_pattern,
        'confidence': self.confidence,
        'expiration_time': self.expiration_time.isoformat() if self.expiration_time else None,
        'passed': self.passed,
        'order_type': self.order_type
    }


# Добавляем методы to_dict к классам
ThreeScreenAnalysis.to_dict = three_screen_analysis_to_dict
Screen1Result.to_dict = screen1_result_to_dict
Screen2Result.to_dict = screen2_result_to_dict
Screen3Result.to_dict = screen3_result_to_dict

__all__ = [
    # Enums
    'Direction',
    'SignalStatus',
    'TradeStatus',
    'SignalType',

    # Screen Results
    'Screen1Result',
    'Screen2Result',
    'Screen3Result',
    'ThreeScreenAnalysis',

    # Trading
    'Signal',
    'Trade',
    'PaperTrade',

    # Database
    'SignalModel',
    'PaperTradeModel'
]