# core/__init__.py
"""
🎯 Пакет core - основные модули анализатора

HOTFIX 1.3.6.2:
- Исправлен импорт Screen2Analyzer (был Screen2EntryZonesAnalyzer)
- Screen2Result импортируется из data_classes
- Добавлен алиас для обратной совместимости

ФАЗА 1.5.0:
- Добавлен экспорт LightTrader
"""

# Экспортируем все основные классы для удобного импорта
from .api_client_bybit import APIClient, BybitAPIClient
from .data_classes import (
    Direction, SignalStatus, TradeStatus,
    Screen1Result, Screen2Result, Screen3Result,
    ThreeScreenAnalysis, Signal, Trade, PaperTrade
)
from .event_bus import EventType, Event, EventBus, event_bus
from .prefilter_liquidity import LiquidityPrefilter, PrefilterResult, LiquidityMetrics
from .screen1_trend_analyzer import Screen1TrendAnalyzer, Screen1Result

# ✅ ИСПРАВЛЕНО: импортируем Screen2Analyzer (реальное имя класса)
# Screen2Result уже импортирован из data_classes выше
from .screen2_entry_zones import Screen2Analyzer

from .screen3_signal_generator import Screen3SignalGenerator, Screen3Result, PatternType
from .signal_repository import SignalRepository, signal_repository
from .three_screen_analyzer import ThreeScreenAnalyzer
from .orchestrator import AnalysisOrchestrator, AnalysisSession

# ФАЗА 1.5.0: Light режим
from .light_trader import LightTrader

# ✅ ДЛЯ ОБРАТНОЙ СОВМЕСТИМОСТИ
# Если какой-то код использует старое имя Screen2EntryZonesAnalyzer
Screen2EntryZonesAnalyzer = Screen2Analyzer

__all__ = [
    # API
    'APIClient',
    'BybitAPIClient',

    # Data Classes
    'Direction',
    'SignalStatus',
    'TradeStatus',
    'Screen1Result',
    'Screen2Result',
    'Screen3Result',
    'ThreeScreenAnalysis',
    'Signal',
    'Trade',
    'PaperTrade',

    # Event Bus
    'EventType',
    'Event',
    'EventBus',
    'event_bus',

    # Modules
    'LiquidityPrefilter',
    'PrefilterResult',
    'LiquidityMetrics',
    'Screen1TrendAnalyzer',
    'Screen2Analyzer',  # ← новое имя (правильное)
    'Screen2EntryZonesAnalyzer',  # ← оставлено для обратной совместимости
    'Screen3SignalGenerator',
    'PatternType',
    'SignalRepository',
    'signal_repository',
    'ThreeScreenAnalyzer',
    'AnalysisOrchestrator',
    'AnalysisSession',

    # ФАЗА 1.5.0: Light режим
    'LightTrader',
]