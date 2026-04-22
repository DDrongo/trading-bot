# analyzer/core/analyst/__init__.py (НОВЫЙ)
"""
🎯 АНАЛИТИЧЕСКИЕ МОДУЛИ ДЛЯ SMC (ФАЗА 2.0)

Модули:
- FVGDetector: обнаружение Fair Value Gaps (дисбалансов)
- LiquidityScanner: обнаружение бассейнов ликвидности
"""

from .fvg_detector import FVGDetector, FVGZone
from .liquidity_scanner import LiquidityScanner, LiquidityPool

__all__ = [
    'FVGDetector',
    'FVGZone',
    'LiquidityScanner',
    'LiquidityPool'
]