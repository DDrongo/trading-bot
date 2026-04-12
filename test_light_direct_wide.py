import asyncio
import yaml
import logging
from analyzer.core.light_trader import LightTrader
from analyzer.core.data_provider import data_provider

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def test():
    with open('analyzer/config/config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # ВРЕМЕННО расширяем зону для теста
    config['light_trader']['zone_proximity_pct'] = 2.0
    print('\n⚠️ ВРЕМЕННО: zone_proximity_pct = 2.0%\n')
    
    data_provider.configure(config)
    light_trader = LightTrader(config, data_provider)
    
    print('🔍 Анализируем BTCUSDT...\n')
    
    result = await light_trader.analyze_symbol('BTCUSDT')
    
    if result and result.should_trade:
        print('=' * 50)
        print('✅✅✅ СИГНАЛ НАЙДЕН!')
        print('=' * 50)
        print(f'   Направление: {result.screen3.signal_type}')
        print(f'   Вход: {result.screen3.entry_price:.2f}')
        print(f'   SL: {result.screen3.stop_loss:.2f}')
        print(f'   TP: {result.screen3.take_profit:.2f}')
        print(f'   Паттерн: {result.screen3.trigger_pattern}')
        print(f'   R/R: {result.risk_reward_ratio:.2f}:1')
        print(f'   Уверенность: {result.overall_confidence:.1%}')
        print('=' * 50)
    else:
        print('=' * 50)
        print('❌ Сигнал не сгенерирован')
        print('=' * 50)
        if result:
            print(f'   Тренд D1: {result.screen1.trend_direction}')
            if result.screen3 and result.screen3.rejection_reason:
                print(f'   Причина: {result.screen3.rejection_reason}')
    
    await data_provider.close()

asyncio.run(test())
