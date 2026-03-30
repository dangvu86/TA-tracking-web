import numpy as np
import pandas as pd


def evaluate_ma_signals(indicators: dict) -> dict:
    """Evaluate Moving Average signals"""
    price = indicators.get('Price', np.nan)
    signals = {}
    
    # Simple Moving Averages
    for period in [10, 20, 30, 50, 100, 200]:
        ma_key = f'SMA_{period}'
        ma_value = indicators.get(ma_key, np.nan)
        
        if pd.isna(price) or pd.isna(ma_value):
            signals[f'MA_{period}_Signal'] = 'Neutral'
        elif price > ma_value:
            signals[f'MA_{period}_Signal'] = 'Buy'
        elif price < ma_value:
            signals[f'MA_{period}_Signal'] = 'Sell'
        else:
            signals[f'MA_{period}_Signal'] = 'Neutral'
    
    # Exponential Moving Averages
    for period in [10, 20, 30, 50, 100, 200]:
        ema_key = f'EMA_{period}'
        ema_value = indicators.get(ema_key, np.nan)
        
        if pd.isna(price) or pd.isna(ema_value):
            signals[f'EMA_{period}_Signal'] = 'Neutral'
        elif price > ema_value:
            signals[f'EMA_{period}_Signal'] = 'Buy'
        elif price < ema_value:
            signals[f'EMA_{period}_Signal'] = 'Sell'
        else:
            signals[f'EMA_{period}_Signal'] = 'Neutral'
    
    # VWMA
    vwma_value = indicators.get('VWMA_20', np.nan)
    if pd.isna(price) or pd.isna(vwma_value):
        signals['VWMA_Signal'] = 'Neutral'
    elif price > vwma_value:
        signals['VWMA_Signal'] = 'Buy'
    elif price < vwma_value:
        signals['VWMA_Signal'] = 'Sell'
    else:
        signals['VWMA_Signal'] = 'Neutral'
    
    # Hull MA
    hull_value = indicators.get('Hull_MA_9', np.nan)
    if pd.isna(price) or pd.isna(hull_value):
        signals['Hull_MA_Signal'] = 'Neutral'
    elif price > hull_value:
        signals['Hull_MA_Signal'] = 'Buy'
    elif price < hull_value:
        signals['Hull_MA_Signal'] = 'Sell'
    else:
        signals['Hull_MA_Signal'] = 'Neutral'
    
    # Ichimoku Signal
    ichimoku_a = indicators.get('Ichimoku_A', np.nan)
    ichimoku_b = indicators.get('Ichimoku_B', np.nan)
    ichimoku_base = indicators.get('Ichimoku_Base', np.nan)
    ichimoku_conversion = indicators.get('Ichimoku_Conversion', np.nan)
    
    if any(pd.isna(val) for val in [ichimoku_a, ichimoku_b, ichimoku_base, ichimoku_conversion, price]):
        signals['Ichimoku_Signal'] = 'Neutral'
    else:
        # Buy condition: Leading Span A > Leading Span B and base line > Leading Span A and conversion line > base line and price > conversion line
        buy_condition = (ichimoku_a > ichimoku_b and 
                        ichimoku_base > ichimoku_a and 
                        ichimoku_conversion > ichimoku_base and 
                        price > ichimoku_conversion)
        
        # Sell condition: Leading Span A < Leading Span B and base line < Leading Span A and conversion line < base line and price < conversion line
        sell_condition = (ichimoku_a < ichimoku_b and 
                         ichimoku_base < ichimoku_a and 
                         ichimoku_conversion < ichimoku_base and 
                         price < ichimoku_conversion)
        
        if buy_condition:
            signals['Ichimoku_Signal'] = 'Buy'
        elif sell_condition:
            signals['Ichimoku_Signal'] = 'Sell'
        else:
            signals['Ichimoku_Signal'] = 'Neutral'
    
    return signals


def evaluate_oscillator_signals(indicators: dict) -> dict:
    """Evaluate Oscillator signals"""
    signals = {}
    
    # RSI Signal
    rsi = indicators.get('RSI_14', np.nan)
    rsi_prev = indicators.get('RSI_Prev', np.nan)
    
    if pd.isna(rsi) or pd.isna(rsi_prev):
        signals['RSI_Signal'] = 'Neutral'
    elif rsi < 30 and rsi > rsi_prev:
        signals['RSI_Signal'] = 'Buy'
    elif rsi > 70 and rsi < rsi_prev:
        signals['RSI_Signal'] = 'Sell'
    else:
        signals['RSI_Signal'] = 'Neutral'
    
    # Stochastic Signal
    stoch_k = indicators.get('Stoch_K', np.nan)
    stoch_d = indicators.get('Stoch_D', np.nan)
    
    if pd.isna(stoch_k) or pd.isna(stoch_d):
        signals['Stochastic_Signal'] = 'Neutral'
    elif stoch_k < 20 and stoch_d < 20 and stoch_k > stoch_d:
        signals['Stochastic_Signal'] = 'Buy'
    elif stoch_k > 80 and stoch_d > 80 and stoch_k < stoch_d:
        signals['Stochastic_Signal'] = 'Sell'
    else:
        signals['Stochastic_Signal'] = 'Neutral'
    
    # CCI Signal
    cci = indicators.get('CCI_20', np.nan)
    cci_prev = indicators.get('CCI_Prev', np.nan)
    
    if pd.isna(cci) or pd.isna(cci_prev):
        signals['CCI_Signal'] = 'Neutral'
    elif cci < -100 and cci > cci_prev:
        signals['CCI_Signal'] = 'Buy'
    elif cci > 100 and cci < cci_prev:
        signals['CCI_Signal'] = 'Sell'
    else:
        signals['CCI_Signal'] = 'Neutral'
    
    # ADX Signal
    dmi_pos = indicators.get('DMI_Positive', np.nan)
    dmi_neg = indicators.get('DMI_Negative', np.nan)
    adx = indicators.get('ADX_14', np.nan)
    adx_prev = indicators.get('ADX_Prev', np.nan)
    
    if any(pd.isna(val) for val in [dmi_pos, dmi_neg, adx, adx_prev]):
        signals['ADX_Signal'] = 'Neutral'
    elif dmi_pos > dmi_neg and adx > 20 and adx > adx_prev:
        signals['ADX_Signal'] = 'Buy'
    elif dmi_pos < dmi_neg and adx > 20 and adx > adx_prev:
        signals['ADX_Signal'] = 'Sell'
    else:
        signals['ADX_Signal'] = 'Neutral'
    
    # Awesome Oscillator Signal
    ao = indicators.get('AO', np.nan)
    ao_prev = indicators.get('AO_Prev', np.nan)
    
    if pd.isna(ao) or pd.isna(ao_prev):
        signals['AO_Signal'] = 'Neutral'
    elif ao > 0 and ao > ao_prev:  # Saucer and values greater than 0, or cross over zero line
        signals['AO_Signal'] = 'Buy'
    elif ao < 0 and ao < ao_prev:  # Saucer and values lower than 0, or cross under zero line
        signals['AO_Signal'] = 'Sell'
    else:
        signals['AO_Signal'] = 'Neutral'
    
    # Momentum Signal
    momentum = indicators.get('Momentum_10', np.nan)
    momentum_prev = indicators.get('Momentum_Prev', np.nan)
    
    if pd.isna(momentum) or pd.isna(momentum_prev):
        signals['Momentum_Signal'] = 'Neutral'
    elif momentum > momentum_prev:
        signals['Momentum_Signal'] = 'Buy'
    elif momentum < momentum_prev:
        signals['Momentum_Signal'] = 'Sell'
    else:
        signals['Momentum_Signal'] = 'Neutral'
    
    # MACD Signal
    macd = indicators.get('MACD', np.nan)
    macd_signal = indicators.get('MACD_Signal', np.nan)
    
    if pd.isna(macd) or pd.isna(macd_signal):
        signals['MACD_Signal'] = 'Neutral'
    elif macd > macd_signal:
        signals['MACD_Signal'] = 'Buy'
    elif macd < macd_signal:
        signals['MACD_Signal'] = 'Sell'
    else:
        signals['MACD_Signal'] = 'Neutral'
    
    # Stochastic RSI Signal
    stochrsi_k = indicators.get('StochRSI_K', np.nan)
    stochrsi_d = indicators.get('StochRSI_D', np.nan)
    
    if pd.isna(stochrsi_k) or pd.isna(stochrsi_d):
        signals['StochRSI_Signal'] = 'Neutral'
    elif stochrsi_k < 20 and stochrsi_d < 20 and stochrsi_k > stochrsi_d:
        signals['StochRSI_Signal'] = 'Buy'
    elif stochrsi_k > 80 and stochrsi_d > 80 and stochrsi_k < stochrsi_d:
        signals['StochRSI_Signal'] = 'Sell'
    else:
        signals['StochRSI_Signal'] = 'Neutral'
    
    # Williams %R Signal
    williams_r = indicators.get('Williams_R', np.nan)
    williams_r_prev = indicators.get('Williams_R_Prev', np.nan)
    
    if pd.isna(williams_r) or pd.isna(williams_r_prev):
        signals['Williams_R_Signal'] = 'Neutral'
    elif williams_r < -80 and williams_r > williams_r_prev:
        signals['Williams_R_Signal'] = 'Buy'
    elif williams_r > -20 and williams_r < williams_r_prev:
        signals['Williams_R_Signal'] = 'Sell'
    else:
        signals['Williams_R_Signal'] = 'Neutral'
    
    # Bull Bear Power Signal
    ema13 = indicators.get('EMA_13', np.nan)
    ema13_prev = indicators.get('EMA_13_Prev', np.nan)
    bull_power = indicators.get('Bull_Power', np.nan)
    bear_power = indicators.get('Bear_Power', np.nan)
    bull_power_prev = indicators.get('Bull_Power_Prev', np.nan)
    bear_power_prev = indicators.get('Bear_Power_Prev', np.nan)
    
    if any(pd.isna(val) for val in [ema13, ema13_prev, bull_power, bear_power, bull_power_prev, bear_power_prev]):
        signals['BBP_Signal'] = 'Neutral'
    elif ema13 > ema13_prev and bear_power < 0 and bear_power > bear_power_prev:
        signals['BBP_Signal'] = 'Buy'
    elif ema13 < ema13_prev and bull_power > 0 and bull_power < bull_power_prev:
        signals['BBP_Signal'] = 'Sell'
    else:
        signals['BBP_Signal'] = 'Neutral'
    
    # Ultimate Oscillator Signal
    uo = indicators.get('UO', np.nan)
    
    if pd.isna(uo):
        signals['UO_Signal'] = 'Neutral'
    elif uo > 70:
        signals['UO_Signal'] = 'Buy'
    elif uo < 30:
        signals['UO_Signal'] = 'Sell'
    else:
        signals['UO_Signal'] = 'Neutral'
    
    return signals


def evaluate_all_signals(indicators: dict) -> dict:
    """Evaluate all trading signals"""
    ma_signals = evaluate_ma_signals(indicators)
    oscillator_signals = evaluate_oscillator_signals(indicators)
    
    all_signals = {**ma_signals, **oscillator_signals}
    return all_signals


def get_signal_summary(signals: dict) -> dict:
    """Get summary of all signals"""
    buy_count = sum(1 for signal in signals.values() if signal == 'Buy')
    sell_count = sum(1 for signal in signals.values() if signal == 'Sell')
    neutral_count = sum(1 for signal in signals.values() if signal == 'Neutral')
    total_signals = len(signals)
    
    if buy_count > sell_count:
        overall_signal = 'Buy'
    elif sell_count > buy_count:
        overall_signal = 'Sell'
    else:
        overall_signal = 'Neutral'
    
    return {
        'Overall_Signal': overall_signal,
        'Buy_Count': buy_count,
        'Sell_Count': sell_count,
        'Neutral_Count': neutral_count,
        'Total_Signals': total_signals,
        'Buy_Percentage': round((buy_count / total_signals) * 100, 1) if total_signals > 0 else 0,
        'Sell_Percentage': round((sell_count / total_signals) * 100, 1) if total_signals > 0 else 0
    }