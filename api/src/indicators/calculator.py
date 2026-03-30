import pandas as pd
import numpy as np
import ta


def calculate_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate all technical indicators for the given dataframe
    
    Args:
        df: DataFrame with OHLCV data
    
    Returns:
        DataFrame with all indicators added
    """
    if df is None or df.empty:
        return pd.DataFrame()
    
    # Create a copy to avoid modifying original data
    data = df.copy()
    
    # Ensure we have the required columns
    required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    if not all(col in data.columns for col in required_cols):
        raise ValueError(f"Missing required columns. Need: {required_cols}")
    
    # Sort by date to ensure proper calculation
    if 'Date' in data.columns:
        data = data.sort_values('Date').reset_index(drop=True)
    
    try:
        # Ichimoku Cloud (9,26,52,26)
        data['Ichimoku_Base'] = ta.trend.ichimoku_base_line(data['High'], data['Low'], window1=9, window2=26)
        data['Ichimoku_Conversion'] = ta.trend.ichimoku_conversion_line(data['High'], data['Low'], window1=9, window2=26) 
        data['Ichimoku_A'] = ta.trend.ichimoku_a(data['High'], data['Low'], window1=9, window2=26)
        data['Ichimoku_B'] = ta.trend.ichimoku_b(data['High'], data['Low'], window2=26, window3=52)
        
        # Simple Moving Averages
        data['SMA_5'] = ta.trend.sma_indicator(data['Close'], window=5)
        data['SMA_10'] = ta.trend.sma_indicator(data['Close'], window=10)
        data['SMA_20'] = ta.trend.sma_indicator(data['Close'], window=20)
        data['SMA_30'] = ta.trend.sma_indicator(data['Close'], window=30)
        data['SMA_50'] = ta.trend.sma_indicator(data['Close'], window=50)
        data['SMA_100'] = ta.trend.sma_indicator(data['Close'], window=100)
        data['SMA_200'] = ta.trend.sma_indicator(data['Close'], window=200)
        
        # Exponential Moving Averages
        data['EMA_10'] = ta.trend.ema_indicator(data['Close'], window=10)
        data['EMA_13'] = ta.trend.ema_indicator(data['Close'], window=13)
        data['EMA_20'] = ta.trend.ema_indicator(data['Close'], window=20)
        data['EMA_30'] = ta.trend.ema_indicator(data['Close'], window=30)
        data['EMA_50'] = ta.trend.ema_indicator(data['Close'], window=50)
        data['EMA_100'] = ta.trend.ema_indicator(data['Close'], window=100)
        data['EMA_200'] = ta.trend.ema_indicator(data['Close'], window=200)
        
        # Volume Weighted Moving Average (20)
        data['VWMA_20'] = ta.volume.volume_weighted_average_price(data['High'], data['Low'], data['Close'], data['Volume'], window=20)
        
        # Hull Moving Average (9)
        data['Hull_MA_9'] = calculate_hull_ma(data['Close'], 9)
        
        # RSI (14)
        data['RSI_14'] = ta.momentum.rsi(data['Close'], window=14)
        
        # Stochastic (14,3,3)
        data['Stoch_K'] = ta.momentum.stoch(data['High'], data['Low'], data['Close'], window=14, smooth_window=3)
        data['Stoch_D'] = ta.momentum.stoch_signal(data['High'], data['Low'], data['Close'], window=14, smooth_window=3)
        
        # CCI (20)
        data['CCI_20'] = ta.trend.cci(data['High'], data['Low'], data['Close'], window=20)
        
        # ADX (14)
        data['ADX_14'] = ta.trend.adx(data['High'], data['Low'], data['Close'], window=14)
        
        # Awesome Oscillator
        data['AO'] = ta.momentum.awesome_oscillator(data['High'], data['Low'])
        
        # Momentum (10) - TradingView style: Current Close - Close N periods ago
        data['Momentum_10'] = data['Close'] - data['Close'].shift(10)
        
        # MACD (12,26) - Scale like TradingView (multiply by 1000)  
        data['MACD'] = ta.trend.macd(data['Close'], window_slow=26, window_fast=12) * 1000
        data['MACD_Signal'] = ta.trend.macd_signal(data['Close'], window_slow=26, window_fast=12) * 1000
        
        # Stochastic RSI (3,3,14,14) - Scale to 0-100 like TradingView
        data['StochRSI_K'] = ta.momentum.stochrsi_k(data['Close'], window=14, smooth1=3, smooth2=3) * 100
        data['StochRSI_D'] = ta.momentum.stochrsi_d(data['Close'], window=14, smooth1=3, smooth2=3) * 100
        
        # Williams %R (14)
        data['Williams_R'] = ta.momentum.williams_r(data['High'], data['Low'], data['Close'], lbp=14)
        
        # Ultimate Oscillator (7,14,28)
        data['UO'] = ta.momentum.ultimate_oscillator(data['High'], data['Low'], data['Close'], window1=7, window2=14, window3=28)
        
        # Directional Movement Index (14)
        data['DMI_Positive'] = ta.trend.adx_pos(data['High'], data['Low'], data['Close'], window=14)
        data['DMI_Negative'] = ta.trend.adx_neg(data['High'], data['Low'], data['Close'], window=14)
        
        # Bull Bear Power (13) - Scale like TradingView (multiply by 1000)
        data['Bull_Power'] = (data['High'] - data['EMA_13']) * 1000
        data['Bear_Power'] = (data['Low'] - data['EMA_13']) * 1000
        
        # Close vs. MA comparisons (as percentages)
        data['Close_vs_MA5'] = ((data['Close'] - data['SMA_5']) / data['SMA_5'] * 100)
        data['Close_vs_MA10'] = ((data['Close'] - data['SMA_10']) / data['SMA_10'] * 100)
        data['Close_vs_MA20'] = ((data['Close'] - data['SMA_20']) / data['SMA_20'] * 100)
        data['Close_vs_MA50'] = ((data['Close'] - data['SMA_50']) / data['SMA_50'] * 100)
        data['Close_vs_MA200'] = ((data['Close'] - data['SMA_200']) / data['SMA_200'] * 100)
        
        # STRENGTH calculations (average of Close vs. MA comparisons)
        data['STRENGTH_ST'] = (data['Close_vs_MA5'] + data['Close_vs_MA10'] + data['Close_vs_MA20']) / 3
        data['STRENGTH_LT'] = (data['Close_vs_MA5'] + data['Close_vs_MA10'] + data['Close_vs_MA20'] + data['Close_vs_MA50'] + data['Close_vs_MA200']) / 5
        
        # MA50 > MA200 comparison
        data['MA50_GT_MA200'] = data['SMA_50'] > data['SMA_200']
        
        # Previous day values
        data['RSI_Prev'] = data['RSI_14'].shift(1)
        data['CCI_Prev'] = data['CCI_20'].shift(1)
        data['ADX_Prev'] = data['ADX_14'].shift(1)
        data['Momentum_Prev'] = data['Momentum_10'].shift(1)
        data['Williams_R_Prev'] = data['Williams_R'].shift(1)
        data['Bull_Power_Prev'] = data['Bull_Power'].shift(1)
        data['Bear_Power_Prev'] = data['Bear_Power'].shift(1)
        data['EMA_13_Prev'] = data['EMA_13'].shift(1)
        data['AO_Prev'] = data['AO'].shift(1)
        
        return data
        
    except Exception as e:
        raise Exception(f"Error calculating indicators: {str(e)}")


def calculate_hull_ma(series: pd.Series, period: int) -> pd.Series:
    """Calculate Hull Moving Average"""
    try:
        half_period = int(period / 2)
        sqrt_period = int(np.sqrt(period))
        
        wma_half = series.rolling(window=half_period).apply(lambda x: np.average(x, weights=np.arange(1, half_period + 1)), raw=True)
        wma_full = series.rolling(window=period).apply(lambda x: np.average(x, weights=np.arange(1, period + 1)), raw=True)
        
        hull_values = 2 * wma_half - wma_full
        hull_ma = hull_values.rolling(window=sqrt_period).apply(lambda x: np.average(x, weights=np.arange(1, sqrt_period + 1)), raw=True)
        
        return hull_ma
    except:
        return pd.Series([np.nan] * len(series), index=series.index)


def get_latest_indicators(df: pd.DataFrame, target_date: pd.Timestamp) -> dict:
    """
    Get indicator values for a specific date
    
    Args:
        df: DataFrame with calculated indicators
        target_date: Target date to get indicators for
    
    Returns:
        Dictionary with indicator values
    """
    try:
        # Find the closest date <= target_date
        available_dates = df[df['Date'].dt.date <= target_date.date()]
        if available_dates.empty:
            return {}
        
        latest_row = available_dates.iloc[-1]
        
        indicators = {
            # Ichimoku
            'Ichimoku_Base': latest_row.get('Ichimoku_Base', np.nan),
            'Ichimoku_Conversion': latest_row.get('Ichimoku_Conversion', np.nan),
            'Ichimoku_A': latest_row.get('Ichimoku_A', np.nan),
            'Ichimoku_B': latest_row.get('Ichimoku_B', np.nan),
            
            # Simple Moving Averages
            'SMA_5': latest_row.get('SMA_5', np.nan),
            'SMA_10': latest_row.get('SMA_10', np.nan),
            'SMA_20': latest_row.get('SMA_20', np.nan),
            'SMA_30': latest_row.get('SMA_30', np.nan),
            'SMA_50': latest_row.get('SMA_50', np.nan),
            'SMA_100': latest_row.get('SMA_100', np.nan),
            'SMA_200': latest_row.get('SMA_200', np.nan),
            
            # Exponential Moving Averages
            'EMA_10': latest_row.get('EMA_10', np.nan),
            'EMA_13': latest_row.get('EMA_13', np.nan),
            'EMA_20': latest_row.get('EMA_20', np.nan),
            'EMA_30': latest_row.get('EMA_30', np.nan),
            'EMA_50': latest_row.get('EMA_50', np.nan),
            'EMA_100': latest_row.get('EMA_100', np.nan),
            'EMA_200': latest_row.get('EMA_200', np.nan),
            
            # Other indicators
            'VWMA_20': latest_row.get('VWMA_20', np.nan),
            'Hull_MA_9': latest_row.get('Hull_MA_9', np.nan),
            'RSI_14': latest_row.get('RSI_14', np.nan),
            'Stoch_K': latest_row.get('Stoch_K', np.nan),
            'Stoch_D': latest_row.get('Stoch_D', np.nan),
            'CCI_20': latest_row.get('CCI_20', np.nan),
            'ADX_14': latest_row.get('ADX_14', np.nan),
            'AO': latest_row.get('AO', np.nan),
            'Momentum_10': latest_row.get('Momentum_10', np.nan),
            'MACD': latest_row.get('MACD', np.nan),
            'MACD_Signal': latest_row.get('MACD_Signal', np.nan),
            'StochRSI_K': latest_row.get('StochRSI_K', np.nan),
            'StochRSI_D': latest_row.get('StochRSI_D', np.nan),
            'Williams_R': latest_row.get('Williams_R', np.nan),
            'UO': latest_row.get('UO', np.nan),
            'DMI_Positive': latest_row.get('DMI_Positive', np.nan),
            'DMI_Negative': latest_row.get('DMI_Negative', np.nan),
            'Bull_Power': latest_row.get('Bull_Power', np.nan),
            'Bear_Power': latest_row.get('Bear_Power', np.nan),
            
            # Previous day values
            'RSI_Prev': latest_row.get('RSI_Prev', np.nan),
            'CCI_Prev': latest_row.get('CCI_Prev', np.nan),
            'ADX_Prev': latest_row.get('ADX_Prev', np.nan),
            'Momentum_Prev': latest_row.get('Momentum_Prev', np.nan),
            'Williams_R_Prev': latest_row.get('Williams_R_Prev', np.nan),
            'Bull_Power_Prev': latest_row.get('Bull_Power_Prev', np.nan),
            'Bear_Power_Prev': latest_row.get('Bear_Power_Prev', np.nan),
            'EMA_13_Prev': latest_row.get('EMA_13_Prev', np.nan),
            'AO_Prev': latest_row.get('AO_Prev', np.nan),
            
            # Close vs. MA comparisons
            'Close_vs_MA5': latest_row.get('Close_vs_MA5', np.nan),
            'Close_vs_MA10': latest_row.get('Close_vs_MA10', np.nan),
            'Close_vs_MA20': latest_row.get('Close_vs_MA20', np.nan),
            'Close_vs_MA50': latest_row.get('Close_vs_MA50', np.nan),
            'Close_vs_MA200': latest_row.get('Close_vs_MA200', np.nan),
            
            # STRENGTH calculations
            'STRENGTH_ST': latest_row.get('STRENGTH_ST', np.nan),
            'STRENGTH_LT': latest_row.get('STRENGTH_LT', np.nan),
            
            # MA comparison
            'MA50_GT_MA200': latest_row.get('MA50_GT_MA200', np.nan),
            
            # Price data
            'Price': latest_row.get('Close', np.nan),
            'High': latest_row.get('High', np.nan),
            'Low': latest_row.get('Low', np.nan),
            'Date': latest_row.get('Date')
        }
        
        return indicators
        
    except Exception as e:
        print(f"Error getting latest indicators: {str(e)}")
        return {}