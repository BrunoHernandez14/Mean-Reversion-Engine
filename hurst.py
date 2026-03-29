import db_write
import math
import numpy as np
n = 200 # number of data points to consider for Hurst exponent calculation

def compute(exchange_symbol_id: int) -> float:
        candles = db_write.fetch_recent_closes(exchange_symbol_id, n)
        if len(candles) < n:
            return 0.0 
        
        log_returns = [math.log(candles[i] / candles[i - 1]) for i in range(1, len(candles))]
        mean_returns = sum(log_returns) / len(log_returns)
        adjusted_returns = [log_returns[i] - mean_returns for i in range(0, len(log_returns))]
        cumsum_series = [sum(adjusted_returns[:i+1]) for i in range(len(adjusted_returns))]
        R = max(cumsum_series) - min(cumsum_series)
        S = np.std(log_returns)
        if S == 0:
            return
        RS = R/S
        return math.log(RS) / math.log(n)