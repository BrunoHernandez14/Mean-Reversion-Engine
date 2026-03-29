import time
import math
import db_write

n = 100  # number of data points to consider for mean and stddev

def compute(exchange_symbol_id: int) -> float:
    candles = db_write.fetch_recent_closes(exchange_symbol_id, n)

    if len(candles) < n:
        return 0.0  # Not enough data to calculate z-score
    mean = sum(candles) / n
    variance = sum((x - mean) ** 2 for x in candles) / n
    stddev = math.sqrt(variance)
    if stddev == 0:
        return 0.0  
    latest_close = candles[n-1]
    z = (latest_close - mean) / stddev
    return z