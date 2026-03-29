import db_write
import math 
n = 200

#Calculates theta and halflife 
def compute(exchange_symbol_id: int) -> tuple[float, float, float] | None:
    candles = db_write.fetch_recent_closes(exchange_symbol_id, n)
    if len(candles) < n:
        return None
    X_lag = candles[:-1]
    X_current = candles[1:]
    deltaX = [a-b for a, b in zip(X_current, X_lag)]    
    pairs = len(X_lag)
    sumX_lag = sum(X_lag)
    sumdeltaX = sum(deltaX)
    sumXX = sum(x**2 for x in X_lag)
    sumXY = sum(x * y for x, y in zip(X_lag, deltaX))  
    b = (pairs * sumXY - sumX_lag * sumdeltaX) / (pairs * sumXX - sumX_lag ** 2)
    a = (sumdeltaX - b * sumX_lag) / pairs

    dt = 5/1440 #5 mins out of the day which is 1440 minutes
    theta = -b / dt

    if theta <= 0:
        return None #not mean reverting

    mean = a / (theta * dt)
    half_life = math.log(2) / theta

    return theta, mean, half_life