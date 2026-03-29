import time 
from datetime import datetime

INTERVAL_SEC = 300 
INTERVAL_MS = INTERVAL_SEC * 1000 
candle = None 
boundary = False 
next_bucket = None
from db_write import setup

def time_bucket(timestamp):
    return (timestamp // INTERVAL_MS) * INTERVAL_MS

def orderbookData(symbol, exchange, bid, bidQty, ask, askQty):
    global boundary, next_bucket
    timestamp = int(time.time() * 1000) # Current time in milliseconds
    bucket = time_bucket(timestamp)

    if not boundary:
        if next_bucket is None:
            next_bucket = bucket + INTERVAL_MS
        if timestamp >= next_bucket:
            boundary = True
        else:
            return # skip until we hit the next bucket boundary

    mid = (bid + ask) / 2
    mid_qty = (bidQty + askQty) / 2
    update_candle(symbol, exchange, mid, mid_qty, timestamp)

def tickerData(symbol, exchange, bid_price, bid_qty, ask_price, ask_qty):
    global boundary, next_bucket
    timestamp = int(time.time() * 1000) # Current time in milliseconds
    bucket = time_bucket(timestamp)
    if not boundary:
        if next_bucket is None:
            next_bucket = bucket + INTERVAL_MS
        if timestamp >= next_bucket:
            boundary = True
        else:
            return # skip until we hit the next bucket boundary

    mid = (bid_price + ask_price) / 2
    mid_qty = (bid_qty + ask_qty) / 2
    update_candle(symbol, exchange, mid, mid_qty, timestamp)

def update_candle(symbol: str, exchange: str, mid: float, qty: float, timestamp: int) -> None:
    global candle
    bucket = time_bucket(timestamp)
    if candle is None:
        open_candle(symbol, exchange, mid, qty, timestamp)
    else:
        if bucket != time_bucket(candle["timestamp"]):
            close_candle()
            open_candle(symbol, exchange, mid, qty, timestamp)
        else: 
            candle["high"] = max(candle["high"], mid)
            candle["low"] = min(candle["low"], mid)
            candle["close"] = mid
            candle["volume"] += qty
            candle["timestamp"] = timestamp

def open_candle(symbol: str, exchange: str, mid: float, qty: float, timestamp: int) -> None:
    global candle
    candle = {
        "symbol": symbol,
        "exchange": exchange,
        "open": mid,
        "high": mid,
        "low": mid,
        "close": mid,
        "volume": qty,
        "timestamp": timestamp
    }

#Once the candle is complete, we can write it to the database and reset for the next candle
def close_candle():
    global candle
    if candle is not None:
        # Here you would write the candle to the database
        print(f"Candle closed: {candle}")
        setup(candle)
        candle = None