import asyncio 
from kraken_ws import connect 
import candle_builder

def handle_message(msg: dict) -> None:
    channel = msg.get("channel")
    if channel == "book":
        book_handler(msg)
    elif channel == "ticker":
        tick_handler(msg)

def tick_handler(msg: dict) -> None:
    for item in msg.get("data", []):
        symbol = item.get("symbol")
        exchange = msg.get("exchange")  
        bid_price = item.get("bid")
        bid_qty = item.get("bid_qty")
        ask_price = item.get("ask")
        ask_qty = item.get("ask_qty")

        if bid_price is not None and ask_price is not None:
            if bid_qty > 0 and ask_qty > 0:
                candle_builder.tickerData(symbol, exchange, bid_price, bid_qty, ask_price, ask_qty)

def book_handler(msg: dict) -> None:
    for item in msg.get("data", []):
        symbol = item.get("symbol")
        exchange = msg.get("exchange")  
        bidLists = item.get("bids", [])
        asksList = item.get("asks", [])
        ask_price, ask_qty = best_ask(asksList) if best_ask(asksList) else (None, None)
        bid_price, bid_qty = best_bid(bidLists) if best_bid(bidLists) else (None, None)

        if bid_price is not None and ask_price is not None:
            if bid_qty > 0 and ask_qty > 0:
                candle_builder.orderbookData(symbol, exchange, bid_price, bid_qty, ask_price, ask_qty)
        
def best_ask(asks: list) -> float:
    if not asks:
        return None, None
    best = next((level for level in asks if level["qty"] > 0), None)
    if best is None:
        return None, None
    return best["price"], best["qty"]
    
def best_bid(bids: list) -> float:
    if not bids:
        return None, None
    best = next((level for level in bids if level["qty"] > 0), None)
    if best is None:
        return None, None
    return best["price"], best["qty"]

if __name__ == "__main__":
    asyncio.run(connect(on_message=handle_message))