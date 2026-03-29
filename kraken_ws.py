import asyncio
import json
import logging
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

WS_URL = "wss://ws.kraken.com/v2"
PAIRS  = ["BTC/USD"]

async def connect(on_message=None) -> None:
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                logger.info("Connected to Kraken WebSocket")

                await ws.send(json.dumps({
                    "method": "subscribe",
                    "params": {"channel": "ticker", "symbol": PAIRS},
                }))

                await ws.send(json.dumps({
                    "method": "subscribe",
                    "params": {"channel": "book", "symbol": PAIRS},
                }))

                async for raw in ws:
                    msg = json.loads(raw)
                    msg["exchange"] = "kraken"
                    if(on_message is not None):
                        on_message(msg)

        except (ConnectionClosedError, ConnectionClosedOK) as exc:
            logger.warning("Disconnected from Kraken WebSocket (%s) — reconnecting in 5s", exc)
        except Exception as exc:
            logger.error("Unexpected error (%s) — reconnecting in 5s", exc)

        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(connect())