#Write dockers to run everything simultaneously
#Use caches to store most recent n data temporarily 
#Composite Primary key 
import mysql.connector
import os 
from dotenv import load_dotenv
import zScore, ou_process, hurst

load_dotenv()  

try:
    mydb = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME")
    )
    print("Successfully connected to the database")
except mysql.connector.Error as err:
    print(f"Error connecting to database: {err}")

mycursor = mydb.cursor()

try:
    algodb = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("ALGODB_NAME")
    )
    print("Sucessfully connected to the algorithm database")
except mysql.connector.Error as err:
    print(f"Error connection to the database")

myalgo = algodb.cursor()

def upsert_exchange(exchange: str) -> int:
    mycursor.execute("SELECT EXCHANGE_ID FROM EXCHANGE_DATA WHERE EXCHANGE_NAME = %s", (exchange,))
    result = mycursor.fetchone()
    if result:
        return result[0]
    mycursor.execute("INSERT INTO EXCHANGE_DATA (EXCHANGE_NAME) VALUES (%s)", (exchange,))
    mydb.commit()
    exchange_id = mycursor.lastrowid
    return exchange_id

def upsert_symbol(symbol: str) -> int:
    mycursor.execute("SELECT SYMBOL_ID FROM SYMBOL_DATA WHERE SYMBOL_CODE = %s", (symbol,))
    result = mycursor.fetchone()
    if result:
        return result[0]
    mycursor.execute("INSERT INTO SYMBOL_DATA (SYMBOL_CODE) VALUES (%s)", (symbol,))
    mydb.commit()
    symbol_id = mycursor.lastrowid
    return symbol_id

def upsert_exchange_symbol(exchange_id: int, symbol_id: int) -> int:
    mycursor.execute("SELECT EXCHANGE_SYMBOL_ID FROM EXCHANGE_SYMBOL WHERE EXCHANGE_ID = %s AND SYMBOL_ID = %s", (exchange_id, symbol_id))
    result = mycursor.fetchone()
    if result:
        return result[0]
    mycursor.execute("INSERT INTO EXCHANGE_SYMBOL (EXCHANGE_ID, SYMBOL_ID) VALUES (%s, %s)", (exchange_id, symbol_id))
    mydb.commit()
    exchange_symbol_id = mycursor.lastrowid
    return exchange_symbol_id

def write_mid_candle(t_ms: int, exchange_symbol_id: int, symbol: str, open_: float, high: float, low: float, close: float, volume: float) -> None:
    mycursor.execute("""
        INSERT INTO MID_CANDLES_5M
            (T_MS, EXCHANGE_SYMBOL_ID, SYMBOL, MID_OPEN, MID_HIGH, MID_LOW, MID_CLOSE, VOLUME)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            MID_OPEN  = VALUES(MID_OPEN),
            MID_HIGH  = VALUES(MID_HIGH),
            MID_LOW   = VALUES(MID_LOW),
            MID_CLOSE = VALUES(MID_CLOSE),
            VOLUME    = VALUES(VOLUME)
    """, (t_ms, exchange_symbol_id, symbol, open_, high, low, close, volume))
    mydb.commit()

def setup(candle: dict) -> None:
    e_id = upsert_exchange(candle["exchange"])
    s_id = upsert_symbol(candle["symbol"])
    ex_s_id = upsert_exchange_symbol(e_id, s_id)
    write_mid_candle(candle["timestamp"], ex_s_id, candle["symbol"], candle["open"], candle["high"], candle["low"], candle["close"], candle["volume"])

    z         = zScore.compute(ex_s_id)
    h         = hurst.compute(ex_s_id)
    ou_result = ou_process.compute(ex_s_id)

    if z is not None and h is not None and ou_result is not None:
        theta, mean, half_life = ou_result
        algorithm_write(
            candle["timestamp"], ex_s_id,
            z, h, theta, half_life, mean
        )

def fetch_recent_closes(exchange_symbol_id: int, n: int) -> list[float]:
    mycursor.execute("""
        SELECT MID_CLOSE FROM MID_CANDLES_5M
        WHERE EXCHANGE_SYMBOL_ID = %s
        ORDER BY T_MS DESC
        LIMIT %s
    """, (exchange_symbol_id, n))
    rows = mycursor.fetchall()
    return [float(row[0]) for row in rows]

def algorithm_write(t_ms: int, exchange_symbol_id: int, zscore_val: float, hurst_val: float, theta: float, half_life: float, mean: float) -> None:
    myalgo.execute("""
        INSERT INTO ALGO_VALUES
            (T_MS, EXCHANGE_SYMBOL_ID, ZSCORE, HURST, THETA, HALFLIFE, MU)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            ZSCORE   = VALUES(ZSCORE),
            HURST    = VALUES(HURST),
            THETA    = VALUES(THETA),
            HALFLIFE = VALUES(HALFLIFE),
            MU       = VALUES(MU)
    """, (t_ms, exchange_symbol_id, zscore_val, hurst_val, theta, half_life, mean))
    algodb.commit()

