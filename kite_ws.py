#kite_ws
import os
import time
import sqlite3
import threading
from datetime import datetime
from collections import deque, defaultdict
import pytz
import my_herlpers as my

from kiteconnect import KiteConnect, KiteTicker

# ================= CONFIG =================
API_KEY = os.environ["KITE_API_KEY"]
ACCESS_TOKEN = os.environ["KITE_ACCESS_TOKEN"]

# Tokens to subscribe (only NSE instrument tokens)
TOKENS = [
    738561,  # RELIANCE
    # Add more tokens here
]
# Load tokens from your existing DB
df = my.get.load_table_as_df("tickers.db", "main_table", ["symbol", "nse_instrument_token"])

# Correct way to get a list
TOKENS = df["nse_instrument_token"].tolist()  # note: .tolist() is a function
print("LOADED TOKENS :"len(TOKENS))
                       
ROLLING_WINDOW = 20  # last 20 seconds
SAVE_INTERVAL = 20   # save to DB every 20 seconds

# IST timezone
IST = pytz.timezone("Asia/Kolkata")
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 30

# Daily DB file
TODAY = datetime.now(IST).strftime("%Y-%m-%d")
DB_FILE = f"kite_{TODAY}.db"

# ================= IN-MEMORY STORE =================
tick_store = defaultdict(lambda: {
    "ltp": None,
    "volume": None,
    "buy_qty": deque(maxlen=ROLLING_WINDOW),
    "sell_qty": deque(maxlen=ROLLING_WINDOW),
})
lock = threading.Lock()
last_save_time = time.time()
# ================================================

# ================= DATABASE =================
def save_to_db(token, ltp, volume, avg_buy, avg_sell):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    table_name = f"stock_{token}"
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TEXT,
            ltp REAL,
            volume INTEGER,
            avg_buy_qty REAL,
            avg_sell_qty REAL
        )
    """)
    ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    c.execute(
        f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)",
        (ts, ltp, volume, avg_buy, avg_sell)
    )
    conn.commit()
    conn.close()
# ================================================

# ================= CALLBACKS =================
def setup_callbacks(kws):
    def on_connect(ws, resp):
        print("✅ WS connected, subscribing...")
        ws.subscribe(TOKENS)
        ws.set_mode(ws.MODE_FULL, TOKENS)
        print("SUSCRIBED TOKENS :"len(TOKENS))

    def on_ticks(ws, ticks):
        with lock:
            for t in ticks:
                token = t["instrument_token"]
                if token not in TOKENS:
                    continue
                store = tick_store[token]
                store["ltp"] = t.get("last_price", 0.0)
                store["volume"] = t.get("volume_traded", 0)
                store["buy_qty"].append(t.get("total_buy_quantity", 0))
                store["sell_qty"].append(t.get("total_sell_quantity", 0))

    def on_close(ws, code, reason):
        print(f"🔴 WS closed: {reason}")

    def on_error(ws, code, reason):
        print(f"❌ WS error: {code} | {reason}")

    kws.on_connect = on_connect
    kws.on_ticks = on_ticks
    kws.on_close = on_close
    kws.on_error = on_error
# ================================================

# ================= PERIODIC SAVE =================
def periodic_save():
    global last_save_time
    now = time.time()
    if now - last_save_time < SAVE_INTERVAL:
        return
    last_save_time = now

    with lock:
        for token, data in tick_store.items():
            if data["ltp"] is None:
                continue
            avg_buy = round(sum(data["buy_qty"]) / len(data["buy_qty"]), 2) if data["buy_qty"] else 0
            avg_sell = round(sum(data["sell_qty"]) / len(data["sell_qty"]), 2) if data["sell_qty"] else 0
            save_to_db(
                token,
                data["ltp"],
                data["volume"],
                avg_buy,
                avg_sell
            )
            print(f"SAVED | Token={token} | LTP={data['ltp']} | AvgBuy={avg_buy} | AvgSell={avg_sell}")
# ================================================

# ================= MARKET HOURS CHECK =================
def market_open():
    now = datetime.now(IST)
    return now.weekday() < 5 and (now.hour > 9 or (now.hour == 9 and now.minute >= 15))

def market_closed():
    now = datetime.now(IST)
    return now.hour > MARKET_CLOSE_HOUR or (now.hour == MARKET_CLOSE_HOUR and now.minute >= MARKET_CLOSE_MINUTE)
# ================================================

# ================= MAIN =================
if __name__ == "__main__":
    print("📊 Starting Kite WebSocket Collector")
    if not market_open():
        print("⛔ Market not open yet. Exiting...")
        exit()

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)

    kws = KiteTicker(API_KEY, ACCESS_TOKEN)
    setup_callbacks(kws)

    kws.connect(threaded=True)
    print("🚀 WebSocket thread started")

    try:
        while True:
            if market_closed():
                print("⏰ Market closed. Stopping collector...")
                kws.close()
                break

            periodic_save()
            time.sleep(1)

    except KeyboardInterrupt:
        print("🛑 Stopped manually")
        kws.close()
