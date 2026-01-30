# kite_ws1_batch.py
import os
import time
import sqlite3
import threading
from datetime import datetime
from collections import deque, defaultdict
import pytz
import my_helpers as my
from kiteconnect import KiteConnect, KiteTicker

# ================= CONFIG =================
API_KEY = os.environ["KITE_API_KEY"]
ACCESS_TOKEN = os.environ["KITE_ACCESS_TOKEN"]

# Load tokens and symbols from DB
df = my.load_table_as_df("tikers.db", "main_table", ["symbol", "nse_instrument_token"])
TOKENS = df["nse_instrument_token"].tolist()
print("LOADED TOKENS:", len(TOKENS))

# Mapping: token -> symbol
token_to_symbol = dict(zip(df["nse_instrument_token"], df["symbol"]))

ROLLING_WINDOW = 20
SAVE_INTERVAL = 20
IST = pytz.timezone("Asia/Kolkata")

# DB file for first half
TODAY = datetime.now(IST).strftime("%Y-%m-%d")
DB_FILE = f"kite_{TODAY}_1.db"

# ================= IN-MEMORY STORE =================
tick_store = defaultdict(lambda: {
    "ltp": None,
    "volume": None,
    "buy_qty": deque(maxlen=ROLLING_WINDOW),
    "sell_qty": deque(maxlen=ROLLING_WINDOW),
})
lock = threading.Lock()
last_save_time = time.time()

# ================= DATABASE SAVE =================
def save_to_db_batch():
    global last_save_time
    now = time.time()
    if now - last_save_time < SAVE_INTERVAL:
        return
    last_save_time = now

    with lock:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        for token, data in tick_store.items():
            if data["ltp"] is None:
                continue
            avg_buy = round(sum(data["buy_qty"]) / len(data["buy_qty"]), 2) if data["buy_qty"] else 0
            avg_sell = round(sum(data["sell_qty"]) / len(data["sell_qty"]), 2) if data["sell_qty"] else 0

            table_name = token_to_symbol.get(token, f"stock_{token}").replace(" ", "_").replace("-", "_")
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
            c.execute(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)",
                      (ts, data["ltp"], data["volume"], avg_buy, avg_sell))
        conn.commit()
        conn.close()
        print(f"💾 Saved batch at {datetime.now(IST).strftime('%H:%M:%S')}")

# ================= CALLBACKS =================
def setup_callbacks(kws):
    def on_connect(ws, resp):
        print("✅ WS connected, subscribing...")
        ws.subscribe(TOKENS)
        ws.set_mode(ws.MODE_FULL, TOKENS)

    def on_ticks(ws, ticks):
        with lock:
            for t in ticks:
                token = t["instrument_token"]
                store = tick_store[token]
                store["ltp"] = t.get("last_price", 0.0)
                store["volume"] = t.get("volume_traded", 0)
                store["buy_qty"].append(t.get("total_buy_quantity", 0))
                store["sell_qty"].append(t.get("total_sell_quantity", 0))

    def on_close(ws, code, reason):
        print(f"🔴 WS closed: {reason}, reconnecting...")
        time.sleep(5)
        try:
            ws.connect(threaded=True)
        except Exception as e:
            print("❌ Reconnect failed:", e)

    def on_error(ws, code, reason):
        print(f"❌ WS error: {code} | {reason}")

    kws.on_connect = on_connect
    kws.on_ticks = on_ticks
    kws.on_close = on_close
    kws.on_error = on_error

# ================= MARKET HOURS CHECK =================
def is_save_time():
    now = datetime.now(IST)
    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end = now.replace(hour=12, minute=0, second=0, microsecond=0)
    return start <= now < end

# ================= MAIN =================
if __name__ == "__main__":
    print("📊 Starting Kite WebSocket Collector (First Half, Batched)")
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)
    kws = KiteTicker(API_KEY, ACCESS_TOKEN)
    setup_callbacks(kws)
    kws.connect(threaded=True)
    print("🚀 WebSocket thread started")

    profile = kite.profile()
    print("User:", profile.get("user_name"))
    print("User type:", profile.get("user_type"))

    try:
        while True:
            if is_save_time():
                save_to_db_batch()
            else:
                print("⏰ Waiting for market hours...")
                time.sleep(60)
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Stopped manually")
        kws.close()
