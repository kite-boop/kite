import os
import time
import sqlite3
import threading
import signal
import sys
from datetime import datetime, time as dtime
from collections import deque, defaultdict

import pytz
import my_helpers as my
from kiteconnect import KiteConnect, KiteTicker

# ================= CONFIG =================
API_KEY = os.environ["KITE_API_KEY"]

# ⚠️ PUT TODAY'S ACCESS TOKEN HERE
ACCESS_TOKEN = "zmxErhY3f95I2XmNdjf2yJcaQoBejUFJ"

MAX_RUNTIME = 5.5 * 60 * 60   # backup safety (seconds)
ROLLING_WINDOW = 20
SAVE_INTERVAL = 20           # seconds

IST = pytz.timezone("Asia/Kolkata")
START_TIME = time.time()

MARKET_START = dtime(9, 15)
MARKET_END   = dtime(15, 30)

# ================= LOAD TOKENS =================
df = my.load_table_as_df(
    "tikers.db",
    "main_table",
    ["symbol", "nse_instrument_token"]
)

TOKENS = df["nse_instrument_token"].tolist()
token_to_symbol = dict(zip(df["nse_instrument_token"], df["symbol"]))

print(f"✅ Tokens loaded: {len(TOKENS)}")

# ================= DB FILE =================
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
shutdown_event = threading.Event()
kws = None

# ================= MARKET TIME CHECK =================
def is_market_open():
    now = datetime.now(IST).time()
    return MARKET_START <= now < MARKET_END

def is_market_closed():
    now = datetime.now(IST).time()
    return now >= MARKET_END

# ================= DATABASE SAVE =================
def save_to_db_batch(force=False):
    global last_save_time

    # 🚫 HARD GATE — NO DB WRITES OUTSIDE MARKET HOURS
    if not force and not is_market_open():
        return

    now = time.time()
    if not force and now - last_save_time < SAVE_INTERVAL:
        return

    last_save_time = now

    with lock:
        if not tick_store:
            return

        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        for token, data in tick_store.items():
            if data["ltp"] is None:
                continue

            avg_buy = (
                round(sum(data["buy_qty"]) / len(data["buy_qty"]), 2)
                if data["buy_qty"] else 0
            )
            avg_sell = (
                round(sum(data["sell_qty"]) / len(data["sell_qty"]), 2)
                if data["sell_qty"] else 0
            )

            table_name = token_to_symbol.get(token, str(token))
            ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

            c.execute(f'''
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    timestamp TEXT,
                    ltp REAL,
                    volume INTEGER,
                    avg_buy_qty REAL,
                    avg_sell_qty REAL
                )
            ''')

            c.execute(
                f'INSERT INTO "{table_name}" VALUES (?, ?, ?, ?, ?)',
                (ts, data["ltp"], data["volume"], avg_buy, avg_sell)
            )

        conn.commit()
        conn.close()

        print(f"💾 DB saved @ {datetime.now(IST).strftime('%H:%M:%S')}")

# ================= GRACEFUL SHUTDOWN =================
def graceful_shutdown(reason):
    print(f"🛑 Shutdown initiated ({reason})")

    try:
        save_to_db_batch(force=True)
    except Exception as e:
        print("DB flush error:", e)

    try:
        if kws:
            kws.close()
    except:
        pass

    print("✅ Final DB flushed, exiting")
    shutdown_event.set()
    sys.exit(0)

def signal_handler(signum, frame):
    graceful_shutdown("SIGTERM / SIGINT")

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ================= WEBSOCKET CALLBACKS =================
def setup_callbacks(ws):
    def on_connect(ws, resp):
        print("✅ WebSocket connected")
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
        print("🔴 WebSocket closed:", reason)

    def on_error(ws, code, reason):
        print("❌ WebSocket error:", code, reason)

    ws.on_connect = on_connect
    ws.on_ticks = on_ticks
    ws.on_close = on_close
    ws.on_error = on_error

# ================= MAIN =================
if __name__ == "__main__":
    print("🚀 Starting Kite Manual WS Collector")

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)

    profile = kite.profile()
    print(f"👤 User: {profile.get('user_name')} | Type: {profile.get('user_type')}")

    kws = KiteTicker(API_KEY, ACCESS_TOKEN)
    setup_callbacks(kws)
    kws.connect(threaded=True)

    print("📡 WebSocket running")

    try:
        while not shutdown_event.is_set():
            elapsed = time.time() - START_TIME

            # ⏱️ backup safety exit
            if elapsed >= MAX_RUNTIME:
                graceful_shutdown("max runtime reached")

            # 🕒 market close exit (PRIMARY)
            if is_market_closed():
                graceful_shutdown("market closed (15:30 IST)")

            # 💾 DB writes only inside market window
            save_to_db_batch()

            time.sleep(1)

    except Exception as e:
        print("❌ Fatal error:", e)
        graceful_shutdown("exception")
