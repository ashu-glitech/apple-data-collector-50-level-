import os
import sys
import time
import threading
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
import uvicorn

# ==============================================================================
# 🍏 24/7 WEBULL NASDAQ QUANT DATA RECORDER WITH 1-CLICK AUTO-UPDATE & DASHBOARD
# ==============================================================================

app = FastAPI(title="NASDAQ Apple 24/7 Quant Collector")

ACCESS_TOKEN = os.getenv("WEBULL_ACCESS_TOKEN", "dc_us_tech1.1a03a513524-f2e7777af5e240e5a062ea36c4ba4b05")
DID = os.getenv("WEBULL_DID", "e4ji2amvxlo3hh491wi0twpal3t5qauo")
TICKER_ID = "913256135" # Apple Inc. (AAPL)
DATA_DIR = "collected_data"
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {
    "access_token": ACCESS_TOKEN,
    "app": "global",
    "app-group": "broker",
    "appid": "wb_web_app",
    "device-type": "Web",
    "did": DID,
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "accept": "application/json, text/plain, */*",
    "origin": "https://app.webull.com",
    "referer": "https://app.webull.com/",
    "hl": "en",
    "locale": "eng"
}

live_state = {
    "status": "RUNNING 🟢",
    "token_health": "100% HEALTHY ✅",
    "last_update": "N/A",
    "price": 0.0,
    "volume_delta": 0.0,
    "net_inst_flow": 0.0,
    "sentiment": "N/A",
    "total_snapshots": 0,
    "current_file": "",
    "current_token": ACCESS_TOKEN[:15] + "..." + ACCESS_TOKEN[-10:]
}

def get_today_parquet_path():
    today_str = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(DATA_DIR, f"webull_aapl_{today_str}.parquet")

def verify_token():
    url_test = f"https://quotes-gw.webullfintech.com/api/bgw/quote/realtime?ids={TICKER_ID}&includeSecu=1&delay=0&more=1"
    try:
        r = requests.get(url_test, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            live_state["token_health"] = "100% HEALTHY ✅"
            return True
        else:
            live_state["token_health"] = f"EXPIRED / ERROR ❌ (Status: {r.status_code})"
            return False
    except Exception as e:
        live_state["token_health"] = f"CONNECTION ERROR: {e}"
        return False

def fetch_quant_snapshot():
    try:
        url_quote = f"https://quotes-gw.webullfintech.com/api/bgw/quote/realtime?ids={TICKER_ID}&includeSecu=1&delay=0&more=1"
        res_quote = requests.get(url_quote, headers=HEADERS, timeout=5).json()
        quote = res_quote[0] if isinstance(res_quote, list) and len(res_quote) > 0 else {}

        url_flow = f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/ticker?tickerId={TICKER_ID}"
        r_flow = requests.get(url_flow, headers=HEADERS, timeout=5).json()

        url_stat = f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/stat?tickerId={TICKER_ID}&type=0&count=50"
        r_stat = requests.get(url_stat, headers=HEADERS, timeout=5).json()

        now = datetime.now()
        price = float(quote.get("close", 0.0))
        vol = float(quote.get("volume", 0.0))

        item = r_flow.get("latest", {}).get("item", {})
        net_inst_flow = float(item.get("largeNetFlow", 0.0))

        buy_vol = float(r_stat.get("buyVolume", 0.0))
        sell_vol = float(r_stat.get("sellVolume", 0.0))
        vol_delta = buy_vol - sell_vol

        row = {
            "timestamp_ms": int(now.timestamp() * 1000),
            "datetime_str": now.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": "AAPL",
            "price": price,
            "open": float(quote.get("open", price)),
            "high": float(quote.get("high", price)),
            "low": float(quote.get("low", price)),
            "volume": vol,
            "buy_volume": buy_vol,
            "sell_volume": sell_vol,
            "volume_delta": vol_delta,
            "large_inflow": float(item.get("largeInflow", 0.0)),
            "large_outflow": float(item.get("largeOutflow", 0.0)),
            "net_institutional_flow": net_inst_flow,
            "flow_sentiment": "BULLISH 🟢" if net_inst_flow > 0 else "BEARISH 🔴"
        }
        return row
    except Exception as e:
        return None

def background_recorder_loop():
    verify_token()
    print("🚀 24/7 Background Data Recorder Thread Running...", flush=True)
    batch = []
    last_health_check_hour = -1

    while True:
        try:
            now = datetime.now()
            if now.hour != last_health_check_hour:
                verify_token()
                last_health_check_hour = now.hour

            snap = fetch_quant_snapshot()
            if snap:
                batch.append(snap)
                live_state["last_update"] = snap["datetime_str"]
                live_state["price"] = snap["price"]
                live_state["volume_delta"] = snap["volume_delta"]
                live_state["net_inst_flow"] = snap["net_institutional_flow"]
                live_state["sentiment"] = snap["flow_sentiment"]
                live_state["total_snapshots"] += 1
                
                target_file = get_today_parquet_path()
                live_state["current_file"] = os.path.basename(target_file)

                if len(batch) >= 10:
                    df = pd.DataFrame(batch)
                    table = pa.Table.from_pandas(df)
                    if not os.path.exists(target_file):
                        pq.write_table(table, target_file)
                    else:
                        existing = pq.read_table(target_file)
                        pq.write_table(pa.concat_tables([existing, table]), target_file)
                    batch = []
        except Exception as e:
            pass
        time.sleep(1)

threading.Thread(target=background_recorder_loop, daemon=True).start()

@app.get("/", response_class=HTMLResponse)
def dashboard():
    net_m = live_state["net_inst_flow"] / 1e6
    delta_k = live_state["volume_delta"] / 1e3
    
    files = [f for f in os.listdir(DATA_DIR) if f.endswith(".parquet")]
    files_html = "".join([f'<li><a href="/download/{f}" style="color:#00e676; text-decoration:none;">📥 {f}</a></li>' for f in files]) or "<li>No files yet...</li>"

    is_healthy = "HEALTHY" in live_state["token_health"]
    badge_bg = "#0e3a24" if is_healthy else "#3a0e0e"
    badge_color = "#00e676" if is_healthy else "#ff5252"

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>🍏 NASDAQ Apple 24/7 Live Quant Recorder</title>
        <meta http-equiv="refresh" content="2">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0b0e14; color: #fff; text-align: center; padding: 40px; }}
            .card {{ background: #151922; border: 1px solid #232936; border-radius: 16px; padding: 25px; max-width: 620px; margin: 0 auto; box-shadow: 0 8px 32px rgba(0,0,0,0.5); }}
            h1 {{ color: #00e676; font-size: 24px; margin-bottom: 5px; }}
            .stat {{ font-size: 32px; font-weight: bold; margin: 15px 0; color: #fff; }}
            .badge {{ display: inline-block; padding: 6px 16px; border-radius: 20px; background: #1e2430; font-size: 14px; margin-bottom: 20px; border: 1px solid #323c4e; }}
            .token-badge {{ display: inline-block; padding: 6px 16px; border-radius: 12px; background: {badge_bg}; font-size: 13px; color: {badge_color}; margin-bottom: 15px; border: 1px solid {badge_color}; font-weight: bold; }}
            .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; text-align: left; margin: 20px 0; }}
            .box {{ background: #1a202c; padding: 15px; border-radius: 10px; }}
            .label {{ color: #8b949e; font-size: 12px; }}
            .val {{ font-size: 18px; font-weight: bold; margin-top: 5px; }}
            .files {{ text-align: left; margin-top: 25px; background: #1a202c; padding: 15px; border-radius: 10px; }}
            .update-box {{ text-align: left; margin-top: 20px; background: #1a202c; padding: 15px; border-radius: 10px; }}
            input[type=text] {{ width: 75%; padding: 8px; border-radius: 6px; border: 1px solid #323c4e; background: #0b0e14; color: #fff; }}
            button {{ padding: 8px 16px; border-radius: 6px; border: none; background: #00e676; color: #000; font-weight: bold; cursor: pointer; }}
            ul {{ list-style-type: none; padding: 0; }}
            li {{ padding: 8px 0; border-bottom: 1px solid #2d3748; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>🍏 AAPL 24/7 Quant Data Engine</h1>
            <div class="token-badge">Token Status: {live_state["token_health"]}</div><br>
            <div class="badge">Engine Status: {live_state["status"]}</div>
            
            <div class="stat">${live_state["price"]:.2f}</div>
            
            <div class="grid">
                <div class="box">
                    <div class="label">Volume Delta</div>
                    <div class="val" style="color: {'#00e676' if delta_k >= 0 else '#ff5252'}">{delta_k:+.1f}K Shares</div>
                </div>
                <div class="box">
                    <div class="label">Net Inst. Flow</div>
                    <div class="val" style="color: {'#00e676' if net_m >= 0 else '#ff5252'}">${net_m:+.2f}M</div>
                </div>
                <div class="box">
                    <div class="label">Sentiment</div>
                    <div class="val">{live_state["sentiment"]}</div>
                </div>
                <div class="box">
                    <div class="label">Total Recorded</div>
                    <div class="val">{live_state["total_snapshots"]:,} Ticks</div>
                </div>
            </div>

            <div class="update-box">
                <div class="label" style="font-weight:bold; margin-bottom: 8px; color: #fff;">🔑 1-Click Token Update (Live Without Restart):</div>
                <form action="/update-token" method="post">
                    <input type="text" name="new_token" placeholder="Paste new access_token here..." required>
                    <button type="submit">Update</button>
                </form>
                <div class="label" style="margin-top:5px;">Active Token: {live_state["current_token"]}</div>
            </div>

            <div class="files">
                <div class="label" style="font-weight:bold; margin-bottom: 8px; color: #fff;">💾 Download Collected Parquet Datasets:</div>
                <ul>{files_html}</ul>
            </div>
            
            <p style="color: #6e7681; font-size: 11px; margin-top: 15px;">Last Update: {live_state["last_update"]}</p>
        </div>
    </body>
    </html>
    """
    return html

@app.post("/update-token")
def update_token(new_token: str = Form(...)):
    global HEADERS
    HEADERS["access_token"] = new_token.strip()
    live_state["current_token"] = new_token[:15] + "..." + new_token[-10:]
    verify_token()
    print(f"🔄 Token Updated via Web Dashboard! Health: {live_state['token_health']}", flush=True)
    return RedirectResponse(url="/", status_code=303)

@app.get("/download/{filename}")
def download_file(filename: str):
    file_path = os.path.join(DATA_DIR, filename)
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="application/octet-stream", filename=filename)
    return {"error": "File not found"}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
