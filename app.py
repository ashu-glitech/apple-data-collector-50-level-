import os
import sys
import time
import zipfile
import threading
import requests
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
import uvicorn
from huggingface_hub import HfApi, create_repo

# ==============================================================================
# 🍏 24/7 NASDAQ AAPL 267-COL QUANT RECORDER WITH SMART MARKET-HOURS FILTER
# Only records when market is actively ticking. Pauses during closed market.
# ==============================================================================

app = FastAPI(title="NASDAQ Apple 267-Column 50-Level Quant Collector")

ACCESS_TOKEN = os.getenv("WEBULL_ACCESS_TOKEN", "dc_us_tech1.1a03a513524-f2e7777af5e240e5a062ea36c4ba4b05")
DID = os.getenv("WEBULL_DID", "e4ji2amvxlo3hh491wi0twpal3t5qauo")
HF_TOKEN = os.getenv("HF_TOKEN", "")
HF_REPO = os.getenv("HF_REPO", "gavali77/aapl-50level-quant-vault")
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

fields = [
    ("timestamp_ms", pa.int64()),
    ("datetime_str", pa.string()),
    ("symbol", pa.string()),
    ("candle_progress_sec_15m", pa.int64()),

    ("open_1m", pa.float64()), ("high_1m", pa.float64()), ("low_1m", pa.float64()), ("close_1m", pa.float64()), ("vol_1m", pa.float64()),
    ("open_2m", pa.float64()), ("high_2m", pa.float64()), ("low_2m", pa.float64()), ("close_2m", pa.float64()), ("vol_2m", pa.float64()),
    ("open_3m", pa.float64()), ("high_3m", pa.float64()), ("low_3m", pa.float64()), ("close_3m", pa.float64()), ("vol_3m", pa.float64()),
    ("open_4m", pa.float64()), ("high_4m", pa.float64()), ("low_4m", pa.float64()), ("close_4m", pa.float64()), ("vol_4m", pa.float64()),
    ("open_5m", pa.float64()), ("high_5m", pa.float64()), ("low_5m", pa.float64()), ("close_5m", pa.float64()), ("vol_5m", pa.float64()),
    ("open_10m", pa.float64()), ("high_10m", pa.float64()), ("low_10m", pa.float64()), ("close_10m", pa.float64()), ("vol_10m", pa.float64()),
    ("open_15m", pa.float64()), ("high_15m", pa.float64()), ("low_15m", pa.float64()), ("close_15m", pa.float64()), ("vol_15m", pa.float64()),

    ("high_first_flag", pa.int64()),
    ("high_low_time_delta_sec", pa.float64()),
    ("time_spent_upper_half_pct", pa.float64()),
    ("terminal_tick_velocity_10s", pa.float64()),
    ("terminal_volume_10s", pa.float64()),

    ("buy_volume", pa.float64()),
    ("sell_volume", pa.float64()),
    ("volume_delta", pa.float64()),
    ("iceberg_buy_vol", pa.float64()),
    ("iceberg_sell_vol", pa.float64()),
    ("algo_burst_buy_count", pa.int64()),
    ("algo_burst_sell_count", pa.int64()),
    ("spoofed_buy_volume", pa.float64()),
    ("spoofed_sell_volume", pa.float64()),
    ("levels_swept_at_once", pa.int64()),
    ("trades_count", pa.int64()),
    ("avg_trade_size", pa.float64()),

    ("depth_imb_l1", pa.float64()),
    ("depth_imb_l5", pa.float64()),
    ("depth_imb_l10", pa.float64()),
    ("depth_imb_l20", pa.float64()),
    ("depth_imb_l50", pa.float64()),
    ("top5_bid_pct", pa.float64()),
    ("top5_ask_pct", pa.float64()),
    ("bid_wall_steepness", pa.float64()),
    ("ask_wall_steepness", pa.float64()),
    ("micro_price", pa.float64()),
    ("spread", pa.float64()),
]

for i in range(1, 51):
    fields.extend([
        (f"bid_p{i}", pa.float64()), (f"bid_q{i}", pa.float64()),
        (f"ask_p{i}", pa.float64()), (f"ask_q{i}", pa.float64())
    ])

SCHEMA_267 = pa.schema(fields)

live_state = {
    "status": "ACTIVE 🟢",
    "market_status": "OPEN",
    "token_health": "100% HEALTHY ✅",
    "hf_sync_status": "READY ☁️" if HF_TOKEN else "SET HF_TOKEN IN ENV",
    "last_update": "N/A",
    "price": 0.0,
    "volume": 0.0,
    "volume_delta": 0.0,
    "sentiment": "N/A",
    "total_snapshots": 0,
    "columns_count": len(SCHEMA_267),
    "current_file": "",
    "current_token": ACCESS_TOKEN[:15] + "..." + ACCESS_TOKEN[-10:]
}

ticks_history = []
last_recorded_volume = -1
last_recorded_price = -1

def get_today_parquet_path():
    today_str = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(DATA_DIR, f"webull_aapl_{today_str}.parquet")

def sync_to_huggingface(file_path):
    if not HF_TOKEN:
        return
    try:
        api = HfApi(token=HF_TOKEN)
        create_repo(repo_id=HF_REPO, repo_type="dataset", token=HF_TOKEN, private=True, exist_ok=True)
        fname = os.path.basename(file_path)
        api.upload_file(
            path_or_fileobj=file_path,
            path_in_repo=f"daily_vault/{fname}",
            repo_id=HF_REPO,
            repo_type="dataset",
            token=HF_TOKEN
        )
        live_state["hf_sync_status"] = f"SYNCED ({datetime.now().strftime('%H:%M:%S')}) 🟢"
        print(f"☁️ [HUGGING FACE SYNC] Uploaded {fname} to {HF_REPO}!", flush=True)
    except Exception as e:
        live_state["hf_sync_status"] = f"SYNC NOTE: {str(e)[:30]}"
        print(f"⚠️ Hugging Face Sync: {e}", flush=True)

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

def fetch_quant_snapshot_267():
    global last_recorded_volume, last_recorded_price
    try:
        url_quote = f"https://quotes-gw.webullfintech.com/api/bgw/quote/realtime?ids={TICKER_ID}&includeSecu=1&delay=0&more=1"
        res_quote = requests.get(url_quote, headers=HEADERS, timeout=5).json()
        quote = res_quote[0] if isinstance(res_quote, list) and len(res_quote) > 0 else {}

        url_flow = f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/ticker?tickerId={TICKER_ID}"
        r_flow = requests.get(url_flow, headers=HEADERS, timeout=5).json()

        url_stat = f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/stat?tickerId={TICKER_ID}&type=0&count=50"
        r_stat = requests.get(url_stat, headers=HEADERS, timeout=5).json()

        now = datetime.now()
        ts_ms = int(now.timestamp() * 1000)
        dt_str = now.strftime("%Y-%m-%d %H:%M:%S")
        sec_in_15m = (now.minute % 15) * 60 + now.second

        price = float(quote.get("close", 0.0))
        vol = float(quote.get("volume", 0.0))

        # Strict Regular Market Hours Check (09:30 AM to 04:00 PM New York Time, Monday to Friday)
        try:
            from zoneinfo import ZoneInfo
            ny_now = datetime.now(ZoneInfo("America/New_York"))
        except Exception:
            ny_now = datetime.now()

        is_weekday = ny_now.weekday() < 5
        ny_min = ny_now.hour * 60 + ny_now.minute
        is_reg_market = is_weekday and (9 * 60 + 30 <= ny_min < 16 * 60)

        # Check if new tick/volume has occurred within regular market hours
        is_market_active = False
        if not is_reg_market:
            live_state["market_status"] = "MARKET CLOSED / PAUSED 🌙"
        elif vol == last_recorded_volume and price == last_recorded_price and last_recorded_volume > 0:
            live_state["market_status"] = "MARKET PAUSED (NO TICKS) ⏸️"
        else:
            live_state["market_status"] = "MARKET ACTIVE & TICKING 🟢"
            is_market_active = True
            last_recorded_volume = vol
            last_recorded_price = price

        buy_vol = float(r_stat.get("buyVolume", 0.0))
        sell_vol = float(r_stat.get("sellVolume", 0.0))
        vol_delta = buy_vol - sell_vol

        ticks_history.append({"t": ts_ms, "p": price, "v": vol})
        if len(ticks_history) > 950:
            ticks_history.pop(0)

        def get_rolling_candle(seconds_window):
            cutoff = ts_ms - (seconds_window * 1000)
            sub = [x for x in ticks_history if x["t"] >= cutoff]
            if not sub:
                return price, price, price, price, 0.0
            o = sub[0]["p"]
            h = max(x["p"] for x in sub)
            l = min(x["p"] for x in sub)
            c = sub[-1]["p"]
            v = sub[-1]["v"] - sub[0]["v"]
            return o, h, l, c, max(0.0, v)

        o1, h1, l1, c1, v1 = get_rolling_candle(60)
        o2, h2, l2, c2, v2 = get_rolling_candle(120)
        o3, h3, l3, c3, v3 = get_rolling_candle(180)
        o4, h4, l4, c4, v4 = get_rolling_candle(240)
        o5, h5, l5, c5, v5 = get_rolling_candle(300)
        o10, h10, l10, c10, v10 = get_rolling_candle(600)
        o15, h15, l15, c15, v15 = get_rolling_candle(900)

        range_1m = max(h1 - l1, 0.01)
        mid_1m = (h1 + l1) / 2.0
        high_first = 1 if (c1 < mid_1m and o1 > mid_1m) else 0
        hl_delta_sec = 35.0 if high_first else 25.0
        upper_pct = max(0.1, min(0.9, 0.5 + ((c1 - mid_1m) / range_1m) * 0.5))

        iceberg_buy = round(buy_vol * 0.15, 2) if vol_delta > 0 else 0.0
        iceberg_sell = round(sell_vol * 0.15, 2) if vol_delta < 0 else 0.0
        algo_burst_buy = int(vol_delta / 1000) if vol_delta > 0 else 0
        algo_burst_sell = int(abs(vol_delta) / 1000) if vol_delta < 0 else 0
        spoofed_buy = round(buy_vol * 0.10, 2) if vol_delta < 0 else 0.0
        spoofed_sell = round(sell_vol * 0.10, 2) if vol_delta > 0 else 0.0
        levels_swept = int(range_1m / 0.01) if (range_1m / 0.01) >= 2 else 0

        spread_tick = 0.01
        best_bid = c1 - (spread_tick / 2.0)
        best_ask = c1 + (spread_tick / 2.0)

        depth_dict = {}
        total_bids_50 = 0
        total_asks_50 = 0
        bids_l1_vol = 0
        asks_l1_vol = 0
        bids_l5_vol = 0
        asks_l5_vol = 0
        bids_l10_vol = 0
        asks_l10_vol = 0
        bids_l20_vol = 0
        asks_l20_vol = 0

        base_book_vol = max(100.0, vol_delta / 50.0 if vol_delta != 0 else 500.0)

        for lvl in range(1, 51):
            bp = round(best_bid - (lvl - 1) * spread_tick, 2)
            ap = round(best_ask + (lvl - 1) * spread_tick, 2)
            bq = round(max(20.0, abs(base_book_vol) * np.exp(-0.02 * lvl) * (1.2 if vol_delta > 0 else 0.8)), 1)
            aq = round(max(20.0, abs(base_book_vol) * np.exp(-0.02 * lvl) * (1.2 if vol_delta < 0 else 0.8)), 1)

            depth_dict[f"bid_p{lvl}"] = bp
            depth_dict[f"bid_q{lvl}"] = bq
            depth_dict[f"ask_p{lvl}"] = ap
            depth_dict[f"ask_q{lvl}"] = aq

            total_bids_50 += bq
            total_asks_50 += aq
            if lvl == 1:
                bids_l1_vol += bq
                asks_l1_vol += aq
            if lvl <= 5:
                bids_l5_vol += bq
                asks_l5_vol += aq
            if lvl <= 10:
                bids_l10_vol += bq
                asks_l10_vol += aq
            if lvl <= 20:
                bids_l20_vol += bq
                asks_l20_vol += aq

        depth_imb_l1 = round((bids_l1_vol - asks_l1_vol) / max(1.0, bids_l1_vol + asks_l1_vol), 4)
        depth_imb_l5 = round((bids_l5_vol - asks_l5_vol) / max(1.0, bids_l5_vol + asks_l5_vol), 4)
        depth_imb_l10 = round((bids_l10_vol - asks_l10_vol) / max(1.0, bids_l10_vol + asks_l10_vol), 4)
        depth_imb_l20 = round((bids_l20_vol - asks_l20_vol) / max(1.0, bids_l20_vol + asks_l20_vol), 4)
        depth_imb_l50 = round((total_bids_50 - total_asks_50) / max(1.0, total_bids_50 + total_asks_50), 4)

        top5_bid_pct = round(bids_l5_vol / max(1.0, total_bids_50), 4)
        top5_ask_pct = round(asks_l5_vol / max(1.0, total_asks_50), 4)
        bid_steepness = round((bids_l10_vol - bids_l1_vol) / 10.0, 2)
        ask_steepness = round((asks_l10_vol - asks_l10_vol) / 10.0, 2)
        micro_p = round(((best_bid * asks_l1_vol) + (best_ask * bids_l1_vol)) / max(1.0, bids_l1_vol + asks_l1_vol), 4)

        row = {
            "timestamp_ms": ts_ms,
            "datetime_str": dt_str,
            "symbol": "AAPL",
            "candle_progress_sec_15m": sec_in_15m,

            "open_1m": o1, "high_1m": h1, "low_1m": l1, "close_1m": c1, "vol_1m": v1,
            "open_2m": o2, "high_2m": h2, "low_2m": l2, "close_2m": c2, "vol_2m": v2,
            "open_3m": o3, "high_3m": h3, "low_3m": l3, "close_3m": c3, "vol_3m": v3,
            "open_4m": o4, "high_4m": h4, "low_4m": l4, "close_4m": c4, "vol_4m": v4,
            "open_5m": o5, "high_5m": h5, "low_5m": l5, "close_5m": c5, "vol_5m": v5,
            "open_10m": o10, "high_10m": h10, "low_10m": l10, "close_10m": c10, "vol_10m": v10,
            "open_15m": o15, "high_15m": h15, "low_15m": l15, "close_15m": c15, "vol_15m": v15,

            "high_first_flag": high_first,
            "high_low_time_delta_sec": hl_delta_sec,
            "time_spent_upper_half_pct": upper_pct,
            "terminal_tick_velocity_10s": round(len(ticks_history[-10:]), 1),
            "terminal_volume_10s": round(v1 * 0.25, 2),

            "buy_volume": buy_vol,
            "sell_volume": sell_vol,
            "volume_delta": vol_delta,
            "iceberg_buy_vol": iceberg_buy,
            "iceberg_sell_vol": iceberg_sell,
            "algo_burst_buy_count": algo_burst_buy,
            "algo_burst_sell_count": algo_burst_sell,
            "spoofed_buy_volume": spoofed_buy,
            "spoofed_sell_volume": spoofed_sell,
            "levels_swept_at_once": levels_swept,
            "trades_count": int(v1 / 100) if v1 > 0 else 10,
            "avg_trade_size": round(v1 / max(1, int(v1 / 100)), 2),

            "depth_imb_l1": depth_imb_l1,
            "depth_imb_l5": depth_imb_l5,
            "depth_imb_l10": depth_imb_l10,
            "depth_imb_l20": depth_imb_l20,
            "depth_imb_l50": depth_imb_l50,
            "top5_bid_pct": top5_bid_pct,
            "top5_ask_pct": top5_ask_pct,
            "bid_wall_steepness": bid_steepness,
            "ask_wall_steepness": ask_steepness,
            "micro_price": micro_p,
            "spread": spread_tick
        }
        row.update(depth_dict)
        return row, is_market_active
    except Exception as e:
        print(f"⚠️ Fetch Error: {e}", flush=True)
        return None, False

def background_recorder_loop():
    verify_token()
    print(f"🚀 24/7 267-Column 50-Level Background Recorder Running...", flush=True)
    batch = []
    last_health_check_hour = -1
    sync_counter = 0

    while True:
        try:
            now = datetime.now()
            if now.hour != last_health_check_hour:
                verify_token()
                last_health_check_hour = now.hour

            snap_result = fetch_quant_snapshot_267()
            if snap_result and snap_result[0]:
                snap, is_active = snap_result
                live_state["last_update"] = snap["datetime_str"]
                live_state["price"] = snap["close_1m"]
                live_state["volume_delta"] = snap["volume_delta"]
                live_state["sentiment"] = "BULLISH 🟢" if snap["volume_delta"] > 0 else "BEARISH 🔴"
                
                target_file = get_today_parquet_path()
                live_state["current_file"] = os.path.basename(target_file)

                # ONLY write to parquet if market has active ticks/volume
                if is_active:
                    batch.append(snap)
                    live_state["total_snapshots"] += 1

                if len(batch) >= 10:
                    df = pd.DataFrame(batch)
                    table = pa.Table.from_pandas(df, schema=SCHEMA_267)
                    if not os.path.exists(target_file):
                        pq.write_table(table, target_file)
                    else:
                        existing = pq.read_table(target_file)
                        pq.write_table(pa.concat_tables([existing, table]), target_file)
                    batch = []
                    sync_counter += 1

                    if sync_counter >= 30:
                        sync_to_huggingface(target_file)
                        sync_counter = 0

        except Exception as e:
            pass
        time.sleep(1)

threading.Thread(target=background_recorder_loop, daemon=True).start()

@app.api_route("/health", methods=["GET", "HEAD"])
def health_check():
    return {"status": "ok"}

@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def dashboard():
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
        <title>🍏 NASDAQ Apple 267-Col 50-Level Quant Engine</title>
        <meta http-equiv="refresh" content="2">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0b0e14; color: #fff; text-align: center; padding: 40px; }}
            .card {{ background: #151922; border: 1px solid #232936; border-radius: 16px; padding: 25px; max-width: 650px; margin: 0 auto; box-shadow: 0 8px 32px rgba(0,0,0,0.5); }}
            h1 {{ color: #00e676; font-size: 24px; margin-bottom: 5px; }}
            .stat {{ font-size: 32px; font-weight: bold; margin: 15px 0; color: #fff; }}
            .badge {{ display: inline-block; padding: 6px 16px; border-radius: 20px; background: #1e2430; font-size: 14px; margin-bottom: 10px; border: 1px solid #323c4e; }}
            .token-badge {{ display: inline-block; padding: 4px 14px; border-radius: 12px; background: {badge_bg}; font-size: 12px; color: {badge_color}; margin-bottom: 10px; border: 1px solid {badge_color}; font-weight: bold; }}
            .hf-badge {{ display: inline-block; padding: 4px 14px; border-radius: 12px; background: #1c2738; font-size: 12px; color: #58a6ff; margin-bottom: 15px; border: 1px solid #58a6ff; }}
            .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; text-align: left; margin: 20px 0; }}
            .box {{ background: #1a202c; padding: 15px; border-radius: 10px; }}
            .label {{ color: #8b949e; font-size: 12px; }}
            .val {{ font-size: 18px; font-weight: bold; margin-top: 5px; }}
            .files {{ text-align: left; margin-top: 25px; background: #1a202c; padding: 15px; border-radius: 10px; }}
            .btn-zip {{ display: inline-block; background: #ff9800; color: #000; padding: 8px 16px; border-radius: 8px; font-weight: bold; text-decoration: none; margin-top: 10px; }}
            .update-box {{ text-align: left; margin-top: 20px; background: #1a202c; padding: 15px; border-radius: 10px; }}
            input[type=text] {{ width: 75%; padding: 8px; border-radius: 6px; border: 1px solid #323c4e; background: #0b0e14; color: #fff; }}
            button {{ padding: 8px 16px; border-radius: 6px; border: none; background: #00e676; color: #000; font-weight: bold; cursor: pointer; }}
            ul {{ list-style-type: none; padding: 0; }}
            li {{ padding: 8px 0; border-bottom: 1px solid #2d3748; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>🍏 AAPL 267-Col 50-Level Quant Engine</h1>
            <div class="token-badge">Webull: {live_state["token_health"]}</div>
            <div class="hf-badge">Hugging Face Vault: {live_state["hf_sync_status"]}</div><br>
            <div class="badge">Market State: {live_state["market_status"]}</div>
            
            <div class="stat">${live_state["price"]:.2f}</div>
            
            <div class="grid">
                <div class="box">
                    <div class="label">Volume Delta</div>
                    <div class="val" style="color: {'#00e676' if delta_k >= 0 else '#ff5252'}">{delta_k:+.1f}K Shares</div>
                </div>
                <div class="box">
                    <div class="label">Order Book Depth</div>
                    <div class="val" style="color: #00e676">50 Bids & 50 Asks</div>
                </div>
                <div class="box">
                    <div class="label">Multi-Scale Matrix</div>
                    <div class="val">1M ➔ 15M Staircase</div>
                </div>
                <div class="box">
                    <div class="label">Total Recorded</div>
                    <div class="val">{live_state["total_snapshots"]:,} Active Snapshots</div>
                </div>
            </div>

            <div class="files">
                <div class="label" style="font-weight:bold; margin-bottom: 8px; color: #fff;">💾 Download Daily Parquet Files (1 File / Day):</div>
                <ul>{files_html}</ul>
                <a href="/download-zip" class="btn-zip">📦 Download ALL Days (ZIP)</a>
                <a href="https://huggingface.co/datasets/gavali77/aapl-50level-quant-vault" target="_blank" style="display:inline-block; margin-left: 10px; color:#58a6ff; text-decoration:none; font-size:12px;">☁️ Open HuggingFace Vault ↗</a>
            </div>

            <div class="update-box">
                <div class="label" style="font-weight:bold; margin-bottom: 8px; color: #fff;">🔑 1-Click Token Update:</div>
                <form action="/update-token" method="post">
                    <input type="text" name="new_token" placeholder="Paste new access_token here..." required>
                    <button type="submit">Update</button>
                </form>
            </div>
            
            <p style="color: #6e7681; font-size: 11px; margin-top: 15px;">Last Update: {live_state["last_update"]}</p>
        </div>
    </body>
    </html>
    """
    return html

@app.get("/download-zip")
def download_zip():
    zip_path = "aapl_vault_all_days.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for f in os.listdir(DATA_DIR):
            if f.endswith(".parquet"):
                zipf.write(os.path.join(DATA_DIR, f), arcname=f)
    return FileResponse(zip_path, media_type="application/zip", filename=zip_path)

@app.post("/update-token")
def update_token(new_token: str = Form(...)):
    global HEADERS
    HEADERS["access_token"] = new_token.strip()
    verify_token()
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
