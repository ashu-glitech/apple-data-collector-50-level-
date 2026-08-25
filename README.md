# 🍏 NASDAQ Apple 24/7 Live Quant Recorder (Render Deploy Package)

## 🚀 How to Deploy on Render (100% Free - 2 Minutes):

1. Go to [render.com](https://render.com) and log in with GitHub.
2. Click **New +** ➔ **Web Service**.
3. Connect your GitHub repository containing the `render_deploy` folder.
4. Set the following settings:
   - **Root Directory:** `render_deploy` (or leave empty if repo root)
   - **Environment:** `Python 3`
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `uvicorn app:app --host 0.0.0.0 --port $PORT`
   - **Instance Type:** `Free` (512 MB RAM / 0.1 CPU)
5. Click **Create Web Service**!

## 🌟 What You Get:
- Live 24/7 dashboard showing Apple Real-Time Price, Volume Delta, Institutional Inflow/Outflow.
- Automatic daily Parquet dataset saving in the background.
- Direct download buttons on the dashboard to download collected `.parquet` files anytime!
