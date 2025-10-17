# server.py
from fastapi import FastAPI, Query, BackgroundTasks
import requests, os, json
from db import create_db, SessionLocal
from poller import start_scheduler
from ai_insights import simple_trend_insight

app = FastAPI(title="Pendle MCP Server")

PENDLE_BASE = os.getenv("PENDLE_API_BASE", "https://api-v2.pendle.finance/core")
# create DB
create_db()

# start background poller (APS scheduler)
start_scheduler()

# Helper to call pendle
def call_pendle(path, params=None):
    url = f"{PENDLE_BASE}{path}"
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

@app.get("/tool/get_active_markets")
def get_active_markets():
    """
    Returns list of active markets as provided by Pendle backend.
    """
    try:
        # adjust path if docs show a different route
        return call_pendle("/markets/active")
    except Exception as e:
        return {"error": str(e)}

@app.get("/tool/get_market")
def get_market(market_id: str = Query(..., description="Market ID or address")):
    try:
        return call_pendle(f"/market/{market_id}")
    except Exception as e:
        return {"error": str(e)}

@app.get("/tool/get_yield")
def get_yield(token_id: str = Query(..., description="Token id like PT-stETH")):
    """
    Example endpoint. Adjust to exact docs path if different.
    """
    try:
        return call_pendle(f"/yield/{token_id}")
    except Exception as e:
        return {"error": str(e)}

@app.get("/tool/simulate_swap")
def simulate_swap(pool_id: str, amount: float, from_token: str = "PT", to_token: str = "SY"):
    # Placeholder: in production you'd calculate using Pendle math or call a simulation endpoint
    fee = 0.002  # example
    out = amount * (1 - fee)
    return {"pool_id": pool_id, "from": from_token, "to": to_token, "in_amount": amount, "out_estimate": out, "fee": fee}

@app.get("/tool/historical/snaps")
def list_historical_snapshots(market_id: str, limit: int = 200):
    """
    Returns stored historical snapshots (from SQLite).
    """
    session = SessionLocal()
    snaps = session.query(__import__("db").MarketSnapshot).filter(
        __import__("db").MarketSnapshot.market_id == market_id
    ).order_by(__import__("db").MarketSnapshot.timestamp.desc()).limit(limit).all()
    session.close()
    return {"market_id": market_id, "count": len(snaps), "data": [ {"timestamp": s.timestamp.isoformat(), "pt_price": s.pt_price, "sy_price": s.sy_price, "tvl": s.tvl} for s in reversed(snaps) ]}

@app.get("/tool/insight")
def get_insight(market_id: str, lookback_hours: int = 72):
    return simple_trend_insight(market_id, lookback_hours=lookback_hours)
