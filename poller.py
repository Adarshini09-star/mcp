# poller.py
import requests
import os
import json
from apscheduler.schedulers.background import BackgroundScheduler
from db import SessionLocal, save_snapshot
from datetime import datetime

PENDLE_BASE = os.getenv("PENDLE_API_BASE", "https://api-v2.pendle.finance/core")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))  # e.g., 5 minutes

def fetch_active_markets():
    # Endpoint path name is from Pendle docs; if the exact path differs, update here.
    url = f"{PENDLE_BASE}/markets/active"  # approximate; adjust if needed
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_market_details(market_id):
    url = f"{PENDLE_BASE}/market/{market_id}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def poll_and_store():
    session = SessionLocal()
    try:
        # 1) get active markets
        markets = fetch_active_markets()
        # depending on API response structure, extract ids:
        for m in markets.get("data", markets if isinstance(markets, list) else []):
            market_id = m.get("id") or m.get("marketId") or m.get("address")
            if not market_id:
                continue
            details = fetch_market_details(market_id)
            # attempt to extract pt/sy price & tvl if available
            pt_price = details.get("ptPrice") or (details.get("prices") or {}).get("pt")
            sy_price = details.get("syPrice") or (details.get("prices") or {}).get("sy")
            tvl = details.get("tvl")
            save_snapshot(session, market_id, json.dumps(details), pt_price=pt_price, sy_price=sy_price, tvl=tvl)
    except Exception as e:
        print("Poller error:", e)
    finally:
        session.close()

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(poll_and_store, "interval", seconds=POLL_INTERVAL_SECONDS)
    scheduler.start()
    print(f"Started poller every {POLL_INTERVAL_SECONDS} seconds")
