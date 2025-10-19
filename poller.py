# poller.py
import requests
import os
import json
from apscheduler.schedulers.background import BackgroundScheduler
from db import SessionLocal, save_snapshot
from datetime import datetime

PENDLE_BASE = os.getenv("PENDLE_API_BASE", "https://api-v2.pendle.finance/core")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))  # every 5 min

def fetch_active_markets():
    url = f"{PENDLE_BASE}/markets"
    print(f"Fetching markets from {url}")
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_market_details(market_id):
    url = f"{PENDLE_BASE}/markets/{market_id}"
    print(f"Fetching details for market: {market_id}")
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def poll_and_store():
    session = SessionLocal()
    try:
        markets = fetch_active_markets()

        # API returns a list of market objects under "data"
        for m in markets.get("data", []):
            market_id = m.get("address") or m.get("marketAddress")
            if not market_id:
                continue

            details = fetch_market_details(market_id)

            pt_price = details.get("ptPrice") or (details.get("prices") or {}).get("pt")
            sy_price = details.get("syPrice") or (details.get("prices") or {}).get("sy")
            tvl = details.get("tvl")

            print(f"Inserting market {market_id} | pt={pt_price}, sy={sy_price}, tvl={tvl}")

            save_snapshot(
                session,
                market_id,
                json.dumps(details),
                pt_price=pt_price,
                sy_price=sy_price,
                tvl=tvl
            )

        print("✅ Poll completed successfully.")

    except Exception as e:
        print("Poller error:", e)
    finally:
        session.close()

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(poll_and_store, "interval", seconds=POLL_INTERVAL_SECONDS)
    scheduler.start()
    print(f"Started poller every {POLL_INTERVAL_SECONDS} seconds")

