import requests
import os
import json
import time
from apscheduler.schedulers.background import BackgroundScheduler
from db import SessionLocal, save_snapshot
from datetime import datetime

# Base API endpoint
PENDLE_BASE = os.getenv("PENDLE_API_BASE", "https://api-v2.pendle.finance/core")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))  # every 5 min


def fetch_active_markets():
    """Fetch list of active markets from Pendle API."""
    url = f"{PENDLE_BASE}/markets"
    print(f"[{datetime.now()}] Fetching markets from {url}")
    try:
        r = requests.get(url, timeout=20)
        print(f"[{datetime.now()}] Response status: {r.status_code}")
        print(f"[{datetime.now()}] Raw response: {r.text[:500]}")  # print first 500 chars
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Error fetching markets: {e}")
        return {}



def fetch_market_details(market_id):
    """Fetch detailed market data for a specific market."""
    url = f"{PENDLE_BASE}/markets/{market_id}"
    print(f"[{datetime.now()}] Fetching details for market: {market_id}")
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()


def poll_and_store():
    """Poll Pendle API and save data to database."""
    print(f"[{datetime.now()}] ⏳ Starting data polling...")
    session = SessionLocal()
    try:
        markets = fetch_active_markets()

        data_list = markets.get("data", [])
        if not data_list:
            print(f"[{datetime.now()}] ⚠️ No markets returned from API.")
            return

        for m in data_list:
            market_id = m.get("address") or m.get("marketAddress")
            if not market_id:
                continue

            details = fetch_market_details(market_id)

            pt_price = details.get("ptPrice") or (details.get("prices") or {}).get("pt")
            sy_price = details.get("syPrice") or (details.get("prices") or {}).get("sy")
            tvl = details.get("tvl")

            print(f"[{datetime.now()}] ✅ Inserting market {market_id} | pt={pt_price}, sy={sy_price}, tvl={tvl}")

            save_snapshot(
                session,
                market_id,
                json.dumps(details),
                pt_price=pt_price,
                sy_price=sy_price,
                tvl=tvl
            )

        session.commit()
        print(f"[{datetime.now()}] ✅ Poll completed and data saved.\n")

    except Exception as e:
        print(f"[{datetime.now()}] ❌ Poller error: {e}")
    finally:
        session.close()


def start_scheduler():
    """Start APScheduler background job."""
    scheduler = BackgroundScheduler()
    scheduler.add_job(poll_and_store, "interval", seconds=POLL_INTERVAL_SECONDS)
    scheduler.start()
    print(f"[{datetime.now()}] 🟢 Started poller every {POLL_INTERVAL_SECONDS} seconds")


if __name__ == "__main__":
    print(f"[{datetime.now()}] 🚀 Starting Pendle poller service...")
    start_scheduler()

    # Run once immediately
    poll_and_store()

    # Keep running indefinitely
    while True:
        time.sleep(30)

