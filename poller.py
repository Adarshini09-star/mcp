# poller.py - Fixed with correct API structure
import requests
import os
import json
from apscheduler.schedulers.background import BackgroundScheduler
from db import SessionLocal, save_snapshot
from datetime import datetime
import time

PENDLE_BASE = "https://api-v2.pendle.finance/core"
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))

def fetch_all_markets():
    """Fetch all markets - v1 endpoint works"""
    url = f"{PENDLE_BASE}/v1/1/markets"
    print(f"📡 Fetching markets from: {url}")
    
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        print(f"✅ Successfully fetched {len(data.get('results', []))} markets")
        return data
    except Exception as e:
        print(f"❌ Error fetching markets: {e}")
        return {"results": []}

def fetch_market_data_v1(chain_id, market_address):
    """
    Try v1 endpoint for market details
    The v1 endpoint structure includes the data we need
    """
    url = f"{PENDLE_BASE}/v1/{chain_id}/markets/{market_address}"
    print(f"📊 Fetching from v1: {market_address[:10]}...")
    
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"   v1 failed: {e}")
        return None

def poll_and_store():
    """Main polling function"""
    print("\n" + "="*60)
    print(f"🔄 Starting poll at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    session = SessionLocal()
    stored_count = 0
    
    try:
        # Get all markets
        markets_response = fetch_all_markets()
        markets = markets_response.get("results", [])
        
        if not markets:
            print("⚠️  No markets found!")
            return
        
        print(f"\n📋 Processing {min(len(markets), 10)} markets...")
        
        # Process markets - let's try more to find data
        for idx, market in enumerate(markets[:10], 1):
            market_address = market.get("address")
            chain_id = market.get("chainId", 1)
            market_name = market.get("name", "Unknown")
            
            if not market_address:
                continue
            
            print(f"\n[{idx}/10] {market_name}")
            print(f"    Address: {market_address}")
            print(f"    Chain: {chain_id}")
            
            # The market list response already contains some data!
            # Let's use that instead of making another API call
            pt_price = None
            sy_price = None
            tvl = None
            
            # Extract from the market object itself
            if "pt" in market:
                pt_price = market["pt"].get("price", {}).get("usd")
            
            if "sy" in market:
                sy_price = market["sy"].get("price", {}).get("usd")
            
            if "liquidity" in market:
                tvl = market["liquidity"].get("usd")
            
            # Try fetching detailed data as backup
            if not pt_price and not sy_price and not tvl:
                details = fetch_market_data_v1(chain_id, market_address)
                if details:
                    if "pt" in details:
                        pt_price = details["pt"].get("price", {}).get("usd")
                    if "sy" in details:
                        sy_price = details["sy"].get("price", {}).get("usd")
                    if "liquidity" in details:
                        tvl = details["liquidity"].get("usd")
                    market = details  # Use detailed data
            
            print(f"    💰 PT Price: ${pt_price if pt_price else 'N/A'}")
            print(f"    💰 SY Price: ${sy_price if sy_price else 'N/A'}")
            print(f"    💵 TVL: ${tvl if tvl else 'N/A'}")
            
            # Save even if some values are None
            save_snapshot(
                session=session,
                market_id=market_address,
                raw_json=json.dumps(market),
                pt_price=pt_price,
                sy_price=sy_price,
                tvl=tvl
            )
            
            stored_count += 1
            print(f"    ✅ Saved to database")
            time.sleep(0.5)
        
        print(f"\n{'='*60}")
        print(f"✅ Poll completed! Stored {stored_count} market snapshots")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"\n❌ Poller error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()

def start_scheduler():
    """Start background scheduler"""
    scheduler = BackgroundScheduler()
    scheduler.add_job(poll_and_store, "interval", seconds=POLL_INTERVAL_SECONDS)
    scheduler.start()
    print(f"⏰ Scheduler started - polling every {POLL_INTERVAL_SECONDS} seconds")

if __name__ == "__main__":
    print("🚀 Pendle MCP Poller Starting...")
    print(f"📍 API Base: {PENDLE_BASE}")
    print(f"⏱️  Poll Interval: {POLL_INTERVAL_SECONDS}s\n")
    
    # Run immediately
    poll_and_store()
    
    # Start scheduler
    start_scheduler()
    
    # Keep running
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n👋 Shutting down poller...")
