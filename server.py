# server.py - FastAPI server for Pendle MCP
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from db import SessionLocal, MarketSnapshot
from sqlalchemy import func, desc
from datetime import datetime, timedelta
from typing import Optional, List
import json

app = FastAPI(title="Pendle MCP Server", version="1.0.0")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    """Health check endpoint"""
    return {
        "status": "online",
        "service": "Pendle MCP Server",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/markets")
def get_markets():
    """Get list of all tracked markets with latest data"""
    session = SessionLocal()
    try:
        # Get latest snapshot for each market
        subquery = session.query(
            MarketSnapshot.market_id,
            func.max(MarketSnapshot.timestamp).label('max_ts')
        ).group_by(MarketSnapshot.market_id).subquery()
        
        markets = session.query(MarketSnapshot).join(
            subquery,
            (MarketSnapshot.market_id == subquery.c.market_id) &
            (MarketSnapshot.timestamp == subquery.c.max_ts)
        ).all()
        
        result = []
        for m in markets:
            # Parse raw JSON to get market name
            try:
                raw_data = json.loads(m.raw_json)
                market_name = raw_data.get('name', 'Unknown')
            except:
                market_name = 'Unknown'
            
            result.append({
                "market_id": m.market_id,
                "name": market_name,
                "pt_price": m.pt_price,
                "sy_price": m.sy_price,
                "tvl": m.tvl,
                "last_updated": m.timestamp.isoformat()
            })
        
        return {"count": len(result), "markets": result}
    finally:
        session.close()

@app.get("/market/{market_id}/history")
def get_market_history(
    market_id: str,
    hours: Optional[int] = Query(24, description="Hours of history to retrieve")
):
    """Get historical data for a specific market"""
    session = SessionLocal()
    try:
        cutoff = datetime.now() - timedelta(hours=hours)
        
        snapshots = session.query(MarketSnapshot).filter(
            MarketSnapshot.market_id == market_id,
            MarketSnapshot.timestamp >= cutoff
        ).order_by(MarketSnapshot.timestamp.asc()).all()
        
        if not snapshots:
            raise HTTPException(status_code=404, detail="Market not found or no data available")
        
        history = []
        for s in snapshots:
            history.append({
                "timestamp": s.timestamp.isoformat(),
                "pt_price": s.pt_price,
                "sy_price": s.sy_price,
                "tvl": s.tvl
            })
        
        return {
            "market_id": market_id,
            "data_points": len(history),
            "history": history
        }
    finally:
        session.close()

@app.get("/market/{market_id}/latest")
def get_market_latest(market_id: str):
    """Get latest snapshot for a specific market"""
    session = SessionLocal()
    try:
        snapshot = session.query(MarketSnapshot).filter(
            MarketSnapshot.market_id == market_id
        ).order_by(desc(MarketSnapshot.timestamp)).first()
        
        if not snapshot:
            raise HTTPException(status_code=404, detail="Market not found")
        
        # Parse full data
        try:
            full_data = json.loads(snapshot.raw_json)
        except:
            full_data = {}
        
        return {
            "market_id": snapshot.market_id,
            "timestamp": snapshot.timestamp.isoformat(),
            "pt_price": snapshot.pt_price,
            "sy_price": snapshot.sy_price,
            "tvl": snapshot.tvl,
            "full_data": full_data
        }
    finally:
        session.close()

@app.get("/analytics/summary")
def get_analytics_summary():
    """Get overall analytics summary"""
    session = SessionLocal()
    try:
        # Total markets tracked
        total_markets = session.query(func.count(func.distinct(MarketSnapshot.market_id))).scalar()
        
        # Total TVL across all markets (latest snapshots)
        subquery = session.query(
            MarketSnapshot.market_id,
            func.max(MarketSnapshot.timestamp).label('max_ts')
        ).group_by(MarketSnapshot.market_id).subquery()
        
        latest_snapshots = session.query(MarketSnapshot).join(
            subquery,
            (MarketSnapshot.market_id == subquery.c.market_id) &
            (MarketSnapshot.timestamp == subquery.c.max_ts)
        ).all()
        
        total_tvl = sum(s.tvl for s in latest_snapshots if s.tvl)
        avg_pt_price = sum(s.pt_price for s in latest_snapshots if s.pt_price) / len(latest_snapshots) if latest_snapshots else 0
        
        # Total data points
        total_snapshots = session.query(func.count(MarketSnapshot.id)).scalar()
        
        return {
            "total_markets": total_markets,
            "total_tvl": total_tvl,
            "average_pt_price": avg_pt_price,
            "total_snapshots": total_snapshots,
            "timestamp": datetime.now().isoformat()
        }
    finally:
        session.close()

@app.get("/analytics/top-markets")
def get_top_markets(limit: int = Query(5, description="Number of top markets to return")):
    """Get top markets by TVL"""
    session = SessionLocal()
    try:
        subquery = session.query(
            MarketSnapshot.market_id,
            func.max(MarketSnapshot.timestamp).label('max_ts')
        ).group_by(MarketSnapshot.market_id).subquery()
        
        top_markets = session.query(MarketSnapshot).join(
            subquery,
            (MarketSnapshot.market_id == subquery.c.market_id) &
            (MarketSnapshot.timestamp == subquery.c.max_ts)
        ).order_by(desc(MarketSnapshot.tvl)).limit(limit).all()
        
        result = []
        for m in top_markets:
            try:
                raw_data = json.loads(m.raw_json)
                market_name = raw_data.get('name', 'Unknown')
            except:
                market_name = 'Unknown'
            
            result.append({
                "market_id": m.market_id,
                "name": market_name,
                "tvl": m.tvl,
                "pt_price": m.pt_price,
                "sy_price": m.sy_price
            })
        
        return {"top_markets": result}
    finally:
        session.close()

if __name__ == "__main__":
    print("🚀 Starting Pendle MCP API Server...")
    print("📖 API Docs: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)
