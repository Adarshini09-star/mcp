# ai_insights.py
import pandas as pd
import numpy as np
from db import SessionLocal
from datetime import datetime, timedelta

def simple_trend_insight(market_id, lookback_hours=72):
   
    session = SessionLocal()
    cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)
    rows = session.query(__import__("db").MarketSnapshot).filter(
        __import__("db").MarketSnapshot.market_id == market_id,
        __import__("db").MarketSnapshot.timestamp >= cutoff
    ).order_by(__import__("db").MarketSnapshot.timestamp.asc()).all()
    session.close()

    if len(rows) < 3:
        return {"insight": "Not enough historical data yet to generate trend insights."}

    df = pd.DataFrame([{"t": r.timestamp, "pt_price": r.pt_price, "sy_price": r.sy_price, "tvl": r.tvl} for r in rows])
   
    if df["pt_price"].notnull().sum() >= 2:
        x = np.arange(len(df))
        y = df["pt_price"].fillna(method='ffill').values
        coeff = np.polyfit(x,y,1)[0]
        slope_pct = (coeff / np.mean(y)) * 100
        if slope_pct > 0.5:
            return {"insight": f"PT price trending up (~{slope_pct:.2f}% slope over last {lookback_hours}h). Consider short-term strategies."}
        elif slope_pct < -0.5:
            return {"insight": f"PT price trending down (~{slope_pct:.2f}% slope over last {lookback_hours}h). Caution advised."}
        else:
            return {"insight": "PT price relatively stable over the selected period."}
    else:
        return {"insight": "Insufficient price data for PT to compute trend."}
