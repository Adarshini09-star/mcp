from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime

DATABASE_URL = "sqlite:///./pendle_history.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"
    id = Column(Integer, primary_key=True, index=True)
    market_id = Column(String, index=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, index=True)
    raw_json = Column(Text)  # full JSON payload as string
    # optional extracted fields
    pt_price = Column(Float, nullable=True)
    sy_price = Column(Float, nullable=True)
    tvl = Column(Float, nullable=True)

def create_db():
    Base.metadata.create_all(bind=engine)

def save_snapshot(session, market_id, raw_json, pt_price=None, sy_price=None, tvl=None):
    snap = MarketSnapshot(
        market_id=market_id,
        raw_json=raw_json,
        pt_price=pt_price,
        sy_price=sy_price,
        tvl=tvl
    )
    session.add(snap)
    session.commit()
    session.refresh(snap)
    return snap
