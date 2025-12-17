import threading
from datetime import datetime

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()
engine = create_engine("sqlite:///ticks_ws.db", echo=False)
Session = sessionmaker(bind=engine)
_db_lock = threading.Lock()


class LiveTick(Base):
    __tablename__ = "live_ticks"

    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True)
    ts = Column(DateTime, index=True)
    price = Column(Float)


def init_storage():
    Base.metadata.create_all(engine)


def write_tick(symbol: str, price: float):
    with _db_lock:
        session = Session()
        session.add(
            LiveTick(
                symbol=symbol,
                ts=datetime.utcnow(),
                price=price,
            )
        )
        session.commit()
        session.close()


def read_ticks(symbols, limit=500):
    session = Session()
    rows = (
        session.query(LiveTick)
        .filter(LiveTick.symbol.in_(symbols))
        .order_by(LiveTick.ts.desc())
        .limit(limit)
        .all()
    )
    session.close()
    return rows
