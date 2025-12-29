from sqlalchemy.orm import Session
from datetime import date
from ..models import Ticker, Snapshot, MarketSnapshot, FundamentalSnapshot, DiscountRateSnapshot, SRimResult

def upsert_snapshot(db: Session, snapshot_id: str, as_of: date, note: str | None = None):
    obj = db.get(Snapshot, snapshot_id)
    if obj is None:
        obj = Snapshot(snapshot_id=snapshot_id, as_of_date=as_of, note=note)
        db.add(obj)
    else:
        obj.as_of_date = as_of
        obj.note = note
    db.commit()

def upsert_discount_rate(db: Session, snapshot_id: str, as_of: date, rate: float, source: str = "manual"):
    obj = db.get(DiscountRateSnapshot, snapshot_id)
    if obj is None:
        obj = DiscountRateSnapshot(snapshot_id=snapshot_id, as_of_date=as_of, rate=rate, source=source)
        db.add(obj)
    else:
        obj.as_of_date = as_of
        obj.rate = rate
        obj.source = source
    db.commit()
