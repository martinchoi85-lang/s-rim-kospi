from sqlalchemy import create_engine, text
from app.config import settings

def main():
    engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        # 가장 최근 스냅샷
        sid = conn.execute(text("select snapshot_id from snapshots order by created_at desc limit 1")).scalar()
        print("latest snapshot_id =", sid)

        cnt = conn.execute(text("select count(*) from market_snapshot where snapshot_id = :sid"), {"sid": sid}).scalar()
        print("market_snapshot rows =", cnt)

        sample = conn.execute(text("""
            select ms.ticker, t.name, ms.close_price, ms.market_cap, ms.shares_out
            from market_snapshot ms
            join tickers t on t.ticker = ms.ticker
            where ms.snapshot_id = :sid
            order by ms.market_cap desc nulls last
            limit 5
        """), {"sid": sid}).fetchall()

        print("top 5 by market cap:")
        for r in sample:
            print(r)

if __name__ == "__main__":
    main()
