from sqlalchemy import create_engine, text
from app.config import settings

def main():
    engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        print("select 1 ->", conn.execute(text("select 1")).scalar())
        rows = conn.execute(text("""
            select table_name
            from information_schema.tables
            where table_schema = 'public'
            order by table_name
        """)).fetchall()
        print("tables:")
        for r in rows:
            print("-", r[0])

if __name__ == "__main__":
    main()
