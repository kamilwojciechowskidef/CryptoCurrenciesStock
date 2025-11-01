import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# wczytaj .env jawnie i po UTF-8
load_dotenv(dotenv_path="etl/.env", encoding="utf-8")

# ZBUDUJ URL BEZPIECZNIE
url = URL.create(
    drivername="postgresql+psycopg",
    username=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=int(os.getenv("POSTGRES_PORT")),
    database=os.getenv("POSTGRES_DB"),
)

# kontrolnie – sprawdź typ i ewentualne nie-ASCII (powinno być pusto)
dsn = str(url)
print("TYPE:", type(dsn).__name__)
print("NON-ASCII:", [(i, ch, hex(ord(ch))) for i, ch in enumerate(dsn) if ord(ch) > 127])

engine = create_engine(url, pool_pre_ping=True, future=True)

with engine.begin() as conn:
    print(conn.execute(text("SELECT version()")).scalar())
