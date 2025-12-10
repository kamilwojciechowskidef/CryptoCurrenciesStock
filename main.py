from etl.fetch_data import fetch_data
from db.db import init_table
from etl.save import save_to_csv
import pandas as pd

def run_etl():
    init_table()
    raw_data = fetch_data()
    print(f"Raw data fetched. Length: {(raw_data)}")

if __name__ == "__main__":
    run_etl()