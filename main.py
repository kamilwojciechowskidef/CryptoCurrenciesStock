from etl.fetch_data import fetch_data
from etl.transform_data import transform_crypto_data
from db.db import save_to_db,init_table

def run_etl():
    init_table()
    raw_data = fetch_data()
    df = transform_crypto_data(raw_data)
    save_to_db(df)

if __name__ == "__main__":
    run_etl()