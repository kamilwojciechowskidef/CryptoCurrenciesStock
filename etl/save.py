import os

def save_to_csv(df,filename="data/crypto_prices.csv"):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")