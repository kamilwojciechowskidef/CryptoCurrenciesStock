import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def fetch_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": "bitcoin,wrapped bitcoin,cmeth,oseth,cgeth,ethereum,bnb,solana,xrp,leo token,ethena usde,jito,ethena,sats (ordinals),dogecoin,arbitrum,wormhole,optimism,toncoin,tron",
        "order": "market_cap_desc"
    }
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    r = s.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()