import requests

def fetch_data():
    url="https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "ids": "bitcoin,wrapped bitcoin,cmeth,oseth,cgeth,ethereum,bnb,solana,xrp,leo token,ethena usde,jito,ethena,sats (ordinals),dogecoin,arbitrum,wormhole,optimism,toncoin,tron", "order": "market_cap_desc"}
    response = requests.get(url, params=params)
    return response.json()