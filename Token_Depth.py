import requests
import pandas as pd

# Top 15 tokens by market cap
TOP_TOKENS = [
    "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "AVAX", "SHIB", "DOT",
    "TRX", "LINK", "MATIC", "LTC", "BCH"
]

def get_coinbase_symbol(token):
    return f"{token}-USD"

def get_kraken_symbol(token):
    if token == "BTC":
        return "XXBTZUSD"
    if token == "ETH":
        return "XETHZUSD"
    if token == "USDT":
        return "USDTZUSD"
    return f"{token}USD"

def get_okx_symbol(token):
    return f"{token}-USDT"

def get_coinbase_24h_volume(symbol):
    url = f"https://api.exchange.coinbase.com/products/{symbol}/stats"
    r = requests.get(url)
    data = r.json()
    try:
        return float(data["volume"])
    except Exception:
        return None

def get_kraken_24h_volume(symbol):
    url = f"https://api.kraken.com/0/public/Ticker?pair={symbol}"
    r = requests.get(url)
    data = r.json()
    try:
        return float(list(data["result"].values())[0]["v"][1])
    except Exception:
        return None

def get_okx_24h_volume(symbol):
    url = f"https://www.okx.com/api/v5/market/ticker?instId={symbol}"
    r = requests.get(url)
    data = r.json()
    try:
        return float(data["data"][0]["vol24h"])
    except Exception:
        return None

def get_coinbase_orderbook(symbol, level=2):
    url = f"https://api.exchange.coinbase.com/products/{symbol}/book?level={level}"
    r = requests.get(url)
    data = r.json()
    bids = [(float(p), float(q)) for p, q, _ in data["bids"]]
    asks = [(float(p), float(q)) for p, q, _ in data["asks"]]
    return bids, asks

def get_kraken_orderbook(symbol, count=100):
    url = f"https://api.kraken.com/0/public/Depth?pair={symbol}&count={count}"
    r = requests.get(url)
    data = r.json()
    try:
        ob = list(data["result"].values())[0]
        bids = [(float(p), float(q)) for p, q, _ in ob["bids"]]
        asks = [(float(p), float(q)) for p, q, _ in ob["asks"]]
        return bids, asks
    except Exception:
        return [], []

def get_okx_orderbook(symbol, size=100):
    url = f"https://www.okx.com/api/v5/market/books?instId={symbol}&sz={size}"
    r = requests.get(url)
    data = r.json()
    try:
        bids = [(float(p), float(q)) for p, q, *_ in data["data"][0]["bids"]]
        asks = [(float(p), float(q)) for p, q, *_ in data["data"][0]["asks"]]
        return bids, asks
    except Exception:
        return [], []

def compute_depth(bids, asks, pct):
    if not bids or not asks:
        return 0, 0
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = (best_bid + best_ask) / 2
    lower = mid * (1 - pct / 100)
    upper = mid * (1 + pct / 100)
    bid_vol = sum(q for p, q in bids if p >= lower)
    ask_vol = sum(q for p, q in asks if p <= upper)
    return bid_vol, ask_vol

results = []

for token in TOP_TOKENS:
    # Coinbase
    try:
        symbol = get_coinbase_symbol(token)
        vol_24h = get_coinbase_24h_volume(symbol)
        bids, asks = get_coinbase_orderbook(symbol)
        bid2, ask2 = compute_depth(bids, asks, 2)
        results.append({"Token": token, "Exchange": "Coinbase", "24h_Volume": vol_24h, "Orderbook_2pct_Bid": bid2, "Orderbook_2pct_Ask": ask2})
    except Exception:
        results.append({"Token": token, "Exchange": "Coinbase", "24h_Volume": None, "Orderbook_2pct_Bid": None, "Orderbook_2pct_Ask": None})

    # Kraken
    try:
        symbol = get_kraken_symbol(token)
        vol_24h = get_kraken_24h_volume(symbol)
        bids, asks = get_kraken_orderbook(symbol)
        bid2, ask2 = compute_depth(bids, asks, 2)
        results.append({"Token": token, "Exchange": "Kraken", "24h_Volume": vol_24h, "Orderbook_2pct_Bid": bid2, "Orderbook_2pct_Ask": ask2})
    except Exception:
        results.append({"Token": token, "Exchange": "Kraken", "24h_Volume": None, "Orderbook_2pct_Bid": None, "Orderbook_2pct_Ask": None})

    # OKX
    try:
        symbol = get_okx_symbol(token)
        vol_24h = get_okx_24h_volume(symbol)
        bids, asks = get_okx_orderbook(symbol)
        bid2, ask2 = compute_depth(bids, asks, 2)
        results.append({"Token": token, "Exchange": "OKX", "24h_Volume": vol_24h, "Orderbook_2pct_Bid": bid2, "Orderbook_2pct_Ask": ask2})
    except Exception:
        results.append({"Token": token, "Exchange": "OKX", "24h_Volume": None, "Orderbook_2pct_Bid": None, "Orderbook_2pct_Ask": None})

df = pd.DataFrame(results)
print(df.head())
