import warnings
warnings.filterwarnings("ignore")
import os
import pandas as pd
import numpy as np
from common.common import *
from src.lend_borrow import *

import schedule
import time
import datetime
from datetime import datetime, date, timedelta
import requests


#coinmarketcap api key
api_key = 'e9051a51-d06e-4e87-9e26-d0c72ae79d2a'
coinapi_key = '03a999a3-a01e-478c-bcd7-7b0b4a943b5c'

def get_coinmarketcap_data():
    url = f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?CMC_PRO_API_KEY={api_key}"
    parameters = {
        'start': '1',
        'limit': '2000',
        'convert': 'USD',
        'aux': 'volume_7d,volume_7d_reported,volume_30d,volume_30d_reported,max_supply,circulating_supply,total_supply,cmc_rank'      
    }
    response = requests.get(url,params=parameters).json()
    #print(pd.json_normalize(response['quote']))
    data = pd.DataFrame(pd.json_normalize(response['data']))[['name','symbol','quote.USD.price','max_supply','circulating_supply','total_supply','quote.USD.market_cap_dominance','quote.USD.market_cap','quote.USD.volume_24h','quote.USD.volume_change_24h','cmc_rank','quote.USD.tvl','tvl_ratio','quote.USD.volume_7d','quote.USD.volume_30d']]
    #data = data[~data['name'].isin(['UNI','MOO DENG (moodeng.vip)','Strike'])]
    return data.rename(columns={'quote.USD.price':'price_usd','quote.USD.market_cap':'mcap_usd','quote.USD.market_cap_dominance':'market_dominance','quote.USD.volume_24h':'volume_24h','quote.USD.volume_change_24h':'change_volume_24h','quote.USD.volume_7d':'volume_7d','quote.USD.volume_30d':'volume_30d','quote.USD.tvl':'tvl_usd'})

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

def get_coinbase_24h_volume_and_price(symbol):
    url = f"https://api.exchange.coinbase.com/products/{symbol}/stats"
    r = requests.get(url)
    data = r.json()
    try:
        vol = float(data["volume"])
        price = float(data["last"])
        return vol, price
    except Exception:
        return None, None

def get_kraken_24h_volume_and_price(symbol):
    url = f"https://api.kraken.com/0/public/Ticker?pair={symbol}"
    r = requests.get(url)
    data = r.json()
    try:
        ticker = list(data["result"].values())[0]
        vol = float(ticker["v"][1])
        price = float(ticker["c"][0])
        return vol, price
    except Exception:
        return None, None

def get_okx_24h_volume_and_price(symbol):
    url = f"https://www.okx.com/api/v5/market/ticker?instId={symbol}"
    r = requests.get(url)
    data = r.json()
    try:
        d = data["data"][0]
        vol = float(d["vol24h"])
        price = float(d["last"])
        return vol, price
    except Exception:
        return None, None

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

def compute_depth_and_mid(bids, asks, pct):
    if not bids or not asks:
        return 0, 0, 0
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = (best_bid + best_ask) / 2
    lower = mid * (1 - pct / 100)
    upper = mid * (1 + pct / 100)
    bid_vol = sum(q for p, q in bids if p >= lower)
    ask_vol = sum(q for p, q in asks if p <= upper)
    return bid_vol, ask_vol, mid


def token_stats():
    market_stats = get_coinmarketcap_data().fillna("")
    market_stats['volume_30d'] = market_stats['volume_30d']/30
    market_stats['volume_7d'] = market_stats['volume_7d']/7
    #update_excel('Risk Management Master','Token_List',market_stats)
    market_stats = market_stats[market_stats.cmc_rank <=50]
    
    top_token_list = list(market_stats[market_stats.cmc_rank <=14]['symbol'])
    # Elements to remove
    remove = {'USDT','USDC','BNB','XRP','HYPE','TRX'}
    # Remove elements using filter
    top_token_list = list(filter(lambda x: x not in remove, top_token_list))
    TOP_TOKENS = top_token_list

    prices = pd.concat([get_spot_prices(x,'USDT') for x in top_token_list],axis=0)
    coinprices = prices[prices.timestamp > datetime(2022, 1, 1, 0, 0, 0).timestamp()]
    coinprices['date'] = coinprices['timestamp'].apply( lambda x: unixtodate(x) )
    coinprices.set_index('date',inplace=True)
    
    #create a list of dataframes for the drawdowns
    DD = [drawdown(token,coinprices,'day','worst') for token in top_token_list] 
    drawdowns = pd.concat([DD[i][0] for i in range(len(DD))],axis=0).rename(columns={'index':'Percentile'})

    results = []

    for token in TOP_TOKENS:
        # Coinbase
        try:
            symbol = get_coinbase_symbol(token)
            vol_24h, price = get_coinbase_24h_volume_and_price(symbol)
            vol_24h_usd = vol_24h * price if vol_24h and price else None
            bids, asks = get_coinbase_orderbook(symbol)
            bid2, ask2, mid = compute_depth_and_mid(bids, asks, 2)
            bid2_usd = bid2 * mid if bid2 and mid else None
            ask2_usd = ask2 * mid if ask2 and mid else None
            results.append({
                "Token": token, "Exchange": "Coinbase",
                "24h_Volume_USD": vol_24h_usd,
                "Orderbook_2pct_Bid": bid2_usd, "Orderbook_2pct_Ask": ask2_usd
            })
        except Exception:
            results.append({
                "Token": token, "Exchange": "Coinbase",
                "24h_Volume_USD": None,
                "Orderbook_2pct_Bid": None, "Orderbook_2pct_Ask": None
            })
    
        # Kraken
        try:
            symbol = get_kraken_symbol(token)
            vol_24h, price = get_kraken_24h_volume_and_price(symbol)
            vol_24h_usd = vol_24h * price if vol_24h and price else None
            bids, asks = get_kraken_orderbook(symbol)
            bid2, ask2, mid = compute_depth_and_mid(bids, asks, 2)
            bid2_usd = bid2 * mid if bid2 and mid else None
            ask2_usd = ask2 * mid if ask2 and mid else None
            results.append({
                "Token": token, "Exchange": "Kraken",
                "24h_Volume_USD": vol_24h_usd,
                "Orderbook_2pct_Bid": bid2_usd, "Orderbook_2pct_Ask": ask2_usd
            })
        except Exception:
            results.append({
                "Token": token, "Exchange": "Kraken",
                "24h_Volume_USD": None,
                "Orderbook_2pct_Bid": None, "Orderbook_2pct_Ask": None
            })
    
        # OKX
        try:
            symbol = get_okx_symbol(token)
            vol_24h, price = get_okx_24h_volume_and_price(symbol)
            vol_24h_usd = vol_24h * price if vol_24h and price else None
            bids, asks = get_okx_orderbook(symbol)
            bid2, ask2, mid = compute_depth_and_mid(bids, asks, 2)
            bid2_usd = bid2 * mid if bid2 and mid else None
            ask2_usd = ask2 * mid if ask2 and mid else None
            results.append({
                "Token": token, "Exchange": "OKX",
                "24h_Volume_USD": vol_24h_usd,
                "Orderbook_2pct_Bid": bid2_usd, "Orderbook_2pct_Ask": ask2_usd
            })
        except Exception:
            results.append({
                "Token": token, "Exchange": "OKX",
                "24h_Volume_USD": None,
                "Orderbook_2pct_Bid": None, "Orderbook_2pct_Ask": None
            })

    df = pd.DataFrame(results)
    
    worst_moves = drawdowns[drawdowns.Percentile == 'Worst 99.9%'][['Token','1day','2day','7day','30day']].rename(columns={'Token':'symbol'})
    depth_2pct = df.pivot(index='Token', columns='Exchange', values='Orderbook_2pct_Bid').reset_index().rename(columns={'Token':'symbol'})
    volume = df.pivot(index='Token', columns='Exchange', values='24h_Volume_USD').reset_index().rename(columns={'Token':'symbol'})
    final_stats = pd.merge(market_stats,pd.merge(depth_2pct,volume,on='symbol',suffixes=('_depth2pct', '_volume_usd')),on='symbol')
    final = pd.merge(final_stats,worst_moves,on='symbol')
    update_excel('Risk Management Master','Token_Stats',final)

# Schedule the job to run every 5 seconds
schedule.every(1200).seconds.do(token_stats)

# Main loop to run pending scheduled jobs
while True:
    schedule.run_pending()
    time.sleep(120) # Sleep to avoid high CPU usage