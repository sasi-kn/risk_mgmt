import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np

from dotenv import load_dotenv
load_dotenv()
api_key = os.getenv('api_key')

import datetime
from datetime import datetime, date, timedelta
import requests

tokens = ['BTC','SOL','ETH','SUI','XRP']
base = 'USD'
pairs = [token+'-'+ base for token in tokens] 

def get_slippage(pair,exchange='coinbase'):
    response = requests.get('https://data-api.coindesk.com/spot/v1/historical/orderbook/l2/metrics/minute',
        params={"market":f"{exchange}","instrument":f"{pair}","depth_percentage_levels":"2,5","slippage_size_limits":"1000000","limit":1,"apply_mapping":"true","response_format":"JSON","groups":"ID,MAPPING,DEPTH_BEST_PRICE,SLIPPAGE_BEST_PRICE,TOP_OF_BOOK","slippage_calculation_asset":"USD","api_key":f"{api_key}"},
        headers={"Content-type":"application/json; charset=UTF-8"}
    )
    
    try:
        df = pd.DataFrame(response.json()['Data'])
        df['date'] = prices['TIMESTAMP'].apply( lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M') )
        df.columns = [x.lower() for x in df.columns]
        
        # Drop columns containing 'Ask' in their name and other redundant ones
        df.drop(columns={'unit','timestamp','ccseq','depth_asset','slippage_asset','instrument'},inplace=True)
        cols_to_drop = df.filter(like='_ask', axis=1).columns
        df_cleaned = df.drop(columns=cols_to_drop)[['mapped_instrument','depth_best_price_bid_2_percent','depth_best_price_bid_5_percent','slippage_best_price_max_bid_1000000']]
        df_cleaned.rename(columns={'mapped_instrument':'symbol','depth_best_price_bid_2_percent':exchange+'_depth_bid_2pct','depth_best_price_bid_5_percent':exchange+'_depth_bid_5pct','slippage_best_price_max_bid_1000000':exchange+'_slippage_max_bid_1mil'},inplace=True)
        return df_cleaned
    except: 
        pass

def get_exchange_slippage(exchange):
    return pd.concat([get_slippage(pair,exchange) for pair in pairs], axis=0)

def get_volume(pair,exchange='coinbase'):

    response = requests.get('https://data-api.coindesk.com/spot/v1/latest/tick',
        params={"market":f"{exchange}","instruments":f"{pair}","apply_mapping":"true","groups":"CURRENT_DAY,MOVING_24_HOUR,CURRENT_WEEK,MOVING_7_DAY,CURRENT_MONTH,MOVING_30_DAY,MOVING_90_DAY,VALUE,MAPPING","api_key":f"{api_key}"},
        headers={"Content-type":"application/json; charset=UTF-8"}
    )
    
    try:
        df = pd.DataFrame(response.json()['Data']).T
        df.columns = [x.lower() for x in df.columns]
        df['date'] = df['price_last_update_ts'].apply( lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M') )
        # Drop columns containing 'Ask' in their name and other redundant ones
        df.drop(columns={'ccseq','price_flag','price_last_update_ts','price_last_update_ts_ns','base','quote'},inplace=True)
        cols_to_drop = df.filter(like='_quote', axis=1).columns
        df_cleaned = df.drop(columns=cols_to_drop)[['date','mapped_instrument','current_day_volume','current_week_volume','current_month_volume']]
        df_cleaned.rename(columns={'mapped_instrument':'symbol','current_day_volume':exchange+'_volume_24h','current_week_volume':exchange+'_volume_7d','current_month_volume':exchange+'_volume_30d'},inplace=True)
        return df_cleaned
    except: 
        pass

def get_exchange_volume(exchange):
    return pd.concat([get_volume(pair,exchange) for pair in pairs], axis=0).reset_index(drop=True)

volume_df = pd.merge(get_exchange_volume('coinbase'),get_exchange_volume('kraken'),on=['date','symbol'],how='left')
slippage_df = pd.merge(get_exchange_slippage('coinbase'),get_exchange_slippage('kraken'),on='symbol',how='left')

final_df = pd.merge(volume_df,slippage_df,on='symbol',how='left')
final_df.head(2).T