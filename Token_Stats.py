import warnings
warnings.filterwarnings("ignore")
import os
import pandas as pd
import numpy as np
from common.common import *

import datetime
from datetime import datetime, date, timedelta
import schedule
import time

#coinmarketcap api key
api_key = os.environ['API_KEY']

def get_coinmarketcap_data():
    url = f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?CMC_PRO_API_KEY={api_key}"
    parameters = {
        'start': '1',
        'limit': '4000',
        'convert': 'USD',
        'aux': 'volume_7d,volume_7d_reported,volume_30d,volume_30d_reported,max_supply,circulating_supply,total_supply,cmc_rank'      
    }
    response = requests.get(url,params=parameters).json()
    #print(pd.json_normalize(response['quote']))
    data = pd.DataFrame(pd.json_normalize(response['data']))[['name','symbol','quote.USD.price','max_supply','circulating_supply','total_supply','quote.USD.market_cap_dominance','quote.USD.market_cap','quote.USD.volume_24h','quote.USD.volume_change_24h','cmc_rank','quote.USD.tvl','tvl_ratio','quote.USD.volume_7d','quote.USD.volume_30d']]
    #data = data[~data['name'].isin(['UNI','MOO DENG (moodeng.vip)','Strike'])]
    return data.rename(columns={'quote.USD.price':'price_usd','quote.USD.market_cap':'mcap_usd','quote.USD.market_cap_dominance':'market_dominance','quote.USD.volume_24h':'volume_24h','quote.USD.volume_change_24h':'change_volume_24h','quote.USD.volume_7d':'volume_7d','quote.USD.volume_30d':'volume_30d','quote.USD.tvl':'tvl_usd'})
    
def token_stats():
    market_stats = get_coinmarketcap_data().fillna("")
    market_stats['volume_30d'] = market_stats['volume_30d']/30
    market_stats['volume_7d'] = market_stats['volume_7d']/7
    update_excel('Risk Management Master','Token_List',market_stats)
    print('updated at: ' + str(datetime.now())[:-7])

if __name__ == "__main__":
    token_stats()
