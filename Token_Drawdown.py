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

tokenlist = ['BTC','ETH','XRP','SOL','ENA']
prices = pd.concat([get_spot_prices(x,'USDT') for x in tokenlist],axis=0)
coinprices = prices[prices.timestamp > datetime(2022, 1, 1, 0, 0, 0).timestamp()]
coinprices['date'] = coinprices['timestamp'].apply( lambda x: unixtodate(x) )
coinprices.set_index('date',inplace=True)

DD = [drawdown(token,coinprices,'day','worst') for token in tokenlist] 

#add new columns to marketdata
marketdata = pd.DataFrame()
marketdata['coin'] = tokenlist
marketdata['30-day vol (%)'] = 0
marketdata['start date'] = 0
marketdata['end date'] = 0
marketdata['days'] = 0
marketdata['ndays of <1day drawdown'] = 0

for i in range(len(tokenlist)):
    pricehistory = coinprices[coinprices.symbol==tokenlist[i]]
    pricehistory['close'] = pricehistory['close'].astype(float)
    marketdata['30-day vol (%)'][i] = '{:.1f}%'.format(100*np.sqrt(365)*np.std(pricehistory.close.pct_change().tail(30)))
    marketdata['start date'][i] = pricehistory.index[0]
    marketdata['end date'][i] = pricehistory.index[-1]
    marketdata['days'][i] = len(pricehistory)-1
    marketdata['ndays of <1day drawdown'][i] = DD[i][1].iloc[4,1]

print('')
marketdata.set_index('coin')

worst = "Worst 99.9%"
drawdowns = pd.concat([DD[i][0] for i in range(len(DD))],axis=0).rename(columns={'index':'Percentile'})
list_tokens = list(drawdowns['Token'].unique())

worst = drawdowns[drawdowns.Percentile == worst].drop(columns={'Percentile','Token','Freq','Move'}).melt()
worst.columns = ['day','moves']
worst['token'] = list_tokens * int(len(worst)/len(list_tokens))

fig = px.bar(worst, x='day', y='moves', color='token', barmode='group',text_auto='.2s')
fig.update_layout(template = 'plotly_white')
fig.update_layout(title = f"Daily Worst 99.9% Drawdown by Token (starting Jan 2022)",xaxis_title='',yaxis_title='moves(%)')
fig.update_traces(marker_line_width=1, opacity=0.8)
#fig.update_layout(yaxis_range=[1.2*min(worst['moves']),0])
fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
fig.show()

upload_to_snowflake(worst,'WORST_DRAWDOWNS','False')