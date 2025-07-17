import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
#from dotenv import load_dotenv
#load_dotenv()
api_key = os.environ['API_KEY']

import gspread
import gspread_dataframe
from gspread_dataframe import set_with_dataframe

from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

def unixtodate(x):
    dt = datetime.fromtimestamp(x).strftime('%m/%d/%Y')
    return dt

def get_date_format(dt):
    try:
        dt = datetime.strptime(dt, "%m/%d/%y")
    except:
        dt = datetime.strptime(dt, "%m/%d/%Y")
    return dt

def get_months(x,y):
    z = y - x
    return round(z.days/30,0)
    
def get_spot_prices(token,base):
    pair = token+'-'+base
    response = requests.get('https://data-api.coindesk.com/spot/v1/historical/days',
        params={"market":"binance","instrument": pair,"limit":2500,"aggregate":1,"fill":"true","apply_mapping":"true","response_format":"JSON","groups":"OHLC,VOLUME","api_key":api_key},
        headers={"Content-type":"application/json; charset=UTF-8"}
    )
    try:
        prices = pd.DataFrame(response.json()['Data'])[['TIMESTAMP','OPEN','HIGH','LOW','CLOSE','VOLUME']]
        prices['date'] = prices['TIMESTAMP'].apply( lambda x: unixtodate(x) )
        prices.columns = [x.lower() for x in prices.columns]
        prices['symbol'] = token
        return prices
    except:
        pass
        
def get_perp_prices(token):
    response = requests.get('https://data-api.coindesk.com/futures/v1/historical/days',
        params={"market":"binance","instrument": token,"limit":2500,"aggregate":1,"fill":"true","apply_mapping":"true","response_format":"JSON","groups":"OHLC,VOLUME","api_key":api_key},
        headers={"Content-type":"application/json; charset=UTF-8"}
    )
    try:
        prices = pd.DataFrame(response.json()['Data'])[['TIMESTAMP','OPEN','HIGH','LOW','CLOSE','VOLUME']]
        prices['date'] = prices['TIMESTAMP'].apply( lambda x: unixtodate(x) )
        prices.columns = [x.lower() for x in prices.columns]
        prices['symbol'] = token
        return prices
    except:
        pass
        
def convert_to_float(value):
    return float(value.replace(",","").replace("$",""))

def get_excel_data(file_name,sheet_name,data_range):
    sa = gspread.service_account(filename="riskmgmt-459617-50039f11332f.json")
    sheet = sa.open(file_name)
    work_sheet = sheet.worksheet(sheet_name)

    df = pd.DataFrame(work_sheet.get(data_range)).dropna()
    df.columns = df.iloc[0]
    df = df[1:]
    return df

def update_excel(file_name,sheet_name,df):
    
    sa = gspread.service_account(filename="credentials.json")
    #open the google spreadsheet 
    sh = sa.open(file_name)
    #select the first sheet 
    wks = sh.worksheet(sheet_name)
    wks.clear()
    set_with_dataframe(wks, df)

def get_latest_mark(data):
    latest = data.tail(1).T.reset_index()
    latest.columns = ['Token','MarkPrice']
    return latest

def upload_to_snowflake(df,table_name,append):

    df = df.reset_index(drop=True)
    # Snowflake connection parameters
    conn = connect(
        user='sasinagalla442@bitgo.com',
        account='YIA63742',
        warehouse='QUERY_WAREHOUSE_XS',
        database='RISK_MANAGEMENT',
        schema='SANDBOX',
        role='RISK_MGMT',  # optional
        authenticator='externalbrowser'  # <-- this triggers browser-based SSO
    )
    if append =='True':
        # Write the DataFrame into a Snowflake table
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            auto_create_table=True,  # creates table if it doesn't exist
            overwrite=False,  #appends the table
            use_logical_type=True,
        )
    else:
        # Write the DataFrame into a Snowflake table
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            auto_create_table=True,  # creates table if it doesn't exist
            overwrite=True,  # drops and recreates the table
            use_logical_type=True
        )

    #write_pandas(conn, final, "LOAN_STATS", schema='SANDBOX', database='RISK_MANAGEMENT', auto_create_table=True, overwrite=True, use_logical_type=True)
    print(f"Write successful for table {table_name}: {success}, rows written: {nrows}")
    
def get_token_data(currency_list):
    #chose a base pair
    base = 'USDT'
    #pairs = [x+'-'+base for x in currency_list]
    prices = pd.concat([get_spot_prices(x,base) for x in currency_list],axis=0)
    
    #close_prices.to_csv('data/token_mark_prices.csv')
    close_prices = prices.pivot(index='timestamp',columns='symbol',values='close').reset_index()
    close_prices.dropna(inplace=True)
    close_prices['USD'] = 1
    close_prices['USDC'] = 1
    #close_prices.to_excel(file_path)
    
    #2022-H2 Prices
    starttime = datetime(2022, 10, 15, 0, 0, 0).timestamp()
    endtime = datetime(2022, 12, 15, 0, 0, 0).timestamp()
    price_2022 = close_prices[(close_prices.timestamp > starttime) & (close_prices.timestamp < endtime)]
    price_2022['timestamp'] = price_2022['timestamp'].apply( lambda x: unixtodate(x) )
    price_2022.set_index('timestamp',inplace=True)
    #price_2022 = (1+price_2022.pct_change().fillna(0)).cumprod()

    #2022-Q2 Prices
    starttime = datetime(2022, 4, 1, 0, 0, 0).timestamp()
    endtime = datetime(2022, 7, 1, 0, 0, 0).timestamp()
    price_luna = close_prices[(close_prices.timestamp > starttime) & (close_prices.timestamp < endtime)]
    price_luna['timestamp'] = price_luna['timestamp'].apply( lambda x: unixtodate(x) )
    price_luna.set_index('timestamp',inplace=True)
    #price_2022 = (1+price_2022.pct_change().fillna(0)).cumprod()

    #2025-H1 Prices
    price_2025 = close_prices[close_prices.timestamp > datetime(2025, 1, 1, 0, 0, 0).timestamp()]
    price_2025['timestamp'] = price_2025['timestamp'].apply( lambda x: unixtodate(x) )
    price_2025.set_index('timestamp',inplace=True)
    #price_2025 = (1+price_2025.pct_change().fillna(0)).cumprod()
    
    close_prices['timestamp'] = close_prices['timestamp'].apply( lambda x: unixtodate(x) )
    close_prices.set_index('timestamp',inplace=True)
    #price_2022 = pd.concat([close_prices.tail(1)[x][0]*price_2022[x]  for x in close_prices.tail(1).columns], axis=1)
    #price_2025 = pd.concat([close_prices.tail(1)[x][0]*price_2025[x]  for x in close_prices.tail(1).columns], axis=1)
    
    return close_prices, price_2022, price_2025, price_luna

def get_scenario_return(prices,latest):
    returns = (prices.pct_change()+1).cumprod().dropna()
    scen_ret = np.matmul(returns,np.diag(latest.MarkPrice).T)
    scen_ret.columns = prices.columns
    return scen_ret
  
def drawdown(token,coinprices,freq='day',move='worst'):

    #remove the first day of prices
    coindata = coinprices[coinprices.symbol==token][1:]#.set_index('date')#[['high','low','close','volume']]
    start = coindata.index[0]
    end = coindata.index[-1]

    if freq == 'day':
        hours_list = [1,2,7,30]
    else:
        hours_list = [1,2,4,8,24,48]
        
    if move == 'worst':
        zmax = pd.DataFrame(columns = hours_list, index = ['Worst 100%','Worst 99.95%','Worst 99.9%','Worst 99.5%','Worst 99%'])
    else:
        zmax = pd.DataFrame(columns = hours_list, index = ['Best 100%','Best 99.95%','Best 99.9%','Best 99.5%','Best 99%'])
    zmaxN = pd.DataFrame(columns = zmax.columns, index = zmax.index)
    
    cur = []
    curN = []
    k=0
    for j in [0,0.0005,0.001,0.005,0.01]:  
        for i in hours_list:
            if move == 'worst':
                Roll_Max = coindata.high.rolling(window = i+1).max()
                zmax_roll = round(100*(coindata.low/Roll_Max - 1.0),2)
                #zmax_roll = coindata.low/(coindata.high.rolling(window = i, min_periods=1).max()) - 1.0
                #print(zmax_roll)
                q = round(zmax_roll.quantile(j),1)
                cur.append(q)
                curN.append(zmax_roll[zmax_roll <= q].count())
            else:
                Roll_Min = coindata.low.rolling(window = i+1).min()
                zmax_roll = round(100*(coindata.high/Roll_Min - 1.0),2)
                #zmax_roll = coindata.low/(coindata.high.rolling(window = i, min_periods=1).max()) - 1.0
                #print(zmax_roll)
                q = round(zmax_roll.quantile(1-j),1)
                cur.append(q)
                curN.append(zmax_roll[zmax_roll >= q].count())
        
        zmax.iloc[k] = cur
        zmaxN.iloc[k] = curN
        k = k+1
        cur = []
        curN = []

    final = zmax
    finalN = zmaxN
    final.columns = [str(i)+'_day' for i in hours_list]
    final.reset_index().rename(columns={'index':'percentile'}).set_index('percentile')
    if freq == 'hour':
        final.columns = [str(x) + "hour" for x in hours_list]
        print(token + f': {move} Hourly Price Drawdown (%)')
        print(start + ' to ' + end)
    else:
        final.columns = [str(x) + "day" for x in hours_list]
        print(token+ f': {move} Daily Price Drawdown (%)')
        print(start + ' to ' + end)
    print('')
    print(final)
    print('')
    final['Token'] = token
    final['Freq'] = freq
    final['Move'] = move
    return [final.reset_index(),finalN]
