# Store latest funding data
import warnings
warnings.filterwarnings("ignore")

import gspread
import pandas as pd
import numpy as np
from common.common import *

import os
from dotenv import load_dotenv
load_dotenv()
api_key = os.getenv('api_key')

import yfinance as yf
import datetime
from datetime import datetime
import requests

import asyncio
import websockets
import json
from rich.console import Console
from rich.table import Table
from rich_tools import table_to_df
from datetime import datetime

console = Console()

# List of symbols to track
symbols = ['btcusdt', 'ethusdt', 'solusdt', 'xrpusdt']

# Build the WebSocket URLs for each symbol
urls = [f"wss://fstream.binance.com/ws/{symbol}@markPrice" for symbol in symbols]

funding_data = {}

def rich_table_to_dataframe(table) -> pd.DataFrame:
    headers = [column.header for column in table.columns]
    data = []
    for row in table.rows:
        data.append(row)
        
    df = pd.DataFrame(data, columns=headers)
    print(df)
    return df
    
def convert_timestamp(ms):
    return datetime.utcfromtimestamp(ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

def display_table():
    table = Table(title="🔄 Live Funding Rates (Binance)", expand=True)
    table.add_column("Symbol", justify="left", style="cyan")
    table.add_column("MarkPrice", justify="right", style="cyan")
    table.add_column("IndexPrice", justify="right", style="cyan")
    table.add_column("Funding Rate", justify="right", style="magenta")
    table.add_column("Annualized", justify="right", style="green")
    table.add_column("Timestamp", justify="right", style="yellow")

    for symbol, data in funding_data.items():
        mark = str(data["p"])
        index = str(data["i"])
        rate = float(data["r"])
        yearly = f"{rate * 3 * 365 * 100:.2f}%"
        timestamp = convert_timestamp(data["E"])
        table.add_row(symbol.upper(),mark,index,f"{rate * 100:.4f}%", yearly, timestamp)
        
    if len(table_to_df(table)) > 0:
        upload_to_snowflake(table_to_df(table),'FUNDING_RATES','True')
    console.clear()
    console.print(table)

async def handle_stream(url):
    async with websockets.connect(url) as ws:
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            symbol = data["s"].lower()
            funding_data[symbol] = data

async def main():
    tasks = [handle_stream(url) for url in urls]

    # Launch display task separately
    async def refresh_display():
        while True:
            display_table()
            await asyncio.sleep(60)

    await asyncio.gather(*tasks, refresh_display())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\\n[red]Stopped by user.[/red]")