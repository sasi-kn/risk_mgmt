import warnings
warnings.filterwarnings("ignore")
import os
import pandas as pd
import numpy as np
from common.common import *

import datetime
from datetime import datetime

#import data
hk_otc = get_excel_data('Risk Management Master','HK CCY Positions','C5:J200')
hk_otc['TotalNet'] = hk_otc['TotalNet'].apply(convert_to_float)
hk_otc = hk_otc[(hk_otc.Asset != "" ) & (hk_otc.TotalNet!=0)]
hk_otc['desk'] = 'HK'

prime_otc = get_excel_data('Risk Management Master','Prime CCY Positions','C5:J200')
prime_otc['TotalNet'] = prime_otc['TotalNet'].apply(convert_to_float)
prime_otc = prime_otc[(prime_otc.Asset != "" ) & (prime_otc.TotalNet!=0)]
prime_otc['desk'] = 'Prime'

final_otc = pd.DataFrame(pd.concat([hk_otc,prime_otc],axis=0))
final_otc['date'] = datetime.now().strftime("%Y-%m-%d")
final_otc.reset_index(inplace=True,drop=True)

final_otc['Trade Notional']  = final_otc['Trade Notional'].apply(convert_to_float)
final_otc['Price']  = final_otc['Price'].apply(convert_to_float)
final_otc['PnL']  = final_otc['PnL'].apply(convert_to_float)
final_otc['Units Traded']  = final_otc['Units Traded'].apply(convert_to_float)
final_otc['NetBaseQuantity']  = final_otc['NetBaseQuantity'].apply(convert_to_float)
final_otc['NetQuoteQuantity']  = final_otc['NetQuoteQuantity'].apply(convert_to_float)

upload_to_snowflake(final_otc,'TRADE_BLOTTER_SUMMARY','True')