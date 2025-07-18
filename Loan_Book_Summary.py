import warnings
warnings.filterwarnings("ignore")
import os
import pandas as pd
import numpy as np
from common.common import *
from common.lend_borrow import *

import datetime
from datetime import datetime

def get_active_book(data):
    dt = datetime.now().strftime('%Y-%m-%d')
    dt_format = datetime.now().strftime('%m/%d')
    active = data.copy()
    #active['Jan_Active'] = np.where(active['Origination Date'] < '2025-02-01', 1, 0)
    active['Jan_Active'] = np.where(active['Origination Date'] < '2025-02-01', active['End Date'].apply( lambda x: '1/31' if x== "Open" else ( '1/31' if x > datetime.strptime('2025-02-01', "%Y-%m-%d") else 0)),0)
    active['Feb_Active'] = np.where(active['Origination Date'] < '2025-03-01', active['End Date'].apply( lambda x: '2/28' if x== "Open" else ( '2/28' if x > datetime.strptime('2025-03-01', "%Y-%m-%d") else 0)),0)
    active['Mar_Active'] = np.where(active['Origination Date'] < '2025-04-01', active['End Date'].apply( lambda x: '3/31' if x== "Open" else ( '3/31' if x > datetime.strptime('2025-04-01', "%Y-%m-%d") else 0)),0)
    active['Apr_Active'] = np.where(active['Origination Date'] < '2025-05-01', active['End Date'].apply( lambda x: '4/30' if x== "Open" else ( '4/30' if x > datetime.strptime('2025-05-01', "%Y-%m-%d") else 0)),0)
    active['May_Active'] = np.where(active['Origination Date'] < '2025-06-01', active['End Date'].apply( lambda x: '5/31' if x== "Open" else ( '5/31' if x > datetime.strptime('2025-06-01', "%Y-%m-%d") else 0)),0)
    active['Jun_Active'] = np.where(active['Origination Date'] < '2025-07-01', active['End Date'].apply( lambda x: '6/30' if x== "Open" else ( '6/30' if x > datetime.strptime('2025-07-01', "%Y-%m-%d") else 0)),0)
    active['Jul_Active'] = np.where(active['Origination Date'] <= dt, active['End Date'].apply( lambda x: dt_format if x== "Open" else ( dt_format if x >= datetime.strptime(dt, "%Y-%m-%d") else 0)),0)
    return active[['Type','Side','Desk','Currency','WithinEntity','Counterparty','Trade #','LoanExposure','Rate','Jan_Active','Feb_Active','Mar_Active','Apr_Active','May_Active','Jun_Active','Jul_Active']]

def get_grouped(flag,df):
    data = df[df[flag]!=0]
    grouped = data.groupby(['Type','Side','Desk','WithinEntity','Counterparty','Trade #','Currency'])[['LoanExposure','Rate']].sum().reset_index()
    grouped['Flag'] = data[flag].unique()[0]
    return grouped
    
def get_loan_summary():

    #loantype = 'Secured'
    #side = 'lend'
    lend_final = get_final_book('lend','Secured','Yes')
    borrow_final = get_final_book('borrow','Secured','Yes')
    unsecured_final = get_final_book('lend','Unsecured','Yes')
    lend_final['Side'] = 'Lend'
    lend_final['Type'] = 'Secured'
    borrow_final['Side'] = 'Borrow'
    borrow_final['Type'] = 'Secured'
    unsecured_final['Type'] = 'Unsecured'
    
    active = pd.concat([get_active_book(lend_final),get_active_book(borrow_final),get_active_book(unsecured_final)],axis=0)
    flags =['Jan_Active','Feb_Active','Mar_Active','Apr_Active','May_Active','Jun_Active','Jul_Active']
    history = pd.concat([get_grouped(x,active) for x in flags],axis=0)[['Type','Side','Desk','WithinEntity','Counterparty','Trade #','LoanExposure','Rate','Flag','Currency']]
    update_excel('Risk Management Master','Lend_Borrow_History',history)
    print('updated lend borrow history sheet at: ' + str(datetime.now())[:-7])

if __name__ == "__main__":
    get_loan_summary()
