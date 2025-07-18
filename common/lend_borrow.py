import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
#from dotenv import load_dotenv
#load_dotenv()
api_key = os.environ['API_KEY']
from common.common import *

#import data
prime_lending = get_excel_data('Risk Management Master','Prime Lending','A1:BP1000')
prime_borrows = get_excel_data('Risk Management Master','Prime Borrows','A1:BP1000')
hk_lending = get_excel_data('Risk Management Master','HK Lending','A1:AG1000')
hk_lending_flows = get_excel_data('Risk Management Master','HK Lending Flows','A1:L1000')
hk_lending_flows['Qty (Units)'] = hk_lending_flows['Qty (Units)'].apply(convert_to_float)
haircut = get_excel_data('Risk Management Master','Haircut','A1:E100')

def get_final_unsecured(data):
    desk = data['Desk'].unique()[0]
    if desk == 'Prime':
        data['LoanExposure'] = data['Outstanding'].apply(convert_to_float)*data['TradePrice'].astype(float)
        data['Amount'] = 0
        #data['Amount USD'] = data['Amount USD'].apply(convert_to_float)
        data['Amount USD'] = 0
        data['CurrentValue'] = data['CurrentValue'].apply(convert_to_float)
        data['Margin Required'] = data['Margin Required'].apply(convert_to_float)
        data['Collateral Required USD'] = data['Collateral Required USD'].apply(convert_to_float)
        #data['Current Net Exposure'] = data['Current Net Exposure'].apply(convert_to_float)
        data['Difference USD'] = data['Difference USD'].apply(convert_to_float)
        data['Liquidation Level'] = 0
        data['Initial Margin'] = 0
        data['Margin Call Level'] = 0
        data['Current Collateral'] = 0
        data['Margin Refund'] = 0
        data['Liquidation Threshold'] = 0
        data['Rate'] = data['Rate'].apply(lambda x: float(x.replace("%",""))/100)
    else:
        data['Trade #'] = data['Trade #'].apply(lambda x: "Loan#"+" "+str(x))
        data['LoanExposure'] = data['CurrentValue'].apply(convert_to_float)
        data['Amount'] = 0
        data['Amount USD'] = 0
        data['CurrentValue'] = data['CurrentValue'].apply(convert_to_float)
        #data['Current Net Exposure'] = data['CurrentValue'] - data['Amount USD']
        data['Liquidation Level'] = 0
        data['Initial Margin'] = 0
        data['Margin Call Level'] = 0
        data['Current Collateral'] = 0
        data['Margin Refund'] = 0
        data['Liquidation Threshold'] = 0   
        data['Margin Required'] = 0
        data['Collateral Required USD'] = 0
        data['Difference USD'] = 0
        data['Current Collateral'] = 0
        data['Rate'] = data['Rate'].apply(lambda x: float(x.replace("%",""))/100)
    
    data['MarkPrice'] = 0
    final = pd.merge(data, haircut[['Locked','Token','Haircut','LiquidityScore','StressHaircut']], left_on=['Collateral','Locked'], right_on=['Token','Locked'],how='left').fillna(0)
    #final['LiquidityScore'] = final['LiquidityScore'].astype(float)
    final['Haircut'] = final['Haircut'].astype(float)/100
    final['StressHaircut'] = final['StressHaircut'].astype(float)/100
    final['Adj Amount USD'] = final['Amount USD']*(1-final['Haircut'])
    final['Stress Amount USD'] = final['Amount USD']*(1-final['StressHaircut'])
    final['Current Net Exposure'] = final['CurrentValue'] - final['Adj Amount USD']
    final['Stress Net Exposure'] = final['CurrentValue'] - final['Stress Amount USD']
    #final['AdjCollateral over MarginCall'] = final['Adj Amount USD'] - final['Margin Required']
    #final['AdjCollateral over Liquidation'] = final['Adj Amount USD'] - final['Liquidation Threshold']
    x = final['Current Collateral'] - final['Margin Call Level']
    y = final['Initial Margin'] - final['Margin Call Level']
    final['MarginBuffer'] = np.minimum(x, y)
    #final['MarginBuffer'] = final['Current Collateral'] - final['Margin Call Level']
    x = final['Current Collateral'] - final['Margin Refund']
    y = final['Current Collateral'] - final['Initial Margin']
    final['ExcessMarginRefund'] = np.maximum(np.minimum(x,y),0)
    final['ExcessRefundAmount'] = final['ExcessMarginRefund']*final['Amount']/100
    final['ExcessRefundUSD'] = final['ExcessMarginRefund']*final['Amount USD']/100
    final['ExcessMargin'] = final['Current Collateral'] - final['Margin Call Level']
    final['ExcessMargin'] = final['ExcessMargin'].apply(lambda x: max(x,0))
    final['Adj CurrentLTV'] = round(100*final['Adj Amount USD']/data['LoanExposure'],1)
    final['Stress CurrentLTV'] = round(100*final['Stress Amount USD']/data['LoanExposure'],1)
    final['AdjExcessMargin'] = final['Adj CurrentLTV'] - final['Margin Call Level']
    final['AdjExcessMargin'] = final['AdjExcessMargin'].apply(lambda x: max(x,0))
    final['StressExcessMargin'] = final['Stress CurrentLTV'] - final['Margin Call Level']
    final['StressExcessMargin'] = final['StressExcessMargin'].apply(lambda x: max(x,0))
    final['End Date'] = final['End Date'].apply( lambda x: "Open" if x == "Open" else ( "Open" if x=="" else np.datetime64(get_format(x)) ) )
    final.replace([np.inf, -np.inf], np.nan, inplace=True)
    #print(final.head())
    #final.dropna(inplace=True)
    return final

def get_format(x):
    try:
        return datetime.strptime(x, "%m/%d/%y")
    except:
        return datetime.strptime(x, "%m/%d/%Y")
 
def get_final_secured(data):
    desk = data['Desk'].unique()[0]
    if desk == 'Prime':
        data['LoanExposure'] = data['Outstanding'].apply(convert_to_float)*data['TradePrice'].astype(float)
        data['Amount'] = data['Amount'].apply(convert_to_float)
        #data['Amount USD'] = data['Amount USD'].apply(convert_to_float)
        data['Amount USD'] = data['Amount'].astype(float)*data['MarkPrice'].astype(float)
        data['CurrentValue'] = data['CurrentValue'].apply(convert_to_float)
        data['Margin Required'] = data['Margin Required'].apply(convert_to_float)
        data['Collateral Required USD'] = data['Collateral Required USD'].apply(convert_to_float)
        #data['Current Net Exposure'] = data['Current Net Exposure'].apply(convert_to_float)
        #data['Current Net Exposure'] = data['CurrentValue'] - data['Amount USD']
        data['Difference USD'] = data['Difference USD'].apply(convert_to_float)
        data['Liquidation Level'] = data['Liquidation Level'].apply(lambda x: 0.0 if x == "" else float(x.replace("%","")))
        data['Initial Margin'] = data['Initial Margin'].apply(lambda x: float(x.replace("%","")))
        data['Margin Call Level'] = data['Margin Call Level'].apply(lambda x: float(x.replace("%","")))
        data['Current Collateral'] = data['Current Collateral'].apply(lambda x: round(float(x.replace("%","")),0))
        data['Current Collateral'] = np.where(data['Trade #'] == 'VL3b', 0, data['Current Collateral'])
        data['Margin Refund'] = data['Margin Refund'].apply(lambda x: 0.0 if x == "" else round(float(x.replace("%","")),0))
        data['Liquidation Threshold'] = data['Liquidation Threshold'].apply(lambda x: 0.0 if x == "" else float(x.replace(",","")))
        data['Rate'] = data['Rate'].apply(lambda x: float(x.replace("%",""))/100)
    else:
        data['Trade #'] = data['Trade #'].apply(lambda x: "Loan#"+" "+str(x))
        data['LoanExposure'] = data['CurrentValue'].apply(convert_to_float)
        try:
            data['Amount'] = data['Amount'].astype(float)
        except:
            data['Amount'] = data['Amount'].apply(convert_to_float)
        data['Amount USD'] = data['Amount'].astype(float)*data['MarkPrice'].astype(float)
        #data['Amount USD'] = data['Amount USD'].apply(convert_to_float)
        data['CurrentValue'] = data['CurrentValue'].apply(convert_to_float)
        #data['Current Net Exposure'] = data['CurrentValue'] - data['Amount USD']
        data['Liquidation Level'] = data['Liquidation Level'].apply(lambda x: 0.0 if x.replace("-","") == "" else ( 0.0 if x.replace("%","") == "-" else float(x.replace("%","") )) )
        data['Initial Margin'] = data['Initial Margin'].apply(lambda x: float(x.replace("%","")))
        data['Margin Call Level'] = data['Margin Call Level'].apply(lambda x: float(x.replace("%","")))   
        data['Margin Required'] = data['CurrentValue'].astype(float)*data['Margin Call Level'].astype(float)/100
        data['Collateral Required USD'] = data['CurrentValue']*data['Initial Margin']/100
        data['Difference USD'] = data['Amount USD'] - data['Collateral Required USD']
        data['Current Collateral'] = round(100*data['Amount USD']/data['LoanExposure'],1)
        data['Rate'] = data['Rate'].apply(lambda x: float(x.replace("%",""))/100)
        try:
            data['Margin Refund'] = data['Margin Refund'].apply(convert_to_float)
        except:
            data['Margin Refund'] = data['Margin Refund'].apply(lambda x: round(float(x.replace("%","")),0))
        data['Liquidation Threshold'] = data['LoanExposure'].astype(float)*data['Liquidation Level'].astype(float)/100
    #try:
    #    data['Origination Date'] = [datetime.strptime(x, "%m/%d/%y") for x in data['Origination Date']]
    #except:
    #    data['Origination Date'] = [datetime.strptime(x, "%m/%d/%Y") for x in data['Origination Date']]

    final = pd.merge(data, haircut[['Locked','Token','Haircut','LiquidityScore','StressHaircut']], left_on=['Collateral','Locked'], right_on=['Token','Locked'],how='left').fillna(0)
    #final['LiquidityScore'] = final['LiquidityScore'].astype(float)
    final['Haircut'] = final['Haircut'].astype(float)/100
    final['StressHaircut'] = final['StressHaircut'].astype(float)/100
    final['Adj Amount USD'] = final['Amount USD']*(1-final['Haircut'])
    final['Stress Amount USD'] = final['Amount USD']*(1-final['StressHaircut'])
    final['Current Net Exposure'] = final['CurrentValue'] - final['Adj Amount USD']
    final['Stress Net Exposure'] = final['CurrentValue'] - final['Stress Amount USD']
    #final['AdjCollateral over MarginCall'] = final['Adj Amount USD'] - final['Margin Required']
    #final['AdjCollateral over Liquidation'] = final['Adj Amount USD'] - final['Liquidation Threshold']
    x = final['Current Collateral'] - final['Margin Call Level']
    y = final['Initial Margin'] - final['Margin Call Level']
    final['MarginBuffer'] = np.minimum(x, y)
    x = final['Current Collateral'] - final['Margin Refund']
    y = final['Current Collateral'] - final['Initial Margin']
    final['ExcessMarginRefund'] = np.maximum(np.minimum(x,y),0)
    final['ExcessRefundAmount'] = (final['ExcessMarginRefund']/final['Current Collateral'])*final['Amount']
    final['ExcessRefundUSD'] = (final['ExcessMarginRefund']/final['Current Collateral'])*final['Amount USD']
    #final['MarginBuffer'] = final['Current Collateral'] - final['Margin Call Level']
    final['ExcessMargin'] = final['Current Collateral'] - final['Margin Call Level']
    final['ExcessMargin'] = final['ExcessMargin'].apply(lambda x: max(x,0))
    final['Adj CurrentLTV'] = round(100*final['Adj Amount USD']/data['LoanExposure'],1)
    final['Stress CurrentLTV'] = round(100*final['Stress Amount USD']/data['LoanExposure'],1)
    final['AdjExcessMargin'] = final['Adj CurrentLTV'] - final['Margin Call Level']
    final['AdjExcessMargin'] = final['AdjExcessMargin'].apply(lambda x: max(x,0))
    final['StressExcessMargin'] = final['Stress CurrentLTV'] - final['Margin Call Level']
    final['StressExcessMargin'] = final['StressExcessMargin'].apply(lambda x: max(x,0))
    #print(final['End Date'])
    final['End Date'] = final['End Date'].apply( lambda x: "Open" if x == "Open" else ( "Open" if x=="" else np.datetime64(get_format(x)) ) )
    final.replace([np.inf, -np.inf], np.nan, inplace=True)
    final.fillna(0,inplace=True)
    return final

def get_book_data(desk,side,loantype,history):
    currency_list, secured_loans = get_desk_data(desk,side,loantype,history)
    if loantype=='Secured':
        if desk == 'HK':
            if side == 'lend':
                #active_flows = hk_lending_flows[(hk_lending_flows.Active == 'Active') & (hk_lending_flows['Side (BGHK)'] == 'Lending')]
                active_flows = hk_lending_flows[(hk_lending_flows.Active != '') & (hk_lending_flows['Side (BGHK)'] == 'Lending')]
                lending_flows = active_flows.groupby(['Entity-LoanID','Entity Code','Related Loan Tag','Asset','Transaction Type'])[['Qty (Units)']].sum().reset_index()
                secured_loans = pd.merge(left=secured_loans, 
                    right=lending_flows[lending_flows['Transaction Type']== 'Collateral'][['Entity-LoanID','Asset','Qty (Units)']],
                    how='left',
                    left_on=['Entity-LoanID','Collateral'],
                    right_on=['Entity-LoanID','Asset'],
                )
                secured_loans.drop(columns={'Asset'},inplace=True)
                #print(secured_loans[['Entity-LoanID','Amount','Qty (Units)']])
                x = list(secured_loans['Amount'])
                y = list(secured_loans['Qty (Units)'])
                secured_loans['Amount'] = [ float(z[0]) if np.isnan(z[1]) else z[1] for z in list(zip(x, y)) ]
                secured_loans.drop(columns={'Qty (Units)'},inplace=True)
                #print(secured_loans[['Entity-LoanID','Amount']])
            else:
                #active_flows = hk_lending_flows[(hk_lending_flows.Active == 'Active') & (hk_lending_flows['Side (BGHK)'] == 'Borrowing')]
                active_flows = hk_lending_flows[(hk_lending_flows.Active != '') & (hk_lending_flows['Side (BGHK)'] == 'Borrowing')]
                borrowing_flows = active_flows.groupby(['Entity-LoanID','Entity Code','Related Loan Tag','Asset','Transaction Type'])[['Qty (Units)']].sum().reset_index()
                secured_loans = pd.merge(left=secured_loans, 
                    right=borrowing_flows[borrowing_flows['Transaction Type']== 'Collateral'][['Entity-LoanID','Asset','Qty (Units)']],
                    how='left',
                    left_on=['Entity-LoanID','Collateral'],
                    right_on=['Entity-LoanID','Asset'],
                )
                #print(secured_loans.T)
                secured_loans['Qty (Units)'] = abs(secured_loans['Qty (Units)'])
                secured_loans.drop(columns={'Asset','Amount'},inplace=True)
                secured_loans.rename(columns={'Qty (Units)':'Amount'},inplace=True)
        close_prices, price_2022, price_2025, price_luna = get_token_data(currency_list)
        latest_mark = get_latest_mark(close_prices)
        secured_loans = pd.merge(secured_loans, latest_mark, left_on='Currency', right_on='Token',how='left').fillna(0).drop(columns={'Token'}).rename(columns={'MarkPrice':'TradePrice'})
        secured_loans = pd.merge(secured_loans, latest_mark, left_on='Collateral', right_on='Token',how='left').fillna(0).drop(columns={'Token'})
        secured_loans['Desk'] = desk
    else:
        if len(currency_list)>1:
            close_prices, price_2022, price_2025, price_luna = get_token_data(currency_list)
            latest_mark = get_latest_mark(close_prices)
            secured_loans = pd.merge(secured_loans, latest_mark, left_on='Currency', right_on='Token',how='left').fillna(0).drop(columns={'Token'}).rename(columns={'MarkPrice':'TradePrice'})
        else:
            secured_loans['TradePrice'] = 1.0
        secured_loans['Desk'] = desk
        #print(secured_loans.head())
    return secured_loans

def get_desk_data(desk,side,loantype='Secured',history='No'):
    if desk == 'Prime':
        if side == 'lend':
            
            #change notional to outstanding for normal use
            if history == 'Yes':
                active_book = prime_lending#[(prime_lending['Active']=='YES')] 
                columns_to_keep = ['WithinEntity','Counterparty','Trade #','Origination Date','Currency','Notional','Current Market Value','Collateral type','Current Collateral %','Locked','Amount','Amount USD','Difference USD','Collateral Required %','Collateral Required USD','Difference','Difference USD','Margin Call Level','Margin Call Threshold','Liquidation Level','Liquidation Threshold','Current Net Exposure','Collateral Return Level','Rate','End Date','Active']
                secured_lends = active_book[(active_book['LoanType']==loantype) & (active_book['Active']!='') ].filter(columns_to_keep)
                secured_lends.rename(columns={'Collateral type':'Collateral','Current Market Value':'CurrentValue','Current Collateral %':'Current Collateral','Collateral Required %':'Initial Margin','Margin Call Threshold':'Margin Required','Collateral Return Level':'Margin Refund','Notional':'Outstanding'},inplace=True)
            else:
                active_book = prime_lending[(prime_lending['Active']=='YES')]
                columns_to_keep = ['WithinEntity','Counterparty','Trade #','Origination Date','Currency','Outstanding','Current Market Value','Collateral type','Current Collateral %','Locked','Amount','Amount USD','Difference USD','Collateral Required %','Collateral Required USD','Difference','Difference USD','Margin Call Level','Margin Call Threshold','Liquidation Level','Liquidation Threshold','Current Net Exposure','Collateral Return Level','Rate','End Date','Active']
                secured_lends = active_book[(active_book['LoanType']==loantype) & (active_book['Active']=='YES') ].filter(columns_to_keep)
                secured_lends.rename(columns={'Collateral type':'Collateral','Current Market Value':'CurrentValue','Current Collateral %':'Current Collateral','Collateral Required %':'Initial Margin','Margin Call Threshold':'Margin Required','Collateral Return Level':'Margin Refund'},inplace=True)
            
            try:
                secured_lends['Origination Date'] = [datetime.strptime(x, "%m/%d/%y") for x in secured_lends['Origination Date']]
            except:
                secured_lends['Origination Date'] = [datetime.strptime(x, "%m/%d/%Y") for x in secured_lends['Origination Date']]
            secured_lends = secured_lends[(secured_lends['Origination Date'] > '2024-01-01')]
            currency_list = list(secured_lends.Currency.unique()) + list(secured_lends.Collateral.unique())
        else:
            
            if history == 'Yes':
                active_book = prime_borrows#[(prime_borrows['Active']=='YES')]
                columns_to_keep = ['WithinEntity','Counterparty','Trade #','Origination Date','Currency','Trade Notional','Current Market Value','Collateral type','Current Collateral %','Locked','Amount','Amount USD','Difference USD','Collateral Required %','Collateral Required USD','Difference','Difference USD','Margin Call Level','Margin Call Threshold','Liquidation Level','Liquidation Threshold','Current Net Exposure','Collateral Return Level','Rate','End Date','Active']
                secured_borrows = prime_borrows[(prime_borrows['LoanType']==loantype) & (prime_borrows['Active']!='') ].filter(columns_to_keep)
                secured_borrows.rename(columns={'Collateral type':'Collateral','Current Market Value':'CurrentValue','Current Collateral %':'Current Collateral','Collateral Required %':'Initial Margin','Margin Call Threshold':'Margin Required','Collateral Return Level':'Margin Refund','Trade Notional':'Outstanding'},inplace=True)
            else:
                active_book = prime_borrows[(prime_borrows['Active']=='YES')]
                columns_to_keep = ['WithinEntity','Counterparty','Trade #','Origination Date','Currency','Outstanding','Current Market Value','Collateral type','Current Collateral %','Locked','Amount','Amount USD','Difference USD','Collateral Required %','Collateral Required USD','Difference','Difference USD','Margin Call Level','Margin Call Threshold','Liquidation Level','Liquidation Threshold','Current Net Exposure','Collateral Return Level','Rate','End Date','Active']
                secured_borrows = prime_borrows[(prime_borrows['LoanType']==loantype) & (prime_borrows['Active']=='YES') ].filter(columns_to_keep)
                secured_borrows.rename(columns={'Collateral type':'Collateral','Current Market Value':'CurrentValue','Current Collateral %':'Current Collateral','Collateral Required %':'Initial Margin','Margin Call Threshold':'Margin Required','Collateral Return Level':'Margin Refund'},inplace=True)
            
            try:
                secured_borrows['Origination Date'] = [datetime.strptime(x, "%m/%d/%y") for x in secured_borrows['Origination Date']]
            except:
                secured_borrows['Origination Date'] = [datetime.strptime(x, "%m/%d/%Y") for x in secured_borrows['Origination Date']]
            secured_borrows = secured_borrows[(secured_borrows['Origination Date'] > '2024-01-01')]
            currency_list = list(secured_borrows.Currency.unique()) + list(secured_borrows.Collateral.unique())
    else:
        if history == 'Yes':
            active_book = hk_lending#[(hk_lending['Active/Closed']=='Active')]
        else:
            active_book = hk_lending[(hk_lending['Active/Closed']=='Active')]
        active_book.rename(columns={'Loan Tag':'Trade #','Collateral Asset':'Collateral','Initial Collateral Units':'Amount'},inplace=True)
        active_book.rename(columns={'Borrowed Asset':'Currency','Borrowed Units':'Outstanding','Borrowed Notional':'CurrentValue'},inplace=True)
        active_book.rename(columns={'IM (%)':'Initial Margin','Margin Call (%)':'Margin Call Level','Liq Lvl (%)':'Liquidation Level','Margin Refund (%)':'Margin Refund','Rate (ann)':'Rate','Actual Expiry':'End Date'},inplace=True)
        if side == 'lend':  
            secured_lends = active_book[(active_book['LoanType']==loantype) & (active_book['Active/Closed']!='') & (active_book['Side (BGHK)']=='Loan')]
            try:
                secured_lends['Origination Date'] = [datetime.strptime(x, "%m/%d/%y") for x in secured_lends['Origination Date']]
            except:
                secured_lends['Origination Date'] = [datetime.strptime(x, "%m/%d/%Y") for x in secured_lends['Origination Date']]
            secured_lends = secured_lends[(secured_lends['Origination Date'] > '2024-01-01')]
            currency_list = list(secured_lends.Currency.unique()) + list(secured_lends.Collateral.unique())
        else:
            secured_borrows = active_book[(active_book['LoanType']==loantype) & (active_book['Active/Closed']!='') & (active_book['Side (BGHK)']=='Borrow')]
            try:
                secured_borrows['Origination Date'] = [datetime.strptime(x, "%m/%d/%y") for x in secured_borrows['Origination Date']]
            except:
                secured_borrows['Origination Date'] = [datetime.strptime(x, "%m/%d/%Y") for x in secured_borrows['Origination Date']]
            secured_borrows = secured_borrows[(secured_borrows['Origination Date'] > '2024-01-01')]
            currency_list = list(secured_borrows.Currency.unique()) + list(secured_borrows.Collateral.unique())
         
    items_to_remove = ['USD','','USDC','-','USDT']
    currency_list = list(set(currency_list) - set(items_to_remove))
    if side == 'lend':
        secured_loan_data = secured_lends
    else:
        secured_loan_data = secured_borrows
    print(currency_list)
    secured_loan_data['Desk'] = desk
    return currency_list, secured_loan_data

def get_final_book(side,loantype,history='No'):
    if loantype == 'Secured':
        prime = get_final_secured(get_book_data('Prime',side,loantype,history))
        hk = get_final_secured(get_book_data('HK',side,loantype,history))
        df = pd.concat([prime,hk],axis=0)
        df['Trade #'] = df['Desk'] + " - " + df['Trade #']
        df = df[['WithinEntity','Desk','Counterparty','Trade #','Currency','Outstanding','LoanExposure','CurrentValue','Collateral','Locked','Amount','Amount USD','MarkPrice','Current Collateral','Margin Required','MarginBuffer','Current Net Exposure','Stress Net Exposure','Initial Margin','Margin Call Level','Liquidation Level','Margin Refund','ExcessMargin','Haircut','StressHaircut','Adj Amount USD','Stress Amount USD','Adj CurrentLTV','Stress CurrentLTV','AdjExcessMargin','StressExcessMargin','ExcessMarginRefund','ExcessRefundAmount','ExcessRefundUSD','Rate','Origination Date','End Date']].sort_values(by='LoanExposure',ascending=False)
    else:
        #get unsecured loans
        prime_lend = get_final_unsecured(get_book_data('Prime','lend',loantype,history))
        hk_lend = get_final_unsecured(get_book_data('HK','lend',loantype,history))
        final_lend = pd.concat([prime_lend,hk_lend],axis=0)
        final_lend['Side'] = 'Lend'
        #get unsecured borrows
        prime_borrow = get_final_unsecured(get_book_data('Prime','borrow',loantype,history))
        hk_borrow = get_final_unsecured(get_book_data('HK','borrow',loantype,history))
        final_borrow = pd.concat([prime_borrow,hk_borrow],axis=0)
        final_borrow['Side'] = 'Borrow'
        #combine them
        df = pd.concat([final_lend,final_borrow],axis=0)
        df['Trade #'] = df['Desk'] + " - " + df['Trade #']
        columns = df.columns.tolist()
        columns.remove('Side')
        columns.insert(0, 'Side')
        df = df[['WithinEntity','Side','Desk','Counterparty','Trade #','Currency','Outstanding','LoanExposure','Collateral','Locked','Amount','Amount USD','MarkPrice','Current Collateral','Margin Required','MarginBuffer','Current Net Exposure','Stress Net Exposure','Initial Margin','Margin Call Level','Liquidation Level','Margin Refund','ExcessMargin','Haircut','StressHaircut','Adj Amount USD','Stress Amount USD','Adj CurrentLTV','Stress CurrentLTV','AdjExcessMargin','StressExcessMargin','ExcessMarginRefund','ExcessRefundAmount','ExcessRefundUSD','Rate','Origination Date','End Date']].sort_values(by='LoanExposure',ascending=False)   
    return df