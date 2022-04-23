# import all the required packages
import pandas as pd
!pip install yfinance
import yfinance as yf
import datetime
from datetime import date
import time
import requests
import io
import json
!pip install yahoofinancials
from yahoofinancials import YahooFinancials 
from bs4 import BeautifulSoup
import numpy as np
import os
import time
!pip install yesg
import yesg

def Transform():
    #company_info table
    company_info_table=pd.read_csv('company_info.csv').dropna()
    company_info_table=company_info_table.drop(columns=['Unnamed: 0'])
    company_info_table.to_csv('company_info.csv',index=None)   

    #year_table
    rng = pd.date_range('2012-01', periods=400, freq='M')   #create an array of 400 dates starting at '2012-01', one per month
    year_table = pd.DataFrame({ 'Date': rng, 'Month_year_id': np.arange(1, len(rng)+1)})
    year_table['Date'] = pd.to_datetime(year_table['Date'])
    year_table['Month_year'] = year_table['Date'].apply(lambda x: x.strftime('%Y-%m'))
    year_table=year_table.drop(columns=['Date'])
    year_table.to_csv('year.csv',index=None)
    
    #financials_table
    df= pd.read_csv('balance_sheet.csv')
    df=df[['endDate','ticker', 'totalAssets', 'totalLiab','totalStockholderEquity']]
    df2= pd.read_csv('income_statement.csv')
    df2=df2[['endDate','ticker','totalRevenue','grossProfit','netIncome']]
    df3= pd.read_csv('cfs.csv')
    df3=df3[['endDate','ticker','totalCashFromOperatingActivities']]
    df4 = pd.merge(df,df2, on=['endDate','ticker'], how='right')
    df5 = pd.merge(df4,df3, on=['endDate','ticker'], how='right')
    company_info_table=pd.read_csv('company_info.csv')
    financial_table=pd.merge(company_info_table,df5, on='ticker', how='right')
    financial_table=financial_table[['endDate','ticker','Security','ticker_id', 'totalAssets', 'totalLiab','totalStockholderEquity','totalRevenue','grossProfit','netIncome','totalCashFromOperatingActivities']]
    financial_table['endDate'] = pd.to_datetime(financial_table['endDate'])
    financial_table['endDate'] = financial_table['endDate'].apply(lambda x: x.strftime('%Y-%m'))
    financial_table.rename(columns={'endDate': "Month_year"}, inplace=True)
    financial_table=pd.merge(year_table, financial_table, on='Month_year', how='right')
    financial_table=financial_table[financial_table.Month_year != 'TTM']
    financial_table["Month_year_id"]=financial_table["Month_year_id"].astype(int)
    financial_table=financial_table.dropna()
    financial_table['index'] = np.arange(1, len(financial_table)+1)
    financial_table['ticker_id']= financial_table['ticker_id'].astype(int)
    financial_table.to_csv('company_financials.csv',index=None)     
    
    #ESG_table
    df= pd.read_csv('ESG_scores.csv')
    df['Date'] = pd.to_datetime(df['Date'])
    df['Month_year'] =df['Date'].apply(lambda x: x.strftime('%Y-%m'))
    df.rename(columns={"Name": "ticker"}, inplace=True)
    ESG_table=pd.merge(company_info_table,df, on='ticker', how='right').dropna()
    ESG_table=ESG_table.drop(columns=['GICS Sector','GICS Sector','GICS Sub-Industry',
                                    'Headquarters Location', 'Date first added','Founded'])
    ESG_table=pd.merge(year_table, ESG_table, on='Month_year', how='right')
    ESG_table['index'] = np.arange(1, len(ESG_table)+1)
    ESG_table=ESG_table[["index","ticker","ticker_id","Security","Date", "Month_year","Month_year_id","E-Score","S-Score","G-Score","Total-Score"]]
    ESG_table.to_csv('company_ESG_scores.csv', index=None)
    
    #Glassdoor_table
    df1= pd.read_csv('citibank_glassdoor_reviews.csv')
    df1['ticker']='C'
    df1['ticker_id']=19
    df2= pd.read_csv('JPmorgan_glassdoor_reviews.csv')
    df2['ticker']='JPM'
    df2['ticker_id']=36
    Glassdoor_table=df1.append(df2).reset_index(drop=True)
    Glassdoor_table['pros']=Glassdoor_table['pros'].str.replace('-','')
    Glassdoor_table['pros']=Glassdoor_table['pros'].str.replace('=','')
    Glassdoor_table['cons']=Glassdoor_table['cons'].str.replace('-','')
    Glassdoor_table['cons']=Glassdoor_table['cons'].str.replace('=','')
    Glassdoor_table['date']=Glassdoor_table['author_info'].str.split(' - ').str[0]
    Glassdoor_table['author']=Glassdoor_table['author_info'].str.split(' - ').str[1]
    Glassdoor_table.drop('author_info', axis=1, inplace=True)
    Glassdoor_table['date'] = pd.to_datetime(Glassdoor_table['date'])
    Glassdoor_table['Month_year'] = Glassdoor_table['date'].apply(lambda x: x.strftime('%Y-%m'))
    Glassdoor_table=pd.merge(year_table,Glassdoor_table, on='Month_year', how='right')
    Glassdoor_table['index'] = np.arange(1, len(Glassdoor_table)+1)
    Glassdoor_table= Glassdoor_table[["index","ticker","ticker_id","date", "Month_year","Month_year_id", "author","pros","cons","rating"]]
    Glassdoor_table.to_csv('glassdoor_reviews_cleaned.csv', index=None)
    
    #stockprices_table
    df= pd.read_csv('stockprices.csv')
    df['Date'] = pd.to_datetime(df['Date'])
    df['Month_year'] =df['Date'].apply(lambda x: x.strftime('%Y-%m'))
    df.rename(columns={"Name": "ticker"}, inplace=True)
    stockprices_table=pd.merge(company_info_table,df, on='ticker', how='right')
    stockprices_table=stockprices_table.drop(columns=['GICS Sector','GICS Sector','GICS Sub-Industry',
                                    'Headquarters Location', 'Date first added','Founded'])
    stockprices_table=pd.merge(year_table, stockprices_table, on='Month_year', how='right').dropna()
    stockprices_table['index'] = np.arange(1, len(stockprices_table)+1)
    stockprices_table=stockprices_table[["index","ticker_id","ticker","Security","Date","Month_year", "Month_year_id","Open", "High","Low","Close","Adj Close","Volume"]]
    stockprices_table.to_csv('company_stockprices.csv', index=None)

Transform()
