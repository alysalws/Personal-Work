
# import all the required packages
import pandas as pd
import yfinance as yf
import datetime
from datetime import date
import time
import requests
import io
import json
from yahoofinancials import YahooFinancials 
from bs4 import BeautifulSoup
import numpy as np
import os
import time
import psycopg2 
import boto3
import io
from io import StringIO, BytesIO
import boto3

# Set up S3 envireonment for connection
REGION = 'us-east-1'
ACCESS_KEY_ID = 'AKIAYTMSKI5VMAF3UZPK'
SECRET_ACCESS_KEY = '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'
BUCKET_NAME = 'deindividualproject'
s3csv = boto3.client('s3', 
        region_name = REGION,
        aws_access_key_id = ACCESS_KEY_ID,
        aws_secret_access_key = SECRET_ACCESS_KEY)

# Step 2 - Create functions to transform data 

def Transform():
    # Function to save file to S3 in CSV file format
    def save_csv_s3(df, folder, name):
        csv_buffer=StringIO()
        df.to_csv(csv_buffer, index=True)
        response=s3csv.put_object(Body=csv_buffer.getvalue(),
                               Bucket=BUCKET_NAME,
                               Key=folder + "/" + name + ".csv")
    # Read data from S3 bucket using Pandas
    obj1 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'RawData/Company Information (raw).csv')
    obj2 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'RawData/Stockprices (raw).csv')
    obj3 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'RawData/JPM Glassdoor Reviews (raw).csv')
    obj4 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'RawData/Citibank Glassdoor Reviews (raw).csv')
    obj5 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'RawData/Balance Sheet (in USD) (raw).csv')
    obj6 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'RawData/Income Statement (in USD) (raw).csv')
    obj7 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'RawData/Cash Flow Statement (in USD) (raw).csv')
    obj8 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'RawData/ESG scores (raw).csv')

    stockprices = pd.read_csv(io.BytesIO(obj2['Body'].read()), encoding='utf8')
    JPM = pd.read_csv(io.BytesIO(obj3['Body'].read()), encoding='utf8')
    Citi = pd.read_csv(io.BytesIO(obj4['Body'].read()), encoding='utf8')
    balance_sheet = pd.read_csv(io.BytesIO(obj5['Body'].read()), encoding='utf8')
    income_statement = pd.read_csv(io.BytesIO(obj6['Body'].read()), encoding='utf8')
    cfs = pd.read_csv(io.BytesIO(obj7['Body'].read()), encoding='utf8')
    ESG = pd.read_csv(io.BytesIO(obj8['Body'].read()), encoding='utf8')
    company_info_table = pd.read_csv(io.BytesIO(obj1['Body'].read()), encoding='utf8')
    
    # create company_info_table
    company_info_table=company_info_table.drop(columns=['Unnamed: 0'])
    
    # create year_table
    rng = pd.date_range('2012-01', periods=400, freq='M')   #create an array of 400 dates starting at '2012-01', one per month
    year_table = pd.DataFrame({ 'Date': rng, 'Month_year_id': np.arange(1, len(rng)+1)}) # create Month_year_id
    year_table['Date'] = pd.to_datetime(year_table['Date'])
    year_table['Month_year'] = year_table['Date'].apply(lambda x: x.strftime('%Y-%m'))
    year_table=year_table.drop(columns=['Date'])
    
    # create financial_table
    balance_sheet=balance_sheet[['endDate','ticker', 'totalAssets', 'totalLiab','totalStockholderEquity']]
    income_statement=income_statement[['endDate','ticker','totalRevenue','grossProfit','netIncome']]
    cfs=cfs[['endDate','ticker','totalCashFromOperatingActivities']]
    # Keep 7 financial indicators only   
    df4 = pd.merge(balance_sheet,income_statement, on=['endDate','ticker'], how='right')
    df5 = pd.merge(df4,cfs, on=['endDate','ticker'], how='right')
    financial_table=pd.merge(company_info_table,df5, on='ticker', how='right')
    financial_table=financial_table[['endDate','ticker','Security','ticker_id', 'totalAssets', 'totalLiab','totalStockholderEquity','totalRevenue','grossProfit','netIncome','totalCashFromOperatingActivities']]
    # Convert the figures into Millions
    financial_table['totalAssets']=financial_table['totalAssets']/1000000
    financial_table['totalLiab']=financial_table['totalLiab']/1000000
    financial_table['totalStockholderEquity']=financial_table['totalStockholderEquity']/1000000
    financial_table['totalRevenue']=financial_table['totalRevenue']/1000000
    financial_table['grossProfit']=financial_table['grossProfit']/1000000
    financial_table['netIncome']=financial_table['netIncome']/1000000
    financial_table['totalCashFromOperatingActivities']=financial_table['totalCashFromOperatingActivities']/1000000
    financial_table['endDate'] = pd.to_datetime(financial_table['endDate'])
    financial_table['endDate'] = financial_table['endDate'].apply(lambda x: x.strftime('%Y-%m'))
    financial_table.rename(columns={'endDate': "Month_year"}, inplace=True)
    financial_table=pd.merge(year_table, financial_table, on='Month_year', how='right')
    financial_table=financial_table[financial_table.Month_year != 'TTM']     # Dropping irrelevant rows 
    financial_table["Month_year_id"]=financial_table["Month_year_id"].astype(int)
    financial_table=financial_table.dropna()
    financial_table['index'] = np.arange(1, len(financial_table)+1)
    financial_table['ticker_id']= financial_table['ticker_id'].astype(int)
    
    # create ESG_table
    ESG['Date'] = pd.to_datetime(ESG['Date'])
    ESG['Month_year'] =ESG['Date'].apply(lambda x: x.strftime('%Y-%m'))
    ESG.rename(columns={"Name": "ticker"}, inplace=True)
    ESG_table=pd.merge(company_info_table,ESG, on='ticker', how='right').dropna()
    ESG_table=ESG_table.drop(columns=['GICS Sector','GICS Sector','GICS Sub-Industry',
                                    'Headquarters Location', 'Date first added','Founded'])
    ESG_table=pd.merge(year_table, ESG_table, on='Month_year', how='right')
    ESG_table['index'] = np.arange(1, len(ESG_table)+1)
    ESG_table=ESG_table[["index","ticker","ticker_id","Security","Date", "Month_year","Month_year_id","E-Score","S-Score","G-Score","Total-Score"]]
    
    # create Glassdoor_table
    Citi['ticker']='C'
    Citi['ticker_id']=19
    JPM['ticker']='JPM'
    JPM['ticker_id']=36
    Glassdoor_table=Citi.append(JPM).reset_index(drop=True)
    Glassdoor_table['pros']=Glassdoor_table['pros'].str.replace('-','') # removing special characters in reviews
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
    
    # create stockprices_table
    stockprices['Date'] = pd.to_datetime(stockprices['Date'])
    stockprices['Month_year'] =stockprices['Date'].apply(lambda x: x.strftime('%Y-%m'))
    stockprices.rename(columns={"Name": "ticker"}, inplace=True)
    stockprices_table=pd.merge(company_info_table,stockprices, on='ticker', how='right')
    stockprices_table=stockprices_table.drop(columns=['GICS Sector','GICS Sector','GICS Sub-Industry',
                                    'Headquarters Location', 'Date first added','Founded'])
    stockprices_table=pd.merge(year_table, stockprices_table, on='Month_year', how='right').dropna()
    stockprices_table['index'] = np.arange(1, len(stockprices_table)+1)
    stockprices_table=stockprices_table[["index","ticker_id","ticker","Security","Date","Month_year", "Month_year_id","Open", "High","Low","Close","Adj Close","Volume"]]
    
    # save the cleaned tables to S3
    save_csv_s3(financial_table, 'project', 'company_financials (in USD millions)')
    save_csv_s3(Glassdoor_table, 'project','glassdoor_reviews_cleaned')
    save_csv_s3(company_info_table, 'project','company_info')
    save_csv_s3(stockprices_table, 'project','company_stockprices')
    save_csv_s3(ESG_table, 'project','company_ESG_scores')
    save_csv_s3(year_table, 'project','year')


