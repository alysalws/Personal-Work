#!/usr/bin/env python
# coding: utf-8

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
import yesg
import yahoo_fin.stock_info as yfs
from functools import reduce
import psycopg2 
import boto3
import io
from io import StringIO, BytesIO
import boto3


REGION = 'us-east-1'
ACCESS_KEY_ID = 'AKIAYTMSKI5VMAF3UZPK'
SECRET_ACCESS_KEY = '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'
BUCKET_NAME = 'deindividualproject'
s3csv = boto3.client('s3', 
        region_name = REGION,
        aws_access_key_id = ACCESS_KEY_ID,
        aws_secret_access_key = SECRET_ACCESS_KEY)

# Step 1 - Create functions to scrap data 

def Extract():
    
    def save_csv_s3(df, folder, name):
        csv_buffer=StringIO()
        df.to_csv(csv_buffer, index=True)
        response=s3csv.put_object(Body=csv_buffer.getvalue(),
                               Bucket=BUCKET_NAME,
                               Key=folder + "/" + name + ".csv")

    # Scrape company information from Wikipedia
    payload=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = payload[0] # There are 2 tables on the Wikipedia page, we want the first table
    finance_df = df[df['GICS Sector'] == 'Financials'].reset_index().drop(['SEC filings','CIK'],axis=1)
    finance_df['ticker_id'] = np.arange(1, len(finance_df)+1)
    finance_df.rename(columns={"Symbol": "ticker"}, inplace=True)
    #finance_df=finance_df.drop(columns={"index"}).to_csv('info.csv')
     # Create an indexed list for SP500 companies in the finance sector
    finance_symbols = finance_df['ticker'].values.tolist()

    # Scrape the historical stockprices from YahooFinance API
    stock_final = pd.DataFrame()
    for i in finance_symbols:  
        try:
            stock = yf.download(i,start='2021-01-01', end=date.today(), progress=False) 
            # download the stock price from 2021-01-01 onwards
            if len(stock) == 0:
                pass
            else:
                stock['Name']=i
                stock_final = stock_final.append(stock,sort=False)
        except Exception:
                None
    #stock_final.to_csv('stockprices.csv')
    
    # Scrape the Environment, Social and Governance (ESG) scores from Yahoo ESG API (Yesg)
    ESG_final = pd.DataFrame()
    for i in finance_symbols:  
        try:
            ESG = yesg.get_historic_esg(i) # if the message 'something wrong with the symbol...' pops up, wait for at least 15 minutes to run the file again.
            if len(ESG) == 0:
                pass
            else:
                ESG['Name']=i
                ESG_final = ESG_final.append(ESG,sort=False)
        except Exception:
            None
    #ESG_final.to_csv('ESG_scores.csv')
     
    # Scrape the historical balance sheets from Yahoo Finance API
    balance_sheet_final=pd.DataFrame()
    for ticker in finance_symbols: 
        try:
            balance_sheet = yfs.get_balance_sheet(ticker).T
            if len(balance_sheet)==0 :
                pass
            else:
                balance_sheet['ticker']=ticker
                balance_sheet_final=balance_sheet_final.append(balance_sheet, sort=False)  

        except Exception:
            None
    #balance_sheet_final.to_csv('balance_sheet.csv') 
    
    # Scrape the historical cash flow statements (cfs) from Yahoo Finance API
    cfs_final=pd.DataFrame()
    for ticker in finance_symbols: 
        try:
            cfs = yfs.get_cash_flow(ticker).T
            if len(cfs) == 0:
                pass
            else:
                cfs['ticker']=ticker
                cfs_final= cfs_final.append(cfs, sort=False)

        except Exception:
            None
    #cfs_final.to_csv('cfs.csv')     
    
    # Scrape the historical income statements from Yahoo Finance API
    income_statement_final=pd.DataFrame()
    for ticker in finance_symbols: 
        try:
            income_statement=yfs.get_income_statement(ticker, yearly = True).T
            if len(income_statement) == 0:
                pass
            else:
                income_statement['ticker']=ticker
                income_statement_final=income_statement_final.append(income_statement, sort=False)

        except Exception:
            None
    #income_statement_final.to_csv('income_statement.csv') 
    
    # 7th function - Scrape the glassdoor reviews of 2 companies (Citibank, JPMorgan) using Page2api
    # If the API Key is no longer working, that means all free credits have been consumed and 
    #we need to create a new one by registering a new account at https://www.page2api.com
    
    api_url = 'https://www.page2api.com/api/v1/scrape'
    payload = {
          "api_key": "923b14e27effbd37f71558cad97d2da20eb18b0f",
          "url": "https://www.glassdoor.co.uk/Reviews/Citi-Reviews-E8843.htm", #citibank
          "real_browser": True,
          "merge_loops": True,
          "premium_proxy": "us",
          "scenario": [
            {
              "loop": [
                { "wait_for": "div.gdReview" },
                { "execute": "parse" },
                { "execute_js": "document.querySelector(\".nextButton\").click()" }
              ],
              "iterations":2 #only scrap 2 pages
            }
          ],
          "parse": {
            "reviews": [
              {
                "_parent": "div.gdReview",
                "author_info": ".authorInfo >> text",
                "rating": "span.ratingNumber >> text",
                "pros": "span[data-test=pros] >> text",
                "cons": "span[data-test=cons] >> text"
              }
            ]
          }
        }

    payload2 = {
          "api_key": "923b14e27effbd37f71558cad97d2da20eb18b0f",
          "url": "https://www.glassdoor.co.uk/Reviews/J-P-Morgan-Reviews-E145.htm", #JPMorgan
          "real_browser": True,
          "merge_loops": True,
          "premium_proxy": "us",
          "scenario": [
            {
              "loop": [
                { "wait_for": "div.gdReview" },
                { "execute": "parse" },
                { "execute_js": "document.querySelector(\".nextButton\").click()" }
              ],
              "iterations":2 #only scrap 2 pages
            }
          ],
          "parse": {
            "reviews": [
              {
                "_parent": "div.gdReview",
                "author_info": ".authorInfo >> text",
                "rating": "span.ratingNumber >> text",
                "pros": "span[data-test=pros] >> text",
                "cons": "span[data-test=cons] >> text"
              }
            ]
          }
        }

    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    response = requests.post(api_url, data=json.dumps(payload), headers=headers)
    result = json.loads(response.text)
    response2 = requests.post(api_url, data=json.dumps(payload2), headers=headers)
    result2 = json.loads(response.text)

    # flatten the texts
    def flattenjson(b, delim):
        val = {}
        for i in b.keys():
            if isinstance(b[i], dict):
                get = flattenjson(b[i], delim)
                for j in get.keys():
                    val[i + delim + j] = get[j]
            else:
                val[i] = b[i]

        return val

    data=flattenjson(result,'__')
    data2=flattenjson(result2,'__')
    citi_glassdoor_reviews=pd.json_normalize(data,record_path='result__reviews')
    JPMorgan_glassdoor_reviews=pd.json_normalize(data2,record_path='result__reviews')
    #citi_glassdoor_reviews.to_csv('citibank_glassdoor_reviews.csv',index=None)
    #JPMorgan_glassdoor_reviews.to_csv('JPmorgan_glassdoor_reviews.csv',index=None)
    
    #Save files to S3
    save_csv_s3(finance_df, 'RawData', 'Company Information (raw)')
    save_csv_s3(stock_final, 'RawData', 'Stockprices (raw)')
    save_csv_s3(JPMorgan_glassdoor_reviews, 'RawData', 'JPM Glassdoor Reviews (raw)')
    save_csv_s3(citi_glassdoor_reviews, 'RawData', 'Citibank Glassdoor Reviews (raw)')
    save_csv_s3(balance_sheet_final, 'RawData', 'Balance Sheet (in USD) (raw)')
    save_csv_s3(income_statement_final, 'RawData', 'Income Statement (in USD) (raw)')
    save_csv_s3(cfs_final, 'RawData', 'Cash Flow Statement (in USD) (raw)')
    save_csv_s3(ESG_final, 'RawData', 'ESG scores (raw)')


