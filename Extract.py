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
!pip install mfinancials
!pip install yahoo_fin
import yahoo_fin.stock_info as yfs
from functools import reduce

# Step 1 - Create functions to scrap data 

# 1st function - Scrape the company information from Wikipedia
def Extract():
    
    def company_info():
        payload=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        df = payload[0] # There are 2 tables on the Wikipedia page, we want the first table
        finance_df = df[df['GICS Sector'] == 'Financials'].reset_index().drop(['SEC filings','CIK'],axis=1)
        finance_df['ticker_id'] = np.arange(1, len(finance_df)+1)
        finance_df.rename(columns={"Symbol": "ticker"}, inplace=True)
        finance_df.drop(columns={"index"}).to_csv('company_info.csv')

    company_info()
    
    # Create an indexed list for SP500 companies in the finance sector
    payload=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = payload[0] 
    finance_df = df[df['GICS Sector'] == 'Financials'].reset_index().drop(['SEC filings','CIK'],axis=1)
    finance_df['ticker_id'] = np.arange(1, len(finance_df)+1)
    finance_df.rename(columns={"Symbol": "ticker"}, inplace=True)
    finance_symbols = finance_df['ticker'].values.tolist()

    # 2nd function - Scrape the historical stockprices from YahooFinance API

    def stockprices_scraper():
        stock_final = pd.DataFrame()
        # iterate over each symbol
        for i in finance_symbols:  
            try:
                # download the stock price from 2018-01-01 to current date
                stock = yf.download(i,start='2018-01-01', end=date.today(), progress=False)
                # append the individual stock prices 
                if len(stock) == 0:
                    pass
                else:
                    stock['Name']=i
                    stock_final = stock_final.append(stock,sort=False)
            except Exception:
                    None
        stock_final.to_csv('stockprices.csv')

    stockprices_scraper()
    
    # 3rd function - Scrape the Environment, Social and Governance (ESG) scores from Yahoo ESG API (Yesg)

    def ESG_scraper():
        ESG_final = pd.DataFrame()
        # iterate over each symbol
        for i in finance_symbols:  
            try:
                ESG = yesg.get_historic_esg(i)
                # append the individual ESG scores 
                if len(ESG) == 0:
                    None
                else:
                    ESG['Name']=i
                    ESG_final = ESG_final.append(ESG,sort=False)
            except Exception:
                None
        ESG_final.to_csv('ESG_scores.csv')

    ESG_scraper()
    
    # 4th function - Scrape the historical balance sheets from Yahoo Finance API

    def balance_sheet_scraper():
        summarytable=pd.DataFrame()
        # iterate over each symbol
        for ticker in finance_symbols: 
            try:
                balance_sheet = yfs.get_balance_sheet(ticker).T
                if len(balance_sheet)==0 :
                    pass
                else:
                    balance_sheet['ticker']=ticker
                    summarytable=summarytable.append(balance_sheet, sort=False)       
            except Exception:
                None
        summarytable.to_csv('balance_sheet.csv') 

    balance_sheet_scraper()
    
    # 5th function - Scrape the historical cash flow statements (cfs) from Yahoo Finance API

    def cfs_scraper():
        summarytable=pd.DataFrame()
        # iterate over each symbol
        for ticker in finance_symbols: 
            try:
                cfs = yfs.get_cash_flow(ticker).T
                if len(cfs) == 0:
                    pass
                else:
                    cfs['ticker']=ticker
                    summarytable=summarytable.append(cfs, sort=False)
            except Exception:
                None
        summarytable.to_csv('cfs.csv') 
    cfs_scraper()
    
    # 6th function - Scrape the historical income statements from Yahoo Finance API

    def income_statement_scraper():
        # iterate over each symbol
        summarytable=pd.DataFrame()
        for ticker in finance_symbols: 
            try:
                income_statement=yfs.get_income_statement(ticker, yearly = True).T
                if len(income_statement) == 0:
                    pass
                else:
                    income_statement['ticker']=ticker
                    summarytable=summarytable.append(income_statement, sort=False)
            except Exception:
                None
        summarytable.to_csv('income_statement.csv') 

    income_statement_scraper()

    # 7th function - Scrape the glassdoor reviews of 2 companies (Citibank, JPMorgan) using Page2api
    # If the API Key is no longer working, that means all free credits have been consumed and we need to create a new one by registering a new account at https://www.page2api.com
    
    def glassdoor_scraper():
        api_url = 'https://www.page2api.com/api/v1/scrape'
        payload = {
              "api_key": "fe8f04b55b123afe0a11e39f4ec92db6c935e9f5",
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
              "api_key": "fe8f04b55b123afe0a11e39f4ec92db6c935e9f5",
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
        citi_glassdoor_reviews.to_csv('citibank_glassdoor_reviews.csv',index=None)
        JPMorgan_glassdoor_reviews.to_csv('JPmorgan_glassdoor_reviews.csv',index=None)

    glassdoor_scraper()

Extract()
