#install the required packages
!pip install pyspark
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql import SQLContext
!pip3 install config
import numpy as np 
import pandas as pd 
import pickle 
import json 
import random 
import sqlalchemy 
from sqlalchemy.types import * 

# S3 and postgres
import psycopg2 
import io
from io import StringIO
import boto3

!pip install redshift_connector
import redshift_connector

def Load():
    #create a tuple for each table
    def df_to_tuple(df):
        tup = list(df.itertuples(index=False, name=None))
        return tup

    # Set up S3 envireonment for connection
    REGION = 'us-east-1'
    ACCESS_KEY_ID = 'AKIAYTMSKI5VMAF3UZPK'
    SECRET_ACCESS_KEY = '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'
    BUCKET_NAME = 'deindividualproject'
    s3csv = boto3.client('s3', 
    region_name = REGION,
    aws_access_key_id = ACCESS_KEY_ID,
    aws_secret_access_key = SECRET_ACCESS_KEY)

    # Put the CSV files into S3
    obj1 = open('/project/company_financials (in millions).csv', 'rb')
    obj2 = open('/project/glassdoor_reviews_cleaned.csv', 'rb')
    obj3 = open('/project/company_info.csv', 'rb')
    obj4 = open('/project/company_stockprices.csv', 'rb')
    obj5 = open('/project/company_ESG_scores.csv', 'rb')
    obj6 = open('/project/year.csv', 'rb')
    s3csv.put_object(Key='project/company_financials (in millions).csv', Body=obj1,Bucket=BUCKET_NAME)
    s3csv.put_object(Key='project/glassdoor_reviews_cleaned.csv', Body=obj2,Bucket=BUCKET_NAME)
    s3csv.put_object(Key='project/company_info.csv', Body=obj3,Bucket=BUCKET_NAME)
    s3csv.put_object(Key='project/company_stockprices.csv', Body=obj4,Bucket=BUCKET_NAME)
    s3csv.put_object(Key='project/company_ESG_scores.csv', Body=obj5,Bucket=BUCKET_NAME)
    s3csv.put_object(Key='project/year.csv', Body=obj6,Bucket=BUCKET_NAME)

    # Read the CSV file using pandas
    obj1 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'project/company_financials (in millions).csv')
    obj2 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'project/glassdoor_reviews_cleaned.csv')
    obj3 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'project/company_info.csv')
    obj4 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'project/company_stockprices.csv')
    obj5 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'project/company_ESG_scores.csv')
    obj6 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'project/year.csv')

    company_financials_table=pd.read_csv(io.BytesIO(obj1['Body'].read()), encoding='utf8')
    company_glassdoor_reviews_table=pd.read_csv(io.BytesIO(obj2['Body'].read()), encoding='utf8')
    company_info_table=pd.read_csv(io.BytesIO(obj3['Body'].read()), encoding='utf8')
    company_stockprices_table=pd.read_csv(io.BytesIO(obj4['Body'].read()), encoding='utf8')
    company_ESG_scores_table=pd.read_csv(io.BytesIO(obj5['Body'].read()), encoding='utf8')
    year_table=pd.read_csv(io.BytesIO(obj6['Body'].read()), encoding='utf8')
    
    # Create a schema and tables on postgres
    conn = psycopg2.connect(
       database="deproject", user='alysalws', password='Qwerty123',
        host="database-1.ckkjqqmywcta.us-east-1.rds.amazonaws.com", port= '5432'
    )
    conn.autocommit = True
    
    cursor = conn.cursor()
    cursor.execute("DROP SCHEMA IF EXISTS pp_schema CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.company_info_table CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.year_table CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.company_esg_scores_table CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.company_stockprices_table CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.company_financials_table CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.company_glassdoor_reviews_table CASCADE")

    schema = """
            CREATE SCHEMA pp_schema
            """
    
    company = """
            CREATE TABLE pp_schema.company_info_table (
            ticker varchar,
            security varchar,
            sector varchar,
            subindustry varchar,
            headquarters varchar,
            dateadded date,
            founded varchar,
            ticker_id int PRIMARY KEY
            )
            """
    
    year = """
            CREATE TABLE pp_schema.year_table (
            month_year_id int PRIMARY KEY,
            month_year varchar
            )
            """
    
    ESG_scores = """
            CREATE TABLE pp_schema.company_esg_scores_table (
            index int PRIMARY KEY,
            ticker varchar,
            ticker_id int not null references pp_schema.company_info_table("ticker_id"),
            security varchar,
            date date,
            month_year varchar,
            month_year_id int not null references pp_schema.year_table("month_year_id"),
            e_score float,
            s_score float,
            g_score float,
            total_score float
            )
            """
    
    stockprices="""
            CREATE TABLE pp_schema.company_stockprices_table (
            index int PRIMARY KEY,
            ticker_id int not null references pp_schema.company_info_table("ticker_id"),
            ticker varchar,
            security varchar,
            date date,
            month_year varchar,
            month_year_id int not null references pp_schema.year_table("month_year_id"),
            opening float,
            high float,
            low float,
            closing float,
            adj_close float,
            volume int
            )
            """
    
    financials = """
            CREATE TABLE pp_schema.company_financials_table (
            month_year_id int not null references pp_schema.year_table("month_year_id"),
            month_year varchar,
            ticker varchar,
            security varchar,
            ticker_id int not null references pp_schema.company_info_table("ticker_id"),
            total_assets int,
            total_liability int,
            total_shareholder_equity int,
            total_revenue int,
            gross_profit int,
            net_income int,
            total_operating_cashflow int,
            index int PRIMARY KEY
             )
            """
    
    glassdoor_reviews = """
            CREATE TABLE pp_schema.company_glassdoor_reviews_table (
            index int PRIMARY KEY,
            ticker varchar,
            ticker_id int not null references pp_schema.company_info_table("ticker_id"),
            date date,
            month_year varchar,
            month_year_id int not null references pp_schema.year_table("month_year_id"),
            author varchar,
            pros varchar,
            cons varchar,
            ratings float
            )
            """
    
    cursor.execute(schema)
    cursor.execute(company)
    cursor.execute(year)
    cursor.execute(ESG_scores)
    cursor.execute(stockprices)
    cursor.execute(financials)
    cursor.execute(glassdoor_reviews)

    # insert data into the tables created 
    company_info_tuple = df_to_tuple(company_info_table)
    company_ESG_scores_tuple = df_to_tuple(company_ESG_scores_table)
    company_stockprices_tuple = df_to_tuple(company_stockprices_table)
    company_financials_tuple = df_to_tuple(company_financials_table)
    year_tuple = df_to_tuple(year_table)
    company_glassdoor_reviews_tuple = df_to_tuple(company_glassdoor_reviews_table)

    for i in company_info_tuple:
        cursor.execute(
            "INSERT into pp_schema.company_info_table(ticker,security,sector,subindustry,headquarters,dateadded,founded,ticker_id) VALUES (%s, %s,%s,%s,%s, %s,%s,%s)", i)
    print("List has been inserted to company_info table")
    
    for i in year_tuple:
        cursor.execute("INSERT into pp_schema.year_table(month_year_id, month_year) VALUES (%s, %s)", i)
    print("List has been inserted to year table")
    
    for i in company_ESG_scores_tuple:
        cursor.execute(
            "INSERT into pp_schema.company_esg_scores_table(index,ticker,ticker_id,security,date,month_year,month_year_id,e_score,s_score,g_score,total_score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", i)
    print("List has been inserted to company_ESG_scores table")

    for i in company_stockprices_tuple: 
        cursor.execute(
            "INSERT into pp_schema.company_stockprices_table(index,ticker_id,ticker,security,date,month_year,month_year_id,opening,high,low,closing,adj_close,volume) VALUES (%s,%s, %s,%s, %s,%s, %s,%s, %s,%s,%s, %s,%s)", i)
    print("List has been inserted to company_stockprices table")
    
    for i in  company_glassdoor_reviews_tuple:
        cursor.execute("INSERT into pp_schema.company_glassdoor_reviews_table(index,ticker,ticker_id,date,month_year,month_year_id,author,pros,cons,ratings) VALUES (%s, %s,%s, %s, %s, %s, %s, %s, %s, %s)", i)
    print("List has been inserted to company_glassdoor_reviews table")
                   
    for i in company_financials_tuple:
        cursor.execute("INSERT into pp_schema.company_financials_table(month_year_id, month_year,ticker,security,ticker_id,total_assets,total_liability,total_shareholder_equity,total_revenue,gross_profit,net_income,total_operating_cashflow,index) VALUES (%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s, %s)", i)
    print("List has been inserted to company_financials table")
    
    # commit the changes to the database
    conn.commit()
    conn.close()

Load()
