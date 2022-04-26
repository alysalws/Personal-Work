
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
import time
import yesg
import yahoo_fin.stock_info as yfs
from functools import reduce
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql import SQLContext
import pickle 
import random 
# S3 and postgres
import psycopg2 
from io import StringIO
import boto3
import io
from io import StringIO, BytesIO
import boto3
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import pendulum
# DAG
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator



# Set up S3 envireonment for connection
REGION = 'us-east-1'
ACCESS_KEY_ID = 'AKIAYTMSKI5VMAF3UZPK'
SECRET_ACCESS_KEY = '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'
BUCKET_NAME = 'deindividualproject'
s3csv = boto3.client('s3', 
        region_name = REGION,
        aws_access_key_id = ACCESS_KEY_ID,
        aws_secret_access_key = SECRET_ACCESS_KEY)


# Saving csv files to s3
def save_csv_s3(df, folder, name):
    csv_buffer=StringIO()
    df.to_csv(csv_buffer, index=True)
    response=s3csv.put_object(Body=csv_buffer.getvalue(),
                           Bucket=BUCKET_NAME,
                           Key=folder + "/" + name + ".csv")


from py_files import Extract_v2, Transform_v2, Load1_v2, Load2_v2

# Extraction Stage
Extract_ETL=Extract_v2.Extract


# Transformation Stage
Transform_ETL=Transform_v2.Transform

# Loading1 Stage
Load1_ETL = Load1_v2.Load1


# Loading2 Stage
Load2_ETL = Load2_v2.Load2



with DAG(
    dag_id='ETL_DAG_v2',
    description="ETL for financial companies in S&P500 ticker",
    schedule_interval='@daily', 
    start_date=pendulum.datetime(2022, 4, 25, tz="UTC"),
    catchup=False,
    default_args={
    "owner":"alysalws",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=3)},
    tags=['ETL']) as dag:
        
    t1 = PythonOperator(
        task_id='Extract',
        python_callable=Extract_ETL,
    )
    
    t2 = PythonOperator(
        task_id='Transform',
        python_callable=Transform_ETL,
    )
    
    t3 = PythonOperator(
        task_id='Load1',
        python_callable=Load1_ETL,
    )

    t4 = PythonOperator(
        task_id='Load2',
        python_callable=Load2_ETL,
    )

    t1 >> t2 >> t3 >> t4


# In[23]:




