#!/usr/bin/env python
# coding: utf-8

# In[2]:


# import all the required packages
import pandas as pd
get_ipython().system('pip install yfinance')
import yfinance as yf
import datetime
from datetime import date
import time
import requests
import io
import json
get_ipython().system('pip install yahoofinancials')
from yahoofinancials import YahooFinancials 
from bs4 import BeautifulSoup
import numpy as np
import time
get_ipython().system('pip install yesg')
import yesg
get_ipython().system('pip install yahoo_fin')
import yahoo_fin.stock_info as yfs
from functools import reduce
get_ipython().system('pip install pyspark')
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql import SQLContext
get_ipython().system('pip3 install config')
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
get_ipython().system('pip install pendulum')
import pendulum
# DAG
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator


# In[3]:


# Connoect with Spark
spark = SparkSession     .builder     .appName("PySpark App")     .config("spark.jars", "/project/postgresql-42.3.2.jar")     .getOrCreate()

sqlContext = SQLContext(spark)

hostname = "database-1.ckkjqqmywcta.us-east-1.rds.amazonaws.com"
dbname = "deproject"
dbtable = "pp_schema"
user = "alysalws"
password = "Qwerty123"
postgres_uri = "jdbc:postgresql://" + hostname + ":5432/" + dbname

# Set up S3 envireonment for connection
REGION = 'us-east-1'
ACCESS_KEY_ID = 'AKIAYTMSKI5VMAF3UZPK'
SECRET_ACCESS_KEY = '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'
BUCKET_NAME = 'deindividualproject'
s3csv = boto3.client('s3', 
region_name = REGION,
aws_access_key_id = ACCESS_KEY_ID,
aws_secret_access_key = SECRET_ACCESS_KEY)


# In[4]:


from Extract import Extract
from Transform import Transform
from Load1 import Load1
from Load2 import Load2


# In[7]:


with DAG(
    dag_id='ETL_DAG',
    description="ETL for financial companies in S&P500 ticker",
    schedule_interval='@daily', 
    start_date=pendulum.datetime(2022, 4, 26, tz="UTC"),
    catchup=False,
    default_args={
    "owner":"alysalws",
    "depends_on_past": True,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=3)},
    tags=['ETL']) as dag:

    t1 = PythonOperator(
        task_id='Extract',
        python_callable=Extract,
    )
    
    t2 = PythonOperator(
        task_id='Transform',
        python_callable=Transform,
    )
    
    t3 = PythonOperator(
        task_id='Load1',
        python_callable=Load1,
    )

    t4 = PythonOperator(
        task_id='Load2',
        python_callable=Load2,
    )

    t1 >> t2 >> t3 >> t4


# In[ ]:




