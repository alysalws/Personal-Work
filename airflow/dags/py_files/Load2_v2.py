#!/usr/bin/env python
# coding: utf-8

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql import SQLContext
import pyarrow.parquet as pq
import numpy as np # linear algebra
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import sqlalchemy 

import io
from io import StringIO, BytesIO
import boto3
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

# Create another function to use Spark to read the schema tables and load the queries to S3
def Load2():
    company = "pp_schema.company_info_table"
    year = "pp_schema.year_table"
    ESG_scores = "pp_schema.company_esg_scores_table"
    stockprices = "pp_schema.company_stockprices_table"
    financials = "pp_schema.company_financials_table"
    glassdoor_reviews= "pp_schema.company_glassdoor_reviews_table"

    df_company = spark.read         .format("jdbc")         .option("url", postgres_uri)         .option("dbtable", company)         .option("user", user)         .option("password", password)         .option("driver", "org.postgresql.Driver")         .load()

    df_year = spark.read         .format("jdbc")         .option("url", postgres_uri)         .option("dbtable", year)         .option("user", user)         .option("password", password)         .option("driver", "org.postgresql.Driver")         .load()

    df_ESG_scores = spark.read         .format("jdbc")         .option("url", postgres_uri)         .option("dbtable", ESG_scores)         .option("user", user)         .option("password", password)         .option("driver", "org.postgresql.Driver")         .load()

    df_stockprices = spark.read         .format("jdbc")         .option("url", postgres_uri)         .option("dbtable", stockprices)         .option("user", user)         .option("password", password)         .option("driver", "org.postgresql.Driver")         .load()

    df_financials = spark.read         .format("jdbc")         .option("url", postgres_uri)         .option("dbtable", financials)         .option("user", user)         .option("password", password)         .option("driver", "org.postgresql.Driver")         .load()

    df_glassdoor_reviews = spark.read         .format("jdbc")         .option("url", postgres_uri)         .option("dbtable", glassdoor_reviews)         .option("user", user)         .option("password", password)         .option("driver", "org.postgresql.Driver")         .load()
    
    # Putting the panda dataframes in Parquet format in s3 bucket
    REGION = 'us-east-1'
    ACCESS_KEY_ID = '#####'
    SECRET_ACCESS_KEY = '####'
    BUCKET_NAME = '#####'

    s3parquet = boto3.client('s3', 
         region_name = REGION,
         aws_access_key_id = ACCESS_KEY_ID,
         aws_secret_access_key = SECRET_ACCESS_KEY
         )

    def store_in_S3(df, name):
    #name is a string that will be given to the parquet file when placed in S3. The files will be stored in a folder called parquets
        temp = df.toPandas()
        out_buffer=BytesIO()
        temp.to_parquet(out_buffer, index=True)
        s3parquet.put_object(Bucket=BUCKET_NAME, Key='parquets/' + name + '.parquet', Body=out_buffer.getvalue())

    store_in_S3(df_company, 'df_company')
    store_in_S3(df_year, 'df_year')
    store_in_S3(df_ESG_scores, 'df_ESG_scores')
    store_in_S3(df_stockprices, 'df_stockprices')
    store_in_S3(df_financials, 'df_financials')
    store_in_S3(df_glassdoor_reviews, 'df_glassdoor_reviews')
    
    # comment out the codes below if tempview already exists
    df_company.createTempView("company")
    df_year.createTempView("year")
    df_ESG_scores.createTempView("ESG_scores")
    df_stockprices.createTempView("stockprices")
    df_financials.createTempView("financials")
    df_glassdoor_reviews.createTempView("glassdoor_reviews")
    
    # query the data
    # Query 1: Top 10 companies by glassdoor ratings
    query1 = "SELECT glassdoor_reviews.ticker, company.security, AVG(glassdoor_reviews.ratings)                 FROM glassdoor_reviews                 INNER JOIN company ON glassdoor_reviews.ticker = company.ticker                 GROUP BY glassdoor_reviews.ticker,company.security                 ORDER BY AVG(glassdoor_reviews.ratings)                 LIMIT 10"

    # Query 2: Top 10 companies by highest average stockprices in 2021 
    query2 = "SELECT stockprices.ticker, company.security, ROUND((AVG(stockprices.high) + AVG(stockprices.low)/ 2),2) as Avg_stockprices                FROM stockprices                 INNER JOIN company ON stockprices.ticker = company.ticker                 WHERE stockprices.date <= '2021-12-31'                 GROUP BY stockprices.ticker,company.security                 ORDER BY Avg_stockprices DESC                 LIMIT 10"

    # Query 3: Top 10 companies by the highest average ESG scores in 2021 
    query3 = "SELECT ESG_scores.ticker, company.security, AVG(ESG_scores.total_score) as Avg_ESG_Score                 FROM ESG_scores                 INNER JOIN company ON ESG_scores.ticker = company.ticker                 WHERE ESG_scores.date > '2021-01-01' AND ESG_scores.date <= '2021-12-31'                 GROUP BY ESG_scores.ticker,company.security                 ORDER BY Avg_ESG_Score DESC                 LIMIT 10"
    
    # Query 4: Top 5 headquartered locations for SP&500 companies
    query4 = "SELECT COUNT(company.security) As number, company.headquarters                 FROM company                 GROUP BY company.headquarters                 ORDER BY number DESC                 LIMIT 5"
    
    # Query 5: Top 10 stocks by prices change (update daily)  
    query5 = "SELECT stockprices.ticker,company.security, stockprices.date, ROUND((MAX(stockprices.closing)- MIN(stockprices.opening)) / MIN(stockprices.opening) * 100, 2) as stockprice_change, stockprices.opening, stockprices.closing                 FROM stockprices                 JOIN company ON stockprices.ticker = company.ticker                 WHERE CAST(stockprices.date AS DATE) = CAST(current_date()-3 AS DATE)                 GROUP BY stockprices.ticker, company.security, stockprices.date,  stockprices.opening, stockprices.closing                 ORDER BY stockprice_change DESC                 LIMIT 10"

    stats1 = spark.sql(query1).toPandas()
    stats2 = spark.sql(query2).toPandas()
    stats3 = spark.sql(query3).toPandas()
    stats4 = spark.sql(query4).toPandas()
    stats5 = spark.sql(query5).toPandas()

    # Put the queries into S3 in CSV format
    Bucket=BUCKET_NAME
    csv_buffer = StringIO()
    stats1.to_csv(csv_buffer)
    s3csv.put_object(Key='Queries/Top 10 companies by glassdoor ratings.csv', Body=csv_buffer.getvalue(),Bucket=BUCKET_NAME)
    
    csv_buffer2 = StringIO()
    stats2.to_csv(csv_buffer2)
    s3csv.put_object(Key='Queries/Top 10 companies by highest average stockprices from 2020 to 2021.csv', Body=csv_buffer2.getvalue(),Bucket=BUCKET_NAME)
    
    csv_buffer3 = StringIO()
    stats3.to_csv(csv_buffer3)
    s3csv.put_object(Key='Queries/Top 10 companies by the highest average ESG scores in 2021.csv', Body=csv_buffer3.getvalue(),Bucket=BUCKET_NAME)
    
    csv_buffer4 = StringIO()
    stats4.to_csv(csv_buffer4)
    s3csv.put_object(Key='Queries/Top 5 headquartered locations for SP&500 companies.csv', Body=csv_buffer4.getvalue(),Bucket=BUCKET_NAME)
    
    csv_buffer5 = StringIO()
    stats5.to_csv(csv_buffer5)
    s3csv.put_object(Key='Queries/Top 10 stocks by stockprices change.csv', Body=csv_buffer5.getvalue(),Bucket=BUCKET_NAME)
    
    # Send query file to investors by Gmail
    def send_email(send_to, subject, df):
        send_from ="alysal724@gmail.com" # Dummy email account
        password = "Qwerty123!" # This is only a temporary password. It will be changed on 28 May 2022.
        message = """        <p><strong> Hi, here's the updated list of Top 10 Stocks by stockprice changes. Thanks! </strong></p>
        <p><br></p>
        <p><strong>Greetings </strong><br><strong>Alysa     </strong></p>
        """
        for receiver in send_to:
                multipart = MIMEMultipart()
                multipart["From"] = send_from
                multipart["To"] = receiver
                multipart["Subject"] = subject  
                attachment = MIMEApplication(df.to_csv())
                attachment["Content-Disposition"] = 'attachment; filename=" {}"'.format(f"{subject}.csv")
                multipart.attach(attachment)
                multipart.attach(MIMEText(message, "html"))
                server = smtplib.SMTP("smtp.gmail.com", 587)
                server.starttls()
                server.login(multipart["From"], password)
                server.sendmail(multipart["From"], multipart["To"], multipart.as_string())
                server.quit()
    send_email(['alysalws@gmail.com'],'Top 10 Stocks by stockprices change', stats5)  # the receiver email address can be changed to any email address

