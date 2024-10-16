**Projective Description and Objectives:**

The objective of this project is to build an ETL pipeline to source, transform and manipulate online data from more than 4 different sources and use Apache Workflow to automate the workflow. The ETL pipeline uses Amazon S3 (AWS free tier) for data storage, Amazon RDS (PostgreSQL database) for hosting the metadata schema, and Faculty.ai as the main scripting platform.

This Github repository stores two sets of .py files that can be run on faculty (in the 'faculty' folder), and on Airflow (in the 'airflow' folder). 

**Data Extracted:**
1. Company Information (Source: Wikipedia)
2. Company Reviews (Source: Glassdoor)
3. Historical Stock Prices (Source: Yahoo Finance)
4. Historical Company Financials (Source: Yahoo Finance)
5. Historical ESG Scores (Source: Yahoo Finance)

**Schema:**

![image](https://github.com/user-attachments/assets/991b0d9e-3771-43a1-a613-3b6ccf7a712d)

**Explaination of Tables:**
1. <company_info_table> consists of the information of the financial companies trading on S&P 500. A ticker_id is designated the Primary Key (PK) to identify the 66 tickers.
2. <year_table> consists of 400 (adjustable in the Python script) months following January 2012. A month_year_id is designated the Primary Key (PK) to identify the 400 months.
3. <company_financials_table> consists of 7 financial indicators of the companies based on their annual reports. An index is designated the Primary Key to identify every row in them table. The ticker_id and month_year_id are the Foreign Keys that reference <company_info_table> and <year_table> . Worth noting that only 7 out of 56 financial indicators were stored in the dataframe with the intention to simplify the design of the schema in this project.
4. <company_stockprices_table> consists of the stock prices data. An index is designated the Primary Key to identify every row in the table. The ticker_id and month_year_id are the Foreign Keys that reference <company_info_table> and <year_table> .
5. <company_esg_scores_table> consists of 3 sustainability scores (E-score, S-score, G-score) of the companies. An index is designated the Primary Key to identify every row in the table. The ticker_id and month_year_id are the Foreign Keys that reference <company_info_table> and <year_table> .
6. <company_glassdoor_reviews_table> consists of the Glassdoor comments, ratings, and details of the reviewer. An index is designated the Primary Key to identify every row in  the table. The ticker_id and month_year_id are the Foreign Keys that reference <company_info_table> and <year_table> .

**Notes:** 

To connect to Spark, run the following commands on Server: 
1. sudo apt-get -y update
2. sudo apt-get -y upgrade
3. sudo apt-get -y install openjdk-8-jdk-headless
4. wget -q https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz -P /project
5. pushd /project
6. tar -xzvf spark-3.2.1-bin-hadoop3.2.tgz
7. popd
8. pip install -q findspark
9. pip install pyspark

**This project is for purely for personal research purposes. Contributor of this Github retains full copyright ownership of the content.**
