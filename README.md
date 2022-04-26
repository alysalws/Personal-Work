**MSIN0166 Data Engineering Individual Assignment**

This project is about building an Extract, Transform and Load (ETL) pipeline with Apache Airflow that can automate the consolidation and update process of a variety of structured and unstructured data extracted from online sources for 66 financial companies listed on S&P 500. The ETL process involves the use of Amazon S3, Amazon Relational Database Service (RDS) (free-tier) as the data warehouses for data storage, Jupyter Notebook hosted on a cloud-based platform named Faculty.ai to write the scripts, and Python and Spark (SQL) as the scripting languages to transform and query the database. All scripts are version controlled on Github and the orchestration of the tasks is managed by Apache Airflow. No Machine Learning model has been deployed for this project in consideration of immense chanllenge of predicting company's performance base on a small dataset.

This Github repository stores two sets of .py files that can be run on faculty (in the 'faculty' folder), and on Airflow (in the 'Airflow' folder). Please note that only the .py files in these two folders are the final and executable versions. Files in 'Other' folder and 'workfile' folder consists of the working files that are not used for this project. 

To execute Airflow on Faculty, all file paths in the 'airflow.config' file should be changed from '/home/faculty/' to '/project', which is the directory where I store my airflow folder.

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

Disclaimer: This project is for purely for academic purposes. Contributor of this Github retains full copyright ownership of the content.
