#tasks.py

import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from pyspark.sql import SparkSession
import requests
from datetime import datetime

class URLModel(BaseModel):
    url: str

router = APIRouter(
    prefix='/tasks',
    tags=['Tasks']
)

@router.post('/ingest', status_code=200)
def ingest_data(url_model: URLModel):
    url = url_model.url
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.text
        # Save data to a temporary file for Spark to process
        file_path = '/tmp/data.csv'
        with open(file_path, 'w') as f:
            f.write(data)
        
        # Process data with Spark and dynamic table name
        process_data(file_path, url)
        return {"message": "Data ingested and processed successfully"}
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=400, detail=str(e))



def process_data(file_path: str, url: str):
    # Ensure the JDBC driver path is correct
    jdbc_driver_path = "/usr/local/lib/pgJDBC_42.7.4.jar"
    if not os.path.isfile(jdbc_driver_path):
        raise Exception(f"JDBC driver not found at {jdbc_driver_path}")

    spark = SparkSession.builder \
        .appName("Data Ingestion") \
        .config("spark.jars", jdbc_driver_path) \
        .config("spark.driver.extraClassPath", jdbc_driver_path) \
        .getOrCreate()

    # Read CSV data with comma delimiter
    df = spark.read.csv(file_path, header=False, inferSchema=True, sep=',', ignoreLeadingWhiteSpace=True)
    
    # Print the schema to debug column names
    df.printSchema()
    
    # Use `show` to print a few rows of the dataframe
    df.show(5)

    # Generate dynamic column names based on the number of columns
    columns = [f"column_{i}" for i in range(len(df.columns))]
    
    # Rename columns dynamically
    for i, col_name in enumerate(columns):
        df = df.withColumnRenamed(f"_c{i}", col_name)

    # Print schema after renaming columns
    df.printSchema()
    
    # No filter condition, simply insert all data
    transformed_df = df  # No filtering applied
    
    # Generate dynamic table name based on URL and current timestamp
    table_name_prefix = os.path.basename(url).split('.')[0]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = f"{table_name_prefix}_{timestamp}_transformed"

    # Write data to PostgreSQL
    transformed_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/fastapi") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "1219") \
        .option("driver", "org.postgresql.Driver") \
        .save()

    spark.stop()
