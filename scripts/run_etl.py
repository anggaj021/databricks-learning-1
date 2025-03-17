from etl.ingestion.bronze_ingestion import ingest_data
from etl.transformation.silver_transformation import transform_data
from etl.aggregation.gold_aggregation import agg_data
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("ETL_Pipeline").getOrCreate()

def main():
    print("Starting ETL process...")

    # Bronze Layer - Raw Data Ingestion
    print("Ingesting raw data (Bronze)...")
    bronze_df = ingest_data()

    # Silver Layer - Data Cleaning & Transformation
    print("Transforming data (Silver)...")
    silver_df = transform_data()

    # Gold Layer - Aggregation
    print("Aggregating data (Gold)...")
    gold_df = agg_data()

    print("ETL process completed successfully!")

if __name__ == "__main__":
    main()
