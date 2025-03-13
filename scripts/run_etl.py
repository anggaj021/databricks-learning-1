import etl.ingestion.bronze_ingestion as bronze_ingestion
from etl.transformation.silver_transformation import clean_data
from etl.aggregation.gold_aggregation import aggregate_data
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("ETL_Pipeline").getOrCreate()

def main():
    print("Starting ETL process...")

    # Bronze Layer - Raw Data Ingestion
    print("Ingesting raw data (Bronze)...")
    bronze_df = bronze_ingestion(spark)
    bronze_df.show()

    # Silver Layer - Data Cleaning & Transformation
    print("Transforming data (Silver)...")
    silver_df = clean_data(bronze_df)
    silver_df.show()

    # Gold Layer - Aggregation
    print("Aggregating data (Gold)...")
    gold_df = aggregate_data(silver_df)
    gold_df.show()

    print("ETL process completed successfully!")

if __name__ == "__main__":
    main()
