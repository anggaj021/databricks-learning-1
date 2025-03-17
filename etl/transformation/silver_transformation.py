from pyspark.sql.functions import to_date, col
from pyspark.sql import SparkSession
from config.settings import BRONZE_TABLE, SILVER_TABLE

# Initialize Spark Session
spark = SparkSession.builder.appName("SilverTransformation").getOrCreate()

def clean_data(df):
    """Remove duplicates and format date"""
    return df.dropDuplicates(["transaction_id"]).withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))

def save_to_silver(df):
    """Save cleaned data as Silver Delta Table"""
    df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)

def transform_data():
    df_silver = spark.read.table(BRONZE_TABLE)
    df_silver = clean_data(df_silver)
    save_to_silver(df_silver)

if __name__ == "__main__":
    transform_data()