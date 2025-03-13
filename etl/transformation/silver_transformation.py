from pyspark.sql.functions import to_date, col
from pyspark.sql import SparkSession
from config.settings import CATALOG_NAME, SCHEMA_NAME, BRONZE_TABLE, SILVER_TABLE

# Initialize Spark Session
spark = SparkSession.builder.appName("SilverTransformation").getOrCreate()

# Ensure catalog and schema exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

# Fully qualified table names
BRONZE_TABLE_FQN = f"{CATALOG_NAME}.{SCHEMA_NAME}.{BRONZE_TABLE}"
SILVER_TABLE_FQN = f"{CATALOG_NAME}.{SCHEMA_NAME}.{SILVER_TABLE}"

def clean_data(df):
    """Remove duplicates and format date"""
    return df.dropDuplicates(["transaction_id"]).withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))

def save_to_silver(df):
    """Save cleaned data as Silver Delta Table"""
    try:
        df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE_FQN)
        print(f"✅ Successfully saved data to {SILVER_TABLE_FQN}")
    except Exception as e:
        print(f"❌ Error saving data: {str(e)}")

if __name__ == "__main__":
    try:
        df_silver = spark.read.table(BRONZE_TABLE_FQN)
        df_silver = clean_data(df_silver)
        save_to_silver(df_silver)
    except Exception as e:
        print(f"❌ Error reading Bronze table: {str(e)}")
