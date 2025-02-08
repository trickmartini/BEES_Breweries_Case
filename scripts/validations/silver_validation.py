from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# silver validation
# This process verifies the integrity of the Silver layer table, by checking for inconsistent data.
# If any records contain null values in critical fields, an error is raised to prevent downstream issues.

# setup
spark = SparkSession.builder.appName("silver_validation").getOrCreate()
logger = spark._jvm.org.apache.log4j.LogManager.getLogger("silver_validation")
output_path = f"/opt/airflow/output/silver/breweries"

# Process

#read data from raw layer
logger.info(f"Reading {output_path}")
silver_data = spark.read.parquet(output_path)
invalid_data = silver_data.filter(
    (col("country").isNull()) |
    (col("id").isNull()) |
    (col("state").isNull()) |
    (col("city").isNull()) |
    (col("brewery_type").isNull())
)
invalid_records_count = invalid_data.count()

logger.info(f"Total invalid records found: {invalid_records_count}")

if invalid_records_count > 0:
    raise ValueError(f"Found {invalid_records_count} rows with inconsistent data in {output_path}")
