from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# b. Silver Layer: Transform the data to a columnar storage format such as parquet or delta,
# and partition it by brewery location. Please explain any other transformations you
# perform.

# setup
spark = SparkSession.builder.appName("breweries transform").getOrCreate()
logger = spark._jvm.org.apache.log4j.LogManager.getLogger("breweries_transformation")
raw_path = f"/opt/airflow/output/raw/brew.json"
output_path = f"/opt/airflow/output/silver/breweries"

# Define Schema for structured data ingestion
schema = StructType([
    StructField("country", StringType(), True),
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
    StructField("street", StringType(), True),
    StructField("address_1", StringType(), True),
    StructField("address_2", StringType(), True),
    StructField("address_3", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True)
])

#read data from raw layer
logger.info(f"Reading {raw_path}")
raw_data = spark.read.json(raw_path, schema=schema)
if raw_data.rdd.isEmpty():
    logger.warning("Raw data is empty. No process will be performed.")
    raise ValueError("Raw data is empty.")

silver_data = (raw_data
               # cast latitude and longitude as double to maximize compatibility with BI tools, for geographical KPI.
               .withColumn("latitude", col("latitude").cast(DoubleType()))
               .withColumn("longitude", col("longitude").cast(DoubleType()))
               .drop(col("street"))  # street has the same content as address_1, opt for keep address_1 for
               # compatibility with address_2 and 3
               )
if silver_data.rdd.isEmpty():
    logger.warning("Silver data is empty. No data to be writen.")
    raise ValueError("Silver data is empty.")

silver_data.write.mode("overwrite").partitionBy("country").parquet(output_path)
logger.info(f"Data saved in silver layer: {output_path}")
