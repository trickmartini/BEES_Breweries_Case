from pyspark.sql import SparkSession

# Gold Layer: Create an aggregated view with the quantity of breweries per type and location.

# setup
spark = SparkSession.builder.appName("breweries transform").getOrCreate()
logger = spark._jvm.org.apache.log4j.LogManager.getLogger("breweries_transformation")
input_path = f"/opt/airflow/output/silver/breweries"
output_path = f"/opt/airflow/output/gold/breweries_by_location"

logger.info(f"Reading data from: {input_path}")
# Read data from silver layer
silver_data = spark.read.parquet(input_path)

if silver_data.rdd.isEmpty():
    logger.error("Input data is empty. No process will be performed.")
    raise ValueError("Input data is empty.")

detailed_location = (
    silver_data
    .groupBy("country", "state", "city", "brewery_type")
    .count()
    .orderBy("country", "state", "city")
)

if detailed_location.rdd.isEmpty():
    logger.error("Output dataframe is empty. No process will be performed.")
    raise ValueError("Output dataframe is empty. No data to write.")

detailed_location.show()

detailed_location.write.mode("overwrite").partitionBy("country").parquet(output_path)

logger.info(f"Data written to: {output_path}")
