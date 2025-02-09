from pyspark.sql import SparkSession

# Gold Layer: Create an aggregated view with the quantity of breweries per type and location.

# setup
spark = SparkSession.builder.appName("breweries transform").getOrCreate()
logger = spark._jvm.org.apache.log4j.LogManager.getLogger("breweries_transformation")
input_path = f"/opt/airflow/output/silver/breweries"
output_path = f"/opt/airflow/output/gold/breweries_by_location"

# Process
silver_data = spark.read.parquet(input_path)

detailed_location = silver_data.select(
    "country",
    "state",
    "city",
    "brewery_type"
).groupBy("country", "state", "city", "brewery_type").count().orderBy("country", "state", "city")
detailed_location.show()

detailed_location.write.mode("overwrite").partitionBy("country").parquet(output_path)

logger.info(f"Data wrote in: {output_path}")

