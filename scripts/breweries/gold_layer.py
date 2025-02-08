from pyspark.sql import SparkSession
import logging


def gold_layer_transformation(spark: SparkSession, input_path, output_path, logger: logging):
    # Gold Layer: Create an aggregated view with the quantity of breweries per type and location.

    silver_data = spark.read.parquet(input_path)

    country_and_type = silver_data.select(
        "country",
        "brewery_type",
        "state"
    ).groupBy("country", "brewery_type").count()
    country_and_type.show()
    country_and_type.write.mode("overwrite").partitionBy("country").parquet(output_path + "country_type")

    logger.info(f"Data wrote in {output_path}country_type")

    detailed_location = silver_data.select(
        "country",
        "state",
        "city",
        "brewery_type"
    ).groupBy("country", "state", "city", "brewery_type").count().orderBy("country", "state", "city")
    detailed_location.show()

    detailed_location.write.mode("overwrite").partitionBy("country").parquet(output_path + "detailed_location")
    logger.info(f"Data wrote in {output_path}detailed_location")

