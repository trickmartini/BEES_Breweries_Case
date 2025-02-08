from pyspark.sql import SparkSession


def get_spark_session():
    spark = (SparkSession.builder.appName("breweries transform")
             .getOrCreate())
    spark.sparkContext.setLogLevel("INFO")
    return spark
