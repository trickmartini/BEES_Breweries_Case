import requests
import json
import os
from pyspark.sql import SparkSession
# setup
spark = SparkSession.builder.appName("breweries transform").getOrCreate()
logger = spark._jvm.org.apache.log4j.LogManager.getLogger("breweries_transformation")
raw_path = f"/opt/airflow/output/raw/"
url = "https://api.openbrewerydb.org/breweries"

# process
if not os.path.isdir(raw_path):
    os.makedirs(raw_path)

output_file = os.path.join(raw_path, "brew.json")
logger.info("Downloading data from {}".format(url))

response = requests.get(url)

if response.status_code == 200:
    rawData = response.json()
    try:
        with open(output_file, "w") as f:
            json.dump(rawData, f)
        logger.info(f"Data saved in {output_file}")
    except Exception as e:
        logger.warning(f"Error trying write file: {e}")
        raise ValueError(f"Error trying write file: {e}")
else:
    logger.warning(f"Error when connecting API: {response.status_code}")
    raise ValueError(f"Error when connecting API: {response.status_code}")