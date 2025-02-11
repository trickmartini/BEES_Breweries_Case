import logging
from datetime import time

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


# response = requests.get(url)

def fetch_data():
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed (Attempt {attempt + 1}): {e}")
            time.sleep(2)
    raise Exception("Max retries reached. API unavailable.")


response = fetch_data()
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
    raise ValueError(f"Error when connecting API: {response.status_code}")
