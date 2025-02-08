import logging
import requests
import json
import os

from scripts.breweries import spark_config


def bronze_layer_transformation(raw_path, url, file_name, logger: logging):
    # Bronze Layer: Persist the raw data from the API in its native format or any format you
    # find suitable.

    if not os.path.isdir(raw_path):
        os.makedirs(raw_path)
    output_file = os.path.join(raw_path, file_name)

    logger.info("Downloading data from {}".format(url))
    # Fazendo a requisição GET
    response = requests.get(url)

    if response.status_code == 200:
        rawData = response.json()
        try:
            with open(output_file, "w") as f:
                json.dump(rawData, f)
            logger.info(f"Data saved in {raw_path}")
        except Exception as e:
            logger.warning(f"Error trying write file: {e}")
    else:
        logger.warning(f"Error when connecting API: {response.status_code}")

    return


def main():
    spark = spark_config.get_spark_session()
    logger = spark._jvm.org.apache.log4j.LogManager.getLogger("breweries_transformation")
    raw_path = f"/opt/airflow/output/raw/"

    # URL da API pública
    url = "https://api.openbrewerydb.org/breweries"
    bronze_layer_transformation(url=url, file_name="breweries.json", raw_path=raw_path, logger=logger)

if __name__ == "__main__":
    main()