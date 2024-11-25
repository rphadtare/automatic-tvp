import json
import os
from dotenv import load_dotenv
import logging
import colorlog
from pyspark.sql import SparkSession
import requests
import pandas as pd

tvp_app_config = {}
logger = logging.getLogger(__name__)

if load_dotenv():
    colorlog.basicConfig(format=os.getenv("LOG_FORMAT"), level=os.getenv("LOG_LEVEL"), style="{", stream=None)
    tvp_app_config = {
        'app_name': os.getenv('APP'),
        'dune_api_key': os.getenv('DUNE_API_KEY'),
        'dune_tvp_query_id': os.getenv('DUNE_QUERY_ID'),
        'spark_log_level': os.getenv('SPARK_LOG_LEVEl'),
        'dune_query_execution_result_api': os.getenv('DUNE_QUERY_EXECUTION_RESULT_API'),
        'dev_flag': os.getenv('FLAG_FOR_LOCAL_TEST'),
        'local_test_block_date': os.getenv('LOCAL_TEST_BLOCK_DATE'),
        'stg_path': "{curr_path_value}/stg/raw_eth_transfers_data/block_date={block_date_value}/*.csv",
        'bronze_path': "{curr_path_value}/data/bronze/eth_transfers_data/block_date={block_date_value}/",
        'silver_path': "{curr_path_value}/data/silver/eth_transfers_data_quarantined/year={year_value}/month={month_value}/",
        'gold_path_vertical': "{curr_path_value}/data/gold/eth_transfers_data_aggregated_vertical/year={year_value}/month={month_value}/",
        'gold_path_protocol': "{curr_path_value}/data/gold/eth_transfers_data_aggregated_protocol/year={year_value}/month={month_value}/"
    }
    logger.info("APP Env loaded !!")
    logger.debug(tvp_app_config)


def getSpark(appName):
    spark = SparkSession.builder.appName(appName).master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel(tvp_app_config.get("spark_log_level"))
    return spark


def get_dune_query_result(logger_inner, app_config):
    headers = {"X-DUNE-API-KEY": app_config.get('dune_api_key')}
    query_id = app_config.get('dune_tvp_query_id')
    url = app_config.get('dune_query_execution_result_api').format(query_id=query_id, limit_value=1, offset_value=0)
    logger_inner.info(f"URL - {url}")
    response = requests.request("GET", url=url, headers=headers)

    print(json.loads(response.text))
    query_res = json.loads(response.text)["result"]["rows"]
    res_df = pd.DataFrame.from_dict(query_res)
    return res_df
