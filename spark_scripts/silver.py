###
# Processing bronze data by filtering, cleansing and incrementally appending data to existing silver layer as per Month of block date
# This script will use spark to process data from bronze layer for given block date.
# Post quarantine operations, we will model it with fewer attributes which will act as input for next operations in ETL process
#
# E.g.
# Assume block_date = 2024-11-24,
# then read input from - /bronze/eth_transfers_data/block_date=2024-11-24/data.csv
# this script will process and append it to existing silver layer -
# /silver/eth_transfers_data_quarantined/year=2024/month=11/*.parquet
# #

import glob
import logging
import os
import sys

import pandas as pd
from pyspark.sql.functions import to_date, lit, year, month, weekofyear, udf, day, dayofmonth, col, date_format
from pyspark.sql import DataFrame, SparkSession
from delta import *
from delta.tables import *
from pyspark.sql.types import IntegerType

from spark_scripts.common import tvp_app_config, getSpark

logger = logging.getLogger(__name__)

##
# To quarantined latest bronze data
# #
def quarantined_bronze_data(spark: SparkSession, raw_df: DataFrame, block_date):
    # converting normal function to UDF
    # udf_get_week = udf(get_week_of_month)
    # spark.udf.register("udf_get_week", udf_get_week)

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    incr_df = raw_df.select("token_category", "event_name", "tx_hash", "vertical", "protocol", "amount_usd") \
        .filter("event_name is not null and event_name = 'Transfer'") \
        .filter("amount_usd is not null and amount_usd > 0") \
        .filter("tx_hash is not null and vertical is not null and protocol is not null") \
        .drop_duplicates() \
        .withColumn("block_date", to_date(lit(str(block_date)), format("yyyy-MM-dd"))) \
        .withColumn("year", year("block_date")) \
        .withColumn("month", month("block_date")) \
        .withColumn("week_of_year", weekofyear("block_date")) \
        .withColumn("day", dayofmonth(col("block_date"))) \
        .withColumn("week_of_month", date_format(col("block_date"), "W"))\
        .selectExpr("token_category", "event_name", "tx_hash", "vertical", "protocol", "amount_usd",\
                    "block_date", "week_of_year", "week_of_month", "year", "month")


    return incr_df


def main(arg1, date_override_flag=True):
    block_date = arg1

    logger.info("Ingestion to Silver...")
    curr_path = "data"
    logger.info("Local dev flag --> " + str(tvp_app_config.get('dev_flag')))
    logger.info("arg1 --> " + arg1)
    logger.info("date_override_flag --> " + str(date_override_flag))

    if tvp_app_config.get('dev_flag') and not date_override_flag:
        block_date = tvp_app_config.get('local_test_block_date')

    logger.info(f"Block date : {block_date}")
    # Read latest data from bronze layer
    bronze_path = tvp_app_config.get('bronze_path').format(curr_path_value=curr_path, block_date_value=block_date)
    logger.info("Incrementally updating into silver layer for data -> " + bronze_path)

    spark = getSpark("TVP silver")
    # spark.sparkContext.addPyFile(path="/Users/rohitphadtare/IdeaProjects/automatic-tvp/tvp/util.py")
    # spark.sparkContext.addPyFile(path="/Users/rohitphadtare/IdeaProjects/automatic-tvp/tvp/silver.py")
    # print(spark.sparkContext.listFiles)

    df = spark.read.options(header=True, inferSchema=True).format("csv").load(path=bronze_path)
    # df.printSchema()
    # df.show(truncate=False)
    incr_df = quarantined_bronze_data(spark=spark, raw_df=df, block_date=block_date)

    # res_df.show(truncate=False)
    # incr_df.printSchema()
    year_value = int(block_date.split("-")[0])
    month_value = int(block_date.split("-")[1])

    silver_path = tvp_app_config.get('silver_path').format(curr_path_value=curr_path)
    logger.info(f"reading existing data: {silver_path}")
    full_df = incr_df.limit(0)

    if not os.path.exists(silver_path):
        os.makedirs(silver_path)
        logger.info(f"Directory created: {silver_path}")
    else:
        DeltaTable.forPath(spark, silver_path).toDF().printSchema()
        full_df = DeltaTable.forPath(spark, silver_path).toDF().filter(f"year={year_value} and month={month_value}")
        # full_df.printSchema()
        # full_df = spark.read.format("delta").load(silver_path)
        # full_df.show(truncate=False)

    res_df = incr_df.unionByName(full_df).drop_duplicates()

    # res_df.show(truncate=False)
    res_df.write.mode("overwrite").option("partitionOverwriteMode", "dynamic") \
        .format("delta").partitionBy("year", "month").save(silver_path)


if __name__ == "__main__":
    block_date = None
    # To read all arguments
    args = sys.argv[1:]
    if len(args) > 0:
        block_date = args[0]
        logger.info(f"Block date given: {block_date}")

    main(block_date)
