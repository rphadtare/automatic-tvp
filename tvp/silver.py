import glob
import logging
import os
import sys
from calendar import monthcalendar

from common import get_dune_query_result, tvp_app_config, getSpark
import pandas as pd

logger = logging.getLogger(__name__)
from pyspark.sql.functions import to_date, lit, year, month, weekofyear, udf, day, dayofmonth, col


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

def get_week_of_month(year, month, day):
    return next(
        (week_number for week_number, days_of_week in enumerate(monthcalendar(year, month), start=1) if day in days_of_week),
        None,
    )


udf_get_week = udf(get_week_of_month)


def main():
    block_date = None

    # To read all arguments
    args = sys.argv[1:]
    if len(args) > 0:
        block_date = args[0]
        logger.info(f"Block date given: {block_date}")

    logger.info("Ingestion to Silver...")
    curr_path = None
    logger.info("Local dev flag --> " + str(tvp_app_config.get('dev_flag')))

    if tvp_app_config.get('dev_flag'):
        curr_path = os.getcwd()
        block_date = tvp_app_config.get('local_test_block_date')
        logger.info(f"Block date : {block_date}")
    else:
        pass

    # Read latest data from bronze layer
    bronze_path = tvp_app_config.get('bronze_path').format(curr_path_value=curr_path, block_date_value=block_date)
    logger.info("Incrementally updating into silver layer for data -> " + bronze_path)

    spark = getSpark("TVP silver")
    df = spark.read.options(header=True, inferSchema=True).format("csv").load(path=bronze_path)
    # df.printSchema()
    # df.show(truncate=False)

    incr_df = df.select("token_category", "event_name", "tx_hash", "vertical", "protocol", "amount_usd") \
        .filter("event_name is not null and event_name = 'Transfer'") \
        .filter("amount_usd is not null and amount_usd > 0") \
        .filter("tx_hash is not null and vertical is not null and protocol is not null") \
        .drop_duplicates() \
        .withColumn("block_date", to_date(lit(str(block_date)), format("yyyy-MM-dd"))) \
        .withColumn("year", year("block_date")) \
        .withColumn("month", month("block_date")) \
        .withColumn("week_of_year", weekofyear("block_date"))\
        .withColumn("week_of_month", udf_get_week(col("year"), col("month"), dayofmonth(col("block_date"))))\
        .drop("year", "month")

    # res_df.show(truncate=False)
    # res_df.printSchema()
    year_value = block_date.split("-")[0]
    month_value = block_date.split("-")[1]

    silver_path = tvp_app_config.get('silver_path').format(curr_path_value=curr_path, year_value=year_value, month_value=month_value)
    logger.info(f"reading existing data: {silver_path}")
    full_df = incr_df.limit(0)

    if not os.path.exists(silver_path):
        os.makedirs(silver_path)
    else:
        full_df = spark.read.format("parquet").load(silver_path)
        # full_df.show(truncate=False)

    res_df = full_df.unionByName(incr_df).drop_duplicates()
    res_df.write.mode("overwrite").format("parquet").save(silver_path)


if __name__ == "__main__":
    main()
