import glob
import logging
import os
import sys

from pyspark.sql.types import DecimalType

from common import tvp_app_config, getSpark
from pyspark.sql.functions import col, sum, count

logger = logging.getLogger(__name__)

###
# Processing silver data by aggregation and move to existing gold layer as per Month and Year of block date
# This script will use spark to process data from silver layer for given month and year.
# Post aggregation, we can model final layer using DW modelling methods like Star, Snowflake etc.
#
# E.g.
# Assume block_date = 2024-11-24,
# then read input from - /silver/eth_transfers_data_quarantined/year=2024/month=11/*.parquet
# this script will process and overwrite it to existing gold layer partition -
#
# vertical data -
# /gold/eth_transfers_data_aggregated_vertical/year=2024/month=11/*.parquet
#
# protocol data -
# /gold/eth_transfers_data_aggregated_protocol/year=2024/month=11/*.parquet
# #


def main():
    block_date = None

    # To read all arguments
    args = sys.argv[1:]
    if len(args) > 0:
        block_date = args[0]
        logger.info(f"Block date given: {block_date}")

    logger.info("Transformation to Gold...")
    curr_path = None
    logger.info("Local dev flag --> " + str(tvp_app_config.get('dev_flag')))

    if tvp_app_config.get('dev_flag'):
        curr_path = os.getcwd()
        block_date = tvp_app_config.get('local_test_block_date')
        logger.info(f"Block date : {block_date}")
    else:
        pass

    # Read latest data from silver layer
    year_value = block_date.split("-")[0]
    month_value = block_date.split("-")[1]

    silver_path = tvp_app_config.get('silver_path').format(curr_path_value=curr_path, year_value=year_value,
                                                           month_value=month_value)
    logger.info("Aggregating data from -> " + silver_path)

    spark = getSpark("TVP gold")
    df = spark.read.format("parquet").load(silver_path). \
        selectExpr("tx_hash as safe_account", "block_date", "week_of_month", "week_of_year", "vertical", "protocol", \
                   "cast(amount_usd as Decimal(15,2)) as amount_usd")
    # df.printSchema()
    # df.show(truncate=False)

    ###
    # get distinct safe accounts by week and vertical
    df_safe_accounts_per_vertical = df.select("safe_account", "week_of_month", "week_of_year", "vertical").distinct() \
        .groupBy("week_of_month", "week_of_year", "vertical").agg(count("safe_account").alias("unique_safes"))

    # get aggregated usd amount for safe account per vertical and week
    df_total_safe_transactions_per_vertical = df.groupBy("week_of_month", "week_of_year", "vertical", "safe_account") \
        .agg(count("safe_account").alias("total_transactions_per_safe"), \
             sum("amount_usd").cast(DecimalType(15, 2)).alias("total_amount_usd_per_safe"))

    df_vertical_gold = df_safe_accounts_per_vertical.join(df_total_safe_transactions_per_vertical, \
                                                          on=["week_of_month", "week_of_year", "vertical"], how="inner")

    df_vertical_gold.show(10, truncate=False)
    vertical_gold_path = tvp_app_config.get('gold_path_vertical').format(curr_path_value=curr_path,
                                                                         year_value=year_value,
                                                                         month_value=month_value)
    logger.info(f"loading latest data into: {vertical_gold_path}")
    df_vertical_gold.write.mode("overwrite").format("parquet").save(vertical_gold_path)

    ###
    # get distinct safe accounts by week and protocol
    df_safe_accounts_per_protocol = df.select("safe_account", "week_of_month", "week_of_year", "protocol").distinct() \
        .groupBy("week_of_month", "week_of_year", "protocol").agg(count("safe_account").alias("unique_safes"))

    # get aggregated usd amount for safe account per vertical and week
    df_total_safe_transactions_per_protocol = df.groupBy("week_of_month", "week_of_year", "protocol", "safe_account") \
        .agg(count("safe_account").alias("total_transactions_per_safe"), \
             sum("amount_usd").cast(DecimalType(15, 2)).alias("total_amount_usd_per_safe"))

    df_protocol_gold = df_safe_accounts_per_protocol.join(df_total_safe_transactions_per_protocol, \
                                                          on=["week_of_month", "week_of_year", "protocol"], how="inner")

    df_protocol_gold.show(10, truncate=False)
    protocol_gold_path = tvp_app_config.get('gold_path_protocol').format(curr_path_value=curr_path,
                                                                         year_value=year_value,
                                                                         month_value=month_value)
    logger.info(f"loading latest data into: {protocol_gold_path}")
    df_protocol_gold.write.mode("overwrite").format("parquet").save(protocol_gold_path)


if __name__ == "__main__":
    main()
