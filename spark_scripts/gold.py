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

# import glob
import logging
# import os
import sys
import time

sys.path.append(".")

from delta import DeltaTable
from pyspark.sql.types import DecimalType, IntegerType

from spark_scripts.common import tvp_app_config, get_spark
from pyspark.sql.functions import col, sum, count, year, lit
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


##
# Get summarised information per week and vertical
# #
def get_safe_per_vertical_week_df(silver_df: DataFrame):
    df_safe_accounts_per_vertical = silver_df.select("safe_account", "week_of_month", "week_of_year",
                                                     "vertical").distinct() \
        .groupBy("week_of_month", "week_of_year", "vertical").agg(count("safe_account").alias("unique_safes"))

    # get aggregated usd amount for safe account per vertical and week
    df_total_safe_transactions_per_vertical = silver_df.groupBy("week_of_month", "week_of_year", "vertical",
                                                                "safe_account") \
        .agg(count("safe_account").alias("total_transactions_per_safe"),
             sum("amount_usd").cast(DecimalType(15, 2)).alias("total_amount_usd_per_safe"))

    df_vertical_gold = df_safe_accounts_per_vertical.join(df_total_safe_transactions_per_vertical,
                                                          on=["week_of_month", "week_of_year", "vertical"], how="inner")

    # df_vertical_gold.show(10, truncate=False)
    return df_vertical_gold


##
# Get summarised information per week and protocol
# #
def get_safe_per_protocol_week_df(silver_df: DataFrame):
    df_safe_accounts_per_protocol = silver_df.select("safe_account", "week_of_month", "week_of_year",
                                                     "protocol").distinct() \
        .groupBy("week_of_month", "week_of_year", "protocol").agg(count("safe_account").alias("unique_safes"))

    # get aggregated usd amount for safe account per vertical and week
    df_total_safe_transactions_per_protocol = silver_df.groupBy("week_of_month", "week_of_year", "protocol",
                                                                "safe_account") \
        .agg(count("safe_account").alias("total_transactions_per_safe"),
             sum("amount_usd").cast(DecimalType(15, 2)).alias("total_amount_usd_per_safe"))

    df_protocol_gold = df_safe_accounts_per_protocol.join(df_total_safe_transactions_per_protocol,
                                                          on=["week_of_month", "week_of_year", "protocol"], how="inner")

    # df_protocol_gold.show(10, truncate=False)
    return df_protocol_gold


def main(arg1, master_="spark:// spark-master:7077", date_override_flag=True):
    block_date_ = arg1

    logger.info("Transformation to Gold...")
    curr_path = "data"
    logger.info("Local dev flag --> " + tvp_app_config.get('dev_flag'))
    logger.info("arg1 --> " + arg1)
    logger.info("date_override_flag --> " + str(date_override_flag))

    if tvp_app_config.get('dev_flag') and not date_override_flag:
        block_date_ = tvp_app_config.get('local_test_block_date')

    logger.info(f"Block date : {block_date_}")

    # Read latest data from silver layer
    year_value = int(block_date_.split("-")[0])
    month_value = int(block_date_.split("-")[1])

    silver_path = tvp_app_config.get('silver_path').format(curr_path_value=curr_path)
    logger.info("Aggregating data from -> " + silver_path)

    spark = get_spark("Ingestion_to_gold", master=master_)
    df = DeltaTable.forPath(spark, silver_path).toDF().filter(f"year = {year_value} and month = {month_value}"). \
        selectExpr("tx_hash as safe_account", "block_date", "week_of_month", "week_of_year", "vertical", "protocol",
                   "cast(amount_usd as Decimal(15,2)) as amount_usd")
    # df.printSchema()
    # df.show(truncate=False)

    ###
    # get distinct safe accounts by week and vertical
    spark.sparkContext.setJobDescription("Calculating safe accounts by week and vertical")
    df_vertical_gold = get_safe_per_vertical_week_df(df) \
        .withColumn("year", lit(year_value).cast(IntegerType())) \
        .withColumn("month", lit(month_value).cast(IntegerType()))

    vertical_gold_path = tvp_app_config.get('gold_path_vertical').format(curr_path_value=curr_path)
    logger.info(f"loading latest data into: {vertical_gold_path}")
    df_vertical_gold.write.mode("overwrite").option("partitionOverwriteMode", "dynamic") \
        .format("delta").partitionBy("year", "month").save(vertical_gold_path)

    ###
    # get distinct safe accounts by week and protocol
    spark.sparkContext.setJobDescription("Calculating safe accounts by week and protocol")
    df_protocol_gold = get_safe_per_protocol_week_df(silver_df=df) \
        .withColumn("year", lit(year_value).cast(IntegerType())) \
        .withColumn("month", lit(month_value).cast(IntegerType()))

    protocol_gold_path = tvp_app_config.get('gold_path_protocol').format(curr_path_value=curr_path)
    logger.info(f"loading latest data into: {protocol_gold_path}")
    df_protocol_gold.write.mode("overwrite").option("partitionOverwriteMode", "dynamic") \
        .format("delta").partitionBy("year", "month").save(protocol_gold_path)


if __name__ == "__main__":
    block_date = None
    # To read all arguments
    args = sys.argv[1:]
    if len(args) > 0:
        block_date = args[0]
        master = args[1]
        logger.info(f"Block date: {block_date}")
        logger.info(f"Master for spark app: {block_date}")
    else:
        block_date = "2024-11-23"
        master = "local[2]"

    main(block_date, master)
