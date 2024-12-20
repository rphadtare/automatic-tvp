import logging
import sys

sys.path.append(".")
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col, sum, count

from spark_scripts.common import tvp_app_config, get_spark

logger = logging.getLogger(__name__)


def sample():
    print("Hello sample!")


###
# This script is for Top K analysis using gold layer
# #


def main(arg1, master_="spark:// spark-master:7077", date_override_flag=True):
    block_date_ = arg1
    logger.info("Top K analysis using vertical data")

    curr_path = "data"
    logger.info("Local dev flag --> " + tvp_app_config.get('dev_flag'))
    logger.info("arg1 --> " + arg1)
    logger.info("date_override_flag --> " + str(date_override_flag))

    if tvp_app_config.get('dev_flag') and not date_override_flag:
        block_date_ = tvp_app_config.get('local_test_block_date')

    logger.info(f"Block date : {block_date_}")
    spark = get_spark("TOP K analysis", master=master_)
    spark.read.format("delta").load(f"{curr_path}/gold/eth_transfers_data_aggregated_vertical/"). \
        createOrReplaceTempView("eth_transfers_data_aggregated_vertical")

    # to find out top 5 verticals as per the highest transactions count
    spark \
        .sql("""
                select vertical, sum(total_transactions_per_safe) as highest_transaction_count 
                from eth_transfers_data_aggregated_vertical group by 1 order by 2 desc
            """) \
        .limit(5).show(truncate=False)

    # to find out top 5 verticals as per the highest transactions volume
    spark \
        .sql("""
                select vertical, sum(total_amount_usd_per_safe) as highest_transaction_volume
                from eth_transfers_data_aggregated_vertical group by 1 order by 2 desc
            """) \
        .limit(5).show(truncate=False)

    logger.info("Top K analysis using protocol data")
    spark.read.format("delta").load(f"{curr_path}/gold/eth_transfers_data_aggregated_protocol/"). \
        createOrReplaceTempView("eth_transfers_data_aggregated_protocol")

    # to find out top 5 protocol as per the highest transactions count
    spark \
        .sql("""
            select protocol, sum(total_transactions_per_safe) as highest_transaction_count 
            from eth_transfers_data_aggregated_protocol group by 1 order by 2 desc
            """) \
        .limit(5).show(truncate=False)

    # to find out top 5 protocol as per the highest transactions volume
    spark \
        .sql("""
            select protocol, sum(total_amount_usd_per_safe) as highest_transaction_volume
            from eth_transfers_data_aggregated_protocol group by 1 order by 2 desc
            """) \
        .limit(5).show(truncate=False)


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
