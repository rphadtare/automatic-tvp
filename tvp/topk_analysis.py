import glob
import logging
import os
import sys

from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col, sum, count

from tvp.common import tvp_app_config, getSpark

logger = logging.getLogger(__name__)


def sample():
    print("Hello sample!")


###
# This script is for Top K analysis using gold layer
# #


def main():
    curr_path = None
    logger.info("Local dev flag --> " + str(tvp_app_config.get('dev_flag')))

    if tvp_app_config.get('dev_flag'):
        curr_path = os.getcwd()
        block_date = tvp_app_config.get('local_test_block_date')
        logger.info(f"Block date : {block_date}")
    else:
        pass

    logger.info("Top K analysis using vertical data")
    spark = getSpark("TOP K analysis")
    spark.read.format("parquet").load(f"{curr_path}/data/gold/eth_transfers_data_aggregated_vertical/year=*/month=*"). \
        createOrReplaceTempView("eth_transfers_data_aggregated_vertical")

    # to find out top 5 verticals as per the highest transactions count
    spark \
        .sql(
        """
                select vertical, sum(total_transactions_per_safe) as highest_transaction_count 
                from eth_transfers_data_aggregated_vertical group by 1 order by 2 desc
            """) \
        .limit(5).show(truncate=False)

    # to find out top 5 verticals as per the highest transactions volume
    spark \
        .sql(
        """
                select vertical, sum(total_amount_usd_per_safe) as highest_transaction_volume
                from eth_transfers_data_aggregated_vertical group by 1 order by 2 desc
            """) \
        .limit(5).show(truncate=False)

    logger.info("Top K analysis using protocol data")
    spark.read.format("parquet").load(f"{curr_path}/data/gold/eth_transfers_data_aggregated_protocol/year=*/month=*"). \
        createOrReplaceTempView("eth_transfers_data_aggregated_protocol")

    # to find out top 5 protocol as per the highest transactions count
    spark \
        .sql(
        """
            select protocol, sum(total_transactions_per_safe) as highest_transaction_count 
            from eth_transfers_data_aggregated_protocol group by 1 order by 2 desc
            """) \
        .limit(5).show(truncate=False)

    # to find out top 5 protocol as per the highest transactions volume
    spark \
        .sql(
        """
            select protocol, sum(total_amount_usd_per_safe) as highest_transaction_volume
            from eth_transfers_data_aggregated_protocol group by 1 order by 2 desc
            """) \
        .limit(5).show(truncate=False)


if __name__ == "__main__":
    main()
