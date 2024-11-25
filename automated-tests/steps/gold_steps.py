from behave import *
import logging
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType

from tvp.gold import get_safe_per_vertical_week_df, get_safe_per_protocol_week_df

logger = logging.getLogger(__name__)
spark = SparkSession.builder.appName("cucumber gold app").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("error")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

silver_df = spark.createDataFrame(data=[], schema="")
latest_vertical_df = spark.createDataFrame(data=[], schema="")
latest_protocol_df = spark.createDataFrame(data=[], schema="")

expected_vertical_df = spark.createDataFrame(data=[], schema="")
expected_protocol_df = spark.createDataFrame(data=[], schema="")

block_date = "2024-11-23"


@given(
    u'There is no data present in existing gold layer at path {existing_gold_layer_path_vertical} and {existing_gold_layer_path_protocol}')
def step_impl(context, existing_gold_layer_path_vertical, existing_gold_layer_path_protocol):
    logger.info(f"Given Gold layer path for vertical: {existing_gold_layer_path_vertical}")
    logger.info(f"Given Gold layer path for protocol: {existing_gold_layer_path_protocol}")


@given(u'there is silver data at path {silver_path}')
def step_impl(context, silver_path):
    logger.info(f"Given silver layer path: {silver_path}")
    logger.info(f"Table headings: {context.table.headings}")

    global silver_df, spark
    pd_raw_df = pd.DataFrame(data=context.table, columns=context.table.headings)
    silver_df = spark.createDataFrame(data=pd_raw_df) \
        .withColumn("amount_usd", col("amount_usd").cast(DecimalType(15, 2))) \
        .selectExpr("trim(tx_hash) as safe_account", "trim(vertical) as vertical", \
                    "trim(protocol) as protocol", "amount_usd", "to_date(block_date) as block_date", \
                    "cast(week_of_year as int) as week_of_year", "cast(week_of_month as int) as week_of_month")

    # raw_df.printSchema()
    # raw_df.show(truncate=False)


@when(u'transforming data into gold layer')
def step_impl(context):
    logger.info("Transforming silver data !!")
    global silver_df, latest_vertical_df, latest_protocol_df

    latest_vertical_df = get_safe_per_vertical_week_df(silver_df)
    logger.info("Vertical dataframe schema post process ")
    # latest_vertical_df.printSchema()

    latest_protocol_df = get_safe_per_protocol_week_df(silver_df)
    logger.info("Protocol dataframe schema post process ")
    # latest_protocol_df.printSchema()


@then(u'final vertical gold layer at path {gold_vertical_df_dest_path}')
def step_impl(context, gold_vertical_df_dest_path):
    logger.info(f"Final gold layer path for vertical data: {gold_vertical_df_dest_path}")

    global latest_vertical_df, expected_vertical_df

    expected_vertical_df = spark.createDataFrame(data=pd.DataFrame(data=context.table, columns=context.table.headings)) \
        .selectExpr("trim(safe_account) as safe_account", "trim(vertical) as vertical", \
                    "cast(week_of_year as int) as week_of_year", "cast(week_of_month as int) as week_of_month", \
                    "cast(total_amount_usd_per_safe as decimal(15, 2)) as total_amount_usd_per_safe", \
                    "cast(unique_safes as int) as unique_safes",
                    "cast(total_transactions_per_safe as long) as total_transactions_per_safe")

    # expected_df.show(truncate=False)
    logger.info("Expected vertical dataframe schema ...")
    # expected_vertical_df.printSchema()

    latest_vertical_df = latest_vertical_df.select(expected_vertical_df.columns)

    count = latest_vertical_df.exceptAll(expected_vertical_df).count()
    logger.info(f"Result after comparing vertical processed df and expected df: {count}")

    # latest_vertical_df.orderBy(["vertical", "safe_account"]).show(truncate=False)
    # expected_vertical_df.orderBy(["vertical", "safe_account"]).show(truncate=False)

    assert count == 0, f"Expected result is not matched with actual result for vertical data set!!"


@then(u'final protocol gold layer at path {gold_protocol_df_dest_path}')
def step_impl(context, gold_protocol_df_dest_path):
    logger.info(f"Final gold layer path for protocol data: {gold_protocol_df_dest_path}")

    global latest_protocol_df, expected_protocol_df

    expected_protocol_df = spark.createDataFrame(data=pd.DataFrame(data=context.table, columns=context.table.headings)) \
        .selectExpr("trim(safe_account) as safe_account", "trim(protocol) as protocol", \
                    "cast(week_of_year as int) as week_of_year", "cast(week_of_month as int) as week_of_month", \
                    "cast(total_amount_usd_per_safe as decimal(15, 2)) as total_amount_usd_per_safe", \
                    "cast(unique_safes as int) as unique_safes",
                    "cast(total_transactions_per_safe as long) as total_transactions_per_safe")

    # expected_df.show(truncate=False)
    logger.info("Expected protocol dataframe schema ...")
    expected_protocol_df.printSchema()

    latest_protocol_df = latest_protocol_df.select(expected_protocol_df.columns)

    count = latest_protocol_df.exceptAll(expected_protocol_df).count()
    logger.info(f"Result after comparing protocol processed df and expected df: {count}")
    assert count == 0, f"Expected result is not matched with actual result for protocol data set!!"

###
# to test feature file - run the command in terminal
# behave automated-tests/features/gold.feature
