from behave import *
import logging
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import IntegerType, DecimalType

from spark_scripts.silver import quarantined_bronze_data

logger = logging.getLogger(__name__)
spark = SparkSession.builder.appName("cucumber silver app").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("error")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

raw_df = spark.createDataFrame(data=[], schema="")
full_df = spark.createDataFrame(data=[], schema="")
expected_df = spark.createDataFrame(data=[], schema="")
block_date = "2024-11-23"


@given(u'There is no data present in existing silver layer at path {existing_silver_layer_path}')
def step_impl(context, existing_silver_layer_path):
    logger.info(f"Given silver layer path: {existing_silver_layer_path}")
    global raw_df, full_df, spark
    full_df = raw_df.limit(0)


@given(u'there is bronze layer data at path {bronze_path}')
def step_impl(context, bronze_path):
    logger.info(f"Given bronze layer path: {bronze_path}")
    logger.info(f"Table headings: {context.table.headings}")

    global raw_df, spark
    pd_raw_df = pd.DataFrame(data=context.table, columns=context.table.headings)
    raw_df = spark.createDataFrame(data=pd_raw_df)\
        .withColumn("amount_usd", col("amount_usd").cast(DecimalType(15, 2)))

    # raw_df.printSchema()
    # raw_df.show(truncate=False)


@when(u'ingesting data into silver layer')
def step_impl(context):
    logger.info("Quarantining bronze data !!")
    global raw_df, block_date
    raw_df = quarantined_bronze_data(spark=spark, raw_df=raw_df, block_date_=block_date)\
        .withColumn("week_of_month", col("week_of_month").cast(IntegerType()))\
        .selectExpr("trim(token_category) as token_category", "trim(event_name) as event_name",\
                    "trim(tx_hash) as tx_hash", "trim(vertical) as vertical",\
                    "trim(protocol) as protocol", "amount_usd", "block_date", "week_of_year", "week_of_month")

    logger.info("Quarantined dataframe schema ...")
    # raw_df.show(truncate=False)
    raw_df.printSchema()


@then(u'silver layer contains data at path {dest_path}')
def step_impl(context, dest_path):
    logger.info(f"Final path: {dest_path}")

    global expected_df, raw_df
    raw_df = raw_df

    expected_df = spark.createDataFrame(data=pd.DataFrame(data=context.table, columns=context.table.headings))\
        .withColumn("block_date", to_date(col("block_date")))\
        .withColumn("week_of_month", col("week_of_month").cast(IntegerType()))\
        .withColumn("amount_usd", col("amount_usd").cast(DecimalType(15, 2)))\
        .selectExpr("trim(token_category) as token_category", "trim(event_name) as event_name", \
                    "trim(tx_hash) as tx_hash", "trim(vertical) as vertical", \
                    "trim(protocol) as protocol", "amount_usd", "block_date", "week_of_year", "week_of_month")

    # expected_df.show(truncate=False)
    logger.info("Expected dataframe schema ...")
    expected_df.printSchema()

    count = raw_df.exceptAll(expected_df).count()
    logger.info(f"Result after comparing processed df and expected df: {count}")
    assert count == 0, f"Expected result is not matched with actual result !!"

###
# to test feature file - run the command in terminal
# behave automated-tests/features/silver.feature
