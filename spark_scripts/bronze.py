import glob
import logging
import os
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

sys.path.append(".")
from spark_scripts.common import get_dune_query_result, tvp_app_config, get_spark

logger = logging.getLogger(__name__)


##
# Get Dune query result using API
# API - https://api.dune.com/api/v1/query/{query_id}/results
# query_id = 4326954
# #

def get_input_df(logger_inner, app_config, spark: SparkSession):
    spark.sparkContext.setJobDescription("Fetching latest input")
    # spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    rdd = spark.sparkContext.emptyRDD()
    res_df = spark.createDataFrame(data=rdd, schema=StructType())
    # final_df = spark.emptyDataFrame
    curr_path = "data"

    try:
        if app_config.get('dev_flag'):
            # print("bool(app_config.get('dev_flag'))", bool(app_config.get('dev_flag')))
            logger_inner.warning("Dune query not executed. As local testing flag is true !!")
        else:
            schema = StructType([StructField(name="amount", dataType=DoubleType()),
                                 StructField(name="amount_usd", dataType=DoubleType()),
                                 StructField(name="block_date", dataType=StringType()),
                                 StructField(name="block_time", dataType=StringType()),
                                 StructField(name="blockchain", dataType=StringType()),
                                 StructField(name="event_name", dataType=StringType()),
                                 StructField(name="protocol", dataType=StringType()),
                                 StructField(name="receiver", dataType=StringType()),
                                 StructField(name="receiver_contract_name", dataType=StringType()),
                                 StructField(name="receiver_ens", dataType=StringType()),
                                 StructField(name="receiver_label_categories", dataType=StringType()),
                                 StructField(name="receiver_label_models", dataType=StringType()),
                                 StructField(name="receiver_label_names", dataType=StringType()),
                                 StructField(name="receiver_name", dataType=StringType()),
                                 StructField(name="receiver_type", dataType=StringType()),
                                 StructField(name="safe_sender", dataType=StringType()),
                                 StructField(name="symbol", dataType=StringType()),
                                 StructField(name="token_address", dataType=StringType()),
                                 StructField(name="token_category", dataType=StringType()),
                                 StructField(name="tx_hash", dataType=StringType()),
                                 StructField(name="vertical", dataType=StringType()),
                                 ])

            res_df = spark.createDataFrame(get_dune_query_result(logger_inner=logger_inner, app_config=app_config), \
                                           schema=schema)
            logger_inner.info("Dune query executed successfully !!")

            logger_inner.info("Converting dune analytics result to spark dataframe ...")
            res_df.show(truncate=False)
            res_df.printSchema()

    except Exception as e:
        logger_inner.warning("Error occurred when retrieving result from dune analytics !!")
        logger_inner.error(f"Error details - {e}")

    finally:
        if res_df.isEmpty():
            logger_inner.warning(
                f"As result df is none. Running this script to collect sample input from stg path for block data - \
                {app_config.get('local_test_block_date')}")

            stg_path = app_config.get('stg_path').format(curr_path_value=curr_path,
                                                         block_date_value=app_config.get('local_test_block_date'))
            logger_inner.info(f"Staging path to read raw data for this run : {stg_path}")
            res_df = spark.read.option("header", "true").csv(stg_path)

            # all_files = glob.glob(stg_path)
            # input_dfs = []
            # for filename in all_files:
            #     logger.info("Loading file: " + filename)
            #     df = pd.read_csv(filename, index_col=None, header=0)
            #     input_dfs.append(df)

            # if len(input_dfs) > 0:
            #     res_df = pd.concat(input_dfs, axis=0, ignore_index=True)
            # else:
            #     res_df = pd.DataFrame(data=None)

    return res_df, curr_path


def save_to_bronze(res_df: DataFrame, curr_path, logger_inner, app_config):
    block_date_ = None

    # If input dataframe is not empty
    if not res_df.isEmpty():
        if block_date_ is None:
            block_date_ = \
                res_df.select('block_date').drop_duplicates().rdd.map(lambda x: str(x['block_date'])).collect()[0]

        res_df = res_df.drop("block_date")

        # if not os.path.exists(
        #         app_config.get('bronze_path').format(curr_path_value=curr_path, block_date_value=block_date)):
        #     os.makedirs(app_config.get('bronze_path').format(curr_path_value=curr_path, block_date_value=block_date))

        bronze_file_path = app_config.get('bronze_path').format(curr_path_value=curr_path,
                                                                block_date_value=block_date_)  # + "/data.csv"
        logger_inner.info("Loading staging data to bronze --> " + bronze_file_path)
        # res_df.to_csv(path_or_buf=bronze_file_path, header=True, mode="w", index=False)
        res_df.coalesce(1).write.option("header", "true") \
            .mode("overwrite").csv(bronze_file_path)

        return 0
    else:
        logger_inner.warning("Input dataframe is empty !!!")
        return 1


def main(arg1, master_="spark:// spark-master:7077", date_override_flag=True):
    logger.info("Ingestion to Bronze...")
    # spark = getSpark("TVP bronze process")

    logger.info("Local dev flag --> " + str(tvp_app_config.get('dev_flag')))
    logger.info("arg1 --> " + arg1)
    logger.info("date_override_flag --> " + str(date_override_flag))
    logger.info("pwd: " + os.getcwd())

    if date_override_flag:
        tvp_app_config['local_test_block_date'] = arg1

    spark = get_spark("Ingestion_to_bronze", master=master_)

    res_df, curr_path = get_input_df(logger, tvp_app_config, spark=spark)
    print(res_df)
    spark.sparkContext.setJobDescription("storing result into bronze layer")
    save_to_bronze(res_df=res_df, curr_path=curr_path, logger_inner=logger, app_config=tvp_app_config)

    schema = StructType([StructField(name="amount", dataType=DoubleType()),
                         StructField(name="amount_usd", dataType=DoubleType()),
                         StructField(name="block_date", dataType=StringType()),
                         StructField(name="block_time", dataType=StringType()),
                         StructField(name="blockchain", dataType=StringType()),
                         StructField(name="event_name", dataType=StringType()),
                         StructField(name="protocol", dataType=StringType()),
                         StructField(name="receiver", dataType=StringType()),
                         StructField(name="receiver_contract_name", dataType=StringType()),
                         StructField(name="receiver_ens", dataType=StringType()),
                         StructField(name="receiver_label_categories", dataType=StringType()),
                         StructField(name="receiver_label_models", dataType=StringType()),
                         StructField(name="receiver_label_names", dataType=StringType()),
                         StructField(name="receiver_name", dataType=StringType()),
                         StructField(name="receiver_type", dataType=StringType()),
                         StructField(name="safe_sender", dataType=StringType()),
                         StructField(name="symbol", dataType=StringType()),
                         StructField(name="token_address", dataType=StringType()),
                         StructField(name="token_category", dataType=StringType()),
                         StructField(name="tx_hash", dataType=StringType()),
                         StructField(name="vertical", dataType=StringType()),
                         ])

    path = tvp_app_config.get('bronze_path').format(curr_path_value=curr_path, block_date_value=arg1)
    spark.read.option("header", "true").csv(path, schema=schema).show(truncate=False)


if __name__ == "__main__":

    block_date = None
    master = None
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
