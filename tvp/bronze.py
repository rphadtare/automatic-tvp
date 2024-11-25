import glob
import logging
import os
import sys

from common import get_dune_query_result, tvp_app_config
import pandas as pd

logger = logging.getLogger(__name__)


##
# Get Dune query result using API
# API - https://api.dune.com/api/v1/query/{query_id}/results
# query_id = 4326954
# #


def main():
    block_date = None

    # To read all arguments
    args = sys.argv[1:]
    if len(args) > 0:
        block_date = args[0]
        logger.info(f"Block date given: {block_date}")

    logger.info("Ingestion to Bronze...")
    # spark = getSpark("TVP bronze process")
    curr_path = None

    logger.info("Local dev flag --> " + str(tvp_app_config.get('dev_flag')))

    res_df = None
    if not tvp_app_config.get('dev_flag'):
        res_df = get_dune_query_result()
    else:
        curr_path = os.getcwd()
        stg_path = tvp_app_config.get('stg_path').format(curr_path_value=curr_path, block_date_value=tvp_app_config.get('local_test_block_date'))
        logger.info(f"Staging path to read raw data for this run : {stg_path}")

        all_files = glob.glob(stg_path)
        input_dfs = []
        for filename in all_files:
            logger.info("Loading file: " + filename)
            df = pd.read_csv(filename, index_col=None, header=0)
            input_dfs.append(df)

        if len(input_dfs) > 0:
            res_df = pd.concat(input_dfs, axis=0, ignore_index=True)
        else:
            res_df = pd.DataFrame(data=None)

    print(res_df)

    # If input dataframe is not empty
    if not res_df.empty:
        if block_date is None:
            block_date = res_df['block_date'].drop_duplicates()[0]

        res_df.drop(columns=["block_date"], inplace=True)

        if not os.path.exists(tvp_app_config.get('bronze_path').format(curr_path_value=curr_path, block_date_value=block_date)):
            os.makedirs(tvp_app_config.get('bronze_path').format(curr_path_value=curr_path, block_date_value=block_date))

        bronze_file_path = tvp_app_config.get('bronze_path').format(curr_path_value=curr_path, block_date_value=block_date) + "/data.csv"
        logger.info("Loading staging data to bronze --> " + bronze_file_path)
        res_df.to_csv(path_or_buf=bronze_file_path, header=True, mode="w", index=False)
    else:
        logger.warning("Input dataframe is empty !!!")


if __name__ == "__main__":
    main()
