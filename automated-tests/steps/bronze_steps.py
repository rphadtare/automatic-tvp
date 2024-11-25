from behave import *
import logging
import pandas as pd

logger = logging.getLogger(__name__)
raw_df = pd.DataFrame.empty
expected_df = pd.DataFrame.empty


@given(u'there is {source} dataframe with following data at path {source_path}')
def step_impl(context, source, source_path):
    logger.info(f"Given dataframe: {source}")
    logger.info(f"Given source path: {source_path}")
    logger.info(f"Table headings: {context.table.headings}")

    global raw_df
    raw_df = pd.DataFrame(data=context.table, columns=context.table.headings)
    logger.info(raw_df)


@when(u'ingesting data into bronze layer')
def step_impl(context):
    logger.info("When step executing !!")


@then(u'result is {final_df} dataframe with following lines at path {dest_path}')
def step_impl(context, final_df, dest_path):
    logger.info(f"Final dataframe: {final_df}")
    logger.info(f"Final path: {dest_path}")

    global expected_df
    expected_df = pd.DataFrame(data=context.table, columns=context.table.headings)
    logger.info(expected_df)

    flag = raw_df.equals(expected_df)
    logger.info(f"Result after comparing raw df and expected df: {flag}")
    assert flag == True, f"Expected result is not matched with actual result !!"

###
# to test feature file - run the command in terminal
# behave automated-tests/features/bronze.feature

