"""
created 27-11-2024
project automatic-tvp
author rohitphadtare 
"""
import os
import sys
from datetime import date

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType
from delta import *
from delta.tables import *


def main(argv1, argv2):
    flag = None
    appName = None

    if len(sys.argv[0:]) > 1:
        flag = int(sys.argv[1])
        appName = sys.argv[2]
    else:
        flag = int(argv1)
        appName = argv2

    conf = pyspark.SparkConf()

    conf.set("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0," "io.delta:delta-spark_2.12:3.2.1")
    builder = SparkSession.builder.config(conf=conf).appName(appName).master("spark://spark-master:7077")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\

    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    range_end = 100
    if flag == 1:
        range_end = 200

    num_list = [i for i in range(0, range_end)]
    today = date.today()
    df = (spark.createDataFrame(data=num_list, schema=IntegerType()) \
          .withColumnRenamed("value", "id")\
          .withColumn("run_date", lit(today)))

    # printing data from df
    df.show(n=5, truncate=False)
    curr_path = os.getcwd()

    df.write.mode("overwrite").option("partitionOverwriteMode", "dynamic")\
        .format("delta").partitionBy("run_date").save("data/delta_demo")


if __name__ == "__main__":
    main(1, "Delta Demo")
