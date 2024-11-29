"""
created 28-11-2024
project automatic-tvp
author rohitphadtare 
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

spark = SparkSession \
    .builder \
    .appName("Demo Spark App") \
    .getOrCreate()

nums = [i for i in range(0, 100)]
spark.createDataFrame(data=nums, schema=IntegerType()).show(truncate=False)

