"""
created 28-11-2024
project automatic-tvp
author rohitphadtare 
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField, FloatType, StringType
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Demo Spark App") \
    .getOrCreate()

nums = [i for i in range(0, 100)]
d1 = {'id': [10.0, 20.1, 20.2], 'name': ["Rohit", "Rajani", "Pooja"]}
df = pd.DataFrame(data=d1)
print(df)

schema = StructType([StructField(name="id", dataType=FloatType()), StructField(name="name", dataType=StringType())])
# spark.createDataFrame(data=nums, schema=IntegerType()).show(truncate=False)
df1 = spark.createDataFrame(df, schema=schema)
df1.printSchema()
df1.show(truncate=False)

my_list = df1.drop_duplicates().rdd.map(lambda x: int(x['id'])).collect()
my_list.sort()
print(my_list[2])
