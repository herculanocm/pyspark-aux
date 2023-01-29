import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType, DataType, TimestampType, LongType, MapType, ArrayType, StructType, StructField
import json

def parse_json(array_str):
    json_obj = json.loads(array_str)
    yield(json_obj["Zipcode"], json_obj["ZipCodeType"])
    #for item in json_obj:
    #    yield (item["Zipcode"], item["ZipCodeType"])

json_schema = MapType(StructType([StructField('Zipcode', StringType(), nullable=False), StructField('ZipCodeType', StringType(), nullable=False)]))

spark=SparkSession.builder.appName("stringoperations").getOrCreate()
data = [
  (1,2,'''{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}'''),
  (1,2,'''{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}'''),
  (1,2,'''{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}''')
  ]
columns=["id","id2", "jsonval"]
df=spark.createDataFrame(data,columns)

df.printSchema()

udf_parse_json = udf(lambda str: parse_json(str), json_schema)

df2 = df.select(udf_parse_json(df.jsonval).alias("jsonval"))

#df2 = df

df2.printSchema()
df2.show(truncate=False)