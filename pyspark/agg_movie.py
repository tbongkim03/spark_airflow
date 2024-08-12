from pyspark.sql import SparkSession
import sys
import os
import shutil


APP_NAME = sys.argv[1]
LOAD_DT = sys.argv[2]

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

df1 = spark.read.parquet(f"/home/michael/data/movie/hive/load_dt={LOAD_DT}")

df1.show()

spark.stop()
