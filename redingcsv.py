# -*- coding: utf-8 -*-
"""
Created on Sat Jun 13 21:08:30 2020

@author: NNK
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

df2 = spark.read.option("header",True) \
     .csv("dbfs:/FileStore/zipcodes.csv")
df2=df2
df2.write.option("header",True) \
 .csv("dbfs/FileStore/zipcodes1")
 
