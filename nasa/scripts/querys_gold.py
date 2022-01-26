#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa asteroid
#     Repositorio: stagin gold
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports

import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

print("Starting processing for gold file...")

###################extrac########################################

nasa = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/stagin/silver/nasa_asteroid/").createOrReplaceTempView("df")

###################transform########################################

asteroid_full = spark.sql("""SELECT *, 'No' as neo, 'No' as pha FROM df

""")
asteroid_neo = spark.sql("""SELECT *, 'Yes' as neo, 'No' as pha FROM df where diameter < 10

""")

asteroid_pha = spark.sql("SELECT *, 'Yes' as neo, 'Yes' as pha FROM df where diameter > 10 and albedo > 0.7")



###################load########################################

asteroid_full.write.mode("append").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/stagin/gold/nasa_asteroid_full")
asteroid_neo.write.mode("append").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/stagin/gold/nasa_asteroid_neo")
asteroid_pha.write.mode("append").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/stagin/gold/nasa_asteroid_pha")
