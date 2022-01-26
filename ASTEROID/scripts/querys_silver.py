#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa asteroid
#     Repositorio: stagin silver
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports

import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

print("Starting processing for Silver file...")

###################extrac########################################

df = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/stagin/bronze/nasa_asteroid/").createOrReplaceTempView("df")


######################deduplication##################################

df = spark.sql(""" SELECT l. * FROM (SELECT *, row_number() over 
(partition by object_id, orbit_id, spk_id,moid_id order by yearmonthday desc) as row_id FROM df) l WHERE row_id = 1""").createOrReplaceTempView('df')

df = spark.sql("SELECT * FROM df")


df.write.mode("append").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/stagin/silver/nasa_asteroid/")
