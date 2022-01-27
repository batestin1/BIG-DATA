#!/usr/local/bin/python3
#coding: utf-8

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa mars
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

df = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/MARS/stagin/silver/nasa_mars/").createOrReplaceTempView("df")

###################transform########################################

df = spark.sql("""SELECT
monotonically_increasing_id() as id,
crater_id, crater_name,latitude_circle_image, longitude_circle_image,diam_circle_image,depth_rim_floor_topog,
layers, yearmonthday,
COALESCE(morphology_ejecta_1, morphology_ejecta_2, morphology_ejecta_3, 'null') as morphologys FROM df""")


###################load########################################

df.write.mode("append").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/MARS/stagin/gold/nasa_mars")
