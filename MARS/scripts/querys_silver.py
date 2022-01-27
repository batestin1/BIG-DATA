#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa mars
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

df = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/MARS/stagin/bronze/nasa_mars/").createOrReplaceTempView("df")


df = spark.sql("""SELECT * FROM df
WHERE crater_name !='null' and latitude_circle_image != 'null' and 
longitude_circle_image != 'null' and diam_circle_image != 'null' and 
depth_rim_floor_topog != 'null' and morphology_ejecta_1 != 'null' and 
morphology_ejecta_2 != 'null' and morphology_ejecta_3 != 'null'
""").createOrReplaceTempView("df_temp")

######################deduplication##################################


df = spark.sql(""" SELECT l. * FROM (SELECT *, row_number() OVER (PARTITION BY crater_id ORDER BY _id DESC) AS row_id FROM df_temp) l WHERE row_id = 1""")


######################load##################################

df.write.mode("append").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/MARS/stagin/silver/nasa_mars/")
