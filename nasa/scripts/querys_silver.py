#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa
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

nasa = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/nasa/stagin/bronze/nasa/").createOrReplaceTempView("nasa")


######################deduplication##################################
nasa = spark.sql(""" SELECT l. 
* FROM (SELECT *, row_number() over 
(partition by kepler_name, keipID, koi_name,stellar_parameter_provenance order by yearmonthday desc) as row_id FROM nasa) l WHERE row_id = 1""")



nasa.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/nasa/stagin/silver/nasa/")
