#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa mars
#     Repositorio: stagin bronze
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports
# imports
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as sfunc
import pyspark.sql.types as stypes
import pymongo
from pymongo import MongoClient
client = pymongo.MongoClient('localhost', 27017)


spark = SparkSession.builder.appName("MyApp").config("spark.mongodb.input.uri", "mongodb://localhost:27017").config("spark.mongodb.output.uri", "mongodb://localhost:27017").config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").master("local").getOrCreate()

# extract
df = spark.read.format("mongo").option("database", "nasa").option("collection", "mars").load()
print("Starting processing for Bronze file...")
#transform
df.createOrReplaceTempView('df')


df = spark.sql("""SELECT 
monotonically_increasing_id() as _id,
CONCAT(SPLIT(TRIM(CRATER_ID), '-')[0],split(trim(CRATER_ID), '-')[1]) as crater_id,
CASE WHEN CRATER_NAME = '' THEN 'null' ELSE CRATER_NAME END as crater_name,
CASE WHEN LATITUDE_CIRCLE_IMAGE = '' THEN 'null' ELSE LATITUDE_CIRCLE_IMAGE END as latitude_circle_image,
CASE WHEN LONGITUDE_CIRCLE_IMAGE = '' THEN 'null' ELSE LONGITUDE_CIRCLE_IMAGE END as longitude_circle_image,
CASE WHEN DIAM_CIRCLE_IMAGE = '' THEN 'null' ELSE DIAM_CIRCLE_IMAGE END as diam_circle_image,
CASE WHEN DEPTH_RIMFLOOR_TOPOG = '' THEN 'null' ELSE DEPTH_RIMFLOOR_TOPOG END as depth_rim_floor_topog,
CASE WHEN MORPHOLOGY_EJECTA_1 = '' THEN '0' ELSE MORPHOLOGY_EJECTA_1 END as morphology_ejecta_1,
CASE WHEN MORPHOLOGY_EJECTA_2 = '' THEN '0' ELSE MORPHOLOGY_EJECTA_2 END as morphology_ejecta_2,
CASE WHEN MORPHOLOGY_EJECTA_3 = '' THEN '0' ELSE MORPHOLOGY_EJECTA_3 END as morphology_ejecta_3,
NUMBER_LAYERS as layers,
date_format(current_date(),'yyyyMMdd') as yearmonthday
FROM df""")


###########################LOAD###########################
df.write.mode("append").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/MARS/stagin/bronze/nasa_mars/")
