#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa asteroid
#     Repositorio: stagin bronze
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports
# imports
import json
from turtle import clear
from pyspark.sql import SparkSession
import pyspark.sql.functions as sfunc
import pyspark.sql.types as stypes
import pymongo
from pymongo import MongoClient
client = pymongo.MongoClient('localhost', 27017)


spark = SparkSession.builder.appName("MyApp").config("spark.mongodb.input.uri", "mongodb://localhost:27017").config("spark.mongodb.output.uri", "mongodb://localhost:27017").config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").master("local").getOrCreate()

# extract
df = spark.read.format("mongo").option("database", "nasa").option("collection", "asteroid").load()

#transform
df.createOrReplaceTempView('df')

print("Starting processing for Bronze file...")


#asteroid
df = spark.sql("""SELECT a, ad, albedo, class_asteroid, data_of_final_obs as final_data,
data_of_first_obs as start_data, diameter_full.diameter as diameter, diameter_full.diameter_sigma as diameter_sigma,
e, epoch_full.epoch as epoch, epoch_full.epoch_mjd as epoch_mjd, epoch_full.epoch_cal as epoch_cal,
equinox, h, i, ma, moid_full.moid as moid, moid_full.moid_id as moid_id, n, object_fullname.fullname as fullname, 
object_fullname.prefix as prefix, object_fullname.name as name, object_id, 'observation days' as observation_days,
om, orbit_id, pdes, per_full.per as per, per_full.per_y as per_y, q, rms, sigma_full.sigma_e as sigma_e,
sigma_full.sigma_a as sigma_a, sigma_full.sigma_q as sigma_q, sigma_full.sigma_i as sigma_i, sigma_full.sigma_om as sigma_om,
sigma_full.sigma_w as sigma_w, sigma_full.sigma_ma as sigma_ma, sigma_full.sigma_ad as sigma_ad, sigma_full.sigma_n as sigma_n,
sigma_full.sigma_tp as sigma_tp, sigma_full.sigma_per as sigma_per, speed, spk_id, tp_full.tp_cal as tp_cal, tp_full.tp as tp, w,
date_format(current_date(), 'yyyyMMdd') as yearmonthday
FROM df""").createOrReplaceTempView('df')

df = spark.sql("SELECT * FROM df")


###########################LOAD###########################
df.write.mode("append").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/stagin/bronze/nasa_asteroid/")
