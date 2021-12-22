#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: One Piece
#     Repositorio: output/MONGO
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports

import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as sfunc
import pyspark.sql.types as stypes
import pymongo
from pymongo import MongoClient
client = pymongo.MongoClient('localhost', 27017)

db = client['one_piece']
fruit_user = db.fruit_user
print(fruit_user.find_one())

region = fruit_user.aggregate([{"$group: {"_id":"$user"}, ])

agg_result= mycollection.aggregate(
    [{
    "$group" : 
        {"_id" : "$user", 
         "num_tutorial" : {"$sum" : 1}
         }}
    ])
for i in agg_result:
    print(i)