#!/usr/local/bin/python3
#coding: utf-8
#PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa asteroid
#     Repositorio: Json
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports

import json
import csv
from faker import Faker
import faker_commerce
import faker_microservice
from faker_vehicle import VehicleProvider
from faker_music import MusicProvider
import random
from datetime import date, datetime
from create_data import Variables
import pymongo
from pymongo import MongoClient
client = pymongo.MongoClient('localhost', 27017)
faker = Faker()
#variables

db = client['nasa']
Collection = db["asteroid"]
print("""
 

#  #######  ####### ########  #######  #######  #######  ######   ###### #
#       ##             ##                   ##       ##    ##          ## #
#  #######  #######    ##     ####     #######  ##   ##    ##     ##   ## #
#  ##   ##       ##    ##     ##       ##  ##   ##   ##    ##     ##   ## #
#  ##   ##  #######    ##     #######  ##   ##  #######  ######   ###### #


                                                       
Explore the Asteroids
- Big Data Project that simulates the NASA ASTEROIDS ARCHIVE -                                              

""")




data_of_first_obs = int(input("Enter the start date (following the YYYMMDD model) of observation:" ))
data_of_final_obs = int(input("Inform the final date (according to the YYYYMMDD model) of observation: "))
final_sub = data_of_final_obs - data_of_first_obs
val = final_sub + 100
print(f"inserting {val} datas")
Variables(val, data_of_first_obs, data_of_final_obs)
num = 0
for i in range(val):
    num = num + 1
    with open(f'C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/dataset/json_files/tables_{num}.json') as file:
        exam = json.load(file)
        Collection.insert_one(exam)
  



