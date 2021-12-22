#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa
#     Repositorio: output/SQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports

import mysql.connector
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark import SparkContext
import findspark
findspark.add_packages('mysql:mysql-connector-java:8.0.11')
#connection
bank = mysql.connector.connect(
    host = "localhost",
    user= "root",
    password = ""
)

cursor = bank.cursor()
cursor.execute('CREATE DATABASE IF NOT EXISTS nasa')
my_conn = create_engine('mysql+mysqldb://root:@localhost/nasa?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC')


#################################CONFIGURE################################
spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()



print("Starting processing for output file...")

###################extrac########################################

nasa = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/nasa/stagin/gold/nasa_full").createOrReplaceTempView("nasa")
planets_confirmed = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/nasa/stagin/gold/planets_confirmed").createOrReplaceTempView("planets")

###################transform########################################


resume_infos = spark.sql("""SELECT metric, amount FROM(
SELECT 1 as index, 'Total of Planets Confirmed' metric, COUNT(distinct keipID) as amount FROM planets UNION
SELECT 2 as index, 'Total of Planets Observed' metric, COUNT(distinct keipID)  as amount FROM nasa)

""")
nasa = spark.sql("SELECT * FROM nasa")





###################################Load########################

nasa.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/nasa/output/raw/nasa")
resume_infos.write.mode("overwrite").format("parquet").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/nasa/output/resume/resume_info")
nasa.write.format('jdbc').options(url='jdbc:mysql://localhost/nasa?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='archive',user='root',password='').mode('append').save()
resume_infos.write.format('jdbc').options(url='jdbc:mysql://localhost/nasa?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='resume_infos',user='root',password='').mode('append').save()
