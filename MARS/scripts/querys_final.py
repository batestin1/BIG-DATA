#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa mars
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

df = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/MARS/stagin/gold/nasa_mars").createOrReplaceTempView("df")

###################transform########################################
df = spark.sql("SELECT * FROM df")


resume_infos = spark.sql("""SELECT metric, amount FROM(
SELECT 1 as index, 'Total Craters Traveled' metric, COUNT(DISTINCT crater_name) AS amount FROM df UNION
SELECT 2 as index, 'Total Walking Distance(KM)' metric, SUM(translate(latitude_circle_image, '-', ''))  AS amount FROM df UNION
SELECT 3 as index, 'Maximum Distance Traveled(KM)' metric, MAX(translate(latitude_circle_image, '-', '')) AS amount FROM df UNION
SELECT 4 as index, 'Minimum Distance Traveled(KM)' metric, MIN(translate(latitude_circle_image, '-', '')) AS amount FROM df UNION
SELECT 5 as index, 'Total Analyzes Sent' metric, SUM(layers) AS amout FROM df UNION
SELECT 6 as index, 'Total Data Collected' metric, COUNT(DISTINCT crater_id) AS amount FROM df
) order by metric asc

""")




###################################Load########################

df.write.mode("append").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/MARS/output/raw/nasa_mars")
resume_infos.write.mode("overwrite").format("parquet").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/MARS/output/resume/resume_info")
df.write.format('jdbc').options(url='jdbc:mysql://localhost/nasa?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='mars',user='root',password='').mode('append').save()
resume_infos.write.format('jdbc').options(url='jdbc:mysql://localhost/nasa?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='resume_infos_mars',user='root',password='').mode('append').save()
