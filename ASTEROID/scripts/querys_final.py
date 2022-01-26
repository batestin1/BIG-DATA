#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa asteroid
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

asteroid_full = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/stagin/gold/nasa_asteroid_full").createOrReplaceTempView("df_full")
asteroid_neo = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/stagin/gold/nasa_asteroid_neo").createOrReplaceTempView("df_neo")
asteroid_pha = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/stagin/gold/nasa_asteroid_pha").createOrReplaceTempView("df_pha")

###################transform########################################

asteroid_full = spark.sql("""SELECT a,ad ,albedo ,class_asteroid,final_data ,start_data ,diameter ,diameter_sigma ,e ,epoch ,epoch_mjd ,epoch_cal ,equinox ,h ,
i ,ma ,moid ,moid_id ,n ,fullname ,prefix ,name ,object_id ,observation_days ,om ,orbit_id ,pdes ,per ,per_y ,q ,rms ,sigma_e ,sigma_a ,sigma_q ,sigma_i ,sigma_om ,
sigma_w ,sigma_ma ,sigma_ad ,sigma_n ,sigma_tp ,sigma_per ,speed ,spk_id ,tp_cal ,tp ,w ,neo ,pha , yearmonthday as process_data FROM df_full""").createOrReplaceTempView('df_full')
asteroid_neo = spark.sql("""SELECT
a,ad ,albedo ,class_asteroid,final_data ,start_data ,diameter ,diameter_sigma ,e ,epoch ,epoch_mjd ,epoch_cal ,equinox ,h ,
i ,ma ,moid ,moid_id ,n ,fullname ,prefix ,name ,object_id ,observation_days ,om ,orbit_id ,pdes ,per ,per_y ,q ,rms ,sigma_e ,sigma_a ,sigma_q ,sigma_i ,sigma_om ,
sigma_w ,sigma_ma ,sigma_ad ,sigma_n ,sigma_tp ,sigma_per ,speed ,spk_id ,tp_cal ,tp ,w ,neo ,pha , yearmonthday as process_data FROM df_neo""" ).createOrReplaceTempView('df_neo')
asteroid_pha = spark.sql("""SELECT
a,ad ,albedo ,class_asteroid,final_data ,start_data ,diameter ,diameter_sigma ,e ,epoch ,epoch_mjd ,epoch_cal ,equinox ,h ,
i ,ma ,moid ,moid_id ,n ,fullname ,prefix ,name ,object_id ,observation_days ,om ,orbit_id ,pdes ,per ,per_y ,q ,rms ,sigma_e ,sigma_a ,sigma_q ,sigma_i ,sigma_om ,
sigma_w ,sigma_ma ,sigma_ad ,sigma_n ,sigma_tp ,sigma_per ,speed ,spk_id ,tp_cal ,tp ,w ,neo ,pha , yearmonthday as process_data FROM df_pha""").createOrReplaceTempView('df_pha')

asteroid_full = spark.sql("SELECT * FROM df_full")
asteroid_neo =  spark.sql("SELECT * FROM df_neo")
asteroid_pha = spark.sql("SELECT * FROM df_pha")
################################# resume ################################################

resume_infos = spark.sql("""SELECT metric, amount FROM(
SELECT 1 as index, 'Total of Objects in Space' metric, COUNT(distinct name) as amount FROM df_full UNION
SELECT 2 as index, 'Total Objects that Are Asteroids but do not pose a danger to Earth' metric, COUNT(distinct fullname)  as amount FROM df_neo UNION
SELECT 3 as index, 'Total Objects that Are Asteroids but pose a danger to Earth' metric, COUNT(distinct fullname) as amount FROM df_pha)

""")


###################################Load########################

asteroid_full.write.mode("append").format("parquet").partitionBy("process_data").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/output/raw/nasa_asteroid_full")
asteroid_neo.write.mode("append").format("parquet").partitionBy("process_data").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/output/raw/nasa_asteroid_neo")
asteroid_pha.write.mode("append").format("parquet").partitionBy("process_data").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/output/raw/nasa_asteroid_pha")
resume_infos.write.mode("append").format("parquet").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/ASTEROID/output/resume/resume")


asteroid_full.write.format('jdbc').options(url='jdbc:mysql://localhost/nasa?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='asteroid',user='root',password='').mode('append').save()
asteroid_neo.write.format('jdbc').options(url='jdbc:mysql://localhost/nasa?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='asteroid_neo',user='root',password='').mode('append').save()
asteroid_pha.write.format('jdbc').options(url='jdbc:mysql://localhost/nasa?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='asteroid_pha',user='root',password='').mode('append').save()
resume_infos.write.format('jdbc').options(url='jdbc:mysql://localhost/nasa?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='resume_infos',user='root',password='').mode('append').save()
