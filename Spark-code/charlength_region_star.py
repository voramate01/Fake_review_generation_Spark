#!/usr/bin/python
#-*- coding: utf-8 -*-
import os, sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import string
import nltk 
import re
from pyspark.sql.functions import avg

sc = SparkContext()
spark =SparkSession(sc)
sqlContext=SQLContext(sc)
#read csv file to spark dataframe
df_review =spark.read.csv("/data/MSA_8050_Spring_19/2pm_5/yelp_review.csv" , header= True)
df_business =spark.read.csv("/data/MSA_8050_Spring_19/2pm_5/yelp_business.csv" , header =True)

#register spark dataframe as SQL table so that we can perform sql query
df_review.registerTempTable('review_table')
df_business.registerTempTable('business_table')

#Inner join two sql table
business_city=sqlContext.sql('select business_id, city from business_table ')
joined=df_review.join(business_city, df_review.business_id== business_city.business_id,'inner')
CST= joined.select("city","stars","text")
#Drop missing value
CST= CST.na.drop()
#Split dataset to 5 region
CST.registerTempTable("CST_sql")
region_1=sqlContext.sql("select * from CST_sql where city like '%Toronto%' or city like '%Montreal%'")
region_2=sqlContext.sql("select * from CST_sql where city like '%Edinburgh%' or city like '%Stuttgart%'")
region_3=sqlContext.sql("select * from CST_sql where city like '%Pittsbur%' or city like '%Urbana%' or city like '%Champaign%' or city like '%Madison%' or city like '%Cleveland%'")
region_4=sqlContext.sql("select * from CST_sql where city like '%Phoenix%' or city like '%Las vegas%'")
region_5=sqlContext.sql("select * from CST_sql where city like '%Charlot%'")

############################################################################################################
###Find Average length of word 

from pyspark.sql.functions import udf

#Define User-defined function that can be used with spark dataframe
@udf("string")
def findlength(x):
	return len(re.sub('\W+', ' ',x.encode('utf-8').lower()).strip())
#region 1
region_1_length=region_1.select("city","stars","text",findlength("text").alias("text_length"))
region_1_length=region_1_length.groupBy("stars").agg(avg("text_length")).orderBy('stars',ascending=True) 
#region 2
region_2_length=region_2.select("city","stars","text",findlength("text").alias("text_length"))
region_2_length=region_2_length.groupBy("stars").agg(avg("text_length")).orderBy('stars',ascending=True)
#region 3
region_3_length=region_3.select("city","stars","text",findlength("text").alias("text_length"))
region_3_length=region_3_length.groupBy("stars").agg(avg("text_length")).orderBy('stars',ascending=True)
#region 4
region_4_length=region_4.select("city","stars","text",findlength("text").alias("text_length"))
region_4_length=region_4_length.groupBy("stars").agg(avg("text_length")).orderBy('stars',ascending=True)
#region5
region_5_length=region_5.select("city","stars","text",findlength("text").alias("text_length"))
region_5_length=region_5_length.groupBy("stars").agg(avg("text_length")).orderBy('stars',ascending=True)


#Show the result
region_1_length.show()
#region_2_length.show()
#region_3_length.show()
#region_4_length.show()
#region_5_length.show()

