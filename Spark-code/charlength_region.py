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
###Find Average length of Character

from pyspark.sql.functions import udf

#Define User-defined function that can be used with spark dataframe
@udf("string")
def findlength(x):
	return len(re.sub('\W+', ' ',x.encode('utf-8').lower()).strip())


region=region_1.select("city","stars","text",findlength("text").alias("text_length"))

''' This is for region 2 ,3, 4 and 5 respectively
#region=region_2.select("city","stars","text",findlength("text").alias("text_length")) 
#region=region_3.select("city","stars","text",findlength("text").alias("text_length")) 
#region=region_4.select("city","stars","text",findlength("text").alias("text_length")) 
#region=region_5.select("city","stars","text",findlength("text").alias("text_length")) 
'''

#Create dummy column which every row have the same value of dummy so that we can group by dummy
region=region.withColumn("dummy", region.stars*0)
#Group by dummy and show the result
region=region.groupBy("dummy").agg(avg("text_length")).show()
                                                                                                          
