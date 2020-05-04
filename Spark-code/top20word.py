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
#Read csv file to spark dataframe
df_review =spark.read.csv("/data/MSA_8050_Spring_19/2pm_5/yelp_review.csv" , header= True)
df_business =spark.read.csv("/data/MSA_8050_Spring_19/2pm_5/yelp_business.csv" , header =True)

#Register dataframes as SQL TABLE so that we can perfpom sql query on them
df_review.registerTempTable('review_table')
df_business.registerTempTable('business_table')

#Inner join
business_city=sqlContext.sql('select business_id, city from business_table ')
joined=df_review.join(business_city, df_review.business_id== business_city.business_id,'inner')
CST= joined.select("city","stars","text")
#Drop missing value
CST= CST.na.drop()

#Split data into 5 region
CST.registerTempTable("CST_sql")
region_1=sqlContext.sql("select * from CST_sql where city like '%Toronto%' or city like '%Montreal%'")
region_2=sqlContext.sql("select * from CST_sql where city like '%Edinburgh%' or city like '%Stuttgart%'")
region_3=sqlContext.sql("select * from CST_sql where city like '%Pitthsbur%' or city like '%Urbana%' or city like '%Champaign%' or city like 'Madison' or city like 'Cleveland'")
region_4=sqlContext.sql("select * from CST_sql where city like '%Phoenix%' or city like '%Las vegas'")
region_5=sqlContext.sql("select * from CST_sql where city like '%Charlot%'")

############################################################################################################
### Find top common word
from pyspark.sql.functions import udf 
from pyspark.sql import functions as F
from nltk.corpus import stopwords

#Load stop word and also add some unwanted word
nltk.download('stopwords')
stopword=stopwords.words("english")+["back","food","place","good","could","great","service","one","like","would","really","nice","time","us","get","great","go","got","also","even","chicken","2","1","3","4","5","6","7","8","9","0","restaurant"]
#Define regular function
def reducer (x,y):
	x[y] =x.get(y,0)+1
	return x
#Define Spark function
tokenize=udf(lambda x: nltk.word_tokenize(re.sub('\W+', ' ',x.encode('utf-8').lower())))
removestopword=udf(lambda x:  filter(lambda y:y not in stopword,x))

#region 1  
#For other region 
#Ex- region 2-----> change "region_1" to "region_2" 
r=region_1.select("city","stars","text", tokenize("text").alias("top word")) #Edit this line for other region
r=r.select("stars", removestopword("top word").alias("top word"))
r=r.rdd.map(list).collect()
r=sc.parallelize(r)
r=r.reduceByKey(lambda x,y :x+y)
r=r.mapValues(lambda x:re.split(', ',x))
r=r.mapValues(lambda x: reduce(reducer,x,{}))
r=r.mapValues(lambda x: sorted(x.items(),key=lambda y:-y[1]))
r=r.mapValues(lambda x: x[0:20])
r=r.mapValues(lambda x: map(lambda y:y[0] ,x))
#Print out the result
for num in r.collect():
	print "%s star rating: %s" % (num[0],num[1])
                                                                                                                                       

                               
