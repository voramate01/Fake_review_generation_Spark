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
df_review =spark.read.csv("/data/MSA_8050_Spring_19/2pm_5/yelp_review.csv" , header= True)
df_business =spark.read.csv("/data/MSA_8050_Spring_19/2pm_5/yelp_business.csv" , header =True)

#df_review.show()
#df_business.show()

df_review.registerTempTable('review_table')
df_business.registerTempTable('business_table')


business_city=sqlContext.sql('select business_id, city from business_table ')
joined=df_review.join(business_city, df_review.business_id== business_city.business_id,'inner')

CST= joined.select("city","stars","text")
CST= CST.na.drop()

CST.registerTempTable("CST_sql")
region_1=sqlContext.sql("select * from CST_sql where city like '%Toronto%' or city like '%Montreal%'")
region_2=sqlContext.sql("select * from CST_sql where city like '%Edinburgh%' or city like '%Stuttgart%'")
region_3=sqlContext.sql("select * from CST_sql where city like '%Pittsbur%' or city like '%Urbana%' or city like '%Champaign%' or city like '%Madison%' or city like '%Cleveland%'")
region_4=sqlContext.sql("select * from CST_sql where city like '%Phoenix%' or city like '%Las vegas%'")
region_5=sqlContext.sql("select * from CST_sql where city like '%Charlot%'")

region_1.coalesce(1).write.option("header","true").csv("myregion1")
#region_2_coalesce(1).write.option("header","true").csv("myregion2")
#region_3.coalesce(1).write.option("header","true").csv("myregion3")
#region_4.coalesce(1).write.option("header","true").csv("myregion4")
#region_5.coalesce(1).write.option("header","true").csv("myregion5")


