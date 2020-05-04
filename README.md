# Fake_review_generation_Spark

Objective: 
- Analysed a 7 milions Yelp review dataset on 280 cpu cores Hadoop/Spark cluster.
- Generate Real fake review using LSTM model that will bypass the Yelp’ auto fake review detecting system.
* The fake review are removed after this project is finished.

Methodology:
- Performed exploratory data analysis (EDA) with SparkSQL, Spark Dataframe and RDD to identify unique popular words, average word 
  and character length for each star rating and each region.
- Developed a Long short-term memory model (LSTM) to create fake reviews that Yelp fake review detection algorithm could not detect.

