Question 1: read data from a datalake location and handle for duplicates and Write it to a data lake target location 


solution :

#Creation of SparkSession 
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Demo Project").getOrCreate()

#To read a parquet based file format data from a datalake location :
====================================================================

df = spark.read.format("parquet")\
          .option("inferSchema","true")\
     	    .option("mode","PERMISSIVE")\
          .option("path","/data/from/file1.parquet")
          .load()

#Handling the duplicates:
==========================
no_duplicate_df = df.dropDuplicates()

#Writing the dataframe to the datalake location:
================================================
no_duplicate_df.write.format("parquet")\
               .mode(SaveMode.overwrite)\
