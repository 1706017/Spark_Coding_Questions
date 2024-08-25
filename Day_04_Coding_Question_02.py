Q2) Input Data:

 Col1 Col2 Col3
 1     11   a
 1     11   b
 1     11   c
 2     22   c
 2     22   d

 Expected Output:

 Col1 Col2 Col3
  1    11   [a,b,c]
  2    22   [c,d]

 Solution:
 Step1) Creation of SparkSession as above Question 

 Step2) Create data and schema and create a dataframe from it 
 data = [("1","11","a"),
         ("1","11","b"),
         ("1","11","c"),
         ("2","22","c"),
         ("2","22","d")]
 schema = ["Col1","Col2","Col3"]

 df = spark.createDataFrame(data,schema=schema)

 Step3) Doing the group by and aggregation 

 grouped_df = df.groupBy("Col1","Col2").agg(collect_list("Col3")).alias("Col3")

 grouped_df.show()

 spark.stop()
