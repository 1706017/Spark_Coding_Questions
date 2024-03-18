Spark Practical Questions
==========================
Q1) Word count program in spark ?

Input : a file with multiple sentences in multiple lines with many repetative words 

for example : filename: file1.txt

content of file: 

I am big data engineer
I am learning spark
I want to switch to a better company 
Big data is very intresting 

Output we want:
(I,3) , (am,2), (data,2), (big,1), (Big,1), (is,1)


################
Code in scala:
################

$hadoop fs -mkdir /user/cloudera/sparkinput  (creating folder in hdfs)
$hadoop fs -copyFromLocal /home/cloudera/desktop/file1 /user/cloudera/sparkinput  (copying data file from local to hdfs)

$spark-shell (to open the spark shell for scala)

val rdd1 = sc.textFile("/user/cloudera/sparkinput/file1");
val rdd2 = rdd1.flatMap(x => x.split(" "))
val rdd3 = rdd2.map(x => (x,1))
val rdd4 = rdd3.reduceByKey((x,y) => x+y)
rdd4.collect()

################
Code in Python:
################

from pyspark import SparkContext

sc = SparkContext()  #initialize the spark context

rdd1 = sc.textFile("/user/cloudera/sparkinput/file1")
rdd2 = rdd1.flatMap(lambda x : x.split(" "))
rdd3 = rdd2.map(lambda x : (x,1))
rdd4 = rdd3.reduceByKey(lambda x,y : x+y)
rdd4.collect() or rdd4.saveAsTextFile("/user/cloudera/sparkoutput")

**********************************************************************************************************************************************************************************************************************
Q2) Problem statement: we need to find out top 10 customers who spent the maximum anount ?

Dataset : customer-orders.csv

Content:
(customer_id,product_id,amount)
(12,abc,200)
(13,xyz,300)
(12,xcz,300)
(14,bnm,350)

Note: Here we can have duplicate value for customer id as same customer can purchase multiple product 

##############
Code in Scala:
##############

val rdd1 = sc.textFile("/user/cloudera/sparkinput/customer-orders.csv")
val rdd2 = rdd1.map(x =>(x.split(",")(0),x.split(",")(2).toFloat))
val rdd3 = rdd2.reduceByKey((x,y) =>x+y)
val rdd4 = rdd3.sortBy(x=> x._2)
rdd4.saveAsTextFile("/user/cloudera/sparkoutput")

###############
Code in python:
###############

import spark from SparkContext

sc = SparkContext()

rdd1 = sc.textFile("/user/cloudera/sparkinput/customer-orders.csv")
rdd2 = rdd1.map(lambda x: (x.split(",")[0],float(x.split(",")[2])))
rdd3 = rdd2.reduceByKey(lambda x,y: x+y)
rdd4 = rdd3.sortBy(lambda x:x[1])
rdd4.saveAsTextFile("/user/cloudera/sparkoutput")
