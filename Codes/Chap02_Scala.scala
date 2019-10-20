// Databricks notebook source
val MyRange = spark.range(1000).toDF("number")
val divisBy2 = MyRange.where(" number % 2 = 0 ")
divisBy2.count()
//divisBy2.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Upload flight file for: 2015

// COMMAND ----------

val flightData2015=spark
.read
.option("inferSchema","true")
.option("header","true")
.csv("/FileStore/tables/2015_summary-ebaee.csv")


// COMMAND ----------

flightData2015.take(5)

// COMMAND ----------

flightData2015.sort("count").explain()

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions","5")
flightData2015.sort("count").take(2)

// COMMAND ----------

//oldDF.createOrReplaceTempView("newDF") into Table or temp View
flightData2015.createOrReplaceTempView("flight_Data_2015") 

// COMMAND ----------

//use of spark.sql function to Generate a new DF!!!
val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
from flight_Data_2015
GROUP BY DEST_COUNTRY_NAME
""")

val dataFrameWay = flightData2015
.groupBy("DEST_COUNTRY_NAME")
.count()

sqlWay.explain
dataFrameWay.explain

// COMMAND ----------

//find the max count 
import org.apache.spark.sql.functions.max
flightData2015.select(max("count")).take(1)

// COMMAND ----------

// les 5 premiers sum(count) w/ spark.sql
val maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_Data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()

// COMMAND ----------

//les 5 premiers avec DF et logique de l'engine avec explain
import org.apache.spark.sql.functions.desc

flightData2015
.groupBy("DEST_COUNTRY_NAME")
.sum("count")
.withColumnRenamed("sum(count)","destination_total")
.sort(desc("destination_total"))
.limit(5)
.show()
