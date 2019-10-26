
import pyspark.sql.functions as f
#from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark import SparkConf, SparkContext
import pyspark

sqlContext = pyspark.SQLContext(pyspark.SparkContext())



my_spark =  SparkSession.builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/") \
    .getOrCreate()


stores  = my_spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost/PDA",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "stores",
    user="root",
    password="hadoop@123").load()

features  = my_spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost/PDA",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "features",
    user="root",
    password="hadoop@123").load()

wsales = my_spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost/PDA",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "weekly_sales",
    user="root",
    password="hadoop@123").load()

st = stores.alias('st')
fe = features.alias('fe')
ws = wsales.alias('ws')


temp_tab = ws.groupBy("id","s_date")\
    .agg(f.max("weekly_sales").alias('max_ws'))

tt = temp_tab.alias('tt')


inner_join = fe.join(tt, ["id" , "s_date"])


temp_sales = inner_join.select("id","s_date","temperature","max_ws")\
                   .sort(f.desc("max_ws"))\
                   .write.format("com.mongodb.spark.sql.DefaultSource")\
                   .mode("append")\
                   .option("database", "PDA").option("collection", "temp_sales").save()


fuel_sales = inner_join.select("id","s_date","fuel_price","max_ws")\
                   .sort(f.desc("max_ws"))\
                   .write.format("com.mongodb.spark.sql.DefaultSource")\
                   .mode("append")\
                   .option("database", "PDA").option("collection", "fuel_sales").save()



cpi_sales = inner_join.select("id","s_date","cpi","max_ws")\
                   .sort(f.desc("max_ws"))\
                   .write.format("com.mongodb.spark.sql.DefaultSource")\
                   .mode("append")\
                   .option("database", "PDA").option("collection", "cpi_sales").save()




inner_join2 = st.join(ws,"id")

type_sales = inner_join2.groupBy("type")\
                   .agg(f.sum("weekly_sales").alias('sum_ws'))\
                   .select("type","sum_ws")\
                   .sort(f.desc("sum_ws"))\
                   .write.format("com.mongodb.spark.sql.DefaultSource")\
                   .mode("append")\
                   .option("database", "PDA").option("collection", "type_sales").save()



dept_sales = ws.groupBy("dept")\
              .agg(f.sum("weekly_sales").alias("dep_sale"))\
              .select("dept","dep_sale")\
              .sort(f.desc("dep_sale"))\
              .write.format("com.mongodb.spark.sql.DefaultSource")\
              .mode("append")\
              .option("database", "PDA").option("collection", "dept_sales").save()



holiday_sales = ws.groupBy("isholiday")\
                  .agg(f.sum("weekly_sales").alias("holiday_sale"))\
                  .select("isholiday","holiday_sale")\
                  .sort(f.desc("holiday_sale"))\
                  .write.format("com.mongodb.spark.sql.DefaultSource")\
                  .mode("append")\
                  .option("database", "PDA").option("collection", "holiday_sales").save()


