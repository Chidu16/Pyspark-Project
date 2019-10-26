#!/bin/sh

mysql -u root -phadoop@123 PDA < /home/hduser/walmart/mysql_script.sql

spark-submit --packages mysql:mysql-connector-java:5.1.39,org.mongodb.spark:mongo-spark-connector_2.12:2.4.0 /usr/local/spark/walmart.py

mongoexport --db PDA --collection temp_sales --type=csv --fields id,s_date,temperature,max_ws --out /home/hduser/walmart/data/temp_sales.csv
mongoexport --db PDA --collection fuel_sales --type=csv --fields id,s_date,fuel_price,max_ws --out /home/hduser/walmart/data/fuel_sales.csv
mongoexport --db PDA --collection cpi_sales --type=csv --fields id,s_date,cpi,max_ws --out /home/hduser/walmart/data/cpi_sales.csv
mongoexport --db PDA --collection type_sales --type=csv --fields type,sum_ws --out /home/hduser/walmart/data/type_sales.csv
mongoexport --db PDA --collection dept_sales --type=csv --fields dept,dep_sale --out /home/hduser/walmart/data/dept_sales.csv
mongoexport --db PDA --collection holiday_sales --type=csv --fields isholiday,holiday_sale --out /home/hduser/walmart/data/holiday_sales.csv
