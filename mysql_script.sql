CREATE DATABASE dbname;

CREATE TABLE stores (id int, type varchar(1), size int);

CREATE TABLE  features (id int,
			s_date date,
			temperature float,
			fuel_price float,
			markdown1 float,
			markdown2 float,
			markdown3 float,
			markdown4 float,
			markdown5 float,
			cpi float,
			unemployment float,
			isholiday varchar(10));

CREATE TABLE weekly_sales (id int,
			   dept int,
			   s_date date,
			   weekly_sales float,
			   isholiday varchar(10));

LOAD DATA INFILE '/home/hduser/walmart/stores.csv' 
INTO TABLE stores
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/home/hduser/walmart/features.csv' 
INTO TABLE features
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/home/hduser/walmart/train.csv' 
INTO TABLE weekly_sales
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n';
