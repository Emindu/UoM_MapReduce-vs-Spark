# aws_emr_hive_spark

1. connecting to emr cluster though ssh
```
ssh -i ~/vockey.pem <>.amazonaws.com
```

![img.png](Resources/ssh_connection_done.png)


### Attribute names and data type of input csv


|     Column          | Non-Null Count | Dtype   |
|---------------------|----------------|---------|
| Unnamed: 0          | 1000           | int64   |
| Year                | 1000           | int64   |
| Month               | 1000           | int64   |
| DayofMonth          | 1000           | int64   |
| DayOfWeek           | 1000           | int64   |
| DepTime             | 1000           | int64   |
| CRSDepTime          | 1000           | int64   |
| ArrTime             | 990            | float64 |
| CRSArrTime          | 1000           | int64   |
| UniqueCarrier       | 1000           | object  |
| FlightNum           | 1000           | int64   |
| TailNum             | 1000           | object  |
| ActualElapsedTime   | 990            | float64 |
| CRSElapsedTime      | 1000           | int64   |
| AirTime             | 990            | float64 |
| ArrDelay            | 990            | float64 |
| DepDelay            | 1000           | int64   |
| Origin              | 1000           | object  |
| Dest                | 1000           | object  |
| Distance            | 1000           | int64   |
| TaxiIn              | 990            | float64 |
| TaxiOut             | 1000           | int64   |
| Cancelled           | 1000           | int64   |
| CancellationCode    | 1000           | object  |
| Diverted            | 1000           | int64   |
| CarrierDelay        | 760            | float64 |
| WeatherDelay        | 760            | float64 |
| NASDelay            | 760            | float64 |
| SecurityDelay       | 760            | float64 |
| LateAircraftDelay   | 760            | float64 |


### hive sql table create command to create table for matching above data

```
CREATE EXTERNAL TABLE IF NOT EXISTS delayedFlights(
  Counter int,
  Year int, 
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Dest string,
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled string,
  CancellationCode string,
  Diverted string,
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION '/user/tables/delayedFlights';

```

### loading data
```
LOAD DATA INPATH '<s3 bucket link>' OVERWRITE INTO TABLE delayedFlights;
```


#### now we can run sql commands such as,
``` select * from delayedFlights ```


#### The airlines market has been faced losses due to the flight delay and there are many reasons for delaying a flight. In this Analysis, you need to analyse the various delay happens in airlines per year and run the queries as follows.


1. Year wise carrier delay from 2003-2010
```
SELECT Year As Year, SUM(CarrierDelay) As TotalCarrierDelay
FROM delayedflights
WHERE Year BETWEEN 2003 AND  2010
GROUP BY Year
ORDER BY Year;
```

#### results
![img.png](Resources/hadoop_carrier_1.png)


2. Year wise NAS delay from 2003-2010
```
SELECT Year As Year, SUM(NASDelay) AS TotalNASDelay
FROM delayedFlights
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year
ORDER BY Year;
```

#### results
![img.png](Resources/hadoop_nas_1.png)

3. Year wise Weather delay from 2003-2010
```
SELECT Year As Year, SUM(WeatherDelay) AS TotalWeatherDelay
FROM delayedFlights
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year
ORDER BY Year;
```

#### results
![img.png](Resources/hadoop_weather_1.png)


4. Year wise late aircraft delay from 2003-2010

```
SELECT Year As Year, SUM(LateAircraftDelay) AS LateAircraftDelay
FROM delayedFlights
WHERE Year BETWEEN 2003 AND  2010
GROUP BY Year
ORDER BY Year;
```

#### results
![img.png](Resources/hadoop_aircraflt_1.png)

5. Year wise security delay from 2003-2010
```
SELECT Year As Year, SUM(SecurityDelay) AS SecurityDelay
FROM delayedFlights
WHERE Year BETWEEN 2003 AND  2010
GROUP BY Year
ORDER BY Year;

```

#### results
![img.png](Resources/hadoop_security_1.png) 



## With Spark(pyspark)

for that we have to create new emr cluster with spark application installed.

after connecting to the cluster with ssh connection we can use ``` pyspark ``` command to open pyshark shell

used commands in pyspark

importing needed libaries
```
from pyspark.sql import SparkSession
import time
```

importing data

```
# Create a SparkSession object
spark = SparkSession.builder.appName("DelayedFlights").getOrCreate()

# Read in the flight data from a CSV file
delayedFlights = spark.read.format("csv").option("header", "true").load("s3://coursework/input/DelayedFlights-updated.csv")


delayedFlights.createOrReplaceTempView("delayedFlights")

```
#### sample command
````
spark.sql("select * from delayedFlights where year= 2008").show();
````


 ### Running time for queries


#### Carrier- Delay 
```


s_t = time.time()

spark.sql("""
	SELECT Year As Year, SUM(CarrierDelay) As TotalCarrierDelay
	FROM DelayedFlights
	WHERE Year between 2003 AND  2010
	GROUP BY Year
	ORDER BY Year""").show()

e_t = time.time()
run_time = e_t - s_t
print("Time taken: ", run_time, " seconds")
```

#### results
![img.png](Resources/spark_carrier_1.png) 


#### NAS  Delay 


```
s_t = time.time()

spark.sql("""
	SELECT Year As Year, SUM(NASDelay) As TotalNASDelay
	FROM DelayedFlights
	WHERE Year between 2003 AND  2010
	GROUP BY Year
	ORDER BY Year""").show()

e_t = time.time()
run_time = e_t - s_t
print("Time taken: ", run_time, " seconds")

```

#### results
![img.png](Resources/spark_nas_1.png) 


#### Weather- Delay 

```
s_t = time.time()

spark.sql("""
	SELECT Year As Year, SUM(WeatherDelay) As TotalWeatherDelay
	FROM DelayedFlights
	WHERE Year between 2003 AND  2010
	GROUP BY Year
	ORDER BY Year""").show()

e_t = time.time()
run_time = e_t - s_t
print("Time taken: ", run_time, " seconds")
```

#### results
![img.png](Resources/spark_weather_1.png) 

#### LateAircraft- Delay

```
s_t = time.time()

spark.sql("""
	SELECT Year As Year, SUM(LateAircraftDelay) As TotalLateAircraftDelay
	FROM DelayedFlights
	WHERE Year between 2003 AND  2010
	GROUP BY Year
	ORDER BY Year""").show()

e_t = time.time()
run_time = e_t - s_t
print("Time taken: ", run_time, " seconds")
```


#### results
![img.png](Resources/spark_aircraflt_1.png) 

#### Security-Delay

```
s_t = time.time()

spark.sql("""
	SELECT Year As Year, SUM(SecurityDelay) As TotalSecurityDelay
	FROM DelayedFlights
	WHERE Year between 2003 AND  2010
	GROUP BY Year
	ORDER BY Year""").show()

e_t = time.time()
run_time = e_t - s_t
print("Time taken: ", run_time, " seconds")
```
#### results
![img.png](Resources/spark_security_1.png) 



## Comparison


