# aws_emr_hive_spark

1. connecting to emr cluster though ssh
```
ssh -i ~/vockey.pem <>.amazonaws.com
```

![img.png](Resources/ssh_connection_done.png)


Attribute names and data type of input csv


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