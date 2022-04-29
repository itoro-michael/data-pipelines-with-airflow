# data-pipelines-with-airflow
This project implements Airflow pipeline that stages data from S3 on AWS RedShift 
as well as creates fact and dimension tables from the staged data. Quality checks 
are made as part of the data pipeline. The Airflow pipeline features the use of
AWS Connection, AwsHook, PostgresHook, and custom Operators.

The flow diagram for the data pipeline is shown below:

![Data pipeline diagram](https://github.com/itoro-michael/data-pipelines-with-airflow/blob/main/dags/project/data_pipeline.jpg?raw=true)