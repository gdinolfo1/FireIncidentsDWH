# Fire Incidents DWH
This repository is a brief description of the provided solution to the challenge of building a data warehouse from a dataset. 

The dataset provides from the 'San Francisco Datasets' webpage, and it includes a summary of each (non-medical) incident to which the SF Fire Department responded.

The **pipeline** will be the following:

![diagram](https://github.com/gdinolfo1/FireIncidents_DWH/blob/main/assets/Pipeline%20Diagram.JPG?raw=true)

I assume that the datasets are generated daily in the Transactional System on-premise. 

It generates on file per day with the datetime at the end of the file name, format %YYY%MM%DD.

For example: "fireincidents20220401.csv". 

A Python script is going to be run daily scheduled with Apache Airflow. This is going to extract / clean / split the dataset into different dataframes / load the dataframes as tables into BigQuery Platform. It will be an incremental load everydays. I assume that the new file generated has only new incidents numbers.

Python script 'fireIncidentsETL_DWH.py': https://github.com/gdinolfo1/FireIncidents_DWH/blob/main/fireIncidentsETL_DWH.py

For the sake of this exercise, the data warehouse is going to have only two tables:

  - Fact Table: "incidents_fact" with all the columns that will be used to make aggregations and prepared to run queries. It includes the following dimensions: time period, district, battalion and all the meaningful measures from the dataset.
  - Dimensional Table: "incidents_details". It represents all the dimensional tables that would be created if we normalize this database. It is just to simplify the model but it contains all the dimensions.

Finally we could run queries into the BigqQuery GCP as the following:

![query1](https://github.com/gdinolfo1/FireIncidents_DWH/blob/main/assets/query1.JPG?raw=true)

**Results**

![results](https://github.com/gdinolfo1/FireIncidents_DWH/blob/main/assets/Results.JPG?raw=true)




## ORCHESTRATION ##

Using Apache Airflow we could schedule the task with a daily frequency. It will run this scripts everyday at 00:00:00. Assuming that the files are uploaded in the OLTP at 23:00:00.

You have to turn on the DAG at Apache Airflow interface at localhost:8080

![airflow1](https://github.com/gdinolfo1/FireIncidents_DWH/blob/main/assets/Airflow1.JPG?raw=true)
