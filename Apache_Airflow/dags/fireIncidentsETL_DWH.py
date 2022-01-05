import pandas as pd
import io
import requests
from google.cloud import bigquery
import os
from datetime import datetime

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= 'C:/Users/User/Downloads/fireincidents-337202-cb9ff405772d.json'

# IF we want to use it in the OLTP Daily file directory we have to use the following two lines to read the correct csv:
# path = '/OLTP/Fire_Incidents' + datetime.today().strftime('%Y%m%d') + ".csv"
# df = pd.read_csv(path, engine = 'python')

# to run without Apache Airflow
# df = pd.read_csv('C:/Users/User/Documents/Data_Engineering_Projects/FireIncidentsETL/Apache_Airflow/dags/Fire_Incidents.csv', engine = 'python')

df = pd.read_csv('~/dags/Fire_Incidents.csv', engine = 'python')


# Replacing the space with underscore in column names. This is needed to be able to load into Bigquery.
df.columns = df.columns.str.replace(' ','_')

# Creating the "Fact_Table Dataframe" that will be loaded into BigQuery later
# It contains the main measures and the following dimensions: time period, district, battalion. These will be used to aggregate the queries by.

incidentsFactDF = df.iloc[: , [0,4,6,7,8,11,14,15,16,17,18,19,21,22,23,24,25,26,27,62]].copy()

# With the clause: "The copy of the dataset in the data warehouse should reflect exactly the current state of the data at the source."
# I assume that I don't have to drop NaN columns.


# Creating the "Dimensions_Table Dataframe"
incidentsDetailsDF = df.iloc[: , [0,1,2,3,5,9,10,12,13,20,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,63]].copy()

# Appending both tables rows to existing tables in BigQuery

incidentsFactDF.to_gbq(destination_table = 'fireIncidentsDWH.incidents_fact', project_id= 'fireincidents-337202', if_exists= 'append')

incidentsDetailsDF.to_gbq(destination_table= 'fireIncidentsDWH.incidents_details', project_id= 'fireincidents-337202', if_exists= 'append')