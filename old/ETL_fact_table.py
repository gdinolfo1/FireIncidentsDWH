import pandas as pd
import io
import requests
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= 'C:/Users/User/Downloads/fireincidents-337202-cb9ff405772d.json'

incidents_fact = "fireincidents-337202.fireIncidentsDWH.incidents_fact"


df = pd.read_csv('C:/Users/User/Documents/Data Engineering Projects/11. Challenge/Fire_Incidents.csv', engine = 'python')

# Replacing the space with underscore in column names. This is needed to be able to load into Bigquery.
df.columns = df.columns.str.replace(' ','_')

# Creating the "Fact_Table Dataframe" that will be loaded into BigQuery later
# It contains the main measures and the following dimensions: time period, district, battalion. These will be used to aggregate the queries by.

incidentsFactDF = df.iloc[: , [0,4,6,7,8,11,14,15,16,17,18,19,21,22,23,24,25,26,27,62]].copy()

# With these clause: "The copy of the dataset in the data warehouse should reflect exactly the current state of the data at the source."
# I assume that I don't have to drop NaN columns.

incidentsFactDF.to_gbq(destination_table = 'fireIncidentsDWH.incidents_fact', project_id= 'fireincidents-337202', if_exists= 'append')
