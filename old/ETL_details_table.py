import pandas as pd
import io
import requests
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= 'C:/Users/User/Downloads/fireincidents-337202-cb9ff405772d.json'

incidents_details = "fireincidents-337202.fireIncidentsDWH.incidents_details"


df = pd.read_csv('C:/Users/User/Documents/Data Engineering Projects/11. Challenge/Fire_Incidents.csv', engine = 'python')

# Replacing the space with underscore in column names. This is needed to be able to load into Bigquery.
df.columns = df.columns.str.replace(' ','_')

# With these clause: "The copy of the dataset in the data warehouse should reflect exactly the current state of the data at the source."
# I assume that I don't have to drop NaN columns.

# Creating the "Dimensions_Table Dataframe"

incidentsDetailsDF = df.iloc[: , [0,1,2,3,5,9,10,12,13,20,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,63]].copy()


incidentsDetailsDF.to_gbp(destination_table= 'fireIncidentsDWH.incidents_details', project_id= 'fireincidents-337202', if_exists= 'append')

