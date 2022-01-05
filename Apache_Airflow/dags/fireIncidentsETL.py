# para iniciar el Docker tiene que estar corriendo el Docker Desktop (icono verde en barra de inicio) y poner en terminal: 'docker-compose up --build'

from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime

def _process():
    print("Process DONE!. Date: " + datetime.today().strftime('%Y%m%d'))

with DAG('fireIncidentsETL', start_date=datetime(2021, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    t1 = BashOperator(
    task_id='fireIncidentsETL',
    bash_command= 'python ~/dags/fireIncidentsETL_DWH.py',
    dag=dag)


    t2 = PythonOperator(
        task_id="message_Done",
        python_callable= _process
    )

    t1 >> t2


