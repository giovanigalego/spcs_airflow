import os
from datetime import timedelta,datetime
from airflow import DAG
#from infra.common import airflow_slack_alerts
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'catchup':False
}


# Create the DAG with the specified schedule interval
dag = DAG('dbt_dag', default_args=default_args, schedule_interval=timedelta(days=1))
# Define the dbt run command as a BashOperator
run_dbt_model = BashOperator(
    task_id='run_dbt_model',
    bash_command='cd /opt/airflow/dbt/teste_equatorial && dbt run --profiles-dir /opt/airflow/dbt/teste_equatorial/',
    dag=dag
)