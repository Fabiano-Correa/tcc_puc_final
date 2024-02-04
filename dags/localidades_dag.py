from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from functions_dag.localidades import get_ibge, save_local_to_mysql
from airflow.operators.empty import EmptyOperator



with DAG('localidades_dag', start_date = datetime(2024, 2, 4),
         schedule_interval = '10 * * * *', catchup=False,description='Dag para ingestão dos dados de api do IBGE') as dag:
 
    start_task = EmptyOperator(
        task_id = 'start_dag',
        dag=dag
    )

    localidades_task = PythonOperator(
        task_id = 'localidades',
        python_callable = get_ibge
    )

    final_data_localidades_task = PythonOperator(
        task_id = 'final_data_localidades',
        python_callable = save_local_to_mysql,
        dag=dag,
    )

    end_task = EmptyOperator(
    task_id='end_dag',
    dag=dag,
)
    
start_task >> localidades_task >> final_data_localidades_task >> end_task