from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from functions_dag.transform import remove_duplicates, raw_to_trusted, trusted_to_refined, transfer_data_mongodb_to_mysql, remove_refined_duplicates
from airflow.operators.empty import EmptyOperator


with DAG('transform_dag', start_date = datetime(2024, 2, 4),
         schedule_interval = None, catchup=False,description='Dag para transformação dos dados carregados no topico tcc_extract') as dag:
    
    start_task = EmptyOperator(
        task_id = 'start_transform',
        dag=dag
    )

    verify_duplicates_task = PythonOperator(
        task_id = 'verify_duplicates',
        python_callable = remove_duplicates,
        dag=dag,
    )

    raw_to_trusted_task = PythonOperator(
        task_id = 'raw_to_trusted',
        python_callable = raw_to_trusted,
        dag=dag,
    )

    trusted_to_refined_task = PythonOperator(
        task_id = 'trusted_to_refined',
        python_callable = trusted_to_refined,
        dag=dag,
    )

    final_data_task = PythonOperator(
        task_id = 'final_data',
        python_callable = transfer_data_mongodb_to_mysql,
        dag=dag,
    )

    remove_refined_task = PythonOperator(
        task_id = 'removed_refined_duplicates',
        python_callable = remove_refined_duplicates,
        dag=dag,
    )

    end_task = EmptyOperator(
    task_id='end_transform',
    dag=dag,
)

start_task >> raw_to_trusted_task >> verify_duplicates_task  >> trusted_to_refined_task >> remove_refined_task >> final_data_task >> end_task
