from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from functions_dag.extract import validate_file_kafka,process_file
from airflow.operators.empty import EmptyOperator


with DAG('extract_dag', start_date = datetime(2024, 2, 4),
         schedule_interval = '10 * * * *', catchup=False,description='Dag para extração de dados de um csv') as dag:
    
    validate_task = BranchPythonOperator(
        task_id = 'validate',
        python_callable = validate_file_kafka
    )

    validated_task = BashOperator(
        task_id = 'validated',
        bash_command = "echo 'Iniciar Leitura de arquivo!'"
    )

    process_task = PythonOperator(
        task_id = 'process',
        python_callable = process_file
    )

    nvalidated_task = BashOperator(
        task_id = 'nvalidated',
        bash_command = "echo 'Não houve leitura de arquivo!'"
    )

    end_extract_task = EmptyOperator(
    task_id='end_extract_task',
    dag=dag
    )

    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_dag',
        trigger_dag_id='transform_dag',
        dag=dag,
    )

    trigger_localidades_dag = TriggerDagRunOperator(
        task_id='trigger_localidades_dag',
        trigger_dag_id='localidades_dag',
        dag=dag,
    )


validate_task >> [validated_task, nvalidated_task] 
validated_task >> process_task >> end_extract_task >> trigger_transform_dag
end_extract_task >> trigger_localidades_dag