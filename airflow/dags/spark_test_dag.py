from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


from const import HOME_DIR, DATA_DIR

from datetime import datetime

from operators import download_files_and_unzip, delete_files


local_workflow = DAG(
    "SparkTest",
    schedule_interval="@once",
    start_date=datetime(2022, 11, 13),
    max_active_tasks=1,
    max_active_runs=1
)

with local_workflow:

    download_files_task = PythonOperator(
        task_id="download_files",
        python_callable=download_files_and_unzip,
        provide_context=True,
        op_kwargs=dict(
            path=DATA_DIR,
        )
    )

    spark_job_task = BashOperator(
        task_id = "spark-job",
        bash_command=f"python {HOME_DIR}/dags/spark_test.py"
    )

    # spark_job_task = SparkSubmitOperator(
    #     task_id = "spark-job",
    #     conn_id = 'spark-test',
    #     application = f"{HOME_DIR}/dags/spark_test.py"
    # )


    delete_files_task = PythonOperator(
        task_id='delete_files',
        python_callable=delete_files,
        provide_context=True,
        op_kwargs=dict(
            path=DATA_DIR,
        )
    )

    download_files_task >> spark_job_task >> delete_files_task