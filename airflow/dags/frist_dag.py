from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from operators import (
    download_files_and_unzip,
    csv_to_parquet,
    prepare_data,
    upload_parquet_to_postgres,
    delete_files
)

from const import connect_dict, DATA_DIR

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="@daily",
    start_date=datetime(2022, 10, 31),
    end_date=datetime(2022, 11, 27),
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

    # csv_to_parquet_task = PythonOperator(
    #     task_id="csv_to_parquet",
    #     python_callable=csv_to_parquet,
    #     provide_context=True,
    #     op_kwargs=dict(
    #         path=DATA_DIR,
    #     )
    # )

    prepare_data_task = PythonOperator(
        task_id="prepare_data",
        python_callable=prepare_data,
        provide_context=True,
        op_kwargs=dict(
            path=DATA_DIR,
        )
    )

    upload_parquet_to_postgres_task = PythonOperator(
        task_id="upload_parquet_to_postgres",
        python_callable=upload_parquet_to_postgres,
        provide_context=True,
        op_kwargs=dict(
            connect_dict=connect_dict,
            path=DATA_DIR,
            table_name="eleicoes_2022"
        )
    )

    delete_files_task = PythonOperator(
        task_id='delete_files',
        python_callable=delete_files,
        provide_context=True,
        op_kwargs=dict(
            path=DATA_DIR,
        )
    )

    download_files_task >> prepare_data_task >> upload_parquet_to_postgres_task >> delete_files_task