"""
DAG final_project_palchyk1984
----------------------------
Цей DAG визначає 3 задачі обробки даних:
1) З landing до bronze
2) З bronze до silver
3) З silver до gold

Використовує Apache Spark через SparkSubmitOperator для виконання кожного етапу.
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Налаштування за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 0  # Кількість повторних спроб (рекламних) у разі помилки
}

# Ідентифікатор Spark Connection у Airflow
connection_id = 'spark-default'

with DAG(
    dag_id='final_project_palchyk1984',
    default_args=default_args,
    schedule_interval=None,  # Виконується "вручну", без розкладу
    catchup=False,
    tags=["palchyk1984"],
    description="ETL-процес: landing → bronze → silver → gold",
) as dag:
    """
    Визначення DAG:
      1) Task landing_to_bronze
      2) Task bronze_to_silver
      3) Task silver_to_gold
      Виконуються послідовно.
    """

    # 1. landing_to_bronze
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application='dags/palchyk1984_Batch_Data_Lake/jobs/landing_to_bronze.py',
        conn_id=connection_id,
        verbose=True,
        executor_cores=2,
        executor_memory="1g",
        driver_memory="1g",
    )

    # 2. bronze_to_silver
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application='dags/palchyk1984_Batch_Data_Lake/jobs/bronze_to_silver.py',
        conn_id=connection_id,
        verbose=True,
        executor_cores=2,
        executor_memory="1g",
        driver_memory="1g",
    )

    # 3. silver_to_gold
    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application='dags/palchyk1984_Batch_Data_Lake/jobs/silver_to_gold.py',
        conn_id=connection_id,
        verbose=True,
        executor_cores=2,
        executor_memory="1g",
        driver_memory="1g",
    )

    # Послідовність виконання
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
