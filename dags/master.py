from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta


def good_morning_world():
    """
    This is a python Airflow callable that returns 'Good Morning World!'.
    :return: String
    """
    return 'Good Morning World!'


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 8, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(days=1)
}

dag = DAG("Medium-AirFlow-Series", default_args=default_args, schedule_interval=timedelta(1))

start = DummyOperator(
    task_id='start-day',
    dag=dag
)
end = DummyOperator(
    task_id='end-day',
    dag=dag
)

good_morning = PythonOperator(
    task_id='say_good_morning',
    python_callable=good_morning_world,
    dag=dag,
)

start >> good_morning >> end
