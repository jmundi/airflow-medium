from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta

from morning import super_task as morning_st
from afternoon.super_task import sub_dag as afternoon_sub_dag
from evening.super_task import sub_dag as evening_sub_dag


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 8, 7)
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

morning_tasks = SubDagOperator(
  subdag=morning_st.sub_dag('morning_tasks', 'morning', dag.start_date,
                 dag.schedule_interval),
  task_id='morning',
  dag=dag,
)

afternoon_tasks = SubDagOperator(
  subdag=afternoon_sub_dag('afternoon_tasks', 'afternoon', dag.start_date,
                 dag.schedule_interval),
  task_id='afternoon',
  dag=dag,
)

evening_tasks = SubDagOperator(
  subdag=evening_sub_dag('evening_tasks', 'evening', dag.start_date,
                 dag.schedule_interval),
  task_id='evening',
  dag=dag,
)


start >> morning_tasks >> afternoon_tasks >> evening_tasks >> end
