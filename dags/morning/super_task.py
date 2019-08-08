
from airflow import DAG
from python_callables import good_morning_world, check_emails, take_a_shower, take_dog_for_a_walk
from airflow.operators.python_operator import PythonOperator


def sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    """
    This subDAG function will be seen my our main AirFlow application as a
    (super) task. But this DAG will encapsulate a subDAG and this subDAG
    will contain sub tasks.
    :param parent_dag_name:
    :param child_dag_name:
    :param start_date:
    :param schedule_interval:
    :return: DAG object
    """
    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    good_morning = PythonOperator(
        task_id='say_good_morning',
        python_callable=good_morning_world,
        dag=dag,
    )

    check_email = PythonOperator(
        task_id='check_emails',
        python_callable=check_emails,
        dag=dag,
    )

    take_dog_out = PythonOperator(
        task_id='take_dog_out',
        python_callable=take_dog_for_a_walk,
        dag=dag,
    )

    take_shower = PythonOperator(
        task_id='take_dog_out',
        python_callable=take_a_shower,
        dag=dag,
    )

    good_morning >> check_email
    good_morning >> take_dog_out >> take_a_shower
    check_email >> take_shower

    return dag
