from airflow import DAG
from dags.afternoon.python_callables import good_afternoon_world, go_on_lunch_break, pick_up_clothes_at_cleaners
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

    good_afternoon = PythonOperator(
        task_id='say_good_afternoon',
        python_callable=good_afternoon_world,
        dag=dag,
    )

    go_for_lunch_break = PythonOperator(
        task_id='go_for_lunch_break',
        python_callable=go_on_lunch_break,
        dag=dag,
    )

    pick_up_clothes = PythonOperator(
        task_id='take_dog_out',
        python_callable=pick_up_clothes_at_cleaners,
        dag=dag,
    )


    good_afternoon >> go_for_lunch_break
    good_afternoon >> pick_up_clothes


    return dag
