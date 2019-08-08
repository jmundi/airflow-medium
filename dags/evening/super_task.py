from airflow import DAG
from dags.evening.python_callables import good_evening_world, return_home_from_work, pick_up_children_from_school, prepare_dinner, call_mother
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

    good_evening = PythonOperator(
        task_id='say_good_evening',
        python_callable=good_evening_world,
        dag=dag,
    )

    return_home = PythonOperator(
        task_id='return_home',
        python_callable=return_home_from_work,
        dag=dag,
    )

    cook = PythonOperator(
        task_id='prepare_dinner',
        python_callable=prepare_dinner,
        dag=dag,
    )

    make_a_call = PythonOperator(
        task_id='make_a_call',
        python_callable=call_mother,
        dag=dag,
    )

    pick_up_children = PythonOperator(
        task_id='pick_up_children',
        python_callable=pick_up_children_from_school,
        dag=dag,
    )

    good_evening >> return_home
    good_evening >> make_a_call
    return_home >> cook
    return_home >> pick_up_children

    return dag
