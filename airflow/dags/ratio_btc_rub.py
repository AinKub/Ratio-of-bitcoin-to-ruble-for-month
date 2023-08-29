from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 29)
}

with DAG(
    'ratio_bitcoin_rub',
    catchup=False,
    schedule='*/10 * * * *',
    default_args=args
):
    task1 = BashOperator(
        task_id='greetings',
        bash_command='echo "Good morning my diggers!"'
    )