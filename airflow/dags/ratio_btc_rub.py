from datetime import datetime
from typing import Dict, List

import psycopg2
import requests
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG


TIMESERIES_URL = 'https://api.exchangerate.host/{date}'
db_connection_data = dict(
    host='db',
    port='5432',
    user='postgres',
    password='password',
    database='test'
)


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 29)
}


def get_historical_rates_data(date: str, 
                              base_currency: str = 'RUB',
                              places: int = 2,
                              symbols: List[str] = ['BTC']
                              ) -> dict:
    """
    Обращается к api `https://api.exchangerate.host/{конкретная дата}` и
    возвращает цены указанных `symbols` на дату `date` по отношению к валюте `base_currency`

    :param base_currency: Трёхбуквенный код валюты, которая будет
    считаться базовой. Т.е. остальные валюты, указанные в `symbols`
    будут конвертироваться по отношению к ней

    :param plases: До скольки знаков после запятой нужно округлить
    выходные данные

    :param symbols: Трёхбуквенные обозначения валют, курс которых
    нужно получить 
    """
    data = dict(
        base=base_currency,
        places=places,
        symbols=','.join(symbols)
    )
    responce = requests.get(TIMESERIES_URL.format(date=date), params=data)
    return responce.json()


def insert_historical_rates_to_db(connection,
                                  base_currency: str,
                                  historical_rates: Dict[str, str]):
    
    query = ('INSERT INTO historical_rates ("date", first_currency, second_currency, rate) '
             'VALUES (%s, %s, %s, %s)')
    
    now = datetime.now()
    data = []
    for rate_info in historical_rates.items():
        second_currency, rate = rate_info
        data.append((now, base_currency, second_currency, rate))

    cursor = connection.cursor()
    cursor.executemany(query, data)
    connection.commit()
    cursor.close()


def get_and_insert_to_db_historical_rates():
    historical_rates_data = get_historical_rates_data(date=str(datetime.now().date()),
                                                      places=10)
    connection = psycopg2.connect(**db_connection_data)
    insert_historical_rates_to_db(connection,
                                  base_currency='RUB',
                                  historical_rates=historical_rates_data['rates'])
    

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

    task2 = PythonOperator(
        task_id='extract_and_load_historical_rates',
        python_callable=get_and_insert_to_db_historical_rates
    )

    task1 >> task2