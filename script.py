from datetime import date
from typing import Dict, List, Tuple, Union

import psycopg2
import requests

from config import DATABASE, HOST, PASSWORD, PORT, TIMESERIES_URL, USERNAME


def create_historical_rates_table(connection) -> None:
    query = ('CREATE TABLE IF NOT EXISTS historical_rates ('
             'id serial4 PRIMARY KEY, '
             '"date" DATE NOT NULL, '
             'first_currency VARCHAR(3) NOT NULL, '
             'second_currency VARCHAR(3) NOT NULL, '
             'rate REAL NOT NULL)')
    
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()


def create_result_data_mart(connection) -> None:
    query = ('CREATE TABLE IF NOT EXISTS general_statistics_for_month ('
             'max_rate_date DATE NOT NULL, '
             'min_rate_date DATE NOT NULL, '
             'max_rate REAL NOT NULL, '
             'min_rate REAL NOT NULL, '
             'avg_rate REAL NOT NULL, '
             'last_date_rate REAL NOT NULL, '
             'first_currency VARCHAR(3) NOT NULL, '
             'second_currency VARCHAR(3) NOT NULL, '
             'month_num VARCHAR(2) NOT NULL)')
    
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()


def insert_historical_rates_to_db(connection,
                                  base_currency: str,
                                  historical_rates: Dict[str, Dict[str, str]]):
    
    query = ('INSERT INTO historical_rates ("date", first_currency, second_currency, rate) '
             'VALUES (%s, %s, %s, %s)')
    
    data = []
    for date, rate in historical_rates.items():
        second_currency, rate = list(rate.items())[0]
        data.append((date, base_currency, second_currency, rate))

    cursor = connection.cursor()
    cursor.executemany(query, data)
    connection.commit()
    cursor.close()    


def insert_statistics_into_data_mart(connection,
                                     data_to_insert: Tuple):
    
    query = """INSERT INTO general_statistics_for_month (max_rate_date,
                                                         min_rate_date,
                                                         max_rate,
                                                         min_rate,
                                                         avg_rate,
                                                         last_date_rate,
                                                         first_currency,
                                                         second_currency,
                                                         month_num)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
    cursor = connection.cursor()
    cursor.execute(query, data_to_insert)
    connection.commit()
    cursor.close()


def get_analitics(connection
                  ) -> Union[Tuple[date, date, float, float, float, float, str, str, str], None]:
    
    min_or_max_subqury = """SELECT rate, "date" 
                            FROM historical_rates
                            GROUP BY id, "date"
                            ORDER BY rate {desc} LIMIT 1"""
    
    max_hr_subquery = min_or_max_subqury.format(desc='DESC')
    min_hr_subquery = min_or_max_subqury.format(desc='')

    avg_hr_subquery = 'SELECT AVG(rate) AS rate FROM historical_rates'
    last_date_hr_subquery = """SELECT rate, "date", first_currency fc, second_currency sc
                               FROM historical_rates
                               ORDER BY "date" DESC LIMIT 1"""

    query = """SELECT max_hr."date", 
                      min_hr."date", 
                      max_hr.rate, 
                      min_hr.rate,
                      avg_hr.rate, 
                      last_date_hr.rate, 
                      last_date_hr.fc, 
                      last_date_hr.sc,
                      to_char(last_date_hr."date", \'MM\')
               FROM ({max_hr_subquery}) AS max_hr, 
                    ({min_hr_subquery}) AS min_hr,
                    ({avg_hr_subquery}) AS avg_hr,
                    ({last_date_hr_subquery}) AS last_date_hr
            """.format(
        max_hr_subquery=max_hr_subquery,
        min_hr_subquery=min_hr_subquery,
        avg_hr_subquery=avg_hr_subquery,
        last_date_hr_subquery=last_date_hr_subquery
    )
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    return result
    

def get_historical_rates_data(start_date: str, 
                              end_date: str,
                              base_currency: str = 'RUB',
                              places: int = 2,
                              symbols: List[str] = ['BTC']
                              ) -> dict:
    """
    Обращается к api `https://api.exchangerate.host/timeseries` и
    возвращает цены указанных `symbols` на даты со `start_date` по
    `end_date` по отношению к валюте `base_currency`

    :param base_currency: Трёхбуквенный код валюты, которая будет
    считаться базовой. Т.е. остальные валюты, указанные в `symbols`
    будут конвертироваться по отношению к ней

    :param plases: До скольки знаков после запятой нужно округлить
    выходные данные

    :param symbols: Трёхбуквенные обозначения валют, курс которых
    нужно получить 
    """
    data = dict(
        start_date=start_date,
        end_date=end_date,
        base=base_currency,
        places=places,
        symbols=','.join(symbols)
    )
    responce = requests.get(TIMESERIES_URL, params=data)
    return responce.json()


if __name__ == '__main__':
    historical_rates_data = get_historical_rates_data('2023-08-01',
                                                      '2023-08-25',
                                                      places=10)
    print(historical_rates_data)

    connection = psycopg2.connect(host=HOST,
                                  port=PORT,
                                  user=USERNAME,
                                  password=PASSWORD,
                                  database=DATABASE)
    
    create_historical_rates_table(connection)
    insert_historical_rates_to_db(connection,
                                  base_currency='RUB',
                                  historical_rates=historical_rates_data['rates'])
    result = get_analitics(connection)
    create_result_data_mart(connection)
    insert_statistics_into_data_mart(connection, result)
    connection.close()