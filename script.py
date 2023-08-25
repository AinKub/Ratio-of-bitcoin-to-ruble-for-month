from typing import Dict, List

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
                                                      '2023-08-22',
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
    connection.close()