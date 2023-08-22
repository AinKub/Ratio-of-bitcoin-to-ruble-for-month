from typing import List

import psycopg2
import requests

from config import DATABASE, HOST, PASSWORD, PORT, TIMESERIES_URL, USERNAME


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