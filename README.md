# Ratio-of-bitcoin-to-ruble-for-month

Данный проект получает курс BTC к RUB на заданные даты, затем создаёт таблицу в базе данных и вставляет туда все полученные значения.
После создаётся таблица, куда вставляются полученные значения, а именно:
- день, в который значение курса было максимальным
- день, в который значение курса было минимальным
- максимальное значение курса
- минимальное значение курса
- среднее значение курса за весь месяц
- значение курса на последний день месяца
- первая валюта
- вторая валюта
- номер месяца, за которых производились расчёты


Для запуска необходимо: 
- Запустить postgres (или через docker-compose, или другим удобным способом)
- Создать виртуальное окружение и установить все необходимые библиотеки с помощью `pip install -r requirements.txt`
- Запустить скрипт [script.py](./script.py)