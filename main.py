import psycopg2
import requests
import logging
from datetime import date

URL_BREED = "https://catfact.ninja/breeds"
DATABASE = "postgres"
USER = "postgres"
PASSWORD = "postgres"
PORT = "5432"


logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(database=DATABASE, user=USER, password=PASSWORD, port=PORT)


def close_connection(conn):
    conn.close()


def init_table(connection):
    cur = connection.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS breed (
        id SERIAL,
        breed text NOT NULL,
        country text NOT NULL,
        founded_country text NOT NULL,
        origin text NOT NULL,
        coat text NOT NULL,
        pattern text NOT NULL,
        PRIMARY KEY (breed, country, origin, pattern))
        PARTITION BY LIST (country)""")
    connection.commit()


def create_partition(connection, country):
    cur = connection.cursor()
    cur.execute(f"""CREATE TABLE "breed_{country}" PARTITION OF breed FOR VALUES IN ('{country}')""")
    connection.commit()


def get_list_of_countries_partition(connection):
    cur = connection.cursor()
    cur.execute("""
SELECT
child.relname       AS partition_name
FROM pg_inherits
JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
WHERE parent.relname='breed'
    """)
    data = cur.fetchall()
    return [part[0].split('_')[-1] for part in data] if len(data) > 0 else []


class Test:
    def __init__(self, x: int = 10, y: int = 5):
        default_x = 10
        default_y = 5
        if isinstance(x, int):
            self.x = x
        else:
            logger.warning(
                f"Attribute x must be int, not {type(x)} ({x}). The default value ({default_x}) has been assigned")
            self.x = default_x
        if isinstance(y, int):
            self.y = y
        else:
            logger.warning(
                f"Attribute y must be int, not {type(y)} ({y}). The default value ({default_y}) has been assigned")
            self.y = default_y

    def load_breed(self, connection):
        def make_short_country(row: dict):
            spl = row['country'].split('(')

            dev_country = ' '.join([word for word in spl[0].split(' ') if word != '' and word[0].isupper()])
            found_country = ' '.join(
                [word for word in spl[-1].split(')')[0].split(' ')
                 if word != '' and word[0].isupper()
                 ]
            ) if len(spl) > 1 else None
            row['country'] = dev_country
            row['founded_country'] = found_country if found_country is not None else dev_country
            return row

        response = requests.get(URL_BREED, params={'limit': self.x}).json()
        data: list[dict] = response['data']
        data = list(map(lambda row: make_short_country(row), data))
        partition_countries = get_list_of_countries_partition(connection)
        new_countries = {row['country'] for row in data if row['country'] not in partition_countries}

        for country in new_countries:
            create_partition(connection, country)

        cur = connection.cursor()
        cur.executemany("""
        INSERT INTO breed (breed, country, founded_country, origin, coat, pattern) 
        VALUES (%(breed)s, %(country)s, %(founded_country)s, %(origin)s, %(coat)s, %(pattern)s)
        ON CONFLICT (breed, country, origin, pattern) DO NOTHING
        """, data)
        connection.commit()

    def get_by_country_from_db(self, connection, country):
        cur = connection.cursor()
        cur.execute(f"""
        SELECT count(*) FROM breed
        WHERE country LIKE '%{country}%'
        """)
        return cur.fetchone()[0]

    def load_to_json_y_rows_from_db(self, connection, path=''):
        from psycopg2.extras import RealDictCursor
        import json
        cur = connection.cursor(cursor_factory=RealDictCursor)
        cur.execute(f"""
        SELECT * FROM breed
        LIMIT {self.y}
        """)
        with open(path + f'{date.today().strftime("%Y-%m-%d")}.json', 'w') as fp:
            json.dump(cur.fetchall(), fp=fp)


if __name__ == '__main__':
    # Получаем соединение с базой данных
    connection = get_connection()
    # Инициализируем таблицу
    init_table(connection)
    # 3.1 Создание экземпляра класса Test
    obj_test = Test(x=1000, y='wrong!')
    # 3.2 Метод, который будет получать x пород с https://catfact.ninja/ и
    # записывать в бд с проверкой на уникальность.
    # Записи должны быть разложены по странам.
    obj_test.load_breed(connection)
    # Метод получения кол-ва записей по стране.
    # Принимает название страны в качестве аргумента.
    # Аргумент должен быть строкой.
    print(obj_test.get_by_country_from_db(connection, 'United States'))
    # 3.3 Метод, который будет получать y записей из бд и записывать их в json
    # c названием текущей даты запроса данных.
    obj_test.load_to_json_y_rows_from_db(connection)
    connection.close()
