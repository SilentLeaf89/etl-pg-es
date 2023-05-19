from math import exp
from time import sleep
from typing import Generator

from loguru import logger
import psycopg2
from psycopg2.extras import DictCursor

from settings import Dsn
from storage import State


class PostgresExtractor:
    """Класс обработчика и загрузки данных в PostgreSQL"""
    def __init__(self, batch_size):
        self.batch_size = batch_size
        self.dsn = dict(Dsn())
        self.conn = self.connection()
        self.cursor = self.conn.cursor()

    def connection(
            self,
            start_sleep_time=0.1,
            factor=2,
            border_sleep_time=30
            ):
        """Подключение к postgres с использованием backoff"""
        MAX_COUNT = 15
        _count = 1
        t = start_sleep_time

        while True:
            dsn = self.dsn
            try:
                return psycopg2.connect(**dsn, cursor_factory=DictCursor)
            except psycopg2.OperationalError as ex_conn:
                logger.error(
                    '{0} Please check connection and dsn'.format(ex_conn)
                )
            except ValueError as ex_val:
                logger.error(
                    '{0}'.format(ex_val)
                )
            except Exception as ex_gen:
                logger.error(
                    'Unknown error {0}'.format(ex_gen)
                )
            finally:
                t = start_sleep_time * factor * exp(_count)
                if t > border_sleep_time:
                    t = border_sleep_time
                sleep(t)
                _count += 1
                if _count == MAX_COUNT:
                    _count = 1

    def close_connect(self):
        if not self.conn.closed == 0:
            self.conn.commit()
            self.cursor.close()
            self.conn.close()
        logger.info('Сonnect to postgresql close')

    def query_iter(self):
        """
        Генератор запрошенных данных.
        Ограничивает объем единоразово выгружаемых из БД порции объектов

        Args:
            cursor: Объект курсора БД
            arraysize: размер батча

        Returns:
            result: Список кортежей несериализованных данных
                    Для SQLite: sqlite3.Row object
        """
        while results := self.cursor.fetchmany(self.batch_size):
            yield results

    def extract_movies(self, state: State) -> Generator:
        """
            Извлекает все связанные данные из PostgreSQL по каждому фильму.
            Используется для загрузки в ElasticSearch.

        Args:
            state: текущее состояние хранилища

        Return:
            query: Генератор от fetchmany объектов
            типа FilmworkSchema c 'batch_size' количеством записей
        """

        sql_query = """
            SELECT fw.id,
            fw.rating AS imdb_rating,
            fw.title,
            fw.description,
            fw.modified,
            COALESCE(ARRAY_AGG(DISTINCT p.full_name)
                FILTER (WHERE pfw.role = 'actor'),
                ARRAY[]::VARCHAR[]) AS actors_names,
            COALESCE(ARRAY_AGG(DISTINCT p.full_name)
                FILTER(WHERE pfw.role = 'writer'),
                ARRAY[]::VARCHAR[]) AS writers_names,
            COALESCE(ARRAY_AGG(DISTINCT p.full_name)
                FILTER (WHERE pfw.role = 'director'),
                ARRAY[]::VARCHAR[]) AS director,
            ARRAY_AGG(DISTINCT g.name) AS genres_field,
            ARRAY_AGG(DISTINCT JSONB_BUILD_OBJECT(
                'id', p.id, 'name', p.full_name))
                FILTER (WHERE pfw.role ='actor') AS actors,
            ARRAY_AGG(DISTINCT JSONB_BUILD_OBJECT(
                'id', p.id, 'name', p.full_name))
                FILTER (WHERE pfw.role ='writer') AS writers
            FROM content.film_work AS fw
            LEFT OUTER JOIN content.person_film_work as pfw
                ON (fw.id = pfw.film_work_id)
            LEFT OUTER JOIN content.person as p
                ON(pfw.person_id = p.id)
            LEFT OUTER JOIN content.genre_film_work as gfw
                ON (fw.id = gfw.film_work_id)
            LEFT OUTER JOIN content.genre as g
                ON (gfw.genre_id = g.id)
            WHERE fw.modified > %s
            GROUP BY fw.id
            ORDER BY (fw.modified, fw.title)
            OFFSET %s;
            """
        with self.cursor as curs:
            curs.execute(
                sql_query,
                (state.get_state('state_date'), state.get_state('offset'))
            )
            query = self.query_iter()
            yield query
