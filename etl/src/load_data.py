import datetime
from time import sleep

from dotenv import load_dotenv
from elastic_loader import ElasticLoader
from loguru import logger
from models import FilmworkSchema
from postgres_extractor import PostgresExtractor
from storage import JsonFileStorage, State


def init_json_storage(path):
    """Инициализация хранилища"""
    logger.info('Initialize state')
    storage = JsonFileStorage(path)
    return storage


def load_from_pg(
        state: State,
        postgres_extractor: PostgresExtractor
        ):
    """
    Выгрузка данных из Postgres:
    - создание соединения
    - инициализация state
    - загрузка записей через psycopg2.fetchmany

    Args:
      - storage: хранилище, где хранится state для
        контроля успешной загрузки
    Returns:
      - генератор от psycopg2.fetchmany
    """
    if not state.get_state('state_date'):
        # до появления первого фильма
        state.set_state('state_date', str(datetime.date(1880, 1, 1)))
        state.set_state('offset', 0)
    state.storage.save_state(state.state)
    logger.info('load and save state')

    extract_movies = postgres_extractor.extract_movies(state)
    return extract_movies


def prepare_json_bulk(extract_movies, last_modified):
    """
    Подготовка данных для bulk запроса в ES
    Приводит поля `none` к []
    Args:
      - batch_es: батч сериализованных объектов
        из генератора fetchmany
    Returns:
      - форматированные данные для загрузки
        через ElasticSearch.bulk
    """

    data_bulk = []
    for movie in extract_movies:
        last_modified = (movie['modified'])
        movie = FilmworkSchema(**movie)
        # подготовка данных к bulk
        init_line = {
            "index": {
                "_index": 'movies',
                "_id": movie.id
            }
        }
        data_bulk.append(init_line)
        data_bulk.append(movie.json())
    return data_bulk, last_modified


def load_data_es(data_bulk):
    """Загрузка в ES через bulk"""

    logger.info('Create index movies in ES if no exist')
    es_loader.create_index(
        index_name='movies',
        file_schema='init_schema_es.json'
    )
    logger.info('Load data in ES started')
    es_loader.bulk(data_bulk=data_bulk)
    logger.info('Load data in ES successful')


if __name__ == '__main__':
    load_dotenv()

    BATCH_SIZE_LOAD_PG = 100
    UPDATE_TIME = 30  # sec

    logger.add(
        'info.log',
        format='{time} {level} {message}',
        level='INFO',
        rotation='10 Mb',
        compression='zip',
        serialize=True,
        )

    logger.add(
        'error.log',
        format='{time} {level} {message}',
        level='WARNING',
        rotation='10 Mb',
        compression='zip'
        )

    # init storage and state
    storage = init_json_storage('storage.json')
    state = State(storage)
    offset = 0
    last_modified = state.get_state('state_date')

    # основной цикл
    while True:
        # пробуй подключиться
        logger.info('Create pg connection started')
        postgres_extractor = PostgresExtractor(
            batch_size=BATCH_SIZE_LOAD_PG)
        logger.info('Connection pg successful')

        logger.info('Start loading from pg')
        extract_movies = load_from_pg(state, postgres_extractor)
        logger.info('Finish loading from pg')

        logger.info('Create connection ES started')
        es_loader = ElasticLoader()
        logger.info('Connection ES successful')

        data_bulk = []

        # забрать генератор
        for generator_movies in extract_movies:
            # забрать батч из одного генератора
            for batch_movies in generator_movies:
                # преобразовать к загрузке через bulk
                for movie in batch_movies:
                    last_modified = movie['modified']
                    movie = FilmworkSchema(**movie)
                    # подготовка данных к bulk
                    init_line = {
                        "index": {
                            "_index": 'movies',
                            "_id": movie.id
                        }
                    }
                    data_bulk.append(init_line)
                    data_bulk.append(movie.json())

                # bulk загрузка в ES одного батча
                if data_bulk:
                    if es_loader.connect():
                        load_data_es(data_bulk)

                # обновляем состояние внутри запроса
                offset += len(data_bulk)
                state.set_state('offset', offset)
                data_bulk = []

        # по завершению запроса
        postgres_extractor.close_connect()
        offset = 0
        state.set_state('offset', offset)
        state.set_state('state_date', str(last_modified))

        # частота обновления
        sleep(UPDATE_TIME)
