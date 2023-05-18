import json
from math import exp
from time import sleep
# from typing import List

from elasticsearch import Elasticsearch
from loguru import logger
# from models import FilmworkSchema


class ElasticLoader:
    """
    Инициализирует экземпляр Elasticsearch
    библиотеки elasticsearch

    Методы предназначны для:
    - create_index: создания индексов (по умолчанию для 'movies');
    - bulk: загрузки данных в ElasticSearch (ES) методом bulk

    Args:
    - host Хост и порт, где запущен ES
      например, "http://localhost:9200"
    """
    def __init__(
            self,
            host: str = "http://localhost:9200"
            ):
        self.host = host
        self.es = self.connect()

    def connect(
            self,
            start_sleep_time=0.1,
            factor=2,
            border_sleep_time=30
            ):
        """Подключение к host с использованием backoff"""
        MAX_COUNT = 15
        _count = 1
        t = start_sleep_time
        while True:
            if Elasticsearch(self.host).ping():
                return Elasticsearch(self.host)
            else:
                logger.error('Connection ES fail, try again')
                t = start_sleep_time * factor * exp(_count)
                if t > border_sleep_time:
                    t = border_sleep_time
                sleep(t)
                _count += 1
                if _count == MAX_COUNT:
                    _count = 1

    def create_index(
            self,
            index_name: str = 'movies',
            file_schema: str = 'init_schema_es.json'
            ):
        """
        Предназначен для создания индекса, если он еще не создан.
        По умолчанию создается индекс 'movies'
        со схемой из фала init_schema_es.json

        Args:
          - index_name - имя индекса для создания
          - file_schema - схема индекса
        Returns:
          - None
        """

        if not self.es.indices.exists(index=index_name):
            with open(file_schema) as f:
                index_settings = json.load(f)
            self.es.indices.create(index=index_name, body=index_settings)

    def bulk(self, data_bulk):
        """
        Загружает данные в ES
        Args:
          - data: список объектов
              Для 'movies' - список валидированных по схеме
              FilmworkSchema из models.py
              List[FilmworkSchema]
        Return:
          - None
        """
        self.es.bulk(body=data_bulk)
