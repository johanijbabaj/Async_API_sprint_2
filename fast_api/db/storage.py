from abc import ABC, abstractmethod
from elasticsearch import AsyncElasticsearch
from typing import Any, Optional

es: Optional[AsyncElasticsearch] = None


class AbstractStorage(ABC):

    @abstractmethod
    def get(self, some_index, some_id, es_fields):
        pass

    @abstractmethod
    def search(self, some_index, some_body, es_fields):
        pass


class ElasticStorage(AbstractStorage):
    __conn: AsyncElasticsearch

    def __init__(self, elastic: AsyncElasticsearch):
        self.__conn = elastic

    async def get(self, some_index, some_id, _source_includes):
        data = await self.__conn.get(index=some_index, id=some_id, _source_includes=_source_includes)
        return data

    async def search(self, some_index, some_body, es_fields):
        data = await self.__conn.search(index=some_index, body=some_body, _source_includes=es_fields)
        return data

# Функция понадобится при внедрении зависимостей
async def get_elastic() -> AsyncElasticsearch:
    return es

async def get_storage() -> AbstractStorage:
    es_conn = await get_elastic()
    return ElasticStorage(es_conn)
