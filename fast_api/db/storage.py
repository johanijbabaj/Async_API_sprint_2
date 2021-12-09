from abc import ABC, abstractmethod
from elasticsearch import AsyncElasticsearch
from typing import Any, Optional

es: Optional[AsyncElasticsearch] = None


class AbstractStorage(ABC):

    @abstractmethod
    def get(self, *args: Any):
        pass

    @abstractmethod
    def search(self, **kwargs: Any):
        pass


class ElasticStorage(AbstractStorage):
    __conn: AsyncElasticsearch

    def __init__(self, elastic: AsyncElasticsearch):
        self.__conn = elastic

    async def get(self, *args: Any):
        data = self.__conn.get(args)
        return data

    async def search(self, *args: Any):
        data = self.__conn.search(args)
        return data

# Функция понадобится при внедрении зависимостей
async def get_elastic() -> AsyncElasticsearch:
    return es

async def get_storage() -> AbstractStorage:
    es_conn = await get_elastic()
    return ElasticStorage(es_conn)
