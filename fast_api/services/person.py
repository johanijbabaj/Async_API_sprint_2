import orjson
from functools import lru_cache
from typing import List, Optional
from uuid import UUID

from db.elastic import get_elastic
from db.cache import MemoryCache, get_cache
from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from models.person import Person, PersonBrief

PERSON_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class PersonService:
    """
        Сервис для получения информации о человеке по идентификатору
    """

    def __init__(self, cache: MemoryCache, elastic: AsyncElasticsearch):
        self.cache = cache
        self.elastic = elastic

    async def get_by_id(self, person_id: str) -> Optional[Person]:
        """
            Возвращает информацию о человеке по его строке UUID
        """
        person = await self._person_from_cache(person_id)
        if not person:
            # Если фильма нет в кеше, то ищем его в Elasticsearch
            person = await self._get_person_from_elastic(person_id)
            if not person:
                # Если он отсутствует в Elasticsearch, значит, жанра вообще нет в базе
                return None
            # Сохраняем фильм  в кеш
            await self._put_person_to_cache(person)

        return person

    async def _get_person_from_elastic(self, person_id: str) -> Optional[Person]:
        """
            Извлечь информацию о человеке из ElasticSearch по его строке
            идентификатору
        """
        es_fields = ["id", "full_name", "birth_date", "films"]
        doc = await self.elastic.get('persons', person_id, _source_includes=es_fields)
        person_info = doc.get("_source")
        # Спецификация API требует, чтобы поле идентификатора называлось UUID
        person_info["uuid"] = person_info["id"]
        person_info.pop("id")

        return Person(**person_info)

    async def _person_from_cache(self, person_id: str) -> Optional[Person]:
        """
            Чтение данных о человеке из кэша
        """
        data = await self.cache.get(person_id)
        if not data:
            return None

        return Person.parse_raw(data)

    async def _put_person_to_cache(self, person: Person):
        """
            Запись данных о человеке в кэш
        """
        await self.cache.set(str(person.uuid), person.json(), PERSON_CACHE_EXPIRE_IN_SECONDS)

    async def get_by_film_id(self,
                             film_uuid: Optional[UUID],
                             filter_name: Optional[str],
                             sort: Optional[str],
                             page_size: Optional[int],
                             page_number: Optional[int]) -> List[PersonBrief]:
        """
            Получить список людей, участвовавших в работе над определенным
            фильмом.
        """
        persons = await self._get_by_film_id_from_cache(film_uuid, filter_name, sort, page_size, page_number)
        if not persons:
            persons = await self._get_by_film_id_from_elastic(film_uuid, filter_name, sort, page_size, page_number)
            if not persons:
                return []
            await self._put_films_to_cache(persons, film_uuid, filter_name, sort, page_size, page_number)
        return persons

    async def _get_by_film_id_from_elastic(self,
                                           film_uuid: Optional[UUID],
                                           filter_name: Optional[str],
                                           sort: Optional[str],
                                           page_size: Optional[int],
                                           page_number: Optional[int]) -> List[PersonBrief]:
        """
            Получить список людей из ElasticSearch
        """
        page_number = page_number if page_number is not None else 1
        page_size = page_size if page_size is not None else 9999
        search_query = {
            "from": (page_number - 1) * page_size,
            "size": page_size,
            "query": {"match_all": {}},
            "sort": [
                {sort or "full_name.raw": {"order": "asc"}}
            ]
        }
        if film_uuid:
            search_query['query'] = {
                "match": {
                    "films.id": str(film_uuid)
                }
            }
        if filter_name:
            search_query['query'] = {
                "match": {
                    "full_name": filter_name
                }
            }
        es_fields = ["id", "full_name", "birth_date"]
        doc = await self.elastic.search(
            index='persons',
            body=search_query,
            _source_includes=es_fields
        )
        persons_info = doc.get("hits").get("hits")
        person_list = [
            PersonBrief(**person.get("_source")) for person in persons_info
        ]
        return person_list

    async def _get_by_film_id_from_cache(self,
                                         film_uuid: Optional[UUID],
                                         filter_name: Optional[str],
                                         sort: Optional[str],
                                         page_size: Optional[int],
                                         page_number: Optional[int]) -> List[PersonBrief]:

        key = self._get_persons_key(film_uuid, filter_name, sort, page_size, page_number)
        data = await self.cache.get(key)
        if not data:
            return []
        films = [PersonBrief(**film) for film in orjson.loads(data)]
        return films

    async def _put_films_to_cache(self,
                                  persons: List[PersonBrief],
                                  film_uuid: Optional[UUID],
                                  filter_name: Optional[str],
                                  sort: Optional[str],
                                  page_size: Optional[int],
                                  page_number: Optional[int]
                                  ):
        key = self._get_persons_key(film_uuid, filter_name, sort, page_size, page_number)
        json = "[{}]".format(','.join(film.json() for film in persons))
        await self.cache.set(key, json, PERSON_CACHE_EXPIRE_IN_SECONDS)

    def _get_persons_key(self,
                         *args):
        key = ("persons", args)
        return str(key)


@lru_cache()
def get_person_service(
        cache: MemoryCache = Depends(get_cache),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(cache, elastic)
