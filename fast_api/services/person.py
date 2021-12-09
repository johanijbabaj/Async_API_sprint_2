import orjson

from aioredis import Redis
from functools import lru_cache
from typing import List, Optional
from uuid import UUID

from db.redis import get_redis
from db.storage import AbstractStorage
from db.storage import get_storage
from fastapi import Depends
from models.person import Person, PersonBrief

PERSON_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class PersonService:
    """
        Сервис для получения информации о человеке по идентификатору
    """

    def __init__(self, redis: Redis, storage: AbstractStorage):
        self.redis = redis
        self.storage = storage

    async def get_by_id(self, person_id: str) -> Optional[Person]:
        """
            Возвращает информацию о человеке по его строке UUID
        """
        person = await self._person_from_cache(person_id)
        if not person:
            # Если фильма нет в кеше, то ищем его в Elasticsearch
            person = await self.storage.get_person(person_id)
            if not person:
                # Если он отсутствует в Elasticsearch, значит, жанра вообще нет в базе
                return None
            # Сохраняем фильм  в кеш
            await self._put_person_to_cache(person)

        return person


    async def _person_from_cache(self, person_id: str) -> Optional[Person]:
        """
            Чтение данных о человеке из кэша Redis
        """
        data = await self.redis.get(person_id)
        if not data:
            return None

        return Person.parse_raw(data)

    async def _put_person_to_cache(self, person: Person):
        """
            Запись данных о человеке в кэш Redis
        """
        await self.redis.set(str(person.uuid), person.json(), expire=PERSON_CACHE_EXPIRE_IN_SECONDS)

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
            persons = await self.storage.get_persons(film_uuid, filter_name, sort, page_size, page_number)
            if not persons:
                return []
            await self._put_films_to_cache(persons, film_uuid, filter_name, sort, page_size, page_number)
        return persons

    async def _get_by_film_id_from_cache(self,
                                         film_uuid: Optional[UUID],
                                         filter_name: Optional[str],
                                         sort: Optional[str],
                                         page_size: Optional[int],
                                         page_number: Optional[int]) -> List[PersonBrief]:

        key = self._get_persons_key(film_uuid, filter_name, sort, page_size, page_number)
        data = await self.redis.get(key)
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
        await self.redis.set(key, json, expire=PERSON_CACHE_EXPIRE_IN_SECONDS)

    def _get_persons_key(self,
                         *args):
        key = ("persons", args)
        return str(key)


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        storage: AbstractStorage = Depends(get_storage),
) -> PersonService:
    return PersonService(redis, storage)
