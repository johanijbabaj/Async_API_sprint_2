import orjson

from aioredis import Redis
from db.storage import get_storage
from db.redis import get_redis
from db.storage import AbstractStorage
from fastapi import Depends
from functools import lru_cache
from models.film import Film, FilmBrief
from typing import List, Optional, Any
from uuid import UUID

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут

class FilmService:
    """
        FilmService содержит бизнес-логику по работе с фильмами.
    """

    def __init__(self, redis: Redis, storage: AbstractStorage):
        self.redis = redis
        self.storage = storage

    async def get_by_id(self, film_id: str) -> Optional[Film]:

        film = await self._film_from_cache(film_id)
        if not film:
            film = await self.storage.get_film()
            if not film:
                return None
            await self._put_film_to_cache(film)

        return film

    async def _film_from_cache(self, film_id: str) -> Optional[Film]:
        data = await self.redis.get(film_id)
        if not data:
            return None

        film = Film.parse_raw(data)
        return film

    async def _put_film_to_cache(self, film: Film):
        await self.redis.set(str(film.uuid), film.json(), expire=FILM_CACHE_EXPIRE_IN_SECONDS)

    async def get_by_genre_id(self,
                              filter_genre: Optional[UUID],
                              sort: Optional[str],
                              page_size: Optional[int],
                              page_number: Optional[int]
                              ) -> List[FilmBrief]:
        films = await self._get_films_from_cache(filter_genre, sort, page_size, page_number)
        if not films:
            films = await self.storage.get_films(filter_genre=filter_genre, sort=sort,
                                                 page_size=page_size, page_number= page_number)
            if not films:
                return []
            await self._put_films_to_cache(films, filter_genre, sort, page_size, page_number)
        return films

    async def _get_films_from_cache(self,
                                    filter_genre: Optional[UUID],
                                    sort: Optional[str],
                                    page_size: Optional[int],
                                    page_number: Optional[int]
                                    ) -> List[FilmBrief]:
        key = self._get_films_key(filter_genre, sort, page_size, page_number)
        data = await self.redis.get(key)
        if not data:
            return []
        films = [FilmBrief(**film) for film in orjson.loads(data)]
        return films

    async def _put_films_to_cache(self,
                                  films: List[FilmBrief],
                                  filter_genre: Optional[UUID],
                                  sort: Optional[str],
                                  page_size: Optional[int],
                                  page_number: Optional[int]
                                  ):
        key = self._get_films_key(filter_genre, sort, page_size, page_number)
        json = "[{}]".format(','.join(film.json() for film in films))
        await self.redis.set(key, json, expire=FILM_CACHE_EXPIRE_IN_SECONDS)

    def _get_films_key(self, *args):
        key = ("films", args)
        return str(key)

@lru_cache()
def get_film_service(redis: Redis = Depends(get_redis), storage: AbstractStorage = Depends(get_storage),) -> FilmService:
    return FilmService(redis, storage)
