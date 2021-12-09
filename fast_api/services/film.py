import orjson
from functools import lru_cache
from typing import List, Optional
from uuid import UUID

from db.elastic import get_elastic
from db.cache import MemoryCache, get_cache
from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from models.film import Film, FilmBrief

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class FilmService:
    """
        FilmService содержит бизнес-логику по работе с фильмами.
    """

    def __init__(self, cache: MemoryCache, elastic: AsyncElasticsearch):
        self.cache = cache
        self.elastic = elastic

    async def get_by_id(self, film_id: str) -> Optional[Film]:

        # Пытаемся получить данные из кеша, потому что оно работает быстрее
        film = await self._film_from_cache(film_id)
        if not film:
            # Если фильма нет в кеше, то ищем его в Elasticsearch
            film = await self._get_film_from_elastic(film_id)
            if not film:
                # Если он отсутствует в Elasticsearch, значит, фильма вообще нет в базе
                return None
            # Сохраняем фильм в кеш
            await self._put_film_to_cache(film)

        return film

    async def _get_film_from_elastic(self, film_id: str) -> Optional[Film]:

        es_fields = ["id", "title", "imdb_rating", "description", "genres", "actors", "writers"]
        doc = await self.elastic.get('movies', film_id, _source_includes=es_fields)
        film_info = doc.get("_source")
        film_info["uuid"] = film_info["id"]
        film_info.pop("id")
        return Film(**film_info)

    async def _film_from_cache(self, film_id: str) -> Optional[Film]:
        data = await self.cache.get(film_id)
        if not data:
            return None
        return Film.parse_raw(data)

    async def _put_film_to_cache(self, film: Film):
        await self.cache.set(str(film.uuid), film.json(), expire=FILM_CACHE_EXPIRE_IN_SECONDS)

    async def get_by_genre_id(self,
                              filter_genre: Optional[UUID],
                              sort: Optional[str],
                              page_size: Optional[int],
                              page_number: Optional[int]
                              ) -> List[FilmBrief]:
        films = await self._get_films_from_cache(filter_genre, sort, page_size, page_number)
        if not films:
            films = await self._get_films_by_genre_from_elastic(filter_genre, sort, page_size, page_number)
            if not films:
                return []
            await self._put_films_to_cache(films, filter_genre, sort, page_size, page_number)
        return films

    async def _get_films_by_genre_from_elastic(self,
                                               filter_genre: Optional[UUID],
                                               sort: Optional[str],
                                               page_size: Optional[int],
                                               page_number: Optional[int]
                                               ) -> List[FilmBrief]:

        sort_order, sort_column = sort[0], sort[1:]
        sort_order = "desc" if sort_order == "-" else "asc"
        page_number = page_number if page_number is not None else 1
        page_size = page_size if page_size is not None else 9999
        search_query = {
            "from": (page_number - 1) * page_size,
            "size": page_size,
            "query": {
                "nested": {
                    "path": "genres",
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"genres.id": str(filter_genre)}}
                            ]
                        }
                    }
                }
            } if filter_genre else {
                "match_all": {}
            },
            "sort": [
                {
                    sort_column: {"order": sort_order}
                }
            ]
        }
        es_fields = ["id", "title", "imdb_rating"]
        doc = await self.elastic.search(index='movies', body=search_query, _source_includes=es_fields)
        films_info = doc.get("hits").get("hits")
        film_list = [FilmBrief(**film.get("_source")) for film in films_info]
        return film_list

    async def _get_films_from_cache(self,
                                    filter_genre: Optional[UUID],
                                    sort: Optional[str],
                                    page_size: Optional[int],
                                    page_number: Optional[int]
                                    ) -> List[FilmBrief]:
        key = self._get_films_key(filter_genre, sort, page_size, page_number)
        data = await self.cache.get(key)
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
        await self.cache.set(key, json, FILM_CACHE_EXPIRE_IN_SECONDS)

    def _get_films_key(self, *args):
        key = ("films", args)
        return str(key)


@lru_cache()
def get_film_service(
        cache: MemoryCache = Depends(get_cache),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(cache, elastic)
