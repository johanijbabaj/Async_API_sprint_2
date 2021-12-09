import orjson
from functools import lru_cache
from typing import List, Optional
from uuid import UUID

from db.elastic import get_elastic
from db.cache import MemoryCache, get_cache
from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from models.genre import Genre, GenreBrief

GENRE_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class GenreService:
    """
        Сервис для получения жанра по идентификатору, или всех жанров фильма
    """

    def __init__(self, cache: MemoryCache, elastic: AsyncElasticsearch):
        self.cache = cache
        self.elastic = elastic

    async def get_by_id(self, genre_id: str) -> Optional[Genre]:

        # Пытаемся получить данные из кеша, потому что оно работает быстрее
        genre = await self._genre_from_cache(genre_id)
        if not genre:
            # Если фильма нет в кеше, то ищем его в Elasticsearch
            genre = await self._get_genre_from_elastic(genre_id)
            if not genre:
                # Если он отсутствует в Elasticsearch, значит, жанра вообще нет в базе
                return None
            # Сохраняем фильм в кеш
            await self._put_genre_to_cache(genre)

        return genre

    async def _get_genre_from_elastic(self, genre_id: str) -> Optional[Genre]:

        es_fields = ["id", "name", "description", "films"]
        doc = await self.elastic.get('genres', genre_id, _source_includes=es_fields)
        genre_info = doc.get("_source")
        # Спецификация API требует, чтобы поле идентификатора называлось UUID
        genre_info["uuid"] = genre_info["id"]
        genre_info.pop("id")

        return Genre(**genre_info)

    async def _genre_from_cache(self, genre_id: str) -> Optional[Genre]:
        data = await self.cache.get(genre_id)
        if not data:
            return None
        return Genre.parse_raw(data)

    async def _put_genre_to_cache(self, genre: Genre):
        await self.cache.set(str(genre.uuid), genre.json(), GENRE_CACHE_EXPIRE_IN_SECONDS)

    async def get_by_film_id(self,
                             film_uuid: Optional[UUID],
                             sort: str,
                             page_size: int,
                             page_number: int
                             ) -> List[GenreBrief]:
        """
            Получить список жанров, относящихся к определенному
            фильму (если фильм задан, иначе всех жанров).
        """
        genres = await self._get_genres_from_cache(film_uuid, sort, page_size, page_number)
        if not genres:
            genres = await self._get_by_film_id_from_elastic(film_uuid, sort, page_size, page_number)
            if not genres:
                return []
            await self._put_genres_to_cache(genres, film_uuid, sort, page_size, page_number)
        return genres

    async def _get_by_film_id_from_elastic(self,
                                           film_uuid: Optional[UUID],
                                           sort: str,
                                           page_size: int,
                                           page_number: int
                                           ) -> List[GenreBrief]:
        """
            Получить список жанров из ElasticSearch
        """
        search_query = {
            "from": (page_number - 1) * page_size,
            "size": page_size,
            "query": {
                "nested": {
                    "path": "films",
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"films.id": str(film_uuid)}}
                            ]
                        }
                    }
                }
            } if film_uuid else {"match_all": {}},
            "sort": [
                {sort or "name": {"order": "asc"}}
            ]
        }
        es_fields = ["id", "name", "description"]
        doc = await self.elastic.search(
            index='genres',
            body=search_query,
            _source_includes=es_fields
        )
        genres_info = doc.get("hits").get("hits")
        genre_list = [
            GenreBrief(**genre.get("_source")) for genre in genres_info
        ]
        return genre_list

    async def _get_genres_from_cache(self,
                                     film_uuid: Optional[UUID],
                                     sort: str,
                                     page_size: int,
                                     page_number: int
                                     ) -> List[GenreBrief]:
        key = self._get_genre_key(film_uuid, sort, page_size, page_number)
        data = await self.cache.get(key)
        if not data:
            return []
        genres = [GenreBrief(**genre) for genre in orjson.loads(data)]
        return genres

    async def _put_genres_to_cache(self,
                                   genres: List[GenreBrief],
                                   film_uuid: Optional[UUID],
                                   sort: str,
                                   page_size: int,
                                   page_number: int
                                   ):
        key = self._get_genre_key(film_uuid, sort, page_size, page_number)
        json = "[{}]".format(','.join(genre.json() for genre in genres))
        await self.cache.set(key, json, GENRE_CACHE_EXPIRE_IN_SECONDS)

    def _get_genre_key(self, *args):
        key = ("genres", args)
        return str(key)


@lru_cache()
def get_genre_service(
        cache: MemoryCache = Depends(get_cache),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(cache, elastic)
