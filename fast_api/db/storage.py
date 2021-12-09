from abc import ABC, abstractmethod
from elasticsearch import AsyncElasticsearch
from typing import Any, Optional, List
from models.film import Film, FilmBrief
from models.genre import Genre, GenreBrief

from uuid import UUID

es: Optional[AsyncElasticsearch] = None


class AbstractStorage(ABC):

    @abstractmethod
    def get_film(self, *args: Any):
        pass

    @abstractmethod
    def get_films(self, **kwargs: Any):
        pass


class ElasticStorage(AbstractStorage):
    __conn: AsyncElasticsearch

    def __init__(self, elastic: AsyncElasticsearch):
        self.__conn = elastic

    async def get_film(self, *args: str) -> Optional[Film]:
        es_fields = ["id", "title", "imdb_rating", "description", "genres", "actors", "writers"]
        doc = await self.__conn.get("movies", args, _source_includes=es_fields)
        film_info = doc.get("_source")
        film_info["uuid"] = film_info["id"]
        film_info.pop("id")
        return Film(**film_info)

    async def get_films(self, filter_genre: Optional[UUID],
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
        doc = await self.__conn.search(index='movies', body=search_query, _source_includes=es_fields)
        films_info = doc.get("hits").get("hits")
        film_list = [FilmBrief(**film.get("_source")) for film in films_info]
        return film_list

    async def get_genre(self, genre_id: str) -> Optional[Genre]:
        es_fields = ["id", "name", "description", "films"]
        doc = await self.__conn.get('genres', genre_id, _source_includes=es_fields)
        genre_info = doc.get("_source")
        genre_info["uuid"] = genre_info["id"]
        genre_info.pop("id")

        return Genre(**genre_info)

    async def get_genres(self,
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
        doc = await self.__conn.search(
            index='genres',
            body=search_query,
            _source_includes=es_fields
        )
        genres_info = doc.get("hits").get("hits")
        genre_list = [
            GenreBrief(**genre.get("_source")) for genre in genres_info
        ]
        return genre_list


# Функция понадобится при внедрении зависимостей
async def get_elastic() -> AsyncElasticsearch:
    return es


async def get_storage() -> AbstractStorage:
    es_conn = await get_elastic()
    return ElasticStorage(es_conn)
