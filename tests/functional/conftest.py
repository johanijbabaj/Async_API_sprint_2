import json
import os

import pytest
from elasticsearch import Elasticsearch, helpers

# Строка с именем хоста и портом
ELASTIC_HOST = os.getenv("ELASTIC_HOST", "localhost:9200")


@pytest.fixture()
def some_genre(request):
    """Заполнить индекс ElasticSearch тестовыми данными"""
    # Создаем схему индекса для поиска фильмов
    with open("testdata/schemes.json") as schemes_json:
        schemes = json.load(schemes_json)
    scheme = schemes["genre_scheme"]
    docs = [
        {
            "id": "0b105f87-e0a5-45dc-8ce7-f8632088f390",
            "name": "Western",
            "description": "Some description",
            "films": [
                {
                    "id": "02c24a84-1667-4f98-b459-f08933befa3d",
                    "title": "Star in the Dust",
                },
                {"id": "259935b8-d79d-4050-93a7-b3713cfb640c", "title": "North Star"},
            ],
        }
    ]

    elastic_search = Elasticsearch(f"http://{ELASTIC_HOST}")
    try:
        elastic_search.indices.delete("genres")
    except:
        pass
    elastic_search.indices.create("genres", scheme)
    helpers.bulk(
        elastic_search, [{"_index": "genres", "_id": doc["id"], **doc} for doc in docs]
    )

    def teardown():
        """Удалить созданные для тестирования временные объекты"""
        for doc in docs:
            elastic_search.delete("genres", doc["id"])
        elastic_search.indices.delete("genres")

    request.addfinalizer(teardown)


@pytest.fixture()
def empty_genre_index(request):
    """Заполнить индекс ElasticSearch без тестовых записей"""
    # Создаем схему индекса для поиска персон
    with open("testdata/schemes.json") as schemes_json:
        schemes = json.load(schemes_json)
    scheme = schemes["genre_scheme"]
    elastic_search = Elasticsearch(f"http://{ELASTIC_HOST}")
    # FIXME: Индекс может уже существовать из-за хвостов прошлых ошибок
    #        В рабочем варианте этого быть не должно, убрать потом
    # try:
    #    elastic_search.indices.delete("genres")
    # except:
    #    pass
    elastic_search.indices.create("genres", scheme)

    def teardown():
        """Удалить созданные для тестирования временные объекты"""
        elastic_search.indices.delete("genres")

    request.addfinalizer(teardown)


@pytest.fixture()
def some_film(request):
    """
    Заполнить индекс ElasticSearch тестовыми данными
    """
    # Создаем схему индекса для поиска фильмов
    with open("testdata/schemes.json") as schemes_json:
        schemes = json.load(schemes_json)
    scheme = schemes["film_scheme"]
    docs = [
        {
            "id": "bb74a838-584e-11ec-9885-c13c488d29c0",
            "imdb_rating": 5.5,
            "genre": "Action",
            "title": "Some film",
            "description": "Some film used for testing only",
            "genres": [
                {"id": "46e70470-592f-11ec-8b39-d99d30aa920b", "name": "Action"}
            ],
            "director": "John Smith",
            "actors": [],
            "writers": [],
            "actors_names": [],
            "writers_names": [],
        },
        {
            "id": "7ab42811-0872-4305-b140-36546ee6e0b3",
            "imdb_rating": 9.5,
            "genre": "Comedy",
            "title": "Funny film",
            "description": "Funny film used for testing only",
            "genres": [
                {"id": "cec222f5-8550-41e0-80d2-5fb3abe9b5c6", "name": "Comedy"}
            ],
            "director": "Adam Smith",
            "actors": [],
            "writers": [],
            "actors_names": [],
            "writers_names": [],
        },
    ]

    elastic_search = Elasticsearch(f"http://{ELASTIC_HOST}")
    try:
        elastic_search.indices.delete("movies")
    except:
        pass
    elastic_search.indices.create("movies", scheme)
    helpers.bulk(
        elastic_search, [{"_index": "movies", "_id": doc["id"], **doc} for doc in docs]
    )

    def teardown():
        """Удалить созданные для тестирования временные объекты"""
        for doc in docs:
            elastic_search.delete("movies", doc["id"])
        elastic_search.indices.delete("movies")

    request.addfinalizer(teardown)


@pytest.fixture()
def empty_film_index(request):
    """
    Создать пустой индекс ElasticSearch
    """
    # Создаем схему индекса для поиска фильмов
    with open("testdata/schemes.json") as schemes_json:
        schemes = json.load(schemes_json)
    scheme = schemes["film_scheme"]
    elastic_search = Elasticsearch(f"http://{ELASTIC_HOST}")
    try:
        elastic_search.indices.delete("movies")
    except:
        pass
    elastic_search.indices.create("movies", scheme)

    def teardown():
        """Удалить созданные для тестирования временные объекты"""
        elastic_search.indices.delete("movies")

    request.addfinalizer(teardown)
