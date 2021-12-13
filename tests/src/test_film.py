"""
Тесты для доступа к одиночному фильму по API
"""

import json
import os

import aiohttp
import pytest
from elasticsearch import Elasticsearch, helpers

# Строка с именем хоста и портом
ELASTIC_HOST = os.getenv('ELASTIC_HOST')
API_HOST = os.getenv('API_HOST')

@pytest.fixture()
def some_film(request):
    """
    Заполнить индекс ElasticSearch тестовыми данными
    """
    # Создаем схему индекса для поиска фильмов
    with open("testdata/schemes.json") as fd:
        schemes = json.load(fd)
    scheme = schemes['film_scheme']
    docs = [
        {
            "id": "bb74a838-584e-11ec-9885-c13c488d29c0",
            "imdb_rating": 5.5,
            "genre": "Action",
            "title": "Some film",
            "description": "Some film used for testing only",
            "genres": [{"id": "46e70470-592f-11ec-8b39-d99d30aa920b", "name": "Action"}],
            "director": "John Smith",
            "actors": [],
            "writers": [],
            "actors_names": [],
            "writers_names": [],
        }
    ]

    es = Elasticsearch(f"http://{ELASTIC_HOST}")
    try:
        es.indices.delete('movies')
    except:
        pass
    es.indices.create('movies', scheme)
    helpers.bulk(
        es,
        [
            {
                '_index': 'movies',
                '_id': doc["id"],
                **doc
            }
            for doc in docs
        ]
    )

    def teardown():
        """Удалить созданные для тестирования временные объекты"""
        for doc in docs:
            es.delete('movies', doc["id"])
        es.indices.delete('movies')

    request.addfinalizer(teardown)


@pytest.fixture()
def empty_index(request):
    """
    Создать пустой индекс ElasticSearch
    """
    # Создаем схему индекса для поиска фильмов
    with open("testdata/schemes.json") as fd:
        schemes = json.load(fd)
    scheme = schemes['film_scheme']
    es = Elasticsearch(f"http://{ELASTIC_HOST}")
    try:
        es.indices.delete('movies')
    except:
        pass
    es.indices.create('movies', scheme)

    def teardown():
        """Удалить созданные для тестирования временные объекты"""
        es.indices.delete('movies')

    request.addfinalizer(teardown)


@pytest.mark.asyncio
async def test_some_film(some_film):
    """Проверяем, что тестовый фильм доступен по API"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film/bb74a838-584e-11ec-9885-c13c488d29c0") as ans:
            assert ans.status == 200
            data = await ans.json()
            assert data["uuid"] == "bb74a838-584e-11ec-9885-c13c488d29c0"
            assert data["title"] == "Some film"
            assert data["imdb_rating"] == 5.5


@pytest.mark.asyncio
async def test_film_list(some_film):
    """Проверяем, что тестовый фильм отображается в списке всех фильмов"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film/bb74a838-584e-11ec-9885-c13c488d29c0") as ans:
            assert ans.status == 200
            data = await ans.json()
            assert data["uuid"] == "bb74a838-584e-11ec-9885-c13c488d29c0"
            assert data["title"] == "Some film"
            assert data["imdb_rating"] == 5.5
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film") as ans:
            assert ans.status == 200
            data = await ans.json()
            assert isinstance(data, list)
            assert len(data) == 1
            assert data[0]['uuid'] == "bb74a838-584e-11ec-9885-c13c488d29c0"
            assert data[0]["title"] == "Some film"
            assert data[0]["imdb_rating"] == 5.5


@pytest.mark.asyncio
async def test_empty(empty_index):
    """Тест запускается без фикстур и API должен вернуть ошибку 404"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film") as ans:
            assert ans.status == 404


@pytest.mark.asyncio
async def test_no_index():
    """Тест запускается без индекса и API должен вернуть ошибку 500"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film") as ans:
            assert ans.status == 500
