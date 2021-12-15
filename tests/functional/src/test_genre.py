import json
import os

import aiohttp
import pytest
from elasticsearch import Elasticsearch, helpers

# Строка с именем хоста и портом
ELASTIC_HOST = os.getenv('ELASTIC_HOST', 'localhost:9200')
API_HOST = os.getenv('API_HOST', 'localhost:8000')


@pytest.fixture()
def some_genre(request):
    """Заполнить индекс ElasticSearch тестовыми данными"""
    # Создаем схему индекса для поиска фильмов
    with open("testdata/schemes.json") as fd:
        schemes = json.load(fd)
    scheme = schemes['genre_scheme']
    docs = [
        {
            "id": "0b105f87-e0a5-45dc-8ce7-f8632088f390",
            "name": "Western",
            "description": "Some description",
            "films": [
                {"id": "02c24a84-1667-4f98-b459-f08933befa3d", "title": "Star in the Dust"},
                {"id": "259935b8-d79d-4050-93a7-b3713cfb640c", "title": "North Star"}
            ]
        }
    ]

    es = Elasticsearch(f"http://{ELASTIC_HOST}")
    try:
        es.indices.delete('genres')
    except:
        pass
    es.indices.create('genres', scheme)
    helpers.bulk(
        es,
        [
            {
                '_index': 'genres',
                '_id': doc["id"],
                **doc
            }
            for doc in docs
        ]
    )

    def teardown():
        """Удалить созданные для тестирования временные объекты"""
        for doc in docs:
            es.delete('genres', doc["id"])
        es.indices.delete('genres')

    request.addfinalizer(teardown)


@pytest.fixture()
def empty_index(request):
    """Заполнить индекс ElasticSearch без тестовых записей"""
    # Создаем схему индекса для поиска персон
    with open("testdata/schemes.json") as fd:
        schemes = json.load(fd)
    scheme = schemes['genre_scheme']
    es = Elasticsearch(f"http://{ELASTIC_HOST}")
    # FIXME: Индекс может уже существовать из-за хвостов прошлых ошибок
    #        В рабочем варианте этого быть не должно, убрать потом
    try:
        es.indices.delete('genres')
    except:
        pass
    es.indices.create('genres', scheme)

    def teardown():
        """Удалить созданные для тестирования временные объекты"""
        es.indices.delete('genres')

    request.addfinalizer(teardown)


@pytest.mark.asyncio
async def test_some_genre(some_genre):
    """Проверяем, что тестовый элемент доступен по API"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/genre/0b105f87-e0a5-45dc-8ce7-f8632088f390") as ans:
            assert ans.status == 200
            data = await ans.json()
            assert data["uuid"] == "0b105f87-e0a5-45dc-8ce7-f8632088f390"
            assert data["name"] == "Western"
            assert len(data["film_ids"]) == 2


@pytest.mark.asyncio
async def test_empty_index(empty_index):
    """Тест запускается с пустым индексом и API должен вернуть ошибку 404"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/genre") as ans:
            assert ans.status == 404


@pytest.mark.asyncio
async def test_no_index():
    """Тест запускается без индекса и API должен вернуть ошибку 500"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/genre") as ans:
            assert ans.status == 500
