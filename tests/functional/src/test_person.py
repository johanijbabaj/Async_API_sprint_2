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
def some_person(request):
    """
    Заполнить индекс ElasticSearch тестовыми записями персон
    """
    # Создаем схему индекса для поиска персон
    with open("testdata/schemes.json") as fd:
        schemes = json.load(fd)
    scheme = schemes['person_scheme']
    docs = [
        {
            "id": "23d3d644-5abe-11ec-b50c-5378d698a87b",
            "full_name": "John Smith",
            "birth_date": "01.01.2001",
            "films": [{"id": "6fbe525a-5abe-11ec-b50c-5378d698a87b", "title": "John's film", "role": "actor"}],
        }
    ]
    # Создаем поисковый индекс и заполняем документами
    es = Elasticsearch(f"http://{ELASTIC_HOST}")
    es.indices.create('persons', scheme)
    helpers.bulk(
        es,
        [
            {
                '_index': 'persons',
                '_id': doc["id"],
                **doc
            }
            for doc in docs
        ]
    )

    def teardown():
        """Удалить созданные для тестирования временные объекты"""
        for doc in docs:
            es.delete('persons', doc["id"])
        es.indices.delete('persons')

    request.addfinalizer(teardown)


@pytest.fixture()
def empty_index(request):
    """
    Заполнить индекс ElasticSearch без тестовых записей
    """
    # Создаем схему индекса для поиска персон
    with open("testdata/schemes.json") as fd:
        schemes = json.load(fd)
    scheme = schemes['person_scheme']
    es = Elasticsearch(f"http://{ELASTIC_HOST}")
    # FIXME: Индекс может уже существовать из-за хвостов прошлых ошибок
    #        В рабочем варианте этого быть не должно, убрать потом
    try:
        es.indices.delete('persons')
    except:
        pass
    es.indices.create('persons', scheme)

    def teardown():
        """Удалить созданные для тестирования временные объекты"""
        es.indices.delete('persons')

    request.addfinalizer(teardown)


@pytest.mark.asyncio
async def test_some_person(some_person):
    """Проверяем, что тестовый человек доступен по API"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/person/23d3d644-5abe-11ec-b50c-5378d698a87b") as ans:
            assert ans.status == 200
            data = await ans.json()
            assert data["uuid"] == "23d3d644-5abe-11ec-b50c-5378d698a87b"
            assert data["full_name"] == "John Smith"


@pytest.mark.asyncio
async def test_person_list(some_person):
    """Проверяем, что тестовый человек отображается в списке всех людей"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/person") as ans:
            assert ans.status == 200
            data = await ans.json()
            assert isinstance(data, list)
            assert len(data) == 1
            assert data[0]['uuid'] == "23d3d644-5abe-11ec-b50c-5378d698a87b"
            assert data[0]["full_name"] == "John Smith"


@pytest.mark.asyncio
async def test_empty_index(empty_index):
    """Тест запускается с пустым индексом и API должен вернуть ошибку 404"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/person") as ans:
            assert ans.status == 404


@pytest.mark.asyncio
async def test_no_index():
    """Тест запускается без индекса и API должен вернуть ошибку 500"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/person") as ans:
            assert ans.status == 500
