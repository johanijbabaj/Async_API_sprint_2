"""
Тесты для доступа к одиночному фильму по API
"""

import json
import os
from http import HTTPStatus

import aiohttp
import pytest
from elasticsearch import Elasticsearch, helpers

# Строка с именем хоста и портом
ELASTIC_HOST = os.getenv("ELASTIC_HOST", "localhost:9200")
API_HOST = os.getenv("API_HOST", "localhost:8000")


@pytest.fixture()
def some_person(request):
    """
    Заполнить индекс ElasticSearch тестовыми записями персон
    """
    # Создаем схему индекса для поиска персон
    with open("testdata/schemes.json") as file_data:
        schemes = json.load(file_data)
    scheme = schemes["person_scheme"]
    docs = [
        {
            "id": "23d3d644-5abe-11ec-b50c-5378d698a87b",
            "full_name": "John Smith",
            "birth_date": "01.01.2001",
            "films": [
                {
                    "id": "6fbe525a-5abe-11ec-b50c-5378d698a87b",
                    "title": "John's film",
                    "role": "actor",
                }
            ],
        }
    ]
    # Создаем поисковый индекс и заполняем документами
    elastic_search = Elasticsearch(f"http://{ELASTIC_HOST}")
    elastic_search.indices.create("persons", scheme)
    helpers.bulk(
        elastic_search, [{"_index": "persons", "_id": doc["id"], **doc} for doc in docs]
    )

    def teardown():
        """Удалить созданные для тестирования временные объекты"""
        for doc in docs:
            elastic_search.delete("persons", doc["id"])
        elastic_search.indices.delete("persons")

    request.addfinalizer(teardown)


@pytest.fixture()
def empty_index(request):
    """
    Заполнить индекс ElasticSearch без тестовых записей
    """
    # Создаем схему индекса для поиска персон
    with open("testdata/schemes.json") as file_data:
        schemes = json.load(file_data)
    scheme = schemes["person_scheme"]
    elastic_search = Elasticsearch(f"http://{ELASTIC_HOST}")
    # FIXME: Индекс может уже существовать из-за хвостов прошлых ошибок
    #        В рабочем варианте этого быть не должно, убрать потом
    # try:
    #    elastic_search.indices.delete("persons")
    # except:
    #    pass
    elastic_search.indices.create("persons", scheme)

    def teardown():
        """Удалить созданные для тестирования временные объекты"""
        elastic_search.indices.delete("persons")

    request.addfinalizer(teardown)


@pytest.mark.asyncio
async def test_some_person(some_person):  # pylint: disable=unused-argument
    """Проверяем, что тестовый человек доступен по API"""
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"http://{API_HOST}/api/v1/person/23d3d644-5abe-11ec-b50c-5378d698a87b"
        ) as ans:
            assert ans.status == HTTPStatus.OK
            data = await ans.json()
            assert data["uuid"] == "23d3d644-5abe-11ec-b50c-5378d698a87b"
            assert data["full_name"] == "John Smith"


# @pytest.mark.skip(reason="no")
@pytest.mark.asyncio
async def test_person_list(some_person):  # pylint: disable=unused-argument
    """Проверяем, что тестовый человек отображается в списке всех людей"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/person") as ans:
            assert ans.status == HTTPStatus.OK
            data = await ans.json()
            assert isinstance(data, list)
            assert len(data) == 1
            assert data[0]["uuid"] == "23d3d644-5abe-11ec-b50c-5378d698a87b"
            assert data[0]["full_name"] == "John Smith"


@pytest.mark.asyncio
async def test_empty_index(empty_index):  # pylint: disable=unused-argument
    """Тест запускается с пустым индексом и API должен вернуть ошибку 404"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/person") as ans:
            assert ans.status == HTTPStatus.NOT_FOUND


@pytest.mark.asyncio
async def test_no_index():
    """Тест запускается без индекса и API должен вернуть ошибку 500"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/person") as ans:
            assert ans.status == HTTPStatus.INTERNAL_SERVER_ERROR
