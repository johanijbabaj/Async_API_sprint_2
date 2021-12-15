import json
import os

import aiohttp
import pytest
from elasticsearch import Elasticsearch, helpers

# Строка с именем хоста и портом
ELASTIC_HOST = os.getenv('ELASTIC_HOST', 'localhost:9200')
API_HOST = os.getenv('API_HOST', 'localhost:8000')


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
