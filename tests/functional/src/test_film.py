"""
Тесты для доступа к одиночному фильму по API
"""

import json
import os

import aiohttp
import pytest
from elasticsearch import Elasticsearch, helpers

# Строка с именем хоста и портом
ELASTIC_HOST = os.getenv('ELASTIC_HOST', 'localhost:9200')
API_HOST = os.getenv('API_HOST', 'localhost:8000')



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


# @pytest.mark.skip(reason="no")
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
            assert isinstance(data, list) == 1
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
