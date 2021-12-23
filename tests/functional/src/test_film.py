"""
Тесты для доступа к одиночному фильму по API
"""

import os
from http import HTTPStatus

import aiohttp
import json
import pytest

# Строка с именем хоста и портом
ELASTIC_HOST = os.getenv('ELASTIC_HOST', 'localhost:9200')
API_HOST = os.getenv('API_HOST', 'localhost:8000')


@pytest.mark.asyncio
async def test_some_film(some_film):
    """Проверяем, что тестовый фильм доступен по API"""
    # Считать из файла с данными параметры тестового фильма
    with open("testdata/some_film.json") as docs_json:
        docs = json.load(docs_json)
        doc = docs[0]
    # Проверить, что данные, возвращаемые API, совпадают с теми что
    # в файле с тестовыми данными
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film/{doc['id']}") as ans:
            assert ans.status == HTTPStatus.OK
            data = await ans.json()
            assert data["uuid"] == doc['id']
            assert data["title"] == doc['title']
            assert data["imdb_rating"] == doc['imdb_rating']


# @pytest.mark.skip(reason="no")
@pytest.mark.asyncio
async def test_film_list(some_film):
    """Проверяем, что тестовый фильм отображается в списке всех фильмов"""
    # Считать из файла с данными параметры тестового фильма
    with open("testdata/some_film.json") as docs_json:
        docs = json.load(docs_json)
        doc = docs[0]
    # Проверить, что данные, возвращаемые API, совпадают с теми что
    # в файле с тестовыми данными
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film/{doc['id']}") as ans:
            assert ans.status == HTTPStatus.OK
            data = await ans.json()
            assert data["uuid"] == doc['id']
            assert data["title"] == doc['title']
            assert data["imdb_rating"] == doc['imdb_rating']
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film") as ans:
            assert ans.status == HTTPStatus.OK
            data = await ans.json()
            assert isinstance(data, list)
            assert len(data) == len(docs)
            assert data[0]['uuid'] == doc['id']
            assert data[0]["title"] == doc['title']
            assert data[0]["imdb_rating"] == doc['imdb_rating']


@pytest.mark.asyncio
async def test_empty(empty_film_index):
    """Тест запускается без фикстур и API должен вернуть ошибку 404"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film") as ans:
            assert ans.status == HTTPStatus.NOT_FOUND


@pytest.mark.asyncio
async def test_no_index():
    """Тест запускается без индекса и API должен вернуть ошибку 500"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film") as ans:
            assert ans.status == HTTPStatus.INTERNAL_SERVER_ERROR
