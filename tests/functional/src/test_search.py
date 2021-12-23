"""
Тесты поиска фильмов по подстроке наименования фильма
"""

import os
from http import HTTPStatus

import aiohttp
import json
import pytest

# Строка с именем хоста и портом
API_HOST = os.getenv('API_HOST', 'localhost:8000')


@pytest.mark.asyncio
async def test_search_film(some_film):
    """Проверяем, что тестовый фильм доступен по API"""
    # Считать из файла с данными параметры тестового фильма
    with open("testdata/some_film.json") as docs_json:
        docs = json.load(docs_json)
        doc = docs[0]
    # Проверить, что данные, возвращаемые API, совпадают с теми что
    # в файле с тестовыми данными
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film/search?query_string={doc['title'][:4]}") as ans:
            assert ans.status == HTTPStatus.OK
            data = await ans.json()
            assert data[0]["uuid"] == doc['id']
            assert data[0]["title"] == doc['title']
            assert data[0]["imdb_rating"] == doc['imdb_rating']


@pytest.mark.asyncio
async def test_search_empty(empty_film_index):
    """Тест запускается без фикстур и API должен вернуть ошибку 404"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film/search") as ans:
            assert ans.status == HTTPStatus.NOT_FOUND


@pytest.mark.asyncio
async def test_no_index():
    """Тест запускается без индекса и API должен должен вернуть ошибку 500"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film/search=") as ans:
            assert ans.status == HTTPStatus.INTERNAL_SERVER_ERROR
