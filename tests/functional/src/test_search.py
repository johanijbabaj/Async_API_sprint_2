"""
Тесты поиска фильмов по подстроке наименования фильма
"""

import os

import aiohttp
import pytest

# Строка с именем хоста и портом
API_HOST = os.getenv('API_HOST', 'localhost:8000')


@pytest.mark.asyncio
async def test_search_film(some_film):
    """Проверяем, что тестовый фильм доступен по API"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film/search?query_string=Some") as ans:
            assert ans.status == 200
            data = await ans.json()
            assert data[0]["uuid"] == "bb74a838-584e-11ec-9885-c13c488d29c0"
            assert data[0]["title"] == "Some film"
            assert data[0]["imdb_rating"] == 5.5


@pytest.mark.asyncio
async def test_search_empty(empty_film_index):
    """Тест запускается без фикстур и API должен вернуть ошибку 404"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film/search") as ans:
            assert ans.status == 404


@pytest.mark.asyncio
async def test_no_index():
    """Тест запускается без индекса и API должен должен вернуть ошибку 500"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/film/search=") as ans:
            assert ans.status == 500
