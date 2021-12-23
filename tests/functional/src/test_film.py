"""
Тесты для доступа к одиночному фильму по API
"""

import os
from http import HTTPStatus

import aiohttp
import pytest

# Строка с именем хоста и портом
ELASTIC_HOST = os.getenv("ELASTIC_HOST", "localhost:9200")
API_HOST = os.getenv("API_HOST", "localhost:8000")


@pytest.mark.asyncio
async def test_some_film(some_film, make_get_request):
    """Проверяем, что тестовый фильм доступен по API"""
    response = await make_get_request("/film/bb74a838-584e-11ec-9885-c13c488d29c0")
    assert response.status == HTTPStatus.OK
    data = response.body
    assert data["uuid"] == "bb74a838-584e-11ec-9885-c13c488d29c0"
    assert data["title"] == "Some film"
    assert data["imdb_rating"] == 5.5


# @pytest.mark.skip(reason="no")
@pytest.mark.asyncio
async def test_film_list(some_film, make_get_request):
    """Проверяем, что тестовый фильм отображается в списке всех фильмов"""
    # FIXME: Надо переделать на метод получения списка фильмом по жанру
    response = await make_get_request("/film/bb74a838-584e-11ec-9885-c13c488d29c0")
    assert response.status == HTTPStatus.OK
    data = response.body
    assert data["uuid"] == "bb74a838-584e-11ec-9885-c13c488d29c0"
    assert data["title"] == "Some film"
    assert data["imdb_rating"] == 5.5


@pytest.mark.asyncio
async def test_empty(empty_film_index, make_get_request):
    """Тест запускается без фикстур и API должен вернуть ошибку 404"""
    response = await make_get_request("/film")
    assert response.status == HTTPStatus.NOT_FOUND


@pytest.mark.asyncio
async def test_no_index(flush_redis, make_get_request):
    """Тест запускается без индекса и API должен вернуть ошибку 500"""
    response = await make_get_request("/film/bb74a838-584e-11ec-9885-c13c488d29c0")
    assert response.status == HTTPStatus.INTERNAL_SERVER_ERROR
