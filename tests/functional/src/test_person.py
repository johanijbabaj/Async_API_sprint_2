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
async def test_empty_index(empty_person_index):  # pylint: disable=unused-argument
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
