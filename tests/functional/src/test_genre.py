"""Содержит тесты лдля жанров"""
import os
from http import HTTPStatus

import aiohttp
import pytest

# Строка с именем хоста и портом
API_HOST = os.getenv("API_HOST", "localhost:8000")


@pytest.mark.asyncio
async def test_some_genre(some_genre):  # pylint: disable=unused-argument
    """Проверяем, что тестовый элемент доступен по API"""
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"http://{API_HOST}/api/v1/genre/0b105f87-e0a5-45dc-8ce7-f8632088f390"
        ) as ans:
            assert ans.status == HTTPStatus.OK
            data = await ans.json()
            assert data["uuid"] == "0b105f87-e0a5-45dc-8ce7-f8632088f390"
            assert data["name"] == "Western"
            assert len(data["film_ids"]) == 2


@pytest.mark.asyncio
async def test_empty_index(empty_genre_index):  # pylint: disable=unused-argument
    """Тест запускается с пустым индексом и API должен вернуть ошибку 404"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/genre") as ans:
            assert ans.status == HTTPStatus.NOT_FOUND


@pytest.mark.asyncio
async def test_no_index():
    """Тест запускается без индекса и API должен вернуть ошибку 500"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/genre") as ans:
            assert ans.status == HTTPStatus.INTERNAL_SERVER_ERROR
