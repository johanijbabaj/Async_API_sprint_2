import os
import aiohttp
import pytest

# FIXME Я так понимаю импорт фикстур из conftest происходит автотически без явного вызова. Можно удалить.
#from conftest import some_genre, empty_genre_index

# Строка с именем хоста и портом
API_HOST = os.getenv('API_HOST', 'localhost:8000')


@pytest.mark.asyncio
async def test_some_genre(some_genre):
    """Проверяем, что тестовый элемент доступен по API"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://{API_HOST}/api/v1/genre/0b105f87-e0a5-45dc-8ce7-f8632088f390") as ans:
            assert ans.status == 200
            data = await ans.json()
            assert data["uuid"] == "0b105f87-e0a5-45dc-8ce7-f8632088f390"
            assert data["name"] == "Western"
            assert len(data["film_ids"]) == 2


@pytest.mark.asyncio
async def test_empty_index(empty_genre_index):
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
