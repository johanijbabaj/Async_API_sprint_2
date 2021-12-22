"""
Тесты для доступа к одиночному фильму по API
"""

import os
from http import HTTPStatus

import aiohttp
import aioredis
import pytest

# Строка с именем хоста и портом
ELASTIC_HOST = os.getenv("ELASTIC_HOST", "localhost:9200")
API_HOST = os.getenv("API_HOST", "localhost:8000")


@pytest.mark.asyncio
async def test_some_film_cache(some_film, make_get_request):
    """Проверяем, что тестовый фильм кэшируется после запроса по API"""
    response = await make_get_request("/film/bb74a838-584e-11ec-9885-c13c488d29c0")
    assert response.status == HTTPStatus.OK
    data = response.body
    assert data["uuid"] == "bb74a838-584e-11ec-9885-c13c488d29c0"
    assert data["title"] == "Some film"
    assert data["imdb_rating"] == 5.5
    redis = await aioredis.create_redis_pool(
        (os.getenv("REDIS_HOST", "redis"), os.getenv("REDIS_PORT", 6379)),
        maxsize=20,
        password=os.getenv("REDIS_PASSWORD", "password"),
    )
    cached = await redis.get("bb74a838-584e-11ec-9885-c13c488d29c0")
    assert cached is not None
    # AIORedis возвращает строку bytes с объектом в JSON формате. Проверяем,
    # что в ней есть идентификатор и название фидьма
    assert '"uuid":"bb74a838-584e-11ec-9885-c13c488d29c0"' in str(cached)
    assert '"title":"Some film"' in str(cached)
