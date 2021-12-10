from abc import ABC, abstractmethod
from aioredis import Redis
from typing import Optional

redis: Optional[Redis] = None


class MemoryCache(ABC):
    @abstractmethod
    def set(self, key, data, expire):
        pass

    @abstractmethod
    def get(self, key):
        pass


class RedisCache(MemoryCache):
    __con = None

    def __init__(self, redis_instance: Redis):
        self.__con = redis_instance

    async def set(self, key, data, expire):
        await self.__con.set(key, data, expire=expire)

    async def get(self, key):
        data = await self.__con.get(key)
        return data


async def get_redis() -> Redis:
    return redis


async def get_cache() -> MemoryCache:
    redis_instance = await get_redis()
    return RedisCache(redis_instance)
