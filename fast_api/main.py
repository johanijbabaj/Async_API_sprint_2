import logging

import aioredis
import uvicorn
from api.v1 import film, genre, person
from core import config
from core.logger import LOGGING
from db import elastic, cache
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

app = FastAPI(
    # Конфигурируем название проекта. Оно будет отображаться в документации
    title=config.PROJECT_NAME,
    # Адрес документации в красивом интерфейсе
    docs_url='/api/openapi',
    # Адрес документации в формате OpenAPI
    openapi_url='/api/openapi.json',
    # Можно сразу сделать небольшую оптимизацию сервиса
    # и заменить стандартный JSON-сереализатор на более шуструю версию, написанную на Rust
    default_response_class=ORJSONResponse,
)


@app.on_event('startup')
async def startup():
    # Подключаемся к базам при старте сервера
    # Подключиться можем при работающем event-loop
    # Поэтому логика подключения происходит в асинхронной функции
    cache.redis = await aioredis.create_redis_pool((config.REDIS_HOST, config.REDIS_PORT), minsize=10,
                                                   maxsize=20, password=config.REDIS_AUTH)
    elastic.es = AsyncElasticsearch(hosts=[f'{config.ELASTIC_HOST}:{config.ELASTIC_PORT}'])


@app.on_event('shutdown')
async def shutdown():
    # Отключаемся от баз при выключении сервера
    await cache.redis.close()
    await elastic.es.close()


# Подключаем роутеры к серверу, указав префиксы /v1/film,
# v1/genre и v1/person
# Теги указываем для удобства навигации по документации
app.include_router(film.router, prefix='/api/v1/film', tags=['film'])
app.include_router(genre.router, prefix='/api/v1/genre', tags=['genre'])
app.include_router(person.router, prefix='/api/v1/person', tags=['person'])

if __name__ == '__main__':
    # Приложение может запускаться командой
    # `uvicorn main:app --host 0.0.0.0 --port 8000`
    # но чтобы не терять возможность использовать дебагер,
    # запустим uvicorn сервер через python
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=8000,
        log_config=LOGGING,  # Этот параметр присутствовал в первоначальной версии файла но потом исчез
        log_level=logging.DEBUG,  # Этот параметр присутствовал в первоначальной версии файла но потом исчез
    )
