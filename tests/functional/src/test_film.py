"""
Тесты для доступа к одиночному фильму по API
"""

from elasticsearch import Elasticsearch, helpers

import pytest
import requests

from settings import TestSettings


@pytest.fixture()
def some_film(request):
    docs = [
        {
            "id": "bb74a838-584e-11ec-9885-c13c488d29c0",
            "imdb_rating": 5.5,
            "genre": "Action",
            "title": "Some film",
            "description": "Some film used for testing only",
            "genres": [{"id": "46e70470-592f-11ec-8b39-d99d30aa920b", "name": "Action"}],
            "director": "John Smith",
            "actors": [],
            "writers": [],
            "actor_names": [],
            "writer_names": [],
        }
    ]

    es = Elasticsearch(TestSettings().es_host)
    helpers.bulk(
        es,
        [
            {
                '_index': 'movies',
                '_id': doc["id"],
                **doc
            }
            for doc in docs
        ]
    )


def test_some_film(some_film):
    """Проверяем, что тестовый фильм доступен по API"""
    ans = requests.get("http://fast_api:8000/api/v1/film/bb74a838-584e-11ec-9885-c13c488d29c0")
    assert ans.status_code == 200
    data = ans.json()
    assert data["uuid"] == "bb74a838-584e-11ec-9885-c13c488d29c0"
    assert data["title"] == "Some film"
    assert data["imdb_rating"] == 5.5
