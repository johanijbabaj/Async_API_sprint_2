from pydantic import BaseSettings, Field


class TestSettings(BaseSettings):
    es_host: str = Field('http://localhost:9200', env='ELASTIC_HOST')
    api_host: str = Field("http://localhost:8000", env="API_HOST")
    redis_host: str = Field("localhost", env="REDIS_HOST")
    redis_port: int = Field(6379, env="REDIS_PORT")
    redis_password: str = Field("password", env="REDIS_PASSWORD")
