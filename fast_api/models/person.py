import datetime
from typing import List, Optional
from uuid import UUID

from models._base import OrjsonModel


class Person_API(OrjsonModel):
    """
        Возвращаемая информация о человеке - идентификатор,
        имя, дата рождения м список фильмов с его участием.
    """
    uuid: UUID
    full_name: str
    birth_date: Optional[str]
    film_ids: List[str]


class PersonBrief_API(OrjsonModel):
    """
        Сокращенная информация о человеке - возвращается при запросе
        списка
    """
    uuid: UUID
    full_name: str
    birth_date: Optional[str]


class Person(OrjsonModel):
    """
        Информация о человеке
    """
    uuid: UUID
    full_name: str
    birthdate: Optional[datetime.date]
    films: List[dict]


class PersonBrief(OrjsonModel):
    """
        Сокращенная информация о человеке
    """
    id: UUID
    full_name: str
    birth_date: Optional[datetime.date]
