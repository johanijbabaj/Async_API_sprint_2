import logging
from http import HTTPStatus
from typing import List, Literal, Optional
from uuid import UUID

from core.config import ErrorMessage
from fastapi import APIRouter, Depends, HTTPException, Query
from models.person import Person_API, PersonBrief_API
from services.person import PersonService, get_person_service

router = APIRouter()


@router.get('/{person_id}', response_model=Person_API)
async def person_details(
    person_id: str,
    person_service: PersonService = Depends(get_person_service)
) -> Person_API:
    person = await person_service.get_by_id(person_id)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=ErrorMessage.PERSON_NOT_FOUND)
    return Person_API(
        uuid=person.uuid,
        full_name=person.full_name,
        birth_date=person.birthdate,
        film_ids=[film['id'] for film in person.films]
    )


@router.get('/')
async def person_list(sort: Literal["full_name.raw"] = "full_name.raw",
                      filter_film: Optional[UUID] = Query(None, alias="filter[film]"),
                      filter_name: Optional[str] = Query(None, alias="search[name]"),
                      page_size: int = Query(10, alias="page[size]"),
                      page_number: int = Query(1, alias="page[number]"),
                      person_service: PersonService = Depends(get_person_service)
                      ) -> List[PersonBrief_API]:
    """
        Примеры обращений, которые должны обрабатываться API
        #GET /api/v1/person?sort=full_name.raw&page[size]=50&page[number]=1
        #GET /api/v1/person?filter[film]=<uuid:UUID>&sort=name&page[size]=50&page[number]=1
    """
    logging.debug(f"Получили параметры {sort=}-{type(sort)}, {filter_film=}-{type(filter_film)},"
                  f" {page_size=}-{type(page_size)}, {page_number=}-{type(page_number)}")
    persons = await person_service.get_by_film_id(
        filter_film, filter_name, sort, page_size, page_number
    )
    if not persons:
        # Если выборка пустая, отдаём 404 статус
        # Желательно пользоваться уже определёнными HTTP-статусами, которые содержат enum
        # Такой код будет более поддерживаемым
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='persons not found')
    return [
        PersonBrief_API(uuid=person.id, full_name=person.full_name, birth_date=person.birth_date)
        for person in persons
    ]
