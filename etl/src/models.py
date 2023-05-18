from typing import List, Optional
import uuid
import datetime
from dataclasses import dataclass, field
from pydantic import BaseModel, Field


@dataclass(frozen=True)
class TimeStampedMixin:
    created: datetime.datetime
    modified: datetime.datetime


@dataclass(frozen=True)
class UUIDMixin:
    id: uuid.UUID


@dataclass(frozen=True)
class Filmwork(UUIDMixin, TimeStampedMixin):
    title: str
    description: str
    creation_date: datetime.date
    type: str
    rating: float = field(default=0.0)


@dataclass(frozen=True)
class Genre(UUIDMixin, TimeStampedMixin):
    name: str
    description: str


@dataclass(frozen=True)
class Person(UUIDMixin, TimeStampedMixin):
    full_name: str


@dataclass(frozen=True)
class PersonFilmwork(UUIDMixin, TimeStampedMixin):
    role: str
    person_id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class GenreFilmwork(UUIDMixin, TimeStampedMixin):
    genre_id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)


class RoleSchema(BaseModel):
    id: uuid.UUID
    name: str


class FilmworkSchema(BaseModel):
    id: uuid.UUID
    imdb_rating: float | None
    genre: List[str] = Field(None, alias='genres_field')
    title: str
    description: Optional[str]
    director: Optional[List[str]]
    actors_names: Optional[List[str]]
    writers_names: Optional[List[str]]
    actors: Optional[List[RoleSchema]]
    writers: Optional[List[RoleSchema]]

    class Config:
        json_encoders = {
            uuid.UUID: lambda u: str(u),
            datetime: lambda v: v.timestamp(),
            RoleSchema: lambda s: str(s),
        }
