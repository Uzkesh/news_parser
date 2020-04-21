from typing import NamedTuple


class DBParamsDTO(NamedTuple):
    host: str
    port: str
    name: str
    user: str
    password: str


class DBPostTypesDTO(NamedTuple):
    post: int
    comment: int
