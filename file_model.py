from orm_base import BaseModel, BaseManager


class Film(BaseModel):
    manager_class = BaseManager
    table_name = "film"


class User(BaseModel):
    manager_class = BaseManager
    table_name = "users"


class Rating(BaseModel):
    manager_class = BaseManager
    table_name = "rating"


class Transaction(BaseModel):
    manager_class = BaseManager
    table_name = "transaction"


class Session(BaseModel):
    manager_class = BaseManager
    table_name = 'session'


class FilmActors(BaseModel):
    manager_class = BaseManager
    table_name = 'film_actor'


class Actor(BaseModel):
    manager_class = BaseManager
    table_name = 'actor'


class Hall(BaseModel):
    manager_class = BaseManager
    table_name = 'hall'


class Cinema(BaseModel):
    manager_class = BaseManager
    table_name = 'cinema'


class Comment(BaseModel):
    manager_class = BaseManager
    table_name = 'comment'

class Ticket(BaseModel):
    manager_class = BaseManager
    table_name = 'ticket'

