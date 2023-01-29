import orm_base
import file_model
from file_model import Film, User, Actor, FilmActors, Rating, Transaction, Ticket, Session, Comment
from db_connection_entities import get_cursor, get_connection


def print_menu():
    print("1.Show actors list\n"
          "2.Show film list\n"
          "3.Show info about film\n"
          "4.Add film to favourites\n"
          "5.Add rating to film\n"
          "6.Add comment to film\n"
          "7.List comments\n"
          "8.Info about actor (with films)\n"
          "9.Show bought tickets\n"
          "10.Show new tickets\n"
          "11.Actors acted in the same film\n"
          "12.Count of films, where actors played\n"
          "13.Watched films\n"
          "14.Recommendations\n"
          "0.Exit\n")


def show_actors_list():
    actors = file_model.Actor.objects.select(*('first_name', 'last_name', 'birth', 'birth_country'))
    i = 1

    for act in actors:
        print(f"{i}Actor")
        print(f'First Name: {act.first_name}')
        print(f'Last Name: {act.last_name}')
        print(f'Birth: {act.birth}')
        print(f'Birth Country: {act.birth_country}')
        i = i + 1


def show_film_list():
    films = file_model.Film.objects.select(*('id', 'name', 'year', 'topic'))
    i = 1

    for film in films:
        print(f'Film id: {film.id}')
        print(f'Film Name: {film.name}')
        print(f'Year: {film.year}')
        print(f'Topic: {film.topic}\n')
        i = i + 1


def show_film_info():
    print("Choose the film id: ")
    show_film_list()
    choose = input()
    film = file_model.Film.objects.select(*('id', 'year', 'name', 'producer', 'country', 'studio',
                                            'description', 'rating', 'topic'),
                                          where={"": {'=': (f'{Film.table_name}.id', choose)}})
    film = film[0]

    print(f'Film id: {film.id}')
    print(f'Film Name: {film.name}')
    print(f'Year: {film.year}')
    print(f'Produce: {film.producer}')
    print(f'Country: {film.country}')
    print(f'Studio: {film.studio}')
    print(f'Description: {film.description}')
    print(f'Rating: {film.rating}')
    print(f'Topic: {film.topic}')


def add_favourites_film(id):
    print("Choose film id to add to favourites: ")
    show_film_list()
    choose = input()

    connection = get_connection()
    cursor = connection.cursor()
    query = "UPDATE users SET favourite_films = array_append(favourite_films, %s) WHERE users.id=%s"
    cursor.execute(
        query,
        (int(choose), int(id))
    )
    connection.commit()
    user = User.objects.select(*("id", "email", "favourite_films"),
                               where={"": {'=': (f'{User.table_name}.id', id)}})
    user = user[0]
    print(f"Dear {user.email}, your new list of favourite films is: {user.favourite_films}")


def add_rating_film(id):
    print("What film you want to rate?")
    film_id = input()
    print("What type of mark you should give (from 0 to 5)")
    mark = input()
    film = Film.objects.select(*('id', 'rating'),
                               where={"": {'=': (f'id', film_id)}})[0]

    print(f"Film id: {film.id}")
    print(f"Film rating: {film.rating}")

    rating_data = [
        {"mark": mark,
         "user_id": id,
         "film_id": film_id}
    ]
    Rating.objects.insert(rows=rating_data)

    film = Film.objects.select(*('id', 'rating'),
                               where={"": {'=': (f'{Film.table_name}.id', film_id)}})[0]

    print(f"Film id: {film.id}")
    print(f"Film rating: {film.rating}")


def add_comment_film(id):
    print("Enter comment text: ")
    text = input()
    print("Enter film id: ")
    film = input()
    print("Enter rating id: ")
    rating = input()

    if rating != "":
        result = [
            {'com_text': text,
            'film_id': film,
            "rating_id": rating,
            "user_id": id}
        ]
    else:
        result = [
            {'com_text': text,
            'film_id': film,
            "user_id": id,
            'rating_id': None}
        ]

    Comment.objects.insert(rows=result)
    print('Comment added succesfully')


def list_user_comments(id):
    user_comments = Comment.objects.select(*("user_id", "rating_id", "com_text", "film_id"),
                                           where={
                                               "":{'=': ('user_id', id)}
                                           })
    print(user_comments)


def get_actor_info():
    pass


def show_tickets(id):
    joins = [
        {"INNER": ({Transaction.table_name: "user_id"}, f'id')},
        {"INNER": ({Ticket.table_name: "transaction_id"}, f'id')},
        {"INNER": ({Session.table_name: "id"}, 'session_id')}
    ]

    user_tickets = User.objects.select(*(f'{Ticket.table_name}.id', 'busy_count', 'cost', 'tr_type', 'start_time'),
                                       join=joins,
                                       where={'AND':
                                                  ({"<>": (f'tr_type', "online")},
                                                   {"=": (f'{Transaction.table_name}.user_id', id)},
                                                   {"<": (f'{Session.table_name}.start_time', "NOW()")})
                                              })

    for ticket in user_tickets:
        print(
              f"Busy count: {ticket.busy_count}\n"
              f"Cost: {ticket.cost}\n"
              f"Transaction: {ticket.tr_type}\n"
              f"Start: {ticket.start_time}\n")


def show_new_tickets():
    joins = [
        {"INNER": ({Transaction.table_name: "user_id"}, f'id')},
        {"INNER": ({Ticket.table_name: "transaction_id"}, f'id')},
        {"INNER": ({Session.table_name: "id"}, 'session_id')}
    ]

    user_tickets = User.objects.select(*(f'{Ticket.table_name}.id', 'busy_count', 'cost', 'tr_type', 'start_time'),
                                       join=joins,
                                       where={'AND':
                                                  ({"<>": (f'tr_type', "online")},
                                                   {"=": (f'{Transaction.table_name}.user_id', id)},
                                                   {">": (f'{Session.table_name}.start_time', "NOW()")})
                                              })

    for ticket in user_tickets:
        print(
            f"Busy count: {ticket.busy_count}\n"
            f"Cost: {ticket.cost}\n"
            f"Transaction: {ticket.tr_type}\n"
            f"Start: {ticket.start_time}\n")


def actors_in_film():
    joins = [
        {"INNER": ({FilmActors.table_name: "actor_id"}, f'id')},
        {"INNER": ({Film.table_name: "id"}, f'film_id')}
    ]

    actors = Actor.objects.select(*("first_name", "last_name", "name"), join=joins)
    print(actors)


def count_actors_played():
    joins = [
        {"INNER": ({FilmActors.table_name: "film_id"}, f'id')},
        {"INNER": ({Actor.table_name: "id"}, f"actor_id")}
    ]

    actors_films = Film.objects.groupby(f"{Actor.table_name}.id",
                                        *(f'{Actor.table_name}.first_name', f'{Actor.table_name}.last_name'),
                                        COUNT=f"{Actor.table_name}.id", join=joins)

    print(actors_films)


def watched_film(id):
    connection = get_connection()
    cursor = connection.cursor()
    query = 'SELECT name, id FROM film WHERE id IN (SELECT unnest(watched_films) FROM users WHERE id = %s)'

    cursor.execute(
        query,
        (id,)
    )

    new_list = list()
    is_fetching_completed = False

    while not is_fetching_completed:
        result = cursor.fetchmany(2000)
        new_list.append(result)
        is_fetching_completed = len(result) < 2000

    new_list = new_list[0]

    print(f"Fims watched by user {id}")
    for elem in new_list:
        print(f"Film id: {elem[1]} Film name: {elem[0]}")



def recommend_films():
    pass


def user_menu(id):
    print_menu()
    res = int(input())
    while res != 0:
        if res == 1:
            show_actors_list()
        elif res == 2:
            show_film_list()
        elif res == 3:
            show_film_info()
        elif res == 4:
            add_favourites_film(id)
        elif res == 5:
            add_rating_film(id)
        elif res == 6:
            add_comment_film(id)
        elif res == 7:
            list_user_comments(id)
        elif res == 8:
            get_actor_info()
        elif res == 9:
            show_tickets(id)
        elif res == 10:
            show_new_tickets()
        elif res == 11:
            actors_in_film()
        elif res == 12:
            count_actors_played()
        elif res == 13:
            watched_film(id)
        elif res == 14:
            recommend_films()
        res = int(input())
