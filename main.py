from db_connection_entities import get_cursor
from file_model import Film, User, Actor, FilmActors
from user_entities import user_menu
from admin_entities import admin_menu


def check_email(email: str) -> bool:
    users = User.objects.select(*('email',))
    for us in users:
        if email == us.email:
            return False

    return True


def register():
    print("Please enter email: ")
    email = input()
    print("Please enter password: ")
    password = input()
    print("Please enter password confirmation: ")
    pass_conf = input()

    while password != pass_conf:
        print("Please enter correct password in both sides!")
        print("Please enter password: ")
        password = input()
        print("Please enter password confirmation: ")
        pass_conf = input()

    while check_email(email) is False:
        print("That email is already registered, please enter the new one!")
        email = input()

    user_data = [
        {
            "email": email,
            "password": password,
            "user_role": 'user'
        }
    ]
    User.objects.insert(rows=user_data)


def login():
    # authentication phase
    print("Input email")
    # email = input()
    print("Input password")
    # password = input()
    email = 'user@gmail.com'
    password = '12341234'
    user = User.objects.select(*('id', 'email', 'password', 'user_role'),
                               where={'AND': ({"=": (f'{User.table_name}.email', email)},
                                              {"=": (f'{User.table_name}.password', password)})})
    if user is not None:
        user = user[0]
        print(f"Hello my dear {user.email}")
    else:
        print('User with such account does not exists!')

    if user.user_role == 'user':
        user_menu(user.id)
    else:
        admin_menu()
    # authorization phase



def main():
    print("Hello, we glad to see you in Movie App!")
    print("1.Register\n"
          "2.Login")
    a = input()

    while int(a) != 1 and int(a) != 2:
        print("Please enter correct number!")
        a = input()

    if int(a) == 1:
        register()
    else:
        login()
    # BaseManager.set_connection()
    # films = Film.objects.select(*('id', 'name', 'rating'))

    # result = Film.objects.where_configure({'AND': ({"<": (f'{Film.table_name}.rating', 5)}, {">": (f'{Film.table_name}.rating', 2)})})
    # result = Film.objects.where_configure({'NOT': ({'>': (f'{Film.table_name}.rating', 2)})})

    where_test = {'AND': ({"<": (f'{Film.table_name}.rating', 5)}, {"<": (f'{Film.table_name}.rating', 2)})}

    films = Film.objects.select(*('id', 'name', 'year'), where=where_test,
                                orderby=[(f'{Film.table_name}.year', "DESC")])

    grouped_result = Film.objects.groupby('topic', AVG=f'rating', SUM=f'year', orderby=[(f'SUM(year)', 'DESC')])
    # print(result)

    jjoin = [{"INNER": ({FilmActors.table_name: "film_id"}, f'id')},
             {"INNER": ({Actor.table_name: "id"}, f'actor_id')}]

    join_test = Film.objects.select(
        *(f'{Film.table_name}.name', f'{Actor.table_name}.first_name', f'{Actor.table_name}.last_name'),
        join=jjoin)
    print(join_test)
    print()
    print(grouped_result)
    print()
    print(films)

    # for item in films:
    #     if item.id == 8:
    #         print(item)
    # comment = User.objects.select('email', 'watched_films')

    # print(films)
    # print(comment[0])


if __name__ == "__main__":
    main()
