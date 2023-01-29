import psycopg2

DB_SETTINGS = {
    "database": "cinema_db",
    "host": "localhost",
    "user": "postgres",
    "password": "kolyan4ik234",
    "port": "5432"
}


def get_cursor():
    connection = psycopg2.connect(**DB_SETTINGS)
    return connection.cursor()

def get_connection():
    return psycopg2.connect(**DB_SETTINGS)