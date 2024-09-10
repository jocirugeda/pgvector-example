import sqlite3
from sqlite3 import Error

def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


# create_posts = """
# INSERT INTO
#   posts (title, description, user_id)
# VALUES
#   ("Happy", "I am feeling very happy today", 1),
#   ("Hot Weather", "The weather is very hot today", 2),
#   ("Help", "I need some help with my work", 2),
#   ("Great News", "I am getting married", 1),
#   ("Interesting Game", "It was a fantastic game of tennis", 5),
#   ("Party", "Anyone up for a late-night party today?", 3);
# """
#
# execute_query(connection, create_posts)


    def execute_select(sqlite_select, sqlite_db):
        sqliteconnection = None
        try:
            sqliteconnection = sqlite3.connect(sqlite_db)
            cursor = sqliteconnection.cursor()
            print("Database created and Successfully Connected to SQLite")

            sqlite_select_Query = sqlite_select
            cursor.execute(sqlite_select_Query)
            record = cursor.fetchall()
            print("SQLite Database Version is: ", record)
            cursor.close()

        except sqlite3.Error as error:
            print("Error while connecting to sqlite", error)
        finally:
            if sqliteconnection:
                sqliteconnection.close()
                print("The SQLite connection is closed")

# select_posts = "SELECT * FROM posts"
# posts = execute_read_query(connection, select_posts)
#
# for post in posts:
#     print(post)