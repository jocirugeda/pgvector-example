import os

import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
from pgvector.psycopg2 import register_vector

def create_conection_prefix(prefix):
    connection = None
    try:
        db_name=os.environ.get(prefix+"_db_name")
        db_user= os.environ.get(prefix+"_db_user")
        db_password=os.environ.get(prefix+"_db_password")
        db_host =os.environ.get(prefix+"_db_host")
        db_port=os.environ.get(prefix+"_db_port")
        connection=create_connection(db_name, db_user, db_password, db_host, db_port)
    except Exception as error:
        print(f"The error '{error}' occurred")
    return connection

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"The error '{error}' occurred")
    return connection

def create_database(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"The error '{error}' occurred")
    finally:
        if (cursor):
            cursor.close()

def execute_query(connection, query):
    #connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
        connection.commit()
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"The error '{error}' occurred")
    finally:

        if (cursor):
            cursor.close()


#    CREATE TABLE vendors (
#             vendor_id SERIAL PRIMARY KEY,
#             vendor_name VARCHAR(255) NOT NULL
#         )
# sql = """INSERT INTO vendors(vendor_name)
#             VALUES(%s) RETURNING vendor_id;"""
#
def insert_con_serial(connection,sql_insert):
    sql = sql_insert
    conn = connection
    vendor_id = None
    try:
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, sql_insert)
        # get the generated id back
        vendor_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if (cur):
            cur.close()
    return vendor_id

#  """ insert multiple vendors into the vendors table  """
#    sql = "INSERT INTO vendors(vendor_name) VALUES(%s)"
#
#   lista_tuples=[
#         ('AKM Semiconductor Inc.',),
#         ('Asahi Glass Co Ltd.',),
#         ('Daikin Industries Ltd.',),
#         ('Dynacast International Inc.',),
#         ('Foster Electric Co. Ltd.',),
#         ('Murata Manufacturing Co. Ltd.',)
#     ]
#

def insert_list_records(connection, sql_insert,list_tuples):
    sql = sql_insert
    conn = connection

    try:

        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql, list_tuples)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise error
    finally:
        cur.close()
    return 0

def insert_list_tuples(connection, sql_insert,list_tuples):
    sql = sql_insert
    conn = connection

    try:
        user_records = ", ".join(["%s"] * len(list_tuples))

        cad_final=( f"""{sql_insert}  {user_records}   """)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cursor.execute(insert_query, list_tuples)

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        cur.close()
    return 0







if __name__ == "__main__":
    connection = create_connection(
        "mydb", "javier", "finis2405", "127.0.0.1", "5432"
    )

    users = [
        ("James", 25, "male", "USA"),
        ("Leila", 32, "female", "France"),
        ("Brigitte", 35, "female", "England"),
        ("Mike", 40, "male", "Denmark"),
        ("Elizabeth", 21, "female", "Canada"),
    ]

    user_records = ", ".join(["%s"] * len(users))

    insert_query = (
        f"INSERT INTO pruebas.users (name, age, gender, nationality) VALUES {user_records}"
    )

    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(insert_query, users)


    posts = [
    ("Happy", "I am feeling very happy today", 1),
    ("Hot Weather", "The weather is very hot today", 2),
    ("Help", "I need some help with my work", 2),
    ("Great News", "I am getting married", 1),
    ("Interesting Game", "It was a fantastic game of tennis", 5),
    ("Party", "Anyone up for a late-night party today?", 3),
    ]

    post_records = ", ".join(["%s"] * len(posts))

    insert_query = (
    f"INSERT INTO pruebas.posts (title, description, user_id) VALUES {post_records}"
    )

    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(insert_query, posts)


