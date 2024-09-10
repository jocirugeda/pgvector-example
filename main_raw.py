# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


from sentence_transformers import SentenceTransformer

# loads BAAI/bge-small-en
# embed_model = HuggingFaceEmbedding()

# loads BAAI/bge-small-en-v1.5

import sys
import numba
import numpy

from dotenv import load_dotenv

from google.cloud import bigquery

import csv

import openai
import os
import csv
import pandas as pd
import numpy as np
import json
import tiktoken
import psycopg2
import ast
import pgvector
import math
from psycopg2.extras import execute_values
from pgvector.psycopg2 import register_vector

from altas_file import read_file_altas
from ventas_file import read_file_ventas

from Init_pg_tables import init_db_context,init_tables

from AppBase.gestorSQL.gestorPG import execute_query

print("Python version:", sys.version)
print("Numba version:", numba.__version__)
print("Numpy version:", numpy.__version__)


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def bq_test():
    print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    client = bigquery.Client()

    # Perform a query.
    QUERY = (
        'SELECT * FROM `coral-ndp.202205_NDP.202205_NDP_altas_UTMs` LIMIT 1000')
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    for row in rows:
        print(row.Nombre, row.Email)


# update por d_email vxv vxa
def update_d_email(connection):

    query_ventas3 = """
    update cruze_emb.raw_ventas vv
      set num_altas=p.num_altas
      ,email_enlace=p.email_final
    from(

    select a.id,a."Nombre_comprador", a."Nombre_completo_comprador",a."Email_comprador",b."Nombre",b.email_final ,(a.embedding <-> b.embedding) as distancia,1 as num_altas
    from
    (SELECT t1.id, t1.tag_producto,t1.embedding,t1.d_email,t1."Email_comprador", t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais", "Sexo", "Probabilidad", t1.count_data, t1."Hora_local", t1."productOfferPaymentMode",t1.ch_mes_str, t1."ch_mes_N", t1."ch_week_N"
        FROM cruze_emb.raw_ventas t1
        where  tag_producto<>'' and num_altas is null
        ) a

    inner join
    (
    SELECT t2.id, t2."Nombre", t2.d_email,t2."Email" as email_final, t2."UTM_Campaign", t2."UTM_Medium", t2."UTM_Source",
     t2.tag, t2.tag_producto, t2.telefono,  t2.pais, t2.avatar,t2.num_ventas, t2.cadena_busqueda, t2.embedding
        FROM cruze_emb.raw_altas t2
        where t2.tag_producto<>''  )b
     on (  a."d_email"=b."d_email" and b.tag_producto=b.tag_producto and a."Pais"= b.pais)
     --where (a.embedding <-> b.embedding)<0.25
     order by a."Email_comprador",(a.embedding <-> b.embedding) desc
        )p
    where p.id=vv.id	
    """

    execute_query(connection, query_ventas3)

    query_ventas2 = """
        update cruze_emb.raw_ventas vv
      set num_altas=p.num_altas
      ,email_enlace=p.email_final
    from(
    select a.id,a."Nombre_comprador", a."Nombre_completo_comprador",a."Email_comprador",b."Nombre_completo_comprador",b."email_enlace" as email_final,(a.embedding <-> b.embedding) as distancia,b.num_altas
    from
    (SELECT t1.id,t1.d_email,t1.tag_producto,t1.embedding, t1."Email_comprador", t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais", "Sexo", "Probabilidad", t1.count_data, t1."Hora_local", t1."productOfferPaymentMode",t1.ch_mes_str, t1."ch_mes_N", t1."ch_week_N"
    	FROM cruze_emb.raw_ventas t1
    	where  tag_producto<>'' and num_altas is null
        ) a

    inner join
    (
    SELECT t1.id, t1.d_email,t1.tag_producto,t1.embedding,t1.email_enlace, t1."Email_comprador", t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais", num_altas
    	FROM cruze_emb.raw_ventas t1
    	where tag_producto<>'' and num_altas is not null 
     )b
     on ( a."d_email"= b."d_email" and a."Pais"=b."Pais" and a.tag_producto=b.tag_producto)
     --where (a.embedding <-> b.embedding)<0.5
     order by a.id,(a.embedding <-> b.embedding) desc
    )p
    where vv.id=p.id

        """
    execute_query(connection, query_ventas2)

    query_ventas4 = """
        update cruze_emb.raw_ventas vv
        set num_altas=p.num_altas
        ,email_enlace=p.email_final
        from(

            select a.id,a."Nombre_comprador", a."Nombre_completo_comprador",a."Email_comprador",b."Nombre_completo_comprador",b."email_enlace" as email_final,(a.embedding <-> b.embedding) as distancia,b.num_altas
            from
            (SELECT t1.id, t1.tag_producto,t1.embedding, t1."Email_comprador", t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais", "Sexo", "Probabilidad", t1.count_data, t1."Hora_local", t1."productOfferPaymentMode",t1.ch_mes_str, t1."ch_mes_N", t1."ch_week_N"
                FROM cruze_emb.raw_ventas t1
                where  tag_producto<>'' and num_altas is null
                ) a

            inner join
            (
            SELECT t1.id, t1.tag_producto,t1.embedding, t1."Email_comprador",t1.email_enlace, t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais", num_altas
                FROM cruze_emb.raw_ventas t1
                where num_altas is not null and t1.email_enlace is not null
             )b
             on ( a."Email_comprador"=b."Email_comprador" )
             --where (a.embedding <-> b.embedding)<0.5
             order by a.id,(a.embedding <-> b.embedding) desc

        )p
        where vv.id=p.id	    
    """
    execute_query(connection, query_ventas4)


# update prod nombre completo pais
def update_prod_nombre_pais_full(connection):
    query_ventas2 = """
        update cruze_emb.raw_ventas vv
      set num_altas=p.num_altas
      ,email_enlace=p.email_final
    from(
    select a.id,a."Nombre_comprador", a."Nombre_completo_comprador",a."Email_comprador",b."Nombre_completo_comprador",b."email_enlace" as email_final,(a.embedding <-> b.embedding) as distancia,b.num_altas
    from
    (SELECT t1.id, t1.tag_producto,t1.embedding, t1."Email_comprador", t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais", "Sexo", "Probabilidad", t1.count_data, t1."Hora_local", t1."productOfferPaymentMode",t1.ch_mes_str, t1."ch_mes_N", t1."ch_week_N"
    	FROM cruze_emb.raw_ventas t1
    	where  tag_producto<>'' and num_altas is null and a."Pais"<>'*' 
        ) a

    inner join
    (
    SELECT t1.id, t1.tag_producto,t1.embedding, t1."email_enlace", t1.estado, t1."Nombre_comprador", 
          t1."Nombre_completo_comprador", "Pais", num_altas
    	FROM cruze_emb.raw_ventas t1
    	where tag_producto<>'' and num_altas is not null 
     )b
     on ( a.tag_producto=b.tag_producto and b."Pais"=a."Pais" and  a."Nombre_completo_comprador"= b."Nombre_completo_comprador" )
     --where (a.embedding <-> b.embedding)<0.5
     order by a.id,(a.embedding <-> b.embedding) desc
    )p
    where vv.id=p.id

    """

    execute_query(connection, query_ventas2)

    query_ventas4 = """
        update cruze_emb.raw_ventas vv
        set num_altas=p.num_altas
        ,email_enlace=p.email_final
        from(

            select a.id,a."Nombre_comprador", a."Nombre_completo_comprador",a.venta_prod,a."Email_comprador",b."Nombre",b.email_final,b.prod_alta ,(a.embedding <-> b.embedding) as distancia,1 as num_altas
            from
            (SELECT t1.id, t1.tag_producto as venta_prod,t1.embedding, t1."Email_comprador",t1.tag_producto ,t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais"
                FROM cruze_emb.raw_ventas t1
                where  t1.tag_producto<>'' and t1.num_altas is null
                ) a

            inner join
            (
            SELECT t2.id, t2."Nombre", t2."Email" as email_final, t2."UTM_Campaign", t2."UTM_Medium", t2."UTM_Source",
             t2.tag, t2.tag_producto as prod_alta, t2.telefono,  t2.pais, t2.avatar,t2.num_ventas, t2.cadena_busqueda, t2.embedding
                FROM cruze_emb.raw_altas t2
                where t2.tag_producto<>''  )b 
            on (a.venta_prod=b.prod_alta and a."Nombre_completo_comprador"=b."Nombre" and a."Pais"=b."pais")
            -- where (a.embedding <-> b.embedding)<0.25
             order by a."Email_comprador",(a.embedding <-> b.embedding) desc

        )p
        where vv.id=p.id	    
    """
    execute_query(connection, query_ventas4)

def update_prod_nombre_pais(connection):
    query_ventas2 = """
        update cruze_emb.raw_ventas vv
      set num_altas=p.num_altas
      ,email_enlace=p.email_final
    from(
    select a.id,a."Nombre_comprador", a."Nombre_completo_comprador",a."Email_comprador",b."Nombre_completo_comprador",b."email_enlace" as email_final,(a.embedding <-> b.embedding) as distancia,b.num_altas
    from
    (SELECT t1.id, t1.tag_producto,t1.embedding, t1."Email_comprador", t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais", "Sexo", "Probabilidad", t1.count_data, t1."Hora_local", t1."productOfferPaymentMode",t1.ch_mes_str, t1."ch_mes_N", t1."ch_week_N", t1.d_email
    	FROM cruze_emb.raw_ventas t1
    	where  tag_producto<>'' and num_altas is null and "Pais"<>'*' 
        ) a

    inner join
    (
    SELECT t1.id, t1.tag_producto,t1.embedding, t1."email_enlace", t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais", num_altas,d_email
    	FROM cruze_emb.raw_ventas t1
    	where tag_producto<>'' and num_altas is not null and t1."email_enlace" is not null
     )b
     on ( a.tag_producto=b.tag_producto and b."Pais"=a."Pais" and  a."Nombre_completo_comprador"= b."Nombre_completo_comprador" and a.d_email=b.d_email)
     --where (a.embedding <-> b.embedding)<0.5
     order by a.id,(a.embedding <-> b.embedding) desc
    )p
    where vv.id=p.id

    """
    print (query_ventas2)
    print("***********************")
    execute_query(connection, query_ventas2)
    print("***********************")
    query_ventas4 = """
        update cruze_emb.raw_ventas vv
        set num_altas=p.num_altas
        ,email_enlace=p.email_final
        from(

            select a.id,a."Nombre_comprador", a."Nombre_completo_comprador",a.venta_prod,a."Email_comprador",b."Nombre",b.email_final,b.prod_alta ,(a.embedding <-> b.embedding) as distancia,1 as num_altas
            from
            (SELECT t1.id, t1.tag_producto as venta_prod,t1.embedding, t1."Email_comprador",t1.tag_producto ,t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais",d_email
                FROM cruze_emb.raw_ventas t1
                where  t1.tag_producto<>'' and t1.num_altas is null
                ) a

            inner join
            (
            SELECT t2.id, t2."Nombre", t2."Email" as email_final, t2."UTM_Campaign", t2."UTM_Medium", t2."UTM_Source",
             t2.tag, t2.tag_producto as prod_alta, t2.telefono,  t2.pais, t2.avatar,t2.num_ventas, t2.cadena_busqueda, t2.embedding,t2.d_email
                FROM cruze_emb.raw_altas t2
                where t2.tag_producto<>'' and t2.num_ventas is null  )b 
            on (a.venta_prod=b.prod_alta and a."Nombre_completo_comprador"=b."Nombre" and a."Pais"=b."pais" and b.d_email=a.d_email )
            -- where (a.embedding <-> b.embedding)<0.25
             order by a."Email_comprador",(a.embedding <-> b.embedding) desc

        )p
        where vv.id=p.id	    
    """
    print(query_ventas4)
    execute_query(connection, query_ventas4)

def update_prod_nombre_pais(connection):
    query_ventas2 = """
        update cruze_emb.raw_ventas vv
      set num_altas=p.num_altas
      ,email_enlace=p.email_final
    from(
    select a.id,a."Nombre_comprador", a."Nombre_completo_comprador",a."Email_comprador",b."Nombre_completo_comprador",b."email_enlace" as email_final,(a.embedding <-> b.embedding) as distancia,b.num_altas
    from
    (SELECT t1.id, t1.tag_producto,t1.embedding, t1."Email_comprador", t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais", "Sexo", "Probabilidad", t1.count_data, t1."Hora_local", t1."productOfferPaymentMode",t1.ch_mes_str, t1."ch_mes_N", t1."ch_week_N", t1.d_email
    	FROM cruze_emb.raw_ventas t1
    	where  tag_producto<>'' and num_altas is null and "Pais"<>'*' 
        ) a

    inner join
    (
    SELECT t1.id, t1.tag_producto,t1.embedding, t1."email_enlace", t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais", num_altas,d_email
    	FROM cruze_emb.raw_ventas t1
    	where tag_producto<>'' and num_altas is not null and t1."email_enlace" is not null
     )b
     on ( a.tag_producto=b.tag_producto and b."Pais"=a."Pais" and  a."Nombre_completo_comprador"= b."Nombre_completo_comprador" and a.d_email=b.d_email)
     --where (a.embedding <-> b.embedding)<0.5
     order by a.id,(a.embedding <-> b.embedding) desc
    )p
    where vv.id=p.id

    """
    print (query_ventas2)
    print("***********************")
    execute_query(connection, query_ventas2)
    print("***********************")
    query_ventas4 = """
        update cruze_emb.raw_ventas vv
        set num_altas=p.num_altas
        ,email_enlace=p.email_final
        from(

            select a.id,a."Nombre_comprador", a."Nombre_completo_comprador",a.venta_prod,a."Email_comprador",b."Nombre",b.email_final,b.prod_alta ,(a.embedding <-> b.embedding) as distancia,1 as num_altas
            from
            (SELECT t1.id, t1.tag_producto as venta_prod,t1.embedding, t1."Email_comprador",t1.tag_producto ,t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais",d_email
                FROM cruze_emb.raw_ventas t1
                where  t1.tag_producto<>'' and t1.num_altas is null
                ) a

            inner join
            (
            SELECT t2.id, t2."Nombre", t2."Email" as email_final, t2."UTM_Campaign", t2."UTM_Medium", t2."UTM_Source",
             t2.tag, t2.tag_producto as prod_alta, t2.telefono,  t2.pais, t2.avatar,t2.num_ventas, t2.cadena_busqueda, t2.embedding,t2.d_email
                FROM cruze_emb.raw_altas t2
                where t2.tag_producto<>'' and t2.num_ventas is null  )b 
            on (a.venta_prod=b.prod_alta and a."Nombre_completo_comprador"=b."Nombre" and a."Pais"=b."pais" and b.d_email=a.d_email )
            -- where (a.embedding <-> b.embedding)<0.25
             order by a."Email_comprador",(a.embedding <-> b.embedding) desc

        )p
        where vv.id=p.id	    
    """
    print(query_ventas4)
    execute_query(connection, query_ventas4)



def read_files(connection):

    init_tables(connection)

    print_hi('PyCharm')
    embed_model = SentenceTransformer('sentence-transformers/distiluse-base-multilingual-cased-v1')

    read_file_ventas(os.environ.get("FILE_VENTAS_RAW"), "PG", connection, embed_model)
    read_file_altas(os.environ.get("FILE_ALTAS_RAW"), "PG", connection, embed_model)


def marcar_altas(connection):
    query_altas = """ update cruze_emb.raw_altas t2
                set num_ventas=t2.num_ventas
                from (select email_enlace,count(*) as num_ventas
    	        FROM cruze_emb.raw_ventas
    	            group by email_enlace  ) t1
                where t1.email_enlace=t2."Email" and t2.num_ventas is null 	"""
    execute_query(connection, query_altas)

def initial_files_querys(connection):

    query_ventas = """ update cruze_emb.raw_ventas t1
                        set num_altas=null
                        ,init_altas=null 
                        ,email_enlace=null """

    execute_query(connection, query_ventas)

    query_ventas = """ update 	cruze_emb.raw_ventas t1
                        set num_altas=t2.num_altas
                        ,init_altas=t2.num_altas
                        ,email_enlace=t2."Email"
                        from (select "Email" ,count(*) as num_altas
        	                    FROM cruze_emb.raw_altas
        	            group by "Email"  ) t2
                        where t1."Email_comprador"=t2."Email" """

    execute_query(connection, query_ventas)

    query_altas = """ update 	cruze_emb.raw_altas t2
                set num_ventas=t2.num_ventas
                from (select "Email_comprador" ,count(*) as num_ventas
    	        FROM cruze_emb.raw_ventas
    	            group by "Email_comprador"  ) t1
                where t1."Email_comprador"=t2."Email" 	"""

    execute_query(connection, query_altas)

    query_altas = """ update cruze_emb.raw_altas t2
                    set num_ventas=t2.num_ventas
                    from (select "Email_comprador" ,count(*) as num_ventas 
        	        FROM cruze_emb.raw_ventas
        	            group by "Email_comprador"  ) t1
                    where t1."Email_comprador"=t2."Email" 	"""

    execute_query(connection, query_altas)

    query_altas = """ update cruze_emb.raw_altas t2
                    set pais=t1.pais
                    from (select "Email_comprador","Pais" as pais 
        	        FROM cruze_emb.raw_ventas 
        	        where "Pais"<>'' and "Pais"<>'*' and "Pais" is not null  
        	              ) t1
                    where t1."Email_comprador"=t2."Email" and t2.pais='X'  	"""

    execute_query(connection, query_altas)

    query_altas = """ update cruze_emb.raw_altas t2
                    set pais=t1.pais
                    from (select "Email","pais" as pais 
        	        FROM cruze_emb.raw_altas 
        	        where "pais"<>'X'
        	              ) t1
                    where t1."Email"=t2."Email" and t2.pais='X' """

    execute_query(connection, query_altas)


def row_process(key_sql, row_dic):
    print(f" {key_sql}  -- > {row_dic} ")


def select_key(connection, key, sql_select_Query):
    try:
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()

        print("Fetching each row using column name")
        for x in records:
            temp2 = {}
            c = 0
            for col in cursor.description:
                temp2.update({str(col[0]): x[c]})
                c = c + 1
            row_process(key,temp2)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"The error '{error}' occurred")
    finally:
        if (cursor):
            cursor.close()






# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    load_dotenv(".env")
    connection = init_db_context("PG")

    #read_files(connection)

    #initial_files_querys(connection)

    marcar_altas(connection)

    update_prod_nombre_pais(connection)

    marcar_altas(connection)

    update_prod_nombre_pais(connection)

    marcar_altas(connection)

    sql_select=""" select count(*) as contador, sum(case when num_altas is null then 1 else 0 end) as pendientes
from cruze_emb.raw_ventas
where num_altas is null """

    select_key(connection, "00", sql_select)

#    update_d_email(connection)
#    update_prod_nombre_pais(connection)

#    update_d_email(connection)
#    update_prod_nombre_pais(connection)

#    update_d_email(connection)
#    update_prod_nombre_pais(connection)

#    update_d_email(connection)
#    update_prod_nombre_pais(connection)


# bq_test()
# read_file_altas(os.environ.get("FILE_ALTAS"),"PG")
print("Fin Ejecucion")
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
