
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
from pgvector.psycopg2 import register_vector


import AppBase.gestorSQL.gestorPG as pgHandler

def init_db_context(prefix):
    # Connect to PostgreSQL database in Timescale using connection string

    conn =pgHandler.create_conection_prefix("PG")
    cur = conn.cursor()

    #install pgvector
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector");
    conn.commit()

    # Register the vector type with psycopg2
    register_vector(conn)

    return conn

def init_tables(conn):

    cmd_drop="""drop  table if exists  cruze_emb.raw_ventas;"""

    cur = conn.cursor()
    cur.execute(cmd_drop)
    cur.close()
    conn.commit()

    cmd_create=""" create table cruze_emb.raw_ventas( 
        id    bigserial    primary key,
        "Insercion"     text,
        "Fecha_compra"     text,
        "Fecha_garantia"     text,
        "Transaccion"      text,
        "Producto"     text,
        "tag_producto" text,
        "Email_comprador"     text,
        "d_email" text,
        "estado"      text,
        "Aff"      text,
        "Aff_name"    text,
        "currency"     text,
        "off"     text,
        "importe"     text,
        "cms_hotmart"     text,
        "cms_vendor"    text,
        "cms_aff"     text,
        "total"     text,
        "Actualizado"     text,
        "Nombre_comprador"     text,
        "Nombre_completo_comprador"  text,
        "Nombre2"  text,
        "Nombre3"  text,
        "Pais"    text,
        "Sexo"    text,
        "Probabilidad"  text,
        "count_data"     text,
        "Hora_local"    text,
        "productOfferPaymentMode"   text,
        "payment_type"   text,
        "refusal_reason"   text,
        "phone"   text,
        "pasarela"  text,      
        "ch_mes_str"  text,
        "ch_mes_N"  text,
        "ch_week_N" text,
        "num_altas" bigint,
        "init_altas" bigint,
        "num_ventas" bigint,
        "email_enlace" text,
        "cadena_busqueda" text,
        embedding_nombre  vector(512),
        embedding vector(512)
    );
    """
    cur = conn.cursor()
    cur.execute(cmd_create)
    cur.close()
    conn.commit()

    cmd_drop = """drop table if exists  cruze_emb.raw_altas;"""

    cur = conn.cursor()
    cur.execute(cmd_drop)
    cur.close()
    conn.commit()

    cmd_create="""create table  cruze_emb.raw_altas(
        id   bigserial     primary     key,
        "Nombre" text,
        "Nombre2"  text,
        "Nombre3"  text,
        "Email" text,
        d_email text,
        "UTM_Campaign" text,
        "UTM_Medium" text,
        "UTM_Source"  text,
        "UTM_Term" text,
        "UTM_Content" text,
        "Ref"  text,
        "Alta_webinar" text,
        "Alta_lead" text,
        "tag" text,
        "tag_producto" text,
        "last_UTM_Campaign" text, 
        "last_UTM_Medium" text,
        "last_UTM_Source" text,
        "last_UTM_Term" text,
        "last_UTM_Content" text,
        "telefono" text,
        "last_ref" text,
        "pais" text,    
        "flag_pais" text,
        "avatar" text,    
        "landing" text,
        "num_ventas" bigint,
        "cadena_busqueda" text,
        embedding_nombre  vector(512),
        embedding  vector(512)
    );
    """

    cur = conn.cursor()
    cur.execute(cmd_create)
    cur.close()
    conn.commit()


    cmd_drop = """drop table if exists  cruze_emb.mix_venta_altas;"""

    cur = conn.cursor()
    cur.execute(cmd_drop)
    cur.close()
    conn.commit()

    cmd_create="""create table  cruze_emb.mix_venta_altas(
        id_venta   bigint,
        id_alta   bigint,
        "Nombre_completo_comprador"  text,
        "Email_comprador"     text,
        producto_venta text,
        "Nombre_alta" text,
        producto_alta text,
        "Email" text,
        distancia double precision,
        distancia_nombre  double precision     
    );
    """

    cur = conn.cursor()
    cur.execute(cmd_create)
    cur.close()
    conn.commit()

    return conn
