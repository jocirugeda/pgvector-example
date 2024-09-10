
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

    cmd_drop="""drop  table if exists  raw_schema.raw_ventas_id;"""

    cur = conn.cursor()
    cur.execute(cmd_drop)
    cur.close()
    conn.commit()

    cmd_create=""" 
create table raw_schema.raw_ventas_id (
"Insercion" 	timestamp

,"Fecha_compra" 	timestamp

,"Fecha_garantia" 	timestamp

,"Transaccion" varchar(1000)

,"Producto" 	varchar(1000)

,"Email_comprador"  varchar(1000)

,"PR_Email_comprador"  varchar(1000)
,"estado" 	varchar(1000)

,"Aff"  varchar(1000)

,"Aff_name" varchar(1000)

,"currency" varchar(1000)

,"off" varchar(1000)

,"importe" 	varchar(1000)

,"cms_hotmart" varchar(1000)

,"cms_vendor" 	NUMERIC(20,2)

,"cms_aff"  varchar(1000)

,"total" 	varchar(1000)

,"Actualizado" 	timestamp

,"Nombre_comprador" 	varchar(1000)

,"Nombre_completo_comprador" 	varchar(1000)

,"Pais" 	varchar(1000)

,"COD_Pais" 	varchar(1000)

,"Sexo" 	varchar(1000)

,"Probabilidad" varchar(1000)

,"count_data" 	varchar(1000)

,"Hora_local" 	timestamp

,"productOfferPaymentMode" varchar(1000)

,"payment_type" 	varchar(1000)

,"refusal_reason" 	varchar(1000)

,"phone" 	varchar(1000)

,"pasarela" 	varchar(1000)

,"ch_mes_str" 	varchar(1000)

,"ch_mes_N"  	bigint 

,"ch_week_N" 	bigint
 );

    );
    """
    cur = conn.cursor()
    cur.execute(cmd_create)
    cur.close()
    conn.commit()

    cmd_drop = """drop table if exists  raw_schema.raw_altas_id;"""

    cur = conn.cursor()
    cur.execute(cmd_drop)
    cur.close()
    conn.commit()

    cmd_create="""
    CREATE TABLE raw_schema.raw_altas_id(
    
    "Nombre" 	STRING
    
    ,"Email" 	STRING
    
    ,"PR_Email" STRING
    
    ,"UTM_Campaign" 	STRING
    
    ,"UTM_Medium" 	STRING
    
    ,"UTM_Source" 	STRING
    
    ,"UTM_Term"  	STRING
    
    ,"UTM_Content"  STRING
    
    ,"Ref"  	STRING
    
    ,"Alta_webinar" 	DATE
    
    ,"Alta_lead" 	DATE
    
    ,"tag" 	STRING
    
    ,"last_UTM_Campaign"  STRING
    
    ,"last_UTM_Medium"  	STRING
    
    ,"last_UTM_Source"  	STRING
    
    ,"last_UTM_Term"  	STRING
    
    ,"last_UTM_Content"  	STRING
    
    ,"telefono" 	STRING
    
    ,"last_ref" 	STRING
    
    ,"pais" 	STRING
    
    ,"COD_pais"  STRING
    
    ,"avatar" 	STRING
    
    ,"landing" 	STRING
    
    ,"ch_mes_str" 	STRING
    ,"ch_mes_N" 	INTEGER
    ,"ch_week_N"  	INTEGER
    
    ,"Insercion" DATETIME
    );
    """

    cur = conn.cursor()
    cur.execute(cmd_create)
    cur.close()
    conn.commit()

    return conn
