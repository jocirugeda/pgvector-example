import csv

import AppBase.gestorSQL.gestorPG as pgHandler
import urllib.parse
import numpy as np
import pandas as pd

from AppBase.Utiles.caracteres import emmbe_arr,clean_email,procesar_cadena

def conversor_paises(input):
    altas_conv_paises = {
        "costa rica": "Costa Rica",
        "panama": "Panama",
        "guatemala": "Guatemala",
        "china": "China",
        "canada": "Canada",
        "morocco": "Morocco",
        "puerto rico": "Puerto Rico",
        "peru": "Peru",
        "chile": "Chile",
        "spain": "Spain",
        "ecuador": "Ecuador",
        "dominican republic": "Dominican Republic",
        "germany": "Germany",
        "brazil": "Brazil",
        "cuba": "Cuba",
        "belgium": "Belgium",
        "trinidad and tobago": "Trinidad and Tobago",
        "guyana": "Guyana",
        "curacao": "Curaçao",
        "switzerland": "Switzerland",
        "united arab emirates": "United Arab Emirates",
        "bonaire/sint eustatius/saba": "Bonaire, Sint Eustatius and Saba",
        "ireland": "Ireland",
        "nicaragua": "Nicaragua",
        "sri lanka": "Sri Lanka",
        "new zealand": "New Zealand",
        "lebanon": "Lebanon",
        "moldova": "Moldova",
        "malaysia": "Malaysia",
        "norway": "Norway",
        "montenegro": "Montenegro",
        "iceland": "Iceland",
        "slovenia": "Slovenia",
        "luxembourg": "Luxembourg",
        "jamaica": "Jamaica",
        "croatia": "Croatia",
        "indonesia": "Indonesia",
        "viet nam": "Viet Nam",
        "equatorial guinea": "Equatorial Guinea",
        "turks and caicos islands": "Turks and Caicos Islands",
        "cape verde": "Cabo Verde",
        "malta": "Malta",
        "denmark": "Denmark",
        "slovakia (slovak republic)": "Slovakia",
        "sint maarten": "Sint Maarten",
        "egypt": "Egypt",
        "hong kong": "Hong Kong",
        "saudi arabia": "Saudi Arabia",
        "british virgin islands": "Virgin Islands (British)",
        "paraguay": "Paraguay",
        "bolivia": "Bolivia",
        "argentina": "Argentina",
        "uruguay": "Uruguay",
        "austria": "Austria",
        "venezuela": "Venezuela",
        "honduras": "Honduras",
        "el salvador": "El Salvador",
        "united kingdom": "United Kingdom",
        "italy": "Italy",
        "mexico": "Mexico",
        "colombia": "Colombia",
        "united states": "United States of America",
        "australia": "Australia",
        "hungary": "Hungary",
        "france": "France",
        "portugal": "Portugal",
        "japan": "Japan",
        "kazakhstan": "Kazakhstan",
        "netherlands": "Netherlands",
        "israel": "Israel",
        "thailand": "Thailand",
        "sweden": "Sweden",
        "czech republic": "Czechia",
        "poland": "Poland",
        "andorra": "Andorra",
        "turkey": "Turkey",
        "aruba": "Aruba",
        "pakistan": "Pakistan",
        "jordan": "Jordan",
        "india": "India",
        "bahrain": "Bahrain",
        "anzania": "Tanzania",
        "angola": "Angola",
        "haiti": "Haiti",
        "mozambique": "Mozambique",
        "georgia": "Georgia",
        "taiwan province of china": "Taiwan, Province of China",
        "macedonia": "Macedonia",
        "latvia": "Latvia",
        "greece": "Greece",
        "bulgaria": "Bulgaria",
        "romania": "Romania",
        "french guiana": "French Guiana",
        "us virgin islands": "Virgin Islands, U.S.",
        "uriname": "Suriname",
        "serbia": "Serbia",
        "tunisia": "Tunisia",
        "cambodia": "Cambodia",
        "belize": "Belize",
        "finland": "Finland",
        "congo": "Congo",
        "albania": "Albania",
        "yanmar": "Myanmar",
        "cyprus": "Cyprus",
        "cayman islands": "Cayman Islands",
        "antigua and barbuda": "Antigua and Barbuda",
        "south korea": "South Korea",
        "saint lucia": "Saint Lucia"
    }

    if input in altas_conv_paises.keys():
        return altas_conv_paises[input]

    if input=='':
        input='X'

    return input

def detector_tag_producto(input_prod):
    #NDP
    cad_upper=input_prod.upper()
    if cad_upper.startswith("NDP"):
        return "NDP"
    if cad_upper.startswith("VDP"):
        return "VDP"
    if cad_upper.startswith("DTP"):
        return "DTP"

    return ""



def procesar_row(input,embed_model):
    nueva_row = {}
    nueva_row["Nombre2"]=''
    nueva_row["Nombre3"]=''
    nueva_row['flag_pais']=''

    for keydic in input.keys():
        key_nueva=keydic.strip()
        valor = input[keydic].strip()
        valor = urllib.parse.unquote_plus(valor)
        if "Nombre" == keydic:
            valor=procesar_cadena(valor)
            valor = valor.title()
            vtrozos=valor.split()
            if len(vtrozos)>1:
                if (len(vtrozos)>=2):
                    nueva_row["Nombre2"]=vtrozos[0]+" "+vtrozos[1]
                if (len(vtrozos) >= 3):
                    nueva_row["Nombre3"] = vtrozos[0] + " " + vtrozos[1]+" "+vtrozos[2]

        if "Email" == keydic:
            if valor.__contains__("@"):
                tmp=procesar_cadena(valor)
                tmp = tmp.lower().strip()
            else:
                tmp = ""
            valor = tmp
        # "UTM_Campaign",
        # "UTM_Medium",
        # "UTM_Source",
        # "UTM_Term",
        # "UTM_Content",
        # "Ref",
        # "Alta_webinar",
        # "Alta_lead",
        # "tag",
        # "last_UTM_Campaign",
        # "last_UTM_Medium",
        # "last_UTM_Source",
        # "last_UTM_Term",
        # "last_UTM_Content",
        # "telefono",
        # "last_ref",
        # "landing"

        if "pais" == keydic:
            valor = valor.replace("  ", " ")
            valor = conversor_paises(valor)
            if valor !='':
                nueva_row['flag_pais']='SI'
        # "avatar",
        # "landing"
        nueva_row[key_nueva] = valor

    nueva_row["tag_producto"] =detector_tag_producto(nueva_row['tag'])

    nueva_row['d_email']=clean_email(nueva_row['Email'])

    cadena_busqueda = " Nombre " + nueva_row['Nombre'] + " Email " + clean_email(nueva_row['Email']) + "  tag_producto " + nueva_row['tag_producto']

    ##if nueva_row['pais'] !='':
    ##    cadena_busqueda = cadena_busqueda + " Pais " + nueva_row['pais'] + ""


    nueva_row["cadena_busqueda"] = cadena_busqueda
    vector = emmbe_arr(embed_model,cadena_busqueda)
    nueva_row["embedding"] = vector
    vector= emmbe_arr(embed_model,nueva_row['Nombre'])
    nueva_row["embedding_nombre"]=vector


    return nueva_row


def read_file_altas(path_file, prefix, conection,embed_model):
    with open(path_file, newline='') as csvfile:

        reader = csv.DictReader(csvfile)
        sql_insert = lista_campos()
        lista_reg = []
        lotes=0
        for row in reader:
            nueva_row=procesar_row(row,embed_model)
            #print(nueva_row)
            tup = tupla_valores(nueva_row)
            #print(tup)
            lista_reg.append(tup)
            if (len(lista_reg) > 100):
                lotes=lotes+1
                print("lote f altas ")
                pgHandler.insert_list_records(conection, sql_insert, lista_reg)
                lista_reg = []
        if (len(lista_reg) > 0):
            pgHandler.insert_list_records(conection, sql_insert, lista_reg)
        



def lista_campos():
    cad_campos = """ insert into cruze_emb.raw_altas (
            "Nombre",
            "Email",    
            "d_email",
            "UTM_Campaign",    
            "UTM_Medium",
            "UTM_Source",
            "UTM_Term",    
            "UTM_Content",
            "Ref",
            "Alta_webinar",
            "Alta_lead",
            "tag",
            "tag_producto",
            "last_UTM_Campaign",    
            "last_UTM_Medium",
            "last_UTM_Source",
            "last_UTM_Term",
            "last_UTM_Content",
            "telefono",
            "last_ref",
            "pais",
            "flag_pais",
            "avatar",
            "landing",
            "cadena_busqueda" ,
            "Nombre2",
            "Nombre3",
            "embedding_nombre",
            "embedding" 
            ) 
             values (
            %s,
            %s,    
            %s,    
            %s,
            %s,
            %s,
            %s,    
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,    
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s );"""

    return cad_campos


def tupla_valores(row):
    tupla_val = (row['Nombre'],
                 row['Email'],
                 row['d_email'],
                 row['UTM_Campaign'],
                 row['UTM_Medium'],
                 row['UTM_Source'],
                 row['UTM_Term'],
                 row['UTM_Content'],
                 row['Ref'],
                 row['Alta_webinar'],
                 row['Alta_lead'],
                 row['tag'],
                 row['tag_producto'],
                 row['last_UTM_Campaign'],
                 row['last_UTM_Medium'],
                 row['last_UTM_Source'],
                 row['last_UTM_Term'],
                 row['last_UTM_Content'],
                 row['telefono'],
                 row['last_ref'],
                 row['pais'],
                 row['flag_pais'],
                 row['avatar'],
                 row['landing'],
                 row["cadena_busqueda"],
                 row["Nombre2"],
                 row["Nombre3"],
                 str(row["embedding_nombre"]),
                 str(row["embedding"])
                 )
    return tupla_val
