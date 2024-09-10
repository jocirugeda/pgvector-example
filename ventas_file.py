
import csv

import AppBase.gestorSQL.gestorPG as pgHandler
import pandas as pd
import numpy as np

from AppBase.Utiles.caracteres import emmbe_arr,clean_email,procesar_cadena
def conversor_paises(input):
    altas_conv_paises = {
        "Mexico" :"Mexico",
        "United States" :"United States of America",
        "Spain" :"Spain",
        "Uruguay" :"Uruguay",
        "Chile" :"Chile",
        "Costa Rica" :"Costa Rica",
        "Peru" :"Peru",
        "Argentina" :"Argentina",
        "Germany" :"Germany",
        "Colombia" :"Colombia",
        "Ecuador" :"Ecuador",
        "Paraguay" :"Paraguay",
        "Australia" :"Australia",
        "Panama" :"Panama",
        "Bolivia" :"Bolivia",
        "Dominican Republic" :"Dominican Republic",
        "Guatemala" :"Guatemala",
        "Canada" :"Canada",
        "France" :"France",
        "Italy" :"Italy",
        "El Salvador" :"El Salvador",
        "Norway" :"Norway",
        "Puerto Rico" :"Puerto Rico",
        "United States Minor Outlying Islands" :"United States Minor Outlying Islands",
        "Switzerland" :"Switzerland",
        "Canary Islands" :"Spain",
        "Honduras" :"Honduras",
        "Netherlands" :"Netherlands",
        "Oman" :"Oman",
        "Cayman Islands" :"Cayman Islands",
        "Japan" :"Japan",
        "Indonesia" :"Indonesia",
        "Bahamas" :"Bahamas",
        "Brasil" :"Brasil",
        "México" :"Mexico",
        "United Kingdom" :"United Kingdom",
        "Panamá" :"Panama",
        "Estados Unidos" :"United States of America",
        "Perú" :"Peru",
        "Andorra" :"Andorra",
        "Grenada" :"Grenada",
        "Aruba" :"Aruba",
        "United Arab Emirates" :"United Arab Emirates",
        "Israel" :"Israel",
        "Venezuela" :"Venezuela",
        "Ireland" :"Ireland",
        "Luxembourg" :"Luxembourg",
        "Cote D'ivoire" :"Cote D'ivoire",
        "Belgium" :"Belgium",
        "Haiti" :"Haiti",
        "Malaysia" :"Malaysia",
        "Zambia" :"Zambia",
        "Portugal" :"Portugal",
        "Virgin Islands, U.S." :"Virgin Islands, U.S.",
        "EC" :"Ecuador",
        "US" :"United States of America",
        "ES" :"Spain",
        "MX" :"Mexico",
        "CL" :"Chile",
        "UY" :"Uruguay",
        "PE" :"Peru",
        "PA" :"Panama",
        "VE" :"Venezuela",
        "CO" :"Colombia",
        "IT" :"Italy",
        "GT" :"Guatemala",
        "CA" :"Canada",
        "NI" :"Nicaragua",
        "AR" :"Argentina",
        "BO" :"Bolivia",
        "AU" :"Australia",
        "DO" :"Dominican Republic",
        "PY" :"Paraguay",
        "PR" :"Puerto Rico",
        "CR" :"Costa Rica",
        "IE" :"Ireland",
        "GB" :"United Kingdom",
        "BE" :"Belgium"
    }

    if input in altas_conv_paises.keys():
        return altas_conv_paises[input]

    if input=='':
        input='*'

    return input


def detector_tag_producto(input_prod):
    # NDP
    cad_upper = input_prod.upper()
    cad_upper = cad_upper.replace("  ", " ")
    if cad_upper.startswith("NEGOCIO DE PODER"):
        return "NDP"
    if cad_upper.startswith("VENTAS DE PODER"):
        return "VDP"
    if cad_upper.startswith("DOMINA TU PSICOLOGÍA"):
        return "DTP"
    if cad_upper.startswith("DOMINA TU PSICOLOGIA"):
        return "DTP"

    return ""


def procesar_row(input,embed_model):
    nueva_row = {}
    nueva_row["Nombre2"] = ''
    nueva_row["Nombre3"] = ''
    for keydic in input.keys():
        valor = input[keydic].strip()

        if "Email_comprador" == keydic:
            if valor.__contains__("@"):
                tmp = procesar_cadena(valor)
                tmp = tmp.lower().strip()
            else:
                tmp = ''
            valor = tmp

        if "Nombre_comprador" == keydic:
            valor=procesar_cadena(valor)

        if "Producto" == keydic:
            valor = procesar_cadena(valor)

        if "Nombre_completo_comprador"== keydic:
            valor = procesar_cadena(valor)
            vtrozos = valor.split()
            if len(vtrozos) > 1:
                if (len(vtrozos) >= 2):
                    nueva_row["Nombre2"] = vtrozos[0] + " " + vtrozos[1]
                if (len(vtrozos) >= 3):
                    nueva_row["Nombre3"] = vtrozos[0] + " " + vtrozos[1] + " " + vtrozos[2]

        if "Pais"== keydic:
            valor=valor.replace("  "," ")
            valor =conversor_paises(valor)

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

        if "pais" == keydic:
            valor = conversor_paises(valor)
        # "avatar",
        # "landing"
        nueva_row[keydic] = valor

    nueva_row["tag_producto"] = detector_tag_producto(nueva_row['Producto'])
    nueva_row['d_email'] = clean_email(nueva_row['Email_comprador'])
    cadena_busqueda = " Nombre " + nueva_row['Nombre_completo_comprador'] + " Email " + clean_email(nueva_row['Email_comprador']) + "  tag_producto " + nueva_row['tag_producto']
    nueva_row["cadena_busqueda"]=cadena_busqueda

    ##if nueva_row['pais'] !='':
    ##    cadena_busqueda = cadena_busqueda + " Pais " + nueva_row['pais'] + ""

    vector = emmbe_arr(embed_model, cadena_busqueda)
    nueva_row["embedding"]=vector

    vector= emmbe_arr(embed_model,nueva_row['Nombre_completo_comprador'])
    nueva_row["embedding_nombre"]=vector


    return nueva_row

def read_file_ventas(path_file ,prefix ,conection,embed_model):

    with open(path_file, newline='') as csvfile:

        reader = csv.DictReader(csvfile)
        sql_insert = lista_campos()
        lista_reg =[]
        for row in reader:

            nueva_row =procesar_row(row,embed_model)
            #print(nueva_row)
            tup =tupla_valores(nueva_row,embed_model)
            #print(tup)
            lista_reg.append(tup)
            if (len(lista_reg ) >100):
                print("\n lote ventas")
                pgHandler.insert_list_records(conection ,sql_insert ,lista_reg)
                lista_reg =[]
        if(len(lista_reg ) >0):
            pgHandler.insert_list_records(conection, sql_insert, lista_reg)


def lista_campos():
    cad_campos =""" insert into cruze_emb.raw_ventas (
       "Insercion" ,
        "Fecha_compra",
        "Fecha_garantia" ,
        "Transaccion" ,
        "Producto" ,
        "tag_producto",
        "Email_comprador" ,
        "d_email",
        "estado" ,
        "Aff" ,
        "Aff_name" ,
        "currency" ,
        "off" ,
        "importe" ,
        "cms_hotmart" ,
        "cms_vendor" ,
        "cms_aff" ,
        "total" ,
        "Actualizado" ,
        "Nombre_comprador" ,
        "Nombre_completo_comprador" ,
        "Pais" ,
        "Sexo" ,
        "Probabilidad" ,
        "count_data" ,
        "Hora_local" ,
        "productOfferPaymentMode" ,
        "payment_type" ,
        "refusal_reason" ,
        "phone" ,
        "pasarela" ,
        "ch_mes_str" ,
        "ch_mes_N"  ,
        "ch_week_N" ,
        "cadena_busqueda" ,
        "Nombre2",
        "Nombre3",
        "embedding_nombre",
        "embedding" 
         ) values (
        %s ,
        %s,
        %s ,
        %s ,
        %s,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s ,
        %s,
        %s,      
        %s,
        %s,  
        %s,
        %s,
        %s )"""
    return cad_campos


def tupla_valores( row,embed_model):

    tupla_val= (
        row['Insercion'],
        row['Fecha_compra'],
        row['Fecha_garantia'],
        row['Transaccion'],
        row['Producto'],
        row['tag_producto'],
        row['Email_comprador'],
        row['d_email'],
        row['estado'],
        row['Aff'],
        row['Aff_name'],
        row['currency'],
        row['off'],
        row['importe'],
        row['cms_hotmart'],
        row['cms_vendor'],
        row['cms_aff'],
        row['total'],
        row['Actualizado'],
        row['Nombre_comprador'],
        row['Nombre_completo_comprador'],
        row['Pais'],
        row['Sexo'],
        row['Probabilidad'],
        row['count_data'],
        row['Hora_local'],
        row['productOfferPaymentMode'],
        row['payment_type'],
        row['refusal_reason'],
        row['phone'],
        row['pasarela'],
        row['ch_mes_str'],
        row['ch_mes_N'],
        row['ch_week_N'],
        row["cadena_busqueda"],
        row["Nombre2"],
        row["Nombre3"],
        str(row["embedding_nombre"]),
        str(row["embedding"])
        )
    print(tupla_val)
    print(len(tupla_val))
    return tupla_val
