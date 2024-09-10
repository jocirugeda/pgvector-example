

create table `coral-ndp.INPUT_LTV.SHEET_RAW_Ventas_ID` as (

SELECT distinct
 case when Hora_local like '%/%' and CHAR_LENGTH(Hora_local)=19 then  PARSE_DATETIME("%d/%m/%Y %T",Hora_local)
 when Hora_local like '%/%' and CHAR_LENGTH(Hora_local)=16 then  PARSE_DATETIME("%d/%m/%Y %H:%M",Hora_local)
 when Hora_local like '%/%' and CHAR_LENGTH(Hora_local)=17 then  PARSE_DATETIME("%d/%m/%Y %T",Hora_local)
 when Hora_local like '%/%' and CHAR_LENGTH(Hora_local)=18 then  PARSE_DATETIME("%d/%m/%Y %T",Hora_local)
 when Hora_local like'%-%-%' and CHAR_LENGTH(Hora_local)=24 then PARSE_DATETIME("%Y-%m-%dT%T",SUBSTRING(Hora_local,1,19))
 when Hora_local like'%-%-%' and CHAR_LENGTH(Hora_local)=20 then PARSE_DATETIME("%Y-%m-%dT%TZ",Hora_local)
 when Hora_local like'%-%-%' and CHAR_LENGTH(Hora_local)=16 then PARSE_DATETIME("%Y-%m-%d %H:%M",Hora_local)
 when Hora_local like'%-%-%' and CHAR_LENGTH(Hora_local)=15 then PARSE_DATETIME("%Y-%m-%d %H:%M",Hora_local)
 else null
end	as Insercion

,case when Fecha_compra like '%/%' and CHAR_LENGTH(Fecha_compra)=19 then  PARSE_DATETIME("%d/%m/%Y %T",Fecha_compra)
 when Fecha_compra like '%/%' and CHAR_LENGTH(Fecha_compra)=16 then  PARSE_DATETIME("%d/%m/%Y %H:%M",Fecha_compra)
 when Fecha_compra like '%/%' and CHAR_LENGTH(Fecha_compra)=17 then  PARSE_DATETIME("%d/%m/%Y %T",Fecha_compra)
 when Fecha_compra like '%/%' and CHAR_LENGTH(Fecha_compra)=18 then  PARSE_DATETIME("%d/%m/%Y %T",Fecha_compra)
 when Fecha_compra like'%-%-%' and CHAR_LENGTH(Fecha_compra)=24 then PARSE_DATETIME("%Y-%m-%dT%T",SUBSTRING(Fecha_compra,1,19))
 when Fecha_compra like'%-%-%' and CHAR_LENGTH(Fecha_compra)=20 then PARSE_DATETIME("%Y-%m-%dT%TZ",Fecha_compra)
 when Fecha_compra like'%-%-%' and CHAR_LENGTH(Fecha_compra)=16 then PARSE_DATETIME("%Y-%m-%d %H:%M",Fecha_compra)
 when Fecha_compra like'%-%-%' and CHAR_LENGTH(Fecha_compra)=15 then PARSE_DATETIME("%Y-%m-%d %H:%M",Fecha_compra)
 else null
end	as Fecha_compra

,case when Fecha_garantia like '%/%' and CHAR_LENGTH(Fecha_garantia)=19 then  PARSE_DATETIME("%d/%m/%Y %T",Fecha_garantia)
 when Fecha_garantia like '%/%' and CHAR_LENGTH(Fecha_garantia)=16 then  PARSE_DATETIME("%d/%m/%Y %H:%M",Fecha_garantia)
 when Fecha_garantia like '%/%' and CHAR_LENGTH(Fecha_garantia)=17 then  PARSE_DATETIME("%d/%m/%Y %T",Fecha_garantia)
 when Fecha_garantia like '%/%' and CHAR_LENGTH(Fecha_garantia)=18 then  PARSE_DATETIME("%d/%m/%Y %T",Fecha_garantia)
 when Fecha_garantia like'%-%-%' and CHAR_LENGTH(Fecha_garantia)=24 then PARSE_DATETIME("%Y-%m-%dT%T",SUBSTRING(Fecha_garantia,1,19))
 when Fecha_garantia like'%-%-%' and CHAR_LENGTH(Fecha_garantia)=20 then PARSE_DATETIME("%Y-%m-%dT%TZ",Fecha_garantia)
 when Fecha_garantia like'%-%-%' and CHAR_LENGTH(Fecha_garantia)=16 then PARSE_DATETIME("%Y-%m-%d %H:%M",Fecha_garantia)
 when Fecha_garantia like'%-%-%' and CHAR_LENGTH(Fecha_garantia)=15 then PARSE_DATETIME("%Y-%m-%d %H:%M",Fecha_garantia)
 else null
end	as Fecha_garantia


,Transaccion
,Producto
,lower(trim(Email_comprador)) as Email_comprador
,estado
,Aff
,Aff_name
,currency
,off
,importe
,cms_hotmart
,case when strpos(cms_vendor,'.')>0 and strpos(cms_vendor,',')>strpos(cms_vendor,'.') then cast (replace (replace(cms_vendor,'.',''),',','.') as NUMERIC)
else cast(cms_vendor as  NUMERIC )
end as cms_vendor
,cms_aff
,total

,case when Actualizado like '%/%' and CHAR_LENGTH(Actualizado)=19 then  PARSE_DATETIME("%d/%m/%Y %T",Actualizado)
 when Actualizado like '%/%' and CHAR_LENGTH(Actualizado)=16 then  PARSE_DATETIME("%d/%m/%Y %H:%M",Actualizado)
 when Actualizado like '%/%' and CHAR_LENGTH(Actualizado)=17 then  PARSE_DATETIME("%d/%m/%Y %T",Actualizado)
 when Actualizado like '%/%' and CHAR_LENGTH(Actualizado)=18 then  PARSE_DATETIME("%d/%m/%Y %T",Actualizado)
 when Actualizado like'%-%-%' and CHAR_LENGTH(Actualizado)=24 then PARSE_DATETIME("%Y-%m-%dT%T",SUBSTRING(Actualizado,1,19))
 when Actualizado like'%-%-%' and CHAR_LENGTH(Actualizado)=20 then PARSE_DATETIME("%Y-%m-%dT%TZ",Actualizado)
 when Actualizado like'%-%-%' and CHAR_LENGTH(Actualizado)=16 then PARSE_DATETIME("%Y-%m-%d %H:%M",Actualizado)
 when Actualizado like'%-%-%' and CHAR_LENGTH(Actualizado)=15 then PARSE_DATETIME("%Y-%m-%d %H:%M",Actualizado)
 else null
end	as Actualizado

,Nombre_comprador
,Nombre_completo_comprador
,Pais
,Sexo
,Probabilidad
,count_data

,case when Hora_local like '%/%' and CHAR_LENGTH(Hora_local)=19 then  PARSE_DATETIME("%d/%m/%Y %T",Hora_local)
 when Hora_local like '%/%' and CHAR_LENGTH(Hora_local)=16 then  PARSE_DATETIME("%d/%m/%Y %H:%M",Hora_local)
 when Hora_local like '%/%' and CHAR_LENGTH(Hora_local)=17 then  PARSE_DATETIME("%d/%m/%Y %T",Hora_local)
 when Hora_local like '%/%' and CHAR_LENGTH(Hora_local)=18 then  PARSE_DATETIME("%d/%m/%Y %T",Hora_local)
 when Hora_local like'%-%-%' and CHAR_LENGTH(Hora_local)=24 then PARSE_DATETIME("%Y-%m-%dT%T",SUBSTRING(Hora_local,1,19))
 when Hora_local like'%-%-%' and CHAR_LENGTH(Hora_local)=20 then PARSE_DATETIME("%Y-%m-%dT%TZ",Hora_local)
 when Hora_local like'%-%-%' and CHAR_LENGTH(Hora_local)=16 then PARSE_DATETIME("%Y-%m-%d %H:%M",Hora_local)
 when Hora_local like'%-%-%' and CHAR_LENGTH(Hora_local)=15 then PARSE_DATETIME("%Y-%m-%d %H:%M",Hora_local)
 else null
end	as Hora_local

,productOfferPaymentMode
,payment_type
,refusal_reason
,phone
,pasarela

 FROM `coral-ndp.INPUT_LTV.SHEET_RAW_Ventas`
 where estado is not null
 );


delete  FROM `coral-ndp.INPUT_LTV.SHEET_RAW_Ventas_ID`
where Transaccion in (
  SELECT Transaccion FROM `coral-ndp.CORAL_RAW_DATA.RAW_Ventas`
);


create table `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID` as (
select * from `coral-ndp.CORAL_RAW_DATA.RAW_Ventas`
);

alter table `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID` add column uuid_row STRING;

alter table `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID` alter column uuid_row set default GENERATE_UUID();

update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
   set uuid_row=GENERATE_UUID()
   where true;


insert into `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID` (Insercion
,Fecha_compra
,Fecha_garantia
,Transaccion
,Producto
,Email_comprador
,estado
,Aff
,Aff_name
,currency
,off
,importe
,cms_hotmart
,cms_vendor
,cms_aff
,total
,Actualizado
,Nombre_comprador
,Nombre_completo_comprador
,Pais
,Sexo
,Probabilidad
,count_data
,Hora_local
,productOfferPaymentMode
,payment_type
,refusal_reason
,phone
,pasarela
,ch_mes_str
,ch_mes_N
,ch_week_N
)

SELECT Insercion
,Fecha_compra
,Fecha_garantia
,Transaccion
,Producto
,Email_comprador
,estado
,Aff
,Aff_name
,currency
,off
,importe
,cms_hotmart
,cms_vendor
,cms_aff
,total
,Actualizado
,Nombre_comprador
,Nombre_completo_comprador
,Pais
,Sexo
,Probabilidad
,count_data
,Hora_local
,productOfferPaymentMode
,payment_type
,refusal_reason
,phone
,pasarela
,format_date("%Y-%m",Insercion)
, cast(format_date("%Y%m",Insercion) as INT64)
, (cast(format_date("%Y",Insercion) as INT64)*100)+Extract(week from Insercion)
FROM `coral-ndp.INPUT_LTV.SHEET_RAW_Ventas_ID`
;



update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
 set Producto=INITCAP(TRIM(REPLACE(Producto,'  ',' ')))
    ,Email_comprador=LOWER(REPLACE(Email_comprador,' ',''))
    ,Nombre_comprador=INITCAP(TRIM(REPLACE(Nombre_comprador,'  ',' ')))
    ,Nombre_completo_comprador=INITCAP(TRIM(REPLACE(Nombre_completo_comprador,'  ',' ')))
 where true;


       update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='United States of America'
        where Pais='"United States';


update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Spain'
        where Pais='Canary Islands';


update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Mexico'
        where Pais='México';


update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Panama'
        where Pais='Panamá';

update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='United States of America'
        where Pais='Estados Unidos';

        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Peru'
        where Pais='Perú';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Ecuador'
        where Pais='EC';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='United States of America'
        where Pais='US';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Spain'
        where Pais='ES';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Mexico'
        where Pais='MX';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Chile'
        where Pais='CL';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Uruguay'
        where Pais='UY';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Peru'
        where Pais='PE';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Panama'
        where Pais='PA';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Venezuela'
        where Pais='VE';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Colombia'
        where Pais='CO';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Italy'
        where Pais='IT';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Guatemala'
        where Pais='GT';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Canada'
        where Pais='CA';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Nicaragua'
        where Pais='NI';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Argentina'
        where Pais='AR';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Bolivia'
        where Pais='BO';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Australia'
        where Pais='AU';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Dominican Republic'
        where Pais='DO';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Paraguay'
        where Pais='PY';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Puerto Rico'
        where Pais='PR';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Costa Rica'
        where Pais='CR';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Ireland'
        where Pais='IE';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='United Kingdom'
        where Pais='GB';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Ventas_ID`
         set Pais='Belgium'
        where Pais='BE';


create table `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID` as (
select * from `coral-ndp.202205_NDP.202205_NDP_altas_UTMs`
);

CREATE TEMP FUNCTION cleanurl(x STRING)
RETURNS STRING
LANGUAGE js
AS r"""
  return decodeURI(x);
""";

update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
 set Nombre=INITCAP(TRIM(REPLACE(cleanurl(Nombre),'  ',' ')))
 ,Email=LOWER(REPLACE(cleanurl(Email),' ',''))
where true;


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Costa Rica'
        where pais='costa rica';

      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Panama'
        where pais='panama';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Guatemala'
        where pais='guatemala';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='China'
        where pais='china';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Canada'
        where pais='canada';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Morocco'
        where pais='morocco';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Puerto Rico'
        where pais='puerto rico';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Peru'
        where pais='peru';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Chile'
        where pais='chile';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Spain'
        where pais='spain';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Ecuador'
        where pais='ecuador';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Dominican Republic'
        where pais='dominican republic';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Germany'
        where pais='germany';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Brazil'
        where pais='brazil';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Cuba'
        where pais='cuba';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Belgium'
        where pais='belgium';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Trinidad and Tobago'
        where pais='trinidad and tobago';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Guyana'
        where pais='guyana';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Curaçao'
        where pais='curacao';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Switzerland'
        where pais='switzerland';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='United Arab Emirates'
        where pais='united arab emirates';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Bonaire, Sint Eustatius and Saba'
        where pais='bonaire/sint eustatius/saba';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Ireland'
        where pais='ireland';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Nicaragua'
        where pais='nicaragua';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Sri Lanka'
        where pais='sri lanka';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='New Zealand'
        where pais='new zealand';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Lebanon'
        where pais='lebanon';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Moldova'
        where pais='moldova';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Malaysia'
        where pais='malaysia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Norway'
        where pais='norway';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Montenegro'
        where pais='montenegro';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Iceland'
        where pais='iceland';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Slovenia'
        where pais='slovenia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Luxembourg'
        where pais='luxembourg';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Jamaica'
        where pais='jamaica';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Croatia'
        where pais='croatia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Indonesia'
        where pais='indonesia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Viet Nam'
        where pais='viet nam';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Equatorial Guinea'
        where pais='equatorial guinea';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Turks and Caicos Islands'
        where pais='turks and caicos islands';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Cabo Verde'
        where pais='cape verde';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Malta'
        where pais='malta';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Denmark'
        where pais='denmark';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Slovakia'
        where pais='slovakia (slovak republic)';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Sint Maarten'
        where pais='sint maarten';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Egypt'
        where pais='egypt';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Hong Kong'
        where pais='hong kong';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Saudi Arabia'
        where pais='saudi arabia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Virgin Islands (British)'
        where pais='british virgin islands';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Paraguay'
        where pais='paraguay';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Bolivia'
        where pais='bolivia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Argentina'
        where pais='argentina';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Uruguay'
        where pais='uruguay';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Austria'
        where pais='austria';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Venezuela'
        where pais='venezuela';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Honduras'
        where pais='honduras';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='El Salvador'
        where pais='el salvador';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='United Kingdom'
        where pais='united kingdom';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Italy'
        where pais='italy';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Mexico'
        where pais='mexico';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Colombia'
        where pais='colombia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='United States of America'
        where pais='united states';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Australia'
        where pais='australia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Hungary'
        where pais='hungary';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='France'
        where pais='france';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Portugal'
        where pais='portugal';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Japan'
        where pais='japan';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Kazakhstan'
        where pais='kazakhstan';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Netherlands'
        where pais='netherlands';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Israel'
        where pais='israel';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Thailand'
        where pais='thailand';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Sweden'
        where pais='sweden';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Czechia'
        where pais='czech republic';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Poland'
        where pais='poland';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Andorra'
        where pais='andorra';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Turkey'
        where pais='turkey';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Aruba'
        where pais='aruba';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Pakistan'
        where pais='pakistan';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Jordan'
        where pais='jordan';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='India'
        where pais='india';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Bahrain'
        where pais='bahrain';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Tanzania'
        where pais='anzania';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Angola'
        where pais='angola';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Haiti'
        where pais='haiti';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Mozambique'
        where pais='mozambique';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Georgia'
        where pais='georgia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Taiwan, Province of China'
        where pais='taiwan province of china';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Macedonia'
        where pais='macedonia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Latvia'
        where pais='latvia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Greece'
        where pais='greece';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Bulgaria'
        where pais='bulgaria';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Romania'
        where pais='romania';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='French Guiana'
        where pais='french guiana';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Virgin Islands, U.S.'
        where pais='us virgin islands';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Suriname'
        where pais='uriname';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Serbia'
        where pais='serbia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Tunisia'
        where pais='tunisia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Cambodia'
        where pais='cambodia';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Belize'
        where pais='belize';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Finland'
        where pais='finland';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Congo'
        where pais='congo';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Albania'
        where pais='albania';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Myanmar'
        where pais='yanmar';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Cyprus'
        where pais='cyprus';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Cayman Islands'
        where pais='cayman islands';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Antigua and Barbuda'
        where pais='antigua and barbuda';


      update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='South Korea'
        where pais='south korea';


        update `coral-ndp.CORAL_RAW_DATA.RAW_Altas_ID`
         set pais='Saint Lucia'
        where pais='saint lucia';
