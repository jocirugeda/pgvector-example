


CREATE TABLE DEV_RAW_DATA.RAW_Altas_ID(

Nombre 	STRING

,Email 	STRING

,PR_Email STRING

,UTM_Campaign 	STRING

,UTM_Medium 	STRING

,UTM_Source 	STRING

,UTM_Term  	STRING

,UTM_Content  STRING

,Ref  	STRING

,Alta_webinar 	DATE

,Alta_lead 	DATE

,tag 	STRING

,last_UTM_Campaign  STRING

,last_UTM_Medium  	STRING

,last_UTM_Source  	STRING

,last_UTM_Term  	STRING

,last_UTM_Content  	STRING

,telefono 	STRING

,last_ref 	STRING

,pais 	STRING

,COD_pais  STRING

,avatar 	STRING

,landing 	STRING

,ch_mes_str 	STRING
,ch_mes_N 	INTEGER
,ch_week_N  	INTEGER

,Insercion DATETIME
);



create table DEV_RAW_DATA.RAW_Ventas_ID (
Insercion 	DATETIME

,Fecha_compra 	DATETIME

,Fecha_garantia 	DATETIME

,Transaccion STRING

,Producto 	STRING

,Email_comprador  STRING

,PR_Email_comprador  STRING
,estado 	STRING

,Aff  STRING

,Aff_name STRING

,currency STRING

,off STRING

,importe 	STRING

,cms_hotmart STRING

,cms_vendor 	NUMERIC

,cms_aff  STRING

,total 	STRING

,Actualizado 	DATETIME

,Nombre_comprador 	STRING

,Nombre_completo_comprador 	STRING

,Pais 	STRING

,COD_Pais 	STRING

,Sexo 	STRING

,Probabilidad STRING

,count_data 	STRING

,Hora_local 	DATETIME

,productOfferPaymentMode STRING

,payment_type 	STRING

,refusal_reason 	STRING

,phone 	STRING

,pasarela 	STRING

,ch_mes_str 	STRING

,ch_mes_N  	INTEGER

,ch_week_N 	INTEGER
 );



create or replace view `smartpro-396117.DEV_RAW_DATA.last_tag` as (

SELECT distinct tag as tag
FROM `smartpro-396117.DEV_RAW_DATA.RAW_Altas_ID`
where Alta_lead=(
SELECT max(Alta_lead) as f_tope
FROM `smartpro-396117.DEV_RAW_DATA.RAW_Altas_ID`
)
);


create or replace view `smartpro-396117.DEV_RAW_DATA.repes_altas` as (
select distinct aa.lead_email
from
(select distinct trim(lower(Email)) as lead_email, *
from `smartpro-396117.DEV_RAW_DATA.RAW_Altas_ID`
where tag in (select tag from `smartpro-396117.DEV_RAW_DATA.last_tag`) ) aa

inner join
(select distinct trim(lower(Email)) as lead_email
from `smartpro-396117.DEV_RAW_DATA.RAW_Altas_ID`
where tag not in ( select tag from `smartpro-396117.DEV_RAW_DATA.last_tag`))  bb on ( aa.lead_email=bb.lead_email)

);


create or replace view `smartpro-396117.DEV_RAW_DATA.repes_n_ventas_utm` as(

select aa.lead_email,aa.UTM_Campaign,aa.UTM_Medium,aa.UTM_Source,aa.UTM_Term,aa.UTM_Content,aa.landing,aa.num_altas, case when bb.email_comprador is null then 0 else bb.n_ventas end as num_ventas
from
(
select  trim(lower(Email)) as lead_email,UTM_Campaign,UTM_Medium,UTM_Source,UTM_Term,UTM_Content,landing, count(*) as num_altas
from `smartpro-396117.DEV_RAW_DATA.RAW_Altas_ID`
where tag in (select tag from `smartpro-396117.DEV_RAW_DATA.last_tag`) and trim(lower(Email)) in ( select lead_email from `smartpro-396117.DEV_RAW_DATA.repes_altas`)
group by trim(lower(Email)),UTM_Campaign,UTM_Medium,UTM_Source,UTM_Term,UTM_Content,landing
)aa

left join
(
select trim(lower(Email_comprador)) as email_comprador,count(*) as n_ventas
from `smartpro-396117.DEV_RAW_DATA.RAW_Ventas_ID`
where estado in ('completed','approved')
and cms_vendor>0
group by trim(lower(Email_comprador))
)bb on (bb.email_comprador=aa.lead_email)

);

