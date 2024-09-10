
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
FROM `coral-ndp.INPUT_LTV.SHEET_Ventas_ID`




