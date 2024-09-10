select a.id,a."Nombre_comprador", a."Nombre_completo_comprador",a."Email_comprador",b."Nombre",b."Email" ,(a.embedding <-> b.embedding) as distancia

from
(SELECT t1.id, t1.tag_producto,t1.embedding, t1."Email_comprador", t1.estado, t1."Nombre_comprador", t1."Nombre_completo_comprador", "Pais", "Sexo", "Probabilidad", t1.count_data, t1."Hora_local", t1."productOfferPaymentMode",t1.ch_mes_str, t1."ch_mes_N", t1."ch_week_N"
	FROM cruze_emb.raw_ventas t1
	where t1.estado='completed'  and tag_producto<>'' and num_altas is null

    ) a

inner join
(
SELECT t2.id, t2."Nombre", t2."Email", t2."UTM_Campaign", t2."UTM_Medium", t2."UTM_Source",
 t2.tag, t2.tag_producto, t2.telefono,  t2.pais, t2.avatar,t2.num_ventas, t2.cadena_busqueda, t2.embedding
	FROM cruze_emb.raw_altas t2
	where t2.tag_producto<>''  and t2.num_ventas is null )b
 on ( a.tag_producto=b.tag_producto and b.pais=a."Pais")
 where (a.embedding <-> b.embedding)<0.25
 order by a.id,(a.embedding <-> b.embedding) asc