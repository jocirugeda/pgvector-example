# pgvector-example
Prueba de concepto Python Pgvector

Se ha hecho una prueba de concepto , para intentar asociar las ventas provenientes de una plataforma de venta de infoproductos (Hotmart) , con las altas de leads en CRM (Hubspot) en las campañas de marketing, para aquellas ventas que no tengan asociado un alta , para poder imputar un origen de lata como leads a cada comprador (Facebook Ads, Google Ads, etc)

Por medio de librerías Python , se formaron cadenas en base a la información de cada venta , y con la información de cada alta . Posteriormente de cada una de dichas cadenas se calcularon los embeddings de tamaño 512 , y se guardaron en la base de datos postgres con la extensión pgvector .

Por medio de consultas se pudieron asociar a cada venta sin alta , las altas más próximas , por medio de consultas contra la base de datos y usando las métricas de vectores proporcionada por la extensión pgvector.

La base del proyecto son dos CSV , uno con altas de leads con aprox 600.000 filas , y otro de ventas 23614 filas , el objetivo del programa es emparejar las ventas con aquellos registros de leads mas proximos semanticamente por medio 
de los embeddings que se calculan asociados a los registros de altas y de ventas 

Para poder trabjar con los datos se han cargado los datos en tablas de postgres con la extension pgvector , y se han añadido un campo con la cadena resumen para hacer el embedding , y otra con el vector de embeddings 

al hacer la carga de los CSV , se han calculado las cadenas de resumen y los embeddings correspondientes , quedando posteriormente las tablas lista para hacer consultas para ver los emparejamientos obtenidos mediante el uso de las metricas
asociadas a los vectores.

Los requisitos para ejecutar :
 postgres con la extension pgvector https://github.com/pgvector/pgvector   , en esta url se explica como instalarlo en Windows y linux
 python y un entorno de ejecucion con GPU , para el calculo de los embeddings ,  cambiando el motor de  embeddings , se puede usar uno  que no requiera GPU 

