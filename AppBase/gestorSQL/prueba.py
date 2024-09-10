

column_names=['post','comment','name']

row=('ksksk',12,'ccccc')


print(column_names[0])

salida={}
contador=-1
for d in row:
    contador=contador+1
    print(d)
    salida[column_names[contador]]=d


print(salida)