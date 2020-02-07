#!/usr/bin/python3
import json
import pymysql
from datetime import datetime
# Open database connection
db = pymysql.connect("192.168.50.98","appusr","1xz9tv48","intranet" )

# prepare a cursor object using cursor() method
cursor = db.cursor()

# execute SQL query using execute() method.
# cursor.execute("SELECT * from usuario_ WHERE login = '10'")
cursor.execute("select a.user,a.asunto,a.mensaje from app_notif_cuerpo as a join ( select `user`, min(`id_notif_cuerpo`) as `id_notif_cuerpo` from `app_notif_cuerpo` group by `user` ) as b on a.user = b.user where a.id_notif_cuerpo = b.id_notif_cuerpo and a.fk_cabecera=1")
# Fetch a single row using fetchone() method.
data = cursor.fetchall()
db.close()

#print (data)

notificaciones = []
for elem in data:
	match = 0
	#primer elemento que inicializa el diccionario
	for notif in notificaciones:						
		if elem[1] == notif["title"] and elem[2] == notif["body"]:
			if elem[0] not in notif["user"]:
				notif["user"].append(elem[0])	
			match = 1			
			break
	if match == 0:
		match = 1
		notif = {}
		usuarios = []
		usuarios.append(elem[0])
		notif["user"] = usuarios
		notif["title"] = elem[1]
		notif["body"] = elem[2]
		notificaciones.append(notif)

#construir el diccionario resultante
time = datetime.now()
time = time.strftime("%d%m%Y%H%M%S")
name = "push"+time
json = []
for elem in notificaciones:
	notif = {}
	arr_notif = {}
	arr_notif["name"] = name
	arr_notif["title"] = elem["title"]
	arr_notif["body"] = elem["body"]
	notif["notification_content"] = arr_notif
	arr_notif_user_ids = elem["user"]
	notif["notification_target"] = arr_notif_user_ids
	json.append(notif)
print (json)

#{ "notification_content":{"name": name,"title": elem[1],"body": elem[2]},"notification_target": {"type": "user_ids_target","user_ids": ['+str(noti[0])+'] }}
#for noti in data:
#	time = datetime.now()
#	time = time.strftime("%d%m%Y%H%M%S")
#	name = "push"+time
#	salida =  '{ "notification_content":{"name": "'+name+'","title": "'+noti[1]+'","body": "'+noti[2]+'"},"notification_target": {"type": "user_ids_target","user_ids": ['+str(noti[0])+'] }}'
#	mydata = json.loads(salida)
#	print (json.dumps(mydata, indent=4))

# disconnect from server



#{
#  "notification_content": {
#    "name": "test21",
#    "title": "Liquidación",
#    "body": "Buen Día Dr(a)., usted tiene una nueva liquidación ",
#    "custom_data": {"key1": "val1", "key2": "val2"}
#  },
#  "notification_target": null
#}