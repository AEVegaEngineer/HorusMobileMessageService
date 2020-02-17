#!/usr/bin/python3
import time
import json
import pymysql
import requests
import io
from datetime import datetime

# Distintas consultas para distintas audiencias
consultasPersonales = "a.id_notif_cuerpo = b.id_notif_cuerpo and a.fk_cabecera=1"
consultasGenerales = "a.id_notif_cuerpo = b.id_notif_cuerpo and a.fk_cabecera=2"

def mysqlConnect(consulta):
	# Abre la conexión a base de datos
	db = pymysql.connect("192.168.50.98","appusr","1xz9tv48","intranet" )

	# prepara un objeto cursor usando el método cursor()
	cursor = db.cursor()

	# ejecuta un query SQL usando execute()
	cursor.execute("select a.user,a.asunto,a.mensaje from app_notif_cuerpo as a join ( select `user`, min(`id_notif_cuerpo`) as `id_notif_cuerpo` from `app_notif_cuerpo` where `fk_cabecera` = 2 group by `user` ) as b on a.user = b.user where "+consulta)

	data = cursor.fetchall()
	db.close()

	return data

# Diccionario que ordena apropiadamente las notificaciones segun usuarios
# para evitar enviar múltiples mensajes con el mismo contenido a distintos usuarios,
# en lugar de eso se envía una sola notificación con notification_target

#@function: crea un diccionario con la notif y la ingresa en el array de notifs
#@param notificaciones: array(string) donde se guardará esta notificación
#@param match: tinyint que evalúa si debería crear o no una notificación nueva
def crearNotif(notificaciones,elem):
	notif = {}
	usuarios = []
	usuarios.append(str(elem[0]))
	notif["user"] = usuarios
	notif["title"] = elem[1]
	notif["body"] = elem[2]
	notificaciones.append(notif)


#@function: toda la lógica interna para crear la notificación per se en formato json
#@param criterio: criterio bajo el cual se buscará la información en base de datos
def generarJson(criterio):
	data = []
	if criterio == "personal":
		data = mysqlConnect(consultasPersonales)
	
	elif criterio == "general":
		data = mysqlConnect(consultasGenerales)
	
	# función para obtener 
	notificaciones = []
	for elem in data:
		match = 0
		
		for notif in notificaciones:						
			if elem[1] == notif["title"] and elem[2] == notif["body"] and len(notif["user"]) < 99:
				if elem[0] not in notif["user"]:
					notif["user"].append(str(elem[0]))
					match = 1			
					break
			if len(notif["user"]) >= 99:
				crearNotif(notificaciones,elem)
		if len(notificaciones)<1 or match == 0:
			crearNotif(notificaciones,elem)
			match = 1
		

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

		arr_notif_type = "user_ids_target"
		arr_notif_user_ids = elem["user"]
		arr_notification_target = {}
		arr_notification_target["type"] = arr_notif_type
		arr_notification_target["user_ids"] = arr_notif_user_ids
		
		if criterio == "personal":
			notif["notification_target"] = arr_notification_target
	
		elif criterio == "general":
			notif["notification_target"] = None
		
		json.append(notif)

	#array json contiene notificaciones pero deben enviarse individualmente
	return json

def enviar(data):
	data_dumpeada = json.dumps(data)
	url = 'https://appcenter.ms/api/v0.1/apps/Sulfur0/HorusMobile/push/notifications'
	cabecera = {}
	cabecera["X-API-Token"] = 'dc2fd84cfeff71d6bc9b7c1157d8d58645a268d1'
	cabecera["Content-Type"] = 'application/json'
	x = requests.post(url=url, data = data_dumpeada, headers = cabecera);
	print("Enviado el push "+data['notification_content']['name']+ " con resultado:")
	print(x)

def main():
	minutos = 60
	critarr = ["general"]
	while 1:
		arr = []
		for crit in critarr:			
			arr = generarJson(crit)
		
			# envía cada objeto notificación guardado en el array arr
			for data in arr:
				print("Enviando payload "+crit)
				enviar(data)
				#print(data)
				time.sleep(10)
				
		print("Se buscará nuevas notificaciones en "+str(minutos)+" minutos")
		time.sleep(minutos*60)

main()