#!/usr/bin/python3
import time
import json
import pymysql
import requests
import io
from datetime import datetime

# OBTENER LA CABECERA, LUEGO OBTENER TODOS LOS USERIDS QUE LE PEGUEN A ESA CABECERA Y ARMAR LA NOTIFICACION
def mysqlConnect(consulta):
	db = pymysql.connect("192.168.50.81","remoto","1xz9tv48","intranet" )
	cursor = db.cursor()
	cursor.execute(consulta)
	data = cursor.fetchall()
	db.close()
	return data

def obtenerCabeceras():
	# SELECCIONA UNA SOLA CABECERA, LUEGO SE SELECCIONAN VARIOS USUARIOS PARA ESA CABECERA
	consulta = "SELECT DISTINCT cab.* FROM `app_notif_cabecera` as cab join app_notif_cuerpo as cue on cab.id_cabecera = cue.fk_cabecera"# where cue.estado = 0
	data = mysqlConnect(consulta)	
	return data

def getCabeceras(cabecera):	

	consulta = "SELECT d.deviceid FROM `app_user_devices` as d join app_notif_cuerpo as c on d.user_id = c.fk_user_objetivo where c.estado = 0 and fk_cabecera = "+str(cabecera[0])+" and (c.f_hs_ultimo_envio + INTERVAL 1 DAY <= now() or c.f_hs_ultimo_envio is null or c.f_hs_ultimo_envio = '0000-00-00 00:00:00')"
	data = mysqlConnect(consulta)	

	actualizacion = "update app_notif_cuerpo set f_hs_ultimo_envio = now() where estado = 0 and fk_cabecera = "+str(cabecera[0])+" and (f_hs_ultimo_envio + INTERVAL 1 DAY <= now() or f_hs_ultimo_envio is null or f_hs_ultimo_envio = '0000-00-00 00:00:00')"
	mysqlConnect(actualizacion)

	notifs = {}
	if data == ():
		return "empty_notif"
	notifs["cabecera"]=cabecera
	
	objetivos = []
	for target in data:
		objetivos.append(target)
	notifs["objetivos"] = objetivos
	
	return notifs

#@function: toda la lógica interna para crear la notificación per se en formato json
#@param criterio: criterio bajo el cual se buscará la información en base de datos
def generarJson(consulta):
	resultado = {}
	resultado["app_id"] = "5bd931bc-a426-44ec-85a5-bfd47a771213"
	#print(consulta['objetivos'])
	players = []
	for target in consulta['objetivos']:
		players.append(target[0])
	resultado["include_player_ids"] = players

	titulo = {}
	titulo["en"] = consulta["cabecera"][1]
	resultado["headings"] = titulo

	contenido = {}
	text = {}
	text["en"] = consulta["cabecera"][2]
	resultado["contents"] = text
	#print ("**********resultado***********")
	#print (resultado)
	return resultado

def enviar(data):

	data_dumpeada = json.dumps(data)
	url = 'https://onesignal.com/api/v1/notifications'
	cabecera = {}
	cabecera["Authorization"] = 'YTJiMWM1MmQtNWQxZi00OGVhLWI5MjYtYjI5MWY4N2ViZDAw'
	cabecera["Content-Type"] = 'application/json'
	print(data_dumpeada)
	print("Enviado push con resultado:")
	x = requests.post(url=url, data = data_dumpeada, headers = cabecera);	
	print(x)
	# MARCA LA HORA DEL ENVÍO DE LA NOTIFICACIÓN
	print()


def main():

	print ("Servicio de Mensajería Iniciado")
	minutos = 2
	tiempoEntreNotificaciones = 5
	
	while 1:
		tiempo = datetime.now()
		tiempo = time.strftime("%d/%m/%Y %H:%M:%S")
		cabeceras = obtenerCabeceras()
		for cabecera in cabeceras:
			#print ("Obteniendo todas las notificaciones de "+cabecera[1])
			consulta = getCabeceras(cabecera)
			#print (consulta)
			if consulta == "empty_notif":
				continue
			json = generarJson(consulta)
			#print(json)
			tiempo = datetime.now()
			tiempo = time.strftime("%d/%m/%Y %H:%M:%S")
			print (tiempo+" notificación generada")
			enviar(json)
			print("Esperando "+str(tiempoEntreNotificaciones)+" segundos para enviar la siguiente notificación...")
			time.sleep(tiempoEntreNotificaciones)
		
		print(tiempo+" Se buscarán nuevas notificaciones en "+str(minutos)+" minutos")
		time.sleep(minutos*60)
	
main()
