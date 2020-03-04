#!/usr/bin/python3
import time
import json
import pymysql
import requests
import io
from datetime import datetime

# OBTENER LA CABECERA, LUEGO OBTENER TODOS LOS USERIDS QUE LE PEGUEN A ESA CABECERA Y ARMAR LA NOTIFICACION

def obtenerCabeceras():
	db = pymysql.connect("192.168.50.81","remoto","1xz9tv48","intranet" )
	cursor = db.cursor()
	cursor.execute("SELECT * FROM `app_notif_cabecera`")
	data = cursor.fetchall()
	db.close()
	return data

def mysqlConnect(cabecera):
	
	db = pymysql.connect("192.168.50.81","remoto","1xz9tv48","intranet" )
	cursor = db.cursor()
	consulta = "SELECT DISTINCT deviceid FROM `app_user_devices` as d join app_notif_cuerpo as c on d.user_id = c.fk_user_objetivo where c.estado = 0 and fk_cabecera = "+str(cabecera[0])
	#print ("************")
	#print (consulta)
	#print ("************")
	cursor.execute(consulta)

	data = cursor.fetchall()
	db.close()
	notifs = {}
	notifs["cabecera"]=cabecera
	notifs["objetivos"]=data

	return notifs

#@function: toda la lógica interna para crear la notificación per se en formato json
#@param criterio: criterio bajo el cual se buscará la información en base de datos
def generarJson(consulta):
	#print (consulta)
	resultado = {}
	resultado["app_id"] = "5bd931bc-a426-44ec-85a5-bfd47a771213"

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
	print()


def main():
	print ("Servicio de Mensajería Iniciado")
	minutos = 1
	tiempoEntreNotificaciones = 5
	
	while 1:
		cabeceras = obtenerCabeceras()
		for cabecera in cabeceras:
			consulta = mysqlConnect(cabecera)
			json = generarJson(consulta)
			#print(json)
			enviar(json)
			print("Esperando "+str(tiempoEntreNotificaciones)+" segundos para enviar la siguiente notificación...")
			time.sleep(tiempoEntreNotificaciones)
		
		print("Se buscarán nuevas notificaciones en "+str(minutos)+" minutos")
		time.sleep(minutos*60)

main()