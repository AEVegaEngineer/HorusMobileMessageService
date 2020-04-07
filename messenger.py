#!/usr/bin/python3
import time
import json
import pymysql
import requests
import io
from datetime import datetime

#@function: función utilizada para enviar una consulta a base de datos y retornar una respuesta
#@param string consulta: sentencia sql que se ejecutará en base de datos
#@return: data que se retorna como resultado
def ejecutarConsulta(consulta):
	db = pymysql.connect("192.168.50.81","remoto","1xz9tv48","intranet" )
	cursor = db.cursor()
	cursor.execute(consulta)
	# capturo si la sentencia que se ejecuta es un update	
	if consulta[0] == "u":
		db.commit()
		data = None
	else:
		data = cursor.fetchall()	
	db.close()
	return data


def construir_mensajes():

	#obtiene cuerpos y cabeceras
	consulta = "SELECT * FROM `app_notif_cabecera` as cab join app_notif_cuerpo as cue on cab.id_cabecera = cue.fk_cabecera where cue.estado = 0 and (cue.f_hs_ultimo_envio + INTERVAL 1 DAY <= now() or cue.f_hs_ultimo_envio is null or cue.f_hs_ultimo_envio = '0000-00-00 00:00:00') and EXISTS(select * from app_user_devices where user_id = cue.fk_user_objetivo) ORDER BY cab.id_cabecera" 
	data = ejecutarConsulta(consulta)

	#ejecuta la actualización del último envío
	if data == ():
		return "sin_mensajes"
	cadena = ""	
	resultado = []
	for cuerpo in data:
		#print(cuerpo)
		if len(cadena) > 0:
			cadena+=","
		else:
			cadena+="("
		cadena+=str(cuerpo[5])
		contenido = reemplazarVariables(cuerpo[2], str(cuerpo[5]))
		objetivos = buscarObjetivos(cuerpo[7])
		titulo = str(cuerpo[1])
		resultado.append(generarJson(objetivos, titulo, contenido))
	cadena+=")"
	t = datetime.now()
	t = time.strftime("%Y-%m-%d %H:%M:%S")
	actualizacion = "update app_notif_cuerpo set f_hs_ultimo_envio = now() where id_cuerpo in "+cadena
	#print (actualizacion)
	ejecutarConsulta(actualizacion)	
	return resultado

#@function: toda la lógica interna para crear la notificación per se en formato json
#@param criterio: criterio bajo el cual se buscará la información en base de datos
def generarJson(objetivos, titulo, contenido):
	resultado = {}
	resultado["app_id"] = "5bd931bc-a426-44ec-85a5-bfd47a771213"
	#print(consulta['objetivos'])
	players = []
	for target in objetivos:
		players.append(target[0])
	resultado["include_player_ids"] = players

	title = {}
	title["en"] = titulo
	resultado["headings"] = title

	text = {}
	text["en"] = contenido
	resultado["contents"] = text
	#print(resultado)
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

def buscarObjetivos(id_usuario):
	consulta = "SELECT deviceid FROM `app_user_devices` where user_id = "+str(id_usuario)
	data = ejecutarConsulta(consulta)
	return data

def reemplazarVariables(mensaje,foranea_cuerpo):
	# busco las variables y las agrego al mensaje si aplica
	consultavariables = "SELECT * FROM app_notif_relleno where fk_cuerpo = "+foranea_cuerpo+" ORDER BY id"
	variables = ejecutarConsulta(consultavariables)
	texto = mensaje
	if variables != ():
		for variable in variables:
			texto = texto.replace("$VAR",variable[2],1)
	return texto

def main():
	print ("Servicio de Mensajería Iniciado")
	minutos = 2
	while 1:
		tiempo = datetime.now()
		tiempo = time.strftime("%d/%m/%Y %H:%M:%S")
		
		mensajes = construir_mensajes()
		if mensajes == "sin_mensajes":
			print (tiempo+" No hay notificaciones por enviar")
		else:
			for mensaje in mensajes:
				print(tiempo)
				enviar(mensaje)
		print(tiempo+" Se buscarán nuevas notificaciones en "+str(minutos)+" minutos")
		time.sleep(minutos*60)	
main()