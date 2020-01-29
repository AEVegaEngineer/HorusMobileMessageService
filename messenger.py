import json
import requests
import io
from datetime import datetime
titulo = io.open("C:/Users/PC/source/repos/message-service/estructura/titulo1.txt", encoding='utf-8')
cuerpo = io.open("C:/Users/PC/source/repos/message-service/estructura/cuerpo1.txt", encoding='utf-8')

time = datetime.now()
time = time.strftime("%d%m%Y%H%M%S")
name = "push"+time

title = titulo.read()

body = cuerpo.read()
notification_target = None
data = {
  "notification_content": {
    "name": name,
    "title": title,
    "body": body
  },
  "notification_target": notification_target
}
data_dumpeada = json.dumps(data)

url = 'https://appcenter.ms/api/v0.1/apps/Sulfur0/HorusMobile/push/notifications'
cabecera = {}
cabecera["X-API-Token"] = 'dc2fd84cfeff71d6bc9b7c1157d8d58645a268d1'
cabecera["Content-Type"] = 'application/json'
x = requests.post(url=url, data = data_dumpeada, headers = cabecera);
print("Enviado el push "+name+ " con resultado:")
print(x)