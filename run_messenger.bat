title "Servicio de Notificaciones Moviles"
cls
Set Sleep=0
:start
if %Sleep% == 30 ( goto end )
Set /A Sleep+=1
echo %Sleep% ciclos ejecutados
C:\Users\PC\AppData\Local\Programs\Python\Python38\python.exe C:\Users\PC\source\repos\message-service\messenger.py
timeout 60
goto start
:end
pause
