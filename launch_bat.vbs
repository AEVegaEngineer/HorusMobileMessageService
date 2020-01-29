Set WshShell = CreateObject("WScript.Shell") 
WshShell.Run chr(34) & "C:\Users\PC\source\repos\message-service\run_messenger.bat" & Chr(34), 0
Set WshShell = Nothing