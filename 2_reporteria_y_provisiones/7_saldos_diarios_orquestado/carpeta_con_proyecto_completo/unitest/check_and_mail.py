# -*- coding: utf-8 -*-
"""
Created on Mon Oct  7 17:28:44 2019

@author: davochoa
"""

import os
import win32com.client
import json

#Cargar json de usuarios
def cargar_params():
    with open(dir_path+'/config/params.json') as f:
        parametros = json.load(f)
    #Extraer nombre prueba
    test_name=parametros["test_name"]
    #Extrae el dueño del proceso
    test_owner=parametros["test_owner"]
    #Generar cadena de usuarios
    users=""
    for user in parametros["mail_users"]:
        users = users+user+";"
    #retornar los valores
    return [users,test_name,test_owner]

#Entra al archivo y válida si hay errores
def test_decision(mensaje):
    if mensaje=="load_error":
        decision="load_error"
        line="Hay problemas con el log"
    elif mensaje.find("failures=")> 0 :
        decision = "failure"
        line=mensaje.split("\n")[len(mensaje.split("\n"))-2]
    elif mensaje.find("errors=")>0:
        decision = "error"
        line=mensaje.find("errors=")
        line=mensaje.split("\n")[len(mensaje.split("\n"))-2]
    elif mensaje.find("Error")>0:
        decision= "otro_error"
        line=mensaje.split("\n")[len(mensaje.split("\n"))-2]
    else:
        decision="ok"
        line="Pruebas Exitosas"
    return [decision,line]
    
#envíar correo
def send_email(file,log):
    """
    envía un email con el log de las pruebas
    """
    #Se abre la aplicación del correo
    outlook = win32com.client.Dispatch("Outlook.Application")
    #se abre la ventana para mandar el correo
    mail=outlook.CreateItem(0)
    #Usuarios
    mail.To=cargar_params()[0]
    #Encabezado
    mail.Subject=cargar_params()[1]+" - "+test_decision(log)[1]
    #Cuerpo del correo
    mensaje_general="\n\nEste es un mensaje automático, si detecta alguna inconsistencias comunicarse con  "+cargar_params()[2]
    body=""    
    if test_decision(log)[0]=="load_error":
        body="Cordial saludo, hay problemas con el log de la prueba, comuniquese con el responsable\n"+mensaje_general
    elif test_decision(log)[0]=="failure":
        body="Cordial saludo, las pruebas unitarias detectaron una falla en producción - "+test_decision(log)[1]+"\n"+mensaje_general
    elif test_decision(log)[0]=="error":
        body="Cordial saludo, las pruebas unitarias necesitan ser corregidas - "+test_decision(log)[1]+"\n"+mensaje_general
    elif test_decision(log)[0]=="otro_error":
        body="Cordial saludo, las pruebas unitarias no se ejecutaron exitosamente por algún problema externo - "+test_decision(log)[1]+"\n"+mensaje_general 
    else:
        body="Cordial saludo, las pruebas unitarias fueron exitosas \n"+mensaje_general
    mail.body=body
    try:
        mail.Attachments.Add(file)
    except Exception as exc:
        print( traceback.format_exc() )
        #Si hay un problema con el log que continue
        print(exc)
    mail.Send()

###############################################################################
#inicio del programa
if __name__=="__main__":
    try:
        #Encuentra el directorio actual
        dir_path =os.getcwd()
        #encuentra el último log
        actual_log=(max(os.listdir(dir_path+"/logs")))
        #path del último log
        log_path=dir_path+"/logs/"+actual_log
        #Contenido del log
        f=open(log_path,"r")
        contenido_log=f.read()
        f.close()
    except:
        contenido_log="load_error"
    #mandar el correo
    send_email(log_path,contenido_log)
















