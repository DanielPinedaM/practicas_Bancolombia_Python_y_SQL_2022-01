# -*- coding: utf-8 -*-
"""
┌─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬┐▄
├─┼─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴┤█
├─┤ ╔════════════════════════════╗ │█
├─┤ ║┌─┐┬┌─┐┬  ┌─┐┌─┐  ╔═╗╔═╗ ╦  ║ │█
├─┤ ║│  ││  │  │ │└─┐  ╚═╗║═╬╗║  ║ │█
├─┤ ║└─┘┴└─┘┴─┘└─┘└─┘  ╚═╝╚═╝╚╩═╝║ │█
├─┤ ╚════════════════════════════╝ │█
└─┴────────────────────────────────┘█
▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
Gerencia de Información Riesgo de Crédito PyP – VP Riesgos
"""
#%%
# =============================================================================
    # Se debe ingresar la ruta donde se encuentran los archvivos con la Consulta SQL y la variables
# =============================================================================
delimit=";" #<------------CAMBIAR DE SER NECESARIO ENTRE (; y ,)        
macro_variables = r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\1_RODAMIENTOS DIARIOS\PROCESO COMPRIMIDO\RODAMIENTO DIARIO\macro_variables.xlsx' #'Y:\\2.RODAMIENTO DE CARTERA\\EJECUCIONES RODAMIENTOS\\RODAMIENTO DIARIO\\macro_variables.xlsx'
orden_consultas_sql =r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\1_RODAMIENTOS DIARIOS\PROCESO COMPRIMIDO\RODAMIENTO DIARIO\orden.txt'    #'Y:\\2.RODAMIENTO DE CARTERA\\EJECUCIONES RODAMIENTOS\\RODAMIENTO DIARIO\\orden.txt'
archivo_consultas_sql = r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\1_RODAMIENTOS DIARIOS\PROCESO COMPRIMIDO\RODAMIENTO DIARIO\\'          #'Y:\\2.RODAMIENTO DE CARTERA\\EJECUCIONES RODAMIENTOS\\RODAMIENTO DIARIO\\'
nombre_archivo_consultas_sql = ''

#%%
# =============================================================================
    # Se llaman las librerias que se usaran para el proceso
# =============================================================================
import pandas as pd
import pyodbc
from importlib import reload
import time
from time import sleep

#%%
# =============================================================================
    # Se inicia la conexión con Impala
# =============================================================================
def conexionLZ():
    #print("--- Conectando a la LZ ---")
    reload( pyodbc )
    connLZ = pyodbc.connect( "DSN=IMPALA_PROD", autocommit = True  )
    #print("--- Conexión IMPALA establecida ---" + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
    return connLZ

#%%
# =============================================================================
    # Excepción de errores para garantizar la ejecución
# =============================================================================
ERRORES_PYODBC_PASABLES = ["Invalid query handle", "General error: SSL_read"]

def error_pyodbc_pasable(error):
    for error_pasable in ERRORES_PYODBC_PASABLES:
        if error_pasable in str(error):
            return True
    return False

#%%
# =============================================================================
    # Función de lectura de los archivos que contienen los parametros iniciales
# =============================================================================
def parametros_iniciales(macro_variables,orden_consultas_sql,nombre_archivo_consultas_sql):
    
    if macro_variables == '':
        renglon_variables = ['Unica Ejecución','1']
    else:
        archivo_variables = pd.ExcelFile(macro_variables)
        renglon_variables = archivo_variables.parse(archivo_variables.sheet_names[0])
        archivo_variables.close()
    
    if orden_consultas_sql == '':
        orden = [nombre_archivo_consultas_sql]
    else:
        archivo_orden = open(orden_consultas_sql, "r")
        texto_orden = archivo_orden.read()
        archivo_orden.close()
    
        orden= texto_orden.split("\n")
        
    encabezado = renglon_variables.columns
               
    for y in range(0,len(orden)):
        if orden[y]=="":
            del orden[y]
    
    return(renglon_variables,orden,encabezado)

#%%
# =============================================================================
    # Función de la lectura y estructuración de la consulta SQL
# =============================================================================
def lectura_consulta_sql(archivo_consultas_sql,variable,orden,encabezado,b):

    archivo_SQL = open(archivo_consultas_sql + orden[b], encoding="utf8")
    print("El archivo se abrió")
    
    SQL = archivo_SQL.read()
    SQL = SQL.replace("<0x90>","")
    print("El archivo se ha leído")
    
    archivo_SQL.close()
    print("El archivo se ha cerrado")
    
    for r in range(0,len(variable)):
        SQL = SQL.replace(encabezado[r],str(variable[r]))
    sentencias = SQL.split(";")
    
    return (sentencias)

#%%
# =============================================================================
    # Función para ejecutar la consulta SQL en Impala
# =============================================================================
def ejecutar_consulta_sql(consulta_sql):
    
    connLZ = conexionLZ()
    cursor = connLZ.cursor()
    
    ejecucion_exitosa = False
    contador = 0 
    while not ejecucion_exitosa:
        try:
            cursor.execute(consulta_sql)
            ejecucion_exitosa = True
            print("Ejecución exitosa")                    
        except pyodbc.Error as e:
            if error_pyodbc_pasable(e):
                contador += 1
                print("**Intento de ejecución #"+str(contador))
                sleep(2)
            else:
                print("--Error, no es posible ejecutar\n" + str(e))
                break
    
    cursor.close()
    connLZ.close()
    
#%%
def procesar_sql():
    
    tiempoInicio = time.time()
    
    renglon_variables,orden,encabezado = parametros_iniciales(macro_variables,orden_consultas_sql,nombre_archivo_consultas_sql)
    
    porc=0.0

    if (len(renglon_variables) + 1) > 1 and len(orden) > 0 and len(encabezado) > 0:
        #Loop por cantidad de variables
        try:
            for a in range(0,len(renglon_variables)):
                variable = renglon_variables.loc[a] #Mostrar la Información en pantalla
                
                #print("▄▄╔═════════════════════════════════════════════╗")
                #print("█═╣Iniciando Ciclo con los Siguientes Parametros║")
                #print("█═╩═════════════════════════════════════════════╝")
                
                for v in range(0,len(variable)):
                    print("█ "+encabezado[v]+" :"+str(variable[v]))
                    
                #print("█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄\n")
                
                for b in range(0,len(orden)):
                    print("Leyendo Archivo: " + orden[b])
                    
                    sentencias = lectura_consulta_sql(archivo_consultas_sql,variable,orden,encabezado,b)
                    
                    for c in range(0,len(sentencias)):
                        
                        print("Ciclo         :" + str(a + 1) + " de " + str(len(renglon_variables)))
                        print("Archivo-SQL   :" + str(b + 1) + " de " + str(len(orden)))
                        print("Sentencia-SQL :" + str(c + 1) + " de " + str(len(sentencias)))
                        
                        if sentencias[c].strip() == "":
                            print("**Consulta SQL Vacía")
                        else:
                            ejecutar_consulta_sql(sentencias[c])
                        #Información
                        porc += 1/((len(renglon_variables))*len(orden)*len(sentencias))
                        print("Segmento Ejecutado\n▀▄▀▄▀▄▀▄PROGRESO--->"+str(round(porc*100,2))+"%▀▄▀▄▀▄▀▄\n")
                        
            print("\n--- El tiempo de ejecución fue: %s minutos---" % round((time.time() - tiempoInicio)/60))
                        
            # print("▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄")
            # print("█    ┌─┐┬─┐┌─┐┌─┐┌─┐┌─┐┌─┐     █")
            # print("█    ├─┘├┬┘│ ││  ├┤ └─┐│ │     █")
            # print("█    ┴  ┴└─└─┘└─┘└─┘└─┘└─┘     █")
            # print("█┌─┐┌─┐┌┬┐┌─┐┬  ┌─┐┌┬┐┌─┐┌┬┐┌─┐█")
            # print("█│  │ ││││├─┘│  ├┤  │ ├─┤ │││ │█")
            # print("█└─┘└─┘┴ ┴┴  ┴─┘└─┘ ┴ ┴ ┴─┴┘└─┘█")
            # print("▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀")
        except Exception as e:
            print("ha ocurrido un error")
            print(e)
            # print("\nHa ocurrido un")        
            # print("┌─┐┬─┐┬─┐┌─┐┬─┐")
            # print("├┤ ├┬┘├┬┘│ │├┬┘")
            # print("└─┘┴└─┴└─└─┘┴└─")
    else:
        print("Problema de Lectura de los archivos")
        
#%%
procesar_sql()
