"""
Proyecto - automatizacion FNG


1) Todo esto se hace en Python

2) Las consultas SQL las ejecutas desde el Python

3) Leer los Excel q estan en la carpeta ...\1_insumo, se lee la hoja llamda "Base"

4) Ejecutar consultas SQL q estan en "consultas_llenar_campos.sql"  

5) Reemplazar en la consulta_4 los siguientes valores q estan en "catalogo_moras_experian.xlsx":

consulta_4 - calificacion_externa	Descripcion Mora
0	                                EL MES NO EXISTE
1	                                MORA 30 DIAS
2	                                MORA 60 DIAS
3	                                MORA 90 DIAS
4	                                MORA 120 DIAS
5	                                AL DIA
6	                                N/A
7	                                CADUCIDAD
8	                                N/A
9	                                SIN INFORMACION
10	                                MORA 150 DIAS
11	                                MORA 180 DIAS
12	                                MORA 210+ DIAS
13	                                DUDOSO RECAUDO
14	                                CARTERA CASTIGADA
15	                                N/A

6) Crear Excel en la carpeta ...\2_respuesta con las nuevas columnas q contengan los resultados de las consultas de "consultas_llenar_campos.sql"
"""

########################################################################
#%%

""" 
Guardar nombre de usuario corporativo en una variable
y clave en otra variable
https://www.aluracursos.com/blog/la-diferencia-entre-las-funciones-input-y-raw-input-en-python
"""
# Imprimir espacio en blanco
print("\n")
print("\n")

print("Cierre TODOS los Excel de insumos y tambien cierre el Excel 3_macro_variables.xlsx antes de ejecutar el codigo")
print("Escriba los siguientes datos y despues presione la tecla enter...")
print("Si te equivocas al escribir tus datos o no cierras los Excel entonces el codigo falla, en caso de q eso suceda vuelvelo a ejecutar")

#print("Digite su NOMBRE DE USUARIO corporativo: ", end="")
#nombre_usuario = input()

#print("Digite su CONTRASEÑA: ", end="")
#clave = input()

# Convertir las variables a tipo string
# https://www.geeksforgeeks.org/python-str-function/
#nombre_usuario = str(nombre_usuario)
#clave = str(clave)
# Imprimir el tipo de dato de las variable
# Tienen q ser tipo string
#type(nombre_usuario)
#type(clave)
#Out[...]: str

nombre_usuario = "danpined"
clave = "Vancoconecta#2"



########################################################################
#%%

""" 
Ejecutar comandos en CMD con Python para instalar las librerias necesarias usando pip install... 
https://datatofish.com/command-prompt-python/
"""

print("\n")
print("\n")
print("instalando librerias")

import os                                                 # CMD / sistema operativo

#os.system('cmd /c "pip install --upgrade --user pip"')   # Actualizar pip
#os.system('cmd /c "pip install openpyxl --user"')        # Exportar DataFrame de Pandas a Excel
#os.system('cmd /c "pip install pandas --user"')
#os.system('cmd /c "pip install pyodbc --user"')
# Conexion de Python a la base de datos de Bancolombia (LZ - HUE)
#os.system('cmd /c "python -m pip install sparky-bc -i https://artifactory.apps.bancolombia.com/api/pypi/pypi-bancolombia/simple --trusted-host artifactory.apps.bancolombia.com --user"') 
#os.system('cmd /c "python -m pip install Pysftp -i https://artifactory.apps.bancolombia.com/api/pypi/pypi-bancolombia/simple --trusted-host artifactory.apps.bancolombia.com --user"') 



##############
#%%

print("\n")
print("\n")
print("importando librerias")

import pandas as pd                # Para los DataFrame
from os import scandir, getcwd     # scandir manejo de rutas
                                   # getcwd directorio (carpeta) de trabajo actual
from sparky_bc import Sparky       # Conexion de Python a la base de datos de Bancolombia (LZ - HUE), libreria hecha por el banco
import pyodbc                      # Conexion a bases de datos con el estandar ODBC
import datetime                    # Fecha y hora del sistema operativo
from datetime import datetime
import time                        # tiempo
from traceback import format_exc   # Captura de errores try: except Exception:



########################################################################
#%%

# Crear una funcion sin parametros
def imprimir_mensaje_de_error():
      # Guardar en una variable el mensaje de error q muestra Python por consola
      error = format_exc()
      # Imprimir el mensaje de error de Python
      print("Mensaje de error de Python")
      print("\n")
      print(error)



########################################################################
#%%

# Funcion sin parametros
def detener_la_ejecucion_del_codigo():
    #print("se ha ejecutado la funcion sin parametros detener_la_ejecucion_del_codigo()")

    # Imprimir mensaje de error
    print("┌─┐┬─┐┬─┐┌─┐┬─┐")
    print("├┤ ├┬┘├┬┘│ │├┬┘")
    print("└─┘┴└─┴└─└─┘┴└─")
    # Suspender (retrasar) TEMPORALMENTE la ejecucion del codigo
    # durante determinado tiempo, en este caso en especifico durante 1 segundo
    # Esto se hace para q se pueda imprimir por consola el mensaje de error
    time.sleep(1)

    # Detener POR COMPLETO la ejecucion del codigo
    # esto al mismo tiempo tambien va a 
    # eliminar TODAS las variables, DataFrame, etc
    # https://stackoverflow.com/questions/19782075/how-to-stop-terminate-a-python-script-from-running
    os._exit(0)



########################################################################
#%% 

# Cambiar los nombres de los indices de las columnas de df_insumo
# df = df.rename(columns= {"NombreAntiguoDeLaColumna_1": "NuevoNombreColumna_1", 
#                          "NombreAntiguoDeLaColumna_2": "NuevoNombreColumna_2} 
#                          index={NumeroDeIndice: "NuevoNombreIndice"}
#                          inplace=True)
# Funcion sin parametros
# para cambiar los nombres de la columna numero_identificacion
# https://stackoverflow.com/questions/11346283/renaming-column-names-in-pandas

""" Validacion de datos """  
def cambiar_nombre_columna_numero_identificacion():
    #print("se ha ejecutado la funcion llamada cambiar_nombre_columna_numero_identificacion()")
    
    # validar q exista el nombre de la columna en un DataFrame
    # https://stackoverflow.com/questions/24870306/how-to-check-if-a-column-exists-in-pandas
    # esto lo hago para q NO se creen columnas duplicadas

    # cuando NO existe la columna numero_identificacion 
    # entonces INTENTAR cambiar el nombre de la columna
    if not ("numero_identificacion" in df_insumo.columns):
        #print("se a INTENTADO cambiar el nombre de la columna numero_identificacion")

        diccionario_identificacion = { # numero_identificacion = Número de documento = Número de identificación = Numero de ID = ID = Nit 
                                       "Número de id":"numero_identificacion",
                                       "Número_de_id":"numero_identificacion",
                                       "Número-de-id":"numero_identificacion",
                                       "número de ID":"numero_identificacion",
                                       "número-de-ID":"numero_identificacion",
                                       "número_de_ID":"numero_identificacion",
                                       "Número de ID":"numero_identificacion",
                                       "Número-de-ID":"numero_identificacion",
                                       "Número_de_ID":"numero_identificacion",
                                       "Numero de id":"numero_identificacion",
                                       "Numero_de_id":"numero_identificacion",
                                       "Numero-de-id":"numero_identificacion",
                                       "numero de id":"numero_identificacion",
                                       "numero_de_id":"numero_identificacion",
                                       "numero-de-id":"numero_identificacion",
                                       "número de id":"numero_identificacion",
                                       "número_de_id":"numero_identificacion",
                                       "número-de-id":"numero_identificacion",
                                       "NUMERO DE ID":"numero_identificacion",
                                       "NUMERO-DE-ID":"numero_identificacion",
                                       "NÚMERO DE ID":"numero_identificacion",
                                       "NÚMERO_DE_ID":"numero_identificacion",
                                       "NÚMERO-DE-ID":"numero_identificacion",
                                       "NUMERO_DE_ID":"numero_identificacion",
                                              
                                       "Número id":"numero_identificacion",
                                       "Número_id":"numero_identificacion",
                                       "Número-id":"numero_identificacion",
                                       "Numero id":"numero_identificacion",
                                       "Numero_id":"numero_identificacion",
                                       "Numero-id":"numero_identificacion",
                                       "numero id":"numero_identificacion",
                                       "numero_id":"numero_identificacion",
                                       "numero-id":"numero_identificacion",
                                       "número id":"numero_identificacion",
                                       "número_id":"numero_identificacion",
                                       "número-id":"numero_identificacion",
                                       "NUMERO ID":"numero_identificacion",
                                       "NUMERO ID":"numero_identificacion",
                                       "NÚMERO ID":"numero_identificacion",
                                       "NÚMERO_ID":"numero_identificacion",
                                       "NÚMERO-ID":"numero_identificacion",
                                       "NUMERO_ID":"numero_identificacion",
                                              
                                       "Número de identificación":"numero_identificacion",
                                       "Número_de_identificación":"numero_identificacion",
                                       "Número-de-identificación":"numero_identificacion",
                                       "Numero de identificacion":"numero_identificacion",
                                       "Numero_de_identificacion":"numero_identificacion",
                                       "Numero-de-identificacion":"numero_identificacion",
                                       "Número de identificacion":"numero_identificacion",
                                       "Número_de_identificacion":"numero_identificacion",
                                       "Número-de-identificacion":"numero_identificacion",
                                       "Numero de identificación":"numero_identificacion",
                                       "Numero_de_identificación":"numero_identificacion",
                                       "Numero-de-identificación":"numero_identificacion",
                                       "numero de identificacion":"numero_identificacion",
                                       "numero_de_identificacion":"numero_identificacion",
                                       "numero-de-identificacion":"numero_identificacion",
                                       "número de identificación":"numero_identificacion",
                                       "número_de_identificación":"numero_identificacion",
                                       "número-de-identificación":"numero_identificacion",
                                       "número de identificacion":"numero_identificacion",
                                       "número_de_identificacion":"numero_identificacion",
                                       "número-de-identificacion":"numero_identificacion",
                                       "numero de identificación":"numero_identificacion",
                                       "numero_de_identificación":"numero_identificacion",
                                       "numero-de-identificación":"numero_identificacion",
                                       "NUMERO DE IDENTIFICACION":"numero_identificacion",
                                       "NUMERO_DE_IDENTIFICACION":"numero_identificacion",
                                       "NUMERO-DE-IDENTIFICACION":"numero_identificacion",
                                       "NÚMERO DE IDENTIFICACIÓN":"numero_identificacion",
                                       "NÚMERO_DE_IDENTIFICACIÓN":"numero_identificacion",
                                       "NÚMERO-DE-IDENTIFICACIÓN":"numero_identificacion",
                                       "NÚMERO DE IDENTIFICACION":"numero_identificacion",
                                       "NÚMERO_DE_IDENTIFICACION":"numero_identificacion",
                                       "NÚMERO-DE-IDENTIFICACION":"numero_identificacion",
                                       "NUMERO DE IDENTIFICACIÓN":"numero_identificacion",
                                       "NUMERO_DE_IDENTIFICACIÓN":"numero_identificacion",
                                       "NUMERO-DE-IDENTIFICACIÓN":"numero_identificacion",

                                       "Número identificación":"numero_identificacion",
                                       "Número_identificación":"numero_identificacion",
                                       "Número-identificación":"numero_identificacion",
                                       "Numero identificacion":"numero_identificacion",
                                       "Numero_identificacion":"numero_identificacion",
                                       "Numero-identificacion":"numero_identificacion",
                                       "Número identificacion":"numero_identificacion",
                                       "Número_identificacion":"numero_identificacion",
                                       "Número-identificacion":"numero_identificacion",
                                       "Numero identificación":"numero_identificacion",
                                       "Numero_identificación":"numero_identificacion",
                                       "Numero-identificación":"numero_identificacion",
                                       "numero identificacion":"numero_identificacion",
                                       "numero_identificacion":"numero_identificacion",
                                       "numero-identificacion":"numero_identificacion",
                                       "número identificación":"numero_identificacion",
                                       "número_identificación":"numero_identificacion",
                                       "número-identificación":"numero_identificacion",
                                       "número identificacion":"numero_identificacion",
                                       "número_identificacion":"numero_identificacion",
                                       "número-identificacion":"numero_identificacion",
                                       "numero identificación":"numero_identificacion",
                                       "numero_identificación":"numero_identificacion",
                                       "numero-identificación":"numero_identificacion",
                                       "NUMERO IDENTIFICACION":"numero_identificacion",
                                       "NUMERO_IDENTIFICACION":"numero_identificacion",
                                       "NUMERO-IDENTIFICACION":"numero_identificacion",
                                       "NÚMERO IDENTIFICACIÓN":"numero_identificacion",
                                       "NÚMERO_IDENTIFICACIÓN":"numero_identificacion",
                                       "NÚMERO-IDENTIFICACIÓN":"numero_identificacion",
                                       "NÚMERO IDENTIFICACION":"numero_identificacion",
                                       "NÚMERO_IDENTIFICACION":"numero_identificacion",
                                       "NÚMERO-IDENTIFICACION":"numero_identificacion",
                                       "NUMERO IDENTIFICACIÓN":"numero_identificacion",
                                       "NUMERO_IDENTIFICACIÓN":"numero_identificacion",
                                       "NUMERO-IDENTIFICACIÓN":"numero_identificacion",
                                              
                                       "Número de documento":"numero_identificacion",
                                       "Número_de_documento":"numero_identificacion",
                                       "Número-de-documento":"numero_identificacion",
                                       "Numero de documento":"numero_identificacion",
                                       "Numero_de_documento":"numero_identificacion",
                                       "Numero-de-documento":"numero_identificacion",
                                       "numero de documento":"numero_identificacion",
                                       "numero_de_documento":"numero_identificacion",
                                       "numero-de-documento":"numero_identificacion",
                                       "número de documento":"numero_identificacion",
                                       "número_de_documento":"numero_identificacion",
                                       "número-de-documento":"numero_identificacion",
                                       "NUMERO DE DOCUMENTO":"numero_identificacion",
                                       "NUMERO_DE_DOCUMENTO":"numero_identificacion",
                                       "NUMERO-DE-DOCUMENTO":"numero_identificacion",
                                       "NÚMERO DE DOCUMENTO":"numero_identificacion",
                                       "NÚMERO_DE_DOCUMENTO":"numero_identificacion",
                                       "NÚMERO-DE-DOCUMENTO":"numero_identificacion",
                                              
                                       "Número documento":"numero_identificacion",
                                       "Número_documento":"numero_identificacion",
                                       "Número-documento":"numero_identificacion",
                                       "Numero documento":"numero_identificacion",
                                       "Numero_documento":"numero_identificacion",
                                       "Numero-documento":"numero_identificacion",
                                       "numero documento":"numero_identificacion",
                                       "numero_documento":"numero_identificacion",
                                       "numero-documento":"numero_identificacion",
                                       "número documento":"numero_identificacion",
                                       "número_documento":"numero_identificacion",
                                       "número-documento":"numero_identificacion",
                                       "NUMERO DOCUMENTO":"numero_identificacion",
                                       "NUMERO_DOCUMENTO":"numero_identificacion",
                                       "NUMERO-DOCUMENTO":"numero_identificacion",
                                       "NÚMERO DOCUMENTO":"numero_identificacion",
                                       "NÚMERO_DOCUMENTO":"numero_identificacion",
                                       "NÚMERO-DOCUMENTO":"numero_identificacion",
                                              
                                       "identificacion":"numero_identificacion",
                                       "identificación":"numero_identificacion",
                                       "Identificacion":"numero_identificacion",
                                       "Identificación":"numero_identificacion",
                                       "IDENTIFICACION":"numero_identificacion",
                                       "IDENTIFICACIÓN":"numero_identificacion",

                                       "documento":"numero_identificacion",
                                       "Documento":"numero_identificacion",
                                       "DOCUMENTO":"numero_identificacion",

                                       "identificacion cliente":"numero_identificacion",
                                       "identificación cliente":"numero_identificacion",
                                       "Identificación Cliente":"numero_identificacion",
                                       "Identificacion Cliente":"numero_identificacion",
                                       "Identificación cliente":"numero_identificacion",
                                       "IDENTIFICACION CLIENTE":"numero_identificacion",
                                       "identificacion-cliente":"numero_identificacion",
                                       "identificación-cliente":"numero_identificacion",
                                       "Identificación-Cliente":"numero_identificacion",
                                       "Identificacion-Cliente":"numero_identificacion",
                                       "Identificación-cliente":"numero_identificacion",
                                       "IDENTIFICACION-CLIENTE":"numero_identificacion",
                                       "identificacion_cliente":"numero_identificacion",
                                       "identificación_cliente":"numero_identificacion",
                                       "Identificación_Cliente":"numero_identificacion",
                                       "Identificacion_Cliente":"numero_identificacion",
                                       "Identificación_cliente":"numero_identificacion",
                                       "IDENTIFICACION_CLIENTE":"numero_identificacion",
                                              
                                       "identificacion deudor":"numero_identificacion",
                                       "identificación deudor":"numero_identificacion",
                                       "Identificación Deudor":"numero_identificacion",
                                       "Identificacion Deudor":"numero_identificacion",
                                       "Identificación deudor":"numero_identificacion",
                                       "identificacion-deudor":"numero_identificacion",
                                       "identificación-deudor":"numero_identificacion",
                                       "Identificación-Deudor":"numero_identificacion",
                                       "Identificacion-Deudor":"numero_identificacion",
                                       "Identificación-deudor":"numero_identificacion",
                                       "IDENTIFICACION-DEUDOR":"numero_identificacion",
                                       "identificacion_deudor":"numero_identificacion",
                                       "identificación_deudor":"numero_identificacion",
                                       "Identificación_Deudor":"numero_identificacion",
                                       "Identificacion_Deudor":"numero_identificacion",
                                       "Identificación_deudor":"numero_identificacion",
                                       "IDENTIFICACION_DEUDOR":"numero_identificacion",
                                       "IDENTIFICACIÓN_DEUDOR":"numero_identificacion",
                                       "IDENTIFICACION DEUDOR":"numero_identificacion",

                                       "id deudor":"numero_identificacion",
                                       "Id deudor":"numero_identificacion",
                                       "Id Deudor":"numero_identificacion",
                                       "id Deudor":"numero_identificacion",
                                       "ID DEUDOR":"numero_identificacion",
                                       "ID deudor":"numero_identificacion",
                                       "ID Deudor":"numero_identificacion",
                                       "id-deudor":"numero_identificacion",
                                       "Id-deudor":"numero_identificacion",
                                       "Id-Deudor":"numero_identificacion",
                                       "id-Deudor":"numero_identificacion",
                                       "ID-DEUDOR":"numero_identificacion",
                                       "ID-deudor":"numero_identificacion",
                                       "ID-Deudor":"numero_identificacion",
                                       "id_deudor":"numero_identificacion",
                                       "Id_deudor":"numero_identificacion",
                                       "Id_Deudor":"numero_identificacion",
                                       "id_Deudor":"numero_identificacion",
                                       "ID_DEUDOR":"numero_identificacion",
                                       "ID_deudor":"numero_identificacion",
                                       "ID_Deudor":"numero_identificacion",
                                              
                                       "id cliente":"numero_identificacion",
                                       "Id cliente":"numero_identificacion",
                                       "Id Cliente":"numero_identificacion",
                                       "id Cliente":"numero_identificacion",
                                       "ID CLIENTE":"numero_identificacion",
                                       "ID cliente":"numero_identificacion",
                                       "ID Cliente":"numero_identificacion",
                                       "id-cliente":"numero_identificacion",
                                       "Id-cliente":"numero_identificacion",
                                       "Id-Cliente":"numero_identificacion",
                                       "id-Cliente":"numero_identificacion",
                                       "ID-CLIENTE":"numero_identificacion",
                                       "ID-cliente":"numero_identificacion",
                                       "ID-Cliente":"numero_identificacion",
                                       "id_cliente":"numero_identificacion",
                                       "Id_cliente":"numero_identificacion",
                                       "Id_Cliente":"numero_identificacion",
                                       "id_Cliente":"numero_identificacion",
                                       "ID_CLIENTE":"numero_identificacion",
                                       "ID_cliente":"numero_identificacion",
                                       "ID_Cliente":"numero_identificacion",

                                       "id":"numero_identificacion",
                                       "ID":"numero_identificacion",
                                       "Id":"numero_identificacion",

                                       "nit":"numero_identificacion",
                                       "NIT":"numero_identificacion",
                                       "Nit":"numero_identificacion",
                                     }

        # inplace=True hace q se reemplacen los nombres de las columnas y q NO se creen otras columnas duplicadas con los nuevos nombres de los indices
        df_insumo.rename(diccionario_identificacion, axis=1, inplace=True)

    # SI despues de INTENTAR cambiar el nombre de la columna numero_identificacion
    # sigue sin e
    if not ("numero_identificacion" in df_insumo.columns):
        print("\n")
        print("\n")
        print("┌─┐┬─┐┬─┐┌─┐┬─┐")
        print("├┤ ├┬┘├┬┘│ │├┬┘")
        print("└─┘┴└─┴└─└─┘┴└─")
        print("\n")
        print("ERROR - la columna numero_identificacion NO existe", "\n",
              "verifique lo siguiente en TODOS los Excel que estan dentro de la carpeta ...\\2_excel\\1_insumo", "\n",
              "1. Que la columna numero_identificacion exista", "\n",
              "2. Cambiar el nombre de la columna por numero_identificacion (sin tildes, ni mayusculas, ni espacios en blanco)", "\n",
              "3. Despues de hacer lo anterior vuelva a ejecutar el codigo otra vez"
             )

        # Funcion q llama a otra funcion
        # Llamar funcion sin parametros 
        # para detener la ejecucion del codigo
        detener_la_ejecucion_del_codigo()


########################################################################
#%%

# Funcion sin parametros
# para cambiar los nombres de la columna numero_obligacion

""" Validacion de datos """
def cambiar_nombre_columna_numero_obligacion():
    #print("se ha ejecutado la funcion llamada cambiar_nombre_columna_numero_obligacion()")

    if not ("numero_obligacion" in df_insumo.columns):
        diccionario_obligacion = { # numero_obligacion = numero de obligacion
                                  "Número de obligación":"numero_obligacion",
                                  "Número_de_obligación":"numero_obligacion",
                                  "Número-de-obligación":"numero_obligacion",
                                  "Numero de obligacion":"numero_obligacion",
                                  "Numero_de_obligacion":"numero_obligacion",
                                  "Numero-de-obligacion":"numero_obligacion",
                                  "Número de obligacion":"numero_obligacion",
                                  "Número_de_obligacion":"numero_obligacion",
                                  "Número-de-obligacion":"numero_obligacion",
                                  "Numero de obligación":"numero_obligacion",
                                  "Numero_de_obligación":"numero_obligacion",
                                  "Numero-de-obligación":"numero_obligacion",
                                  "numero de obligacion":"numero_obligacion",
                                  "numero_de_obligacion":"numero_obligacion",
                                  "numero-de-obligacion":"numero_obligacion",
                                  "número de obligación":"numero_obligacion",
                                  "número_de_obligación":"numero_obligacion",
                                  "número-de-obligación":"numero_obligacion",
                                  "número de obligacion":"numero_obligacion",
                                  "número_de_obligacion":"numero_obligacion",
                                  "número-de-obligacion":"numero_obligacion",
                                  "numero de obligación":"numero_obligacion",
                                  "numero_de_obligación":"numero_obligacion",
                                  "numero-de-obligación":"numero_obligacion",
                                  "NUMERO DE OBLIGACION":"numero_obligacion",
                                  "NUMERO_DE_OBLIGACION":"numero_obligacion",
                                  "NUMERO-DE-OBLIGACION":"numero_obligacion",
                                  "NÚMERO DE OBLIGACIÓN":"numero_obligacion",
                                  "NÚMERO_DE_OBLIGACIÓN":"numero_obligacion",
                                  "NÚMERO-DE-OBLIGACIÓN":"numero_obligacion",
                                  "NÚMERO DE OBLIGACION":"numero_obligacion",
                                  "NÚMERO_DE_OBLIGACION":"numero_obligacion",
                                  "NÚMERO-DE-OBLIGACION":"numero_obligacion",
                                  "NUMERO DE OBLIGACIÓN":"numero_obligacion",
                                  "NUMERO_DE_OBLIGACIÓN":"numero_obligacion",
                                  "NUMERO-DE-OBLIGACIÓN":"numero_obligacion",

                                  "Número obligación":"numero_obligacion",
                                  "Número_obligación":"numero_obligacion",
                                  "Número-obligación":"numero_obligacion",
                                  "Numero obligacion":"numero_obligacion",
                                  "Numero_obligacion":"numero_obligacion",
                                  "Numero-obligacion":"numero_obligacion",
                                  "Número obligacion":"numero_obligacion",
                                  "Número_obligacion":"numero_obligacion",
                                  "Número-obligacion":"numero_obligacion",
                                  "Numero obligación":"numero_obligacion",
                                  "Numero_obligación":"numero_obligacion",
                                  "Numero-obligación":"numero_obligacion",
                                  "numero obligacion":"numero_obligacion",
                                  "número obligación":"numero_obligacion",
                                  "número_obligación":"numero_obligacion",
                                  "número-obligación":"numero_obligacion",
                                  "número obligacion":"numero_obligacion",
                                  "número_obligacion":"numero_obligacion",
                                  "número-obligacion":"numero_obligacion",
                                  "numero obligación":"numero_obligacion",
                                  "numero_obligación":"numero_obligacion",
                                  "numero-obligación":"numero_obligacion",
                                  "NUMERO OBLIGACION":"numero_obligacion",
                                  "NUMERO_OBLIGACION":"numero_obligacion",
                                  "NUMERO-OBLIGACION":"numero_obligacion",
                                  "NÚMERO OBLIGACIÓN":"numero_obligacion",
                                  "NÚMERO_OBLIGACIÓN":"numero_obligacion",
                                  "NÚMERO-OBLIGACIÓN":"numero_obligacion",
                                  "NUMERO OBLIGACIÓN":"numero_obligacion",
                                  "NUMERO_OBLIGACIÓN":"numero_obligacion",
                                  "NUMERO-OBLIGACIÓN":"numero_obligacion",
                                  "NÚMERO OBLIGACION":"numero_obligacion",
                                  "NÚMERO_OBLIGACION":"numero_obligacion",
                                  "NÚMERO-OBLIGACION":"numero_obligacion", 

                                  "obligacion cliente":"numero_identificacion",
                                  "obligación cliente":"numero_identificacion",
                                  "Obligación Cliente":"numero_identificacion",
                                  "Obligación Cliente":"numero_identificacion",
                                  "Obligación cliente":"numero_identificacion",
                                  "OBLIGACION CLIENTE":"numero_identificacion",
                                  "obligacion-cliente":"numero_identificacion",
                                  "obligación-cliente":"numero_identificacion",
                                  "Obligación-Cliente":"numero_identificacion",
                                  "Obligacion-Cliente":"numero_identificacion",
                                  "Obligación-cliente":"numero_identificacion",
                                  "OBLIGACION-CLIENTE":"numero_identificacion",
                                  "obligacion_cliente":"numero_identificacion",
                                  "obligación_cliente":"numero_identificacion",
                                  "Obligación_Cliente":"numero_identificacion",
                                  "Obligacion_Cliente":"numero_identificacion",
                                  "Obligación_cliente":"numero_identificacion",
                                  "OBLIGACION_CLIENTE":"numero_identificacion",

                                  "obligacion deudor":"numero_identificacion",
                                  "obligación deudor":"numero_identificacion",
                                  "Obligación Deudor":"numero_identificacion",
                                  "Obligacion Deudor":"numero_identificacion",
                                  "Obligación deudor":"numero_identificacion",
                                  "obligacion-deudor":"numero_identificacion",
                                  "obligación-deudor":"numero_identificacion",
                                  "Obligación-Deudor":"numero_identificacion",
                                  "Obligacion-Deudor":"numero_identificacion",
                                  "Obligación-deudor":"numero_identificacion",
                                  "OBLIGACION-DEUDOR":"numero_identificacion",
                                  "obligacion_deudor":"numero_identificacion",
                                  "obligación_deudor":"numero_identificacion",
                                  "Obligación_Deudor":"numero_identificacion",
                                  "Obligación_Deudor":"numero_identificacion",
                                  "Obligación_deudor":"numero_identificacion",
                                  "OBLIGACION_DEUDOR":"numero_identificacion",
                                  "OBLIGACIÓN_DEUDOR":"numero_identificacion",
                                  "OBLIGACION DEUDOR":"numero_identificacion",

                                  "obligacion":"numero_obligacion",
                                  "obligación":"numero_obligacion",
                                  "Obligacion":"numero_obligacion",
                                  "Obligación":"numero_obligacion",
                                  "OBLIGACION":"numero_obligacion",
                                  "OBLIGACIÓN":"numero_obligacion",
                                 }

        df_insumo.rename(diccionario_obligacion, axis=1, inplace=True)

    if not ("numero_obligacion" in df_insumo.columns):
        print("\n")
        print("\n")
        print("┌─┐┬─┐┬─┐┌─┐┬─┐")
        print("├┤ ├┬┘├┬┘│ │├┬┘")
        print("└─┘┴└─┴└─└─┘┴└─")
        print("\n")
        print("ERROR - la columna numero_obligacion NO existe", "\n",
              "verifique lo siguiente en TODOS los Excel que estan dentro de la carpeta ...\\2_excel\\1_insumo", "\n",
              "1. Que la columna numero_obligacion exista", "\n",
              "2. Cambiar el nombre de la columna por numero_obligacion (sin tildes, ni mayusculas, ni espacios en blanco)", "\n",
              "3. Despues de hacer lo anterior vuelva a ejecutar el codigo otra vez"
             )

        detener_la_ejecucion_del_codigo()


########################################################################
#%%

# Funcion sin parametros
# para cambiar el nombre de la columna llamada fecha_desembolso
"""
def cambiar_nombre_columna_fecha_desembolso():
    #print("se ha ejecutado la funcion llamada cambiar_nombre_columna_fecha_desembolso()")

    diccionario_fecha_desembolso = { # fecha_desembolso = fecha de desembolso = fecha de apertura
                                    "FECHA DE DESEMBOLSO":"fecha_desembolso",
                                    "FECHA_DE_DESEMBOLSO":"fecha_desembolso",
                                    "FECHA-DE-DESEMBOLSO":"fecha_desembolso",
                                    "fecha de desembolso":"fecha_desembolso",
                                    "fecha_de_desembolso":"fecha_desembolso",
                                    "fecha-de-desembolso":"fecha_desembolso",
                                    "Fecha de desembolso":"fecha_desembolso",
                                    "Fecha_de_desembolso":"fecha_desembolso",
                                    "Fecha-de-desembolso":"fecha_desembolso",
                                    "Fecha de Desembolso":"fecha_desembolso",
                                    "Fecha_de_Desembolso":"fecha_desembolso",
                                    "Fecha-de-Desembolso":"fecha_desembolso",
                                    "Fecha De Desembolso":"fecha_desembolso",
                                    "Fecha_De_Desembolso":"fecha_desembolso",
                                    "Fecha-De-Desembolso":"fecha_desembolso",
                                    
                                    "FECHA DE DESEMBOLSO2":"fecha_desembolso",
                                    "FECHA_DE_DESEMBOLSO2":"fecha_desembolso",
                                    "FECHA-DE-DESEMBOLSO2":"fecha_desembolso",
                                    "fecha de desembolso2":"fecha_desembolso",
                                    "fecha_de_desembolso2":"fecha_desembolso",
                                    "fecha-de-desembolso2":"fecha_desembolso",
                                    "Fecha de desembolso2":"fecha_desembolso",
                                    "Fecha_de_desembolso2":"fecha_desembolso",
                                    "Fecha-de-desembolso2":"fecha_desembolso",
                                    "Fecha de Desembolso2":"fecha_desembolso",
                                    "Fecha_de_Desembolso2":"fecha_desembolso",
                                    "Fecha-de-Desembolso2":"fecha_desembolso",
                                    "Fecha De Desembolso2":"fecha_desembolso",
                                    "Fecha_De_Desembolso2":"fecha_desembolso",
                                    "Fecha-De-Desembolso2":"fecha_desembolso",
                                    
                                    "FECHA DE DESEMBOLSO 2":"fecha_desembolso",
                                    "FECHA_DE_DESEMBOLSO_2":"fecha_desembolso",
                                    "FECHA-DE-DESEMBOLSO-2":"fecha_desembolso",
                                    "fecha de desembolso 2":"fecha_desembolso",
                                    "fecha_de_desembolso_2":"fecha_desembolso",
                                    "fecha-de-desembolso-2":"fecha_desembolso",
                                    "Fecha de desembolso 2":"fecha_desembolso",
                                    "Fecha_de_desembolso_2":"fecha_desembolso",
                                    "Fecha-de-desembolso-2":"fecha_desembolso",
                                    "Fecha de Desembolso 2":"fecha_desembolso",
                                    "Fecha_de_Desembolso_2":"fecha_desembolso",
                                    "Fecha-de-Desembolso-2":"fecha_desembolso",
                                    "Fecha De Desembolso 2":"fecha_desembolso",
                                    "Fecha_De_Desembolso_2":"fecha_desembolso",
                                    "Fecha-De-Desembolso-2":"fecha_desembolso",
                                    
                                    "FECHA DESEMBOLSO":"fecha_desembolso",
                                    "FECHA_DESEMBOLSO":"fecha_desembolso",
                                    "FECHA-DESEMBOLSO":"fecha_desembolso",
                                    "fecha desembolso":"fecha_desembolso",
                                    "fecha-desembolso":"fecha_desembolso",
                                    "Fecha desembolso":"fecha_desembolso",
                                    "Fecha_desembolso":"fecha_desembolso",
                                    "Fecha-desembolso":"fecha_desembolso",
                                    "Fecha Desembolso":"fecha_desembolso",
                                    "Fecha_Desembolso":"fecha_desembolso",
                                    "Fecha-Desembolso":"fecha_desembolso",
                                    "Fecha Desembolso":"fecha_desembolso",
                                    "Fecha_Desembolso":"fecha_desembolso",
                                    "Fecha-Desembolso":"fecha_desembolso",
                                    
                                    "FECHA DESEMBOLSO2":"fecha_desembolso",
                                    "FECHA_DESEMBOLSO2":"fecha_desembolso",
                                    "FECHA-DESEMBOLSO2":"fecha_desembolso",
                                    "fecha desembolso2":"fecha_desembolso",
                                    "fecha-desembolso2":"fecha_desembolso",
                                    "Fecha desembolso2":"fecha_desembolso",
                                    "Fecha_desembolso2":"fecha_desembolso",
                                    "Fecha-desembolso2":"fecha_desembolso",
                                    "Fecha Desembolso2":"fecha_desembolso",
                                    "Fecha_Desembolso2":"fecha_desembolso",
                                    "Fecha-Desembolso2":"fecha_desembolso",
                                    "Fecha Desembolso2":"fecha_desembolso",
                                    "Fecha_Desembolso2":"fecha_desembolso",
                                    "Fecha-Desembolso2":"fecha_desembolso",
                                    
                                    "FECHA DESEMBOLSO 2":"fecha_desembolso",
                                    "FECHA_DESEMBOLSO_2":"fecha_desembolso",
                                    "FECHA-DESEMBOLSO-2":"fecha_desembolso",
                                    "fecha desembolso 2":"fecha_desembolso",
                                    "fecha-desembolso-2":"fecha_desembolso",
                                    "Fecha desembolso 2":"fecha_desembolso",
                                    "Fecha_desembolso_2":"fecha_desembolso",
                                    "Fecha-desembolso-2":"fecha_desembolso",
                                    "Fecha Desembolso 2":"fecha_desembolso",
                                    "Fecha_Desembolso_2":"fecha_desembolso",
                                    "Fecha-Desembolso-2":"fecha_desembolso",
                                    "Fecha Desembolso 2":"fecha_desembolso",
                                    "Fecha_Desembolso_2":"fecha_desembolso",
                                    "Fecha-Desembolso-2":"fecha_desembolso",
                                    
                                    "FECHA DE APERTURA":"fecha_desembolso",
                                    "FECHA_DE_APERTURA":"fecha_desembolso",
                                    "FECHA-DE-APERTURA":"fecha_desembolso",
                                    "fecha de apertura":"fecha_desembolso",
                                    "fecha_de_apertura":"fecha_desembolso",
                                    "fecha-de-apertura":"fecha_desembolso",
                                    "Fecha de apertura":"fecha_desembolso",
                                    "Fecha_de_apertura":"fecha_desembolso",
                                    "Fecha-de-apertura":"fecha_desembolso",
                                    "Fecha de Apertura":"fecha_desembolso",
                                    "Fecha_de_Apertura":"fecha_desembolso",
                                    "Fecha-de-Apertura":"fecha_desembolso",
                                    "Fecha De Apertura":"fecha_desembolso",
                                    "Fecha_De_Apertura":"fecha_desembolso",
                                    "Fecha-De-Apertura":"fecha_desembolso",
                                    
                                    "FECHA APERTURA":"fecha_desembolso",
                                    "FECHA_APERTURA":"fecha_desembolso",
                                    "FECHA-APERTURA":"fecha_desembolso",
                                    "fecha apertura":"fecha_desembolso",
                                    "fecha-apertura":"fecha_desembolso",
                                    "Fecha apertura":"fecha_desembolso",
                                    "Fecha_apertura":"fecha_desembolso",
                                    "Fecha-apertura":"fecha_desembolso",
                                    "Fecha Apertura":"fecha_desembolso",
                                    "Fecha_Apertura":"fecha_desembolso",
                                    "Fecha-Apertura":"fecha_desembolso",
                                    "Fecha Apertura":"fecha_desembolso",
                                    "Fecha_Apertura":"fecha_desembolso",
                                    "Fecha-Apertura":"fecha_desembolso",
                                    }

    df_insumo.rename(diccionario_fecha_desembolso, axis=1, inplace=True)
"""



########################################################################
#%%

print("\n")
print("\n")
print("Rutas dinamicas")

"""
Rutas de los Excel

obtener (guardar) en una variable el directorio actual
sin importar las carpetas anteriores en las q este  
q es donde esta 2_python_ejecutar_consulta_y_crear_excel.py

la ruta es la siguiente:
 ...\4_Proyecto_requerimientos_FNG\1_codigo_fuente

https://www.geeksforgeeks.org/python-os-getcwd-method/

IMPORTANTE:
- para q esto funcione la terminal (directorio de trabajo / working directory) 
  TIENE q estar situada en la ruta donde esta el archivo 2_python_ejecutar_consulta_y_crear_excel.py
  NO puede estar en C:\\Users\\NombreUsuario

- Para q funcione en VS Code hacer lo siguiente:
  - Dependiendo de la forma en como abres VS Code es probable q el directorio de trabajo sea C:\\Users\\NombreUsuario
  - Abrir el explorador de archivos de Windows
  - Buscar la carpeta en donde tienes guardado el archivo llamado 2_python_ejecutar_consulta_y_crear_excel.py
  - Click derecho
  - Abrir con Code
  - Verifica q el CMD este situado en la ruta donde guardaste el 2_python_ejecutar_consulta_y_crear_excel.py

- Para q funcione en Spyder hay q hacer lo siguiente:
  - Hay q cambiar el directorio de trabajo de Spyder, 
    para ello hacer este video: https://www.youtube.com/watch?v=KWK5Qma-jFQ
  - Spyder por defecto inicia la terminal en C:\\Users\\NombreUsuario
  - Click en el icono de la carpeta en la parte superior derecha Browse a working directory
  - Se abre una ventana con el explorador de archivos
  - Buscar la carpeta en donde tienes guardado el codigo llamado python_transformar_datos_cafe.py
  - En mi caso en especifico, la ruta es:
    C:\\Users\\danpined\\OneDrive - Grupo Bancolombia\\5_ReporteriaYprovisiones\\4_Proyecto_requerimientos_FNG\\1_codigo_fuente
  - Click en Seleccionar Carpeta
  - Se cambiara la ruta en el cuadro de texto de la parte superior derecha
"""

#ruta_carpeta_codigo_fuente = r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\4_Proyecto_requerimientos_FNG\1_codigo_fuente'
ruta_carpeta_codigo_fuente = os.getcwd()

# Imprimir la ruta donde esta 2_python_ejecutar_consulta_y_crear_excel.py
# Se TIENE q imprimir la siguiente ruta:
# ...\4_Proyecto_requerimientos_FNG\1_codigo_fuente
#print(ruta_carpeta_codigo_fuente)

# Convertir variable a tipo string
ruta_carpeta_codigo_fuente = str(ruta_carpeta_codigo_fuente)

# Devolverme entre directorios (carpetas)
#devolver = os.path.abspath(ruta_carpeta_codigo_fuente + "../")

ruta_insumo = os.path.abspath(ruta_carpeta_codigo_fuente + "../../" + "/2_excel/1_insumo")
# Imprimir ruta_insumo
# Se TIENE q imprimir la siguiente ruta:
# ...\4_Proyecto_requerimientos_FNG\2_excel\1_insumo
#print(ruta_insumo)

ruta_respuesta = os.path.abspath(ruta_carpeta_codigo_fuente + "../../" + "/2_excel/2_respuesta")
# Imprimir ruta_respuesta
# Se TIENE q imprimir la siguiente ruta:
# ...\4_Proyecto_requerimientos_FNG\2_excel\2_respuesta
#print(ruta_respuesta)

#ruta_macro_variables = r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\4_Proyecto_requerimientos_FNG\2_excel\3_macro_variables.xlsx' 
ruta_macro_variables = os.path.abspath(ruta_carpeta_codigo_fuente + "../../" + "/2_excel/3_macro_variables.xlsx")
#print(ruta_macro_variables)

# Convertir variable a tipo string
ruta_insumo = str(ruta_insumo)
ruta_respuesta = str(ruta_respuesta)
ruta_macro_variables = str(ruta_macro_variables)



########################################################################
#%%

print("\n")
print("\n")
print("Leyendo los Excel q estan dentro de la carpeta ...\\2_excel\\1_insumo")

# Leer uno o mas archivos q esta en una carpeta pero no se como se llaman los archivos
# ruta relativa (nombre del archivo)
# https://es.stackoverflow.com/questions/24278/c%C3%B3mo-listar-todos-los-archivos-de-una-carpeta-usando-python
# Guardar en una lista el nombre desconocido de los insumos de Excel
lista_nombre_excel_insumo = [arch.name 
                              for arch in scandir(ruta_insumo) 
                              if arch.is_file()
                            ]
# Se TIENE q imprimir el (o los) nombre(s) del Excel q esta dentro de ruta_insumo
#print(lista_nombre_excel_insumo)

# Crear una lista vacia
# En esta lista se guardan TODAS las rutas COMPLETAS (absoluta)
# de donde estan los Excel de los insumos
lista_de_rutas_absolutas = []

# Recorrer los elementos de una lista 
# En este for crearemos una lista con las rutas COMPLETAS (absoluta) de todos los insumos
#lista_de_rutas_absolutas =  r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\4_Proyecto_requerimientos_FNG\2_excel\1_insumo\1_identificacion_y_obligacion_repetidos.xlsx'
for elementos_1 in lista_nombre_excel_insumo: 
    #print("elementos_1", elementos_1)
    #print("lista_nombre_excel_insumo", lista_nombre_excel_insumo)

    # Concatenar (unir) rutas
    #agregar_ruta = ruta_insumo + "\\" + elementos_1
    agregar_ruta = os.path.join(f'{ruta_insumo}', f'{elementos_1}')
    #print(agregar_ruta)
    
    # Convertir variable a tipo string
    agregar_ruta = str(agregar_ruta)
    
    # Agregar nuevos elementos a la lista .append()
    # el resultado es una lista con las rutas absolutas (COMPLETAS) donde estan los Excel de insumo
    # lista_de_rutas_absolutas = (ruta_insumo) + (lista_nombre_excel_insumo)
    lista_de_rutas_absolutas.append(agregar_ruta)

# Imprimir la lista_de_rutas_absolutas
# Esto TIENE q imprimir TODAS las rutas de los Excel de los insumos
#print(lista_de_rutas_absolutas)

##############
#%%

""" CONEXION A LA BASE DE DATOS DE BANCOLOMBIA (LZ - HUE) """

#sp = Sparky('danpined','IMPALA_PROD', hostname='sbmdeblze003', remote=True)
"""
sp = Sparky(nombre_usuario,          # nombre de usuario de la base de datos
            'IMPALA_PROD',           # Nombre de la fuente de datos = DSN = Data Source Name
            clave,                   # contraseña de la base de datos
            hostname='sbmdeblze003', # nombre del Host
            remote=True              # como es igual a =True entonces la conexion a Sparky SI se hace de forma remota
           )
"""
# Verificar conexion a la base de datos
#print(sp)

"""
Sintaxis de sp = Sparky(...)

sp = Sparky(username, 
            ask_pwd=True, 
            password=None, 
            remote='infer', 
            hostname="sbmdeblze003.bancolombia.corp", 
            port=22, 
            show_outp=False, 
            helper=None, 
            logger=None
           )

Parameters
username : str
    Nombre de usuario con el que se va a conectar

ask_pwd : bool, Opcional, Default: True
    Si es True y no se ha proporcionado una contraseña en el parametro password se pedira la contraseña

password : str, Opcional
    Contraseña para conectarse, si no se proporciona y el parametro ask_pwd está en verdadero, sparky preguntará por ella

remote : bool, Opcional, Default: "infer"
    Determina si la ejecucion de Spark debe se hacerse de manera remota o local por defecto el propio Sparky determina si debe ser remoto o local

hostname : str, Opcional, Default: "sbmdeblze003.bancolombia.corp"
    Direccion del servidor al cual se desea conectarse si la conexion es remota

port : int, Opcional, Default: 22
    Puerto al que se conectará al servidor

show_outp : bool, Opcional, Default: False
    Si es True mostrará en tiempo real la salida del comando ejecutado

logger : Logger or dict, Opcional
    Objeto encargado de administrar y mostrar el avance del plan de ejecucion. Si se pasa un diccionario, este debe contener los parametros para la creacion del logger. Ver la documentacion de Logger para mayor informacion

"""

# Ejecutar consultas en la base de datos del banco y guardarlas en un DataFrame
# Para ver esta informacion: - Abrir inicio
#                            - Buscar ODBC
#                            - Click donde dice IMPALA_PROD
#                            - Se abre una ventana emergente, 
#                              ahi esta toda la info para conectarme a la BD
# https://stackoverflow.com/questions/60941924/connection-error-with-pyodbc-function-takes-at-most-1-non-keyword-argument

try:
    cn = pyodbc.connect(dsn="IMPALA_PROD",                        # Nombre de la fuente de datos = DSN = Data Source Name
                        driver="Cloudera ODBC Driver for Impala", # Nombre del Driver del ODBC para conectar a la base de datos
                        host="impala.bancolombia.corp",           # nombre del Host
                        #database="de",                           # Nombre de la base de datos
                        uid=nombre_usuario,                       # nombre de usuario de la base de datos = user ID = uid
                        pwd=clave,                                # contraseña de la base de datos
                        #Port=21050,                              # Numero del puerto
                        autocommit=True                           # Confirmacion automatica:
                                                                  # Cuando las consultas se ejecutan en la base de datos se confirman (hacen) los cambios inmediatamente
                        )
    # Verificar conexion a la base de datos
    #print(cn)

    print("\n")
    print("\n")
    print("Conectando a la base de datos de Bancolombia LZ - HUE")

except Exception:
    # Llamar funciones sin parametros
    print("\n")
    print("\n")
    imprimir_mensaje_de_error()
    
    print("\n")
    print("ERROR en la conexion a la base de datos de Bancolombia LZ - HUE", "\n",
          "posibles razones del error:", "\n",
          "1. No ha activado el VPN de Bancolombia", "\n",
          "2. LZ esta caido", "\n",
          "3. Digito mal su usuario o contraseña"
         )

    detener_la_ejecucion_del_codigo()



########################################################################
#%%

print("\n")
print("\n")
print("Leyendo Excel 3_macro_variables.xlsx")
print("\n")

"""
NOMBRE DE TABLAS, BASE DE DATOS (ZONA) Y COLUMNAS DINAMICAS

Permitir al usuario final q cuando abra el Excel macro_variables.xlsx
pueda modificar lo siguiente:
- Nombre de las nuevas columnas de los Excel q estan en ...\2_excel\2_respuesta
- Fechas de las consultas SQL (year, corte, ingestion_year, ingestion_month)
- Nombre de las tablas y base de datos (zona) de las consultas SQL
"""

# En cada variable voy a guardar cada uno de los datos 
# q se necesitan para ejecutar las consultas SQL  
# Definir las variables como vacias
# Esto se hace para q las variables existan
# y q NO aparezca el error "variable indefinida"
# variable=""

# variables de la consulta_1
nombre_columna_lz_identificacion_1           =""
max_lz_1                                     =""
nombre_nueva_columna_excel_respuesta_1       =""
nombre_base_de_datos_zona_lz_1               =""
nombre_tabla_lz_1                            =""
nombre_columna_lz_year_1                     =""
igual_year_1                                 =""
nombre_columna_lz_corte_1                    =""
igual_corte_1                                =""

# variables de la consulta_2
nombre_columna_lz_identificacion_2           =""
nombre_columna_lz_obligacion_2               =""
nombre_columna_lz_calife_2                   =""
nombre_nueva_columna_excel_respuesta_2       =""
nombre_columna_lz_fdesem_2                   =""
nombre_base_de_datos_zona_lz_2               =""
nombre_tabla_lz_2                            =""
nombre_columna_lz_year_2                     =""
nombre_columna_lz_ingestion_month_2          =""

# variables de la consulta_3
nombre_columna_lz_identificacion_3           =""
max_lz_3                                     =""
nombre_nueva_columna_excel_respuesta_3       =""
nombre_base_de_datos_zona_lz_3               =""
nombre_tabla_lz_3                            =""
nombre_columna_lz_year_3                     =""
igual_year_3                                 =""
nombre_columna_lz_corte_3                    =""
igual_corte_3                                =""

# variables de la consulta_4
# Forma 1 - consulta_4
nombre_columna_lz_identificacion_4_forma_1   =""
max_lz_4_forma_1                             =""
nombre_nueva_columna_excel_respuesta_4       =""
nombre_base_de_datos_zona_lz_4_forma_1       =""
nombre_tabla_lz_4_forma_1                    =""
nombre_columna_lz_year_4_forma_1             =""
igual_year_4_forma_1                         =""
nombre_columna_lz_month_4_forma_1            =""
igual_month_4_forma_1                        =""

# Forma 2 - consulta_4
nombre_columna_lz_identificacion_4_forma_2   =""
max_lz_4_forma_2                             =""
nombre_base_de_datos_zona_lz_4_forma_2       =""
nombre_tabla_lz_4_forma_2                    =""
nombre_columna_lz_year_4_forma_2             =""
igual_year_4_forma_2                         =""
nombre_columna_lz_month_4_forma_2            =""
igual_month_4_forma_2                        =""

# Forma 3 - consulta_4
nombre_columna_lz_identificacion_4_forma_3   =""
max_lz_4_forma_3                             =""
nombre_base_de_datos_zona_lz_4_forma_3       =""
nombre_tabla_lz_4_forma_3                    =""
nombre_columna_lz_year_4_forma_3             =""
igual_year_4_forma_3                         =""
nombre_columna_lz_month_4_forma_3            =""
igual_month_4_forma_3                        =""

""" consulta_1 - max_altamora_a_corte_de_febrero_20_del_cliente_interna """
# Leer los datos de entrada del usuario final
df_macro_variables_1 = pd.read_excel (
                       ruta_macro_variables,            
                       sheet_name = "consulta_1",    
                       header = 0,             
                       skiprows = 1,           
                       usecols = "A:I",
                       )
# Verificar q si se haya leido el DataFrame
#print(df_macro_variables_1)

# Extraer cada uno de los datos del DataFrame por separado
# Convertir de DataFrame a variable, para ello hacemos lo siguiente...

# .iloc[fila, columna] sirve para seleccionar filas y columnas de un DataFrame
# https://www.delftstack.com/es/howto/python-pandas/pandas-loc-vs-iloc-python/
nombre_columna_lz_identificacion_1 = df_macro_variables_1.iloc[0,0]
#print(nombre_columna_lz_identificacion_1)

max_lz_1 = df_macro_variables_1.iloc[0,1]
#print(max_lz_1)

nombre_nueva_columna_excel_respuesta_1 = df_macro_variables_1.iloc[0,2]
#print(nombre_nueva_columna_excel_respuesta_1)

nombre_base_de_datos_zona_lz_1 = df_macro_variables_1.iloc[0,3]
#print(nombre_base_de_datos_zona_lz_1)

nombre_tabla_lz_1 = df_macro_variables_1.iloc[0,4]
#print(nombre_tabla_lz_1)

nombre_columna_lz_year_1 = df_macro_variables_1.iloc[0,5]
#print(nombre_columna_lz_year_1)

igual_year_1 = df_macro_variables_1.iloc[0,6]
#print(igual_year_1)

nombre_columna_lz_corte_1 = df_macro_variables_1.iloc[0,7]
#print(nombre_columna_lz_corte_1)

igual_corte_1 = df_macro_variables_1.iloc[0,8]
#print(igual_corte_1)

# Convertir variable a tipo string
nombre_columna_lz_identificacion_1        = str(nombre_columna_lz_identificacion_1)
max_lz_1                                  = str(max_lz_1)
nombre_nueva_columna_excel_respuesta_1    = str(nombre_nueva_columna_excel_respuesta_1)
nombre_base_de_datos_zona_lz_1            = str(nombre_base_de_datos_zona_lz_1)
nombre_tabla_lz_1                         = str(nombre_tabla_lz_1)
nombre_columna_lz_year_1                  = str(nombre_columna_lz_year_1)
igual_year_1                              = str(igual_year_1)
nombre_columna_lz_corte_1                 = str(nombre_columna_lz_corte_1)
igual_corte_1                             = str(igual_corte_1)


""" consulta_2 - calificacion_al_momento_del_desembolso """
df_macro_variables_2 = pd.read_excel (
                       ruta_macro_variables,            
                       sheet_name = "consulta_2",    
                       header = 0,             
                       skiprows = 2,           
                       usecols = "A:I",
                       )
# Verificar q si se haya leido el DataFrame
#print(df_macro_variables_2)

nombre_columna_lz_identificacion_2 = df_macro_variables_2.iloc[0,0]
#print(nombre_columna_lz_identificacion_2)

nombre_columna_lz_obligacion_2 = df_macro_variables_2.iloc[0,1]
#print(nombre_columna_lz_obligacion_2)

nombre_columna_lz_calife_2 = df_macro_variables_2.iloc[0,2]
#print(nombre_columna_lz_calife_2)

nombre_nueva_columna_excel_respuesta_2 = df_macro_variables_2.iloc[0,3]
#print(nombre_nueva_columna_excel_respuesta_2)

nombre_columna_lz_fdesem_2 = df_macro_variables_2.iloc[0,4]
#print(nombre_columna_lz_fdesem_2)

nombre_base_de_datos_zona_lz_2 = df_macro_variables_2.iloc[0,5]
#print(nombre_base_de_datos_zona_lz_2)

nombre_tabla_lz_2 = df_macro_variables_2.iloc[0,6]
#print(nombre_tabla_lz_2)

nombre_columna_lz_year_2 = df_macro_variables_2.iloc[0,7]
#print(nombre_columna_lz_year_2)

nombre_columna_lz_ingestion_month_2 = df_macro_variables_2.iloc[0,8]
#print(nombre_columna_lz_ingestion_month_2)

nombre_columna_lz_identificacion_2       = str(nombre_columna_lz_identificacion_2)
nombre_columna_lz_obligacion_2           = str(nombre_columna_lz_obligacion_2)
nombre_columna_lz_calife_2               = str(nombre_columna_lz_calife_2)
nombre_nueva_columna_excel_respuesta_2   = str(nombre_nueva_columna_excel_respuesta_2)
nombre_columna_lz_fdesem_2               = str(nombre_columna_lz_fdesem_2 )
nombre_base_de_datos_zona_lz_2           = str(nombre_base_de_datos_zona_lz_2)
nombre_tabla_lz_2                        = str(nombre_tabla_lz_2)
nombre_columna_lz_year_2                 = str(nombre_columna_lz_year_2)
nombre_columna_lz_ingestion_month_2      = str(nombre_columna_lz_ingestion_month_2)


""" consulta_3 - max_calificacion_del_cliente_a_corte_de_junio_20 """
df_macro_variables_3 = pd.read_excel (
                       ruta_macro_variables,
                       sheet_name = "consulta_3",    
                       header = 0,             
                       skiprows = 1,           
                       usecols = "A:I",
                       )

#print(df_macro_variables_3)

nombre_columna_lz_identificacion_3 = df_macro_variables_3.iloc[0,0]
#print(nombre_columna_lz_identificacion_3)

max_lz_3 = df_macro_variables_3.iloc[0,1]
#print(max_lz_3)

nombre_nueva_columna_excel_respuesta_3 = df_macro_variables_3.iloc[0,2]
#print(nombre_nueva_columna_excel_respuesta_3)

nombre_base_de_datos_zona_lz_3 = df_macro_variables_3.iloc[0,3]
#print(nombre_base_de_datos_zona_lz_3)

nombre_tabla_lz_3 = df_macro_variables_3.iloc[0,4]
#print(nombre_tabla_lz_3)

nombre_columna_lz_year_3 = df_macro_variables_3.iloc[0,5]
#print(nombre_columna_lz_year_3)

igual_year_3 = df_macro_variables_3.iloc[0,6]
#print(igual_year_3)

nombre_columna_lz_corte_3 = df_macro_variables_3.iloc[0,7]
#print(nombre_columna_lz_corte_3)

igual_corte_3 = df_macro_variables_3.iloc[0,8]
#print(igual_corte_3)

nombre_columna_lz_identificacion_3       = str(nombre_columna_lz_identificacion_3)
max_lz_3                                 = str(max_lz_3)
nombre_nueva_columna_excel_respuesta_3   = str(nombre_nueva_columna_excel_respuesta_3)
nombre_base_de_datos_zona_lz_3           = str(nombre_base_de_datos_zona_lz_3)
nombre_tabla_lz_3                        = str(nombre_tabla_lz_3)
nombre_columna_lz_year_3                 = str(nombre_columna_lz_year_3)
igual_year_3                             = str(igual_year_3)
nombre_columna_lz_corte_3                = str(nombre_columna_lz_corte_3 )
igual_corte_3                            = str(igual_corte_3)

""" consulta_4 - max_altamora_a_corte_de_febrero_20_del_cliente_externa """
df_macro_variables_4 = pd.read_excel (
                       ruta_macro_variables,
                       sheet_name = "consulta_4",    
                       header = 0,             
                       skiprows = 1,           
                       usecols = "A:Y",
                       )

#print(df_macro_variables_4)

nombre_columna_lz_identificacion_4_forma_1 = df_macro_variables_4.iloc[0,0]
#print(nombre_columna_lz_identificacion_4_forma_1)

max_lz_4_forma_1 = df_macro_variables_4.iloc[0,1]
#print(max_lz_4_forma_1)

nombre_nueva_columna_excel_respuesta_4 = df_macro_variables_4.iloc[0,2]
#print(nombre_nueva_columna_excel_respuesta_4)

nombre_base_de_datos_zona_lz_4_forma_1 = df_macro_variables_4.iloc[0,3]
#print(nombre_base_de_datos_zona_lz_4_forma_1)

nombre_tabla_lz_4_forma_1 = df_macro_variables_4.iloc[0,4]
#print(nombre_tabla_lz_4_forma_1)

nombre_columna_lz_year_4_forma_1 = df_macro_variables_4.iloc[0,5]
#print(nombre_columna_lz_year_4_forma_1)

igual_year_4_forma_1 = df_macro_variables_4.iloc[0,6]
#print(igual_year_4_forma_1)

nombre_columna_lz_month_4_forma_1 = df_macro_variables_4.iloc[0,7]
#print(nombre_columna_lz_month_4_forma_1)

igual_month_4_forma_1 = df_macro_variables_4.iloc[0,8]
#print(igual_month_4_forma_1)

nombre_columna_lz_identificacion_4_forma_2 = df_macro_variables_4.iloc[0,9]
#print(nombre_columna_lz_identificacion_4_forma_2)

max_lz_4_forma_2 = df_macro_variables_4.iloc[0,10]
#print(max_lz_4_forma_2)

nombre_base_de_datos_zona_lz_4_forma_2 = df_macro_variables_4.iloc[0,11]
#print(nombre_base_de_datos_zona_lz_4_forma_2)

nombre_tabla_lz_4_forma_2 = df_macro_variables_4.iloc[0,12]
#print(nombre_tabla_lz_4_forma_2)

nombre_columna_lz_year_4_forma_2 = df_macro_variables_4.iloc[0,13]
#print(nombre_columna_lz_year_4_forma_2)

igual_year_4_forma_2 = df_macro_variables_4.iloc[0,14]
#print(igual_year_4_forma_2)

nombre_columna_lz_month_4_forma_2 = df_macro_variables_4.iloc[0,15]
#print(nombre_columna_lz_month_4_forma_2)

igual_month_4_forma_2 = df_macro_variables_4.iloc[0,16]
#print(igual_month_4_forma_2)

nombre_columna_lz_identificacion_4_forma_3 = df_macro_variables_4.iloc[0,17]
#print(nombre_columna_lz_identificacion_4_forma_3)

max_lz_4_forma_3 = df_macro_variables_4.iloc[0,18]
#print(max_lz_4_forma_3)

nombre_base_de_datos_zona_lz_4_forma_3 = df_macro_variables_4.iloc[0,19]
#print(nombre_base_de_datos_zona_lz_4_forma_3)

nombre_tabla_lz_4_forma_3 = df_macro_variables_4.iloc[0,20]
#print(nombre_tabla_lz_4_forma_3)

nombre_columna_lz_year_4_forma_3 = df_macro_variables_4.iloc[0,21]
#print(nombre_columna_lz_year_4_forma_3)

igual_year_4_forma_3 = df_macro_variables_4.iloc[0,22]
#print(igual_year_4_forma_3)

nombre_columna_lz_month_4_forma_3 = df_macro_variables_4.iloc[0,23]
#print(nombre_columna_lz_month_4_forma_3)

igual_month_4_forma_3 = df_macro_variables_4.iloc[0,24]
#print(igual_month_4_forma_3)

# Forma 1 - consulta_4
nombre_columna_lz_identificacion_4_forma_1   = str(nombre_columna_lz_identificacion_4_forma_1)
max_lz_4_forma_1                             = str(max_lz_4_forma_1)
nombre_nueva_columna_excel_respuesta_4       = str(nombre_nueva_columna_excel_respuesta_4)
nombre_base_de_datos_zona_lz_4_forma_1       = str(nombre_base_de_datos_zona_lz_4_forma_1)
nombre_tabla_lz_4_forma_1                    = str(nombre_tabla_lz_4_forma_1)
nombre_columna_lz_year_4_forma_1             = str(nombre_columna_lz_year_4_forma_1)
igual_year_4_forma_1                         = str(igual_year_4_forma_1)
nombre_columna_lz_month_4_forma_1            = str(nombre_columna_lz_month_4_forma_1)
igual_month_4_forma_1                        = str(igual_month_4_forma_1)

# Forma 2 - consulta_4
nombre_columna_lz_identificacion_4_forma_2   = str(nombre_columna_lz_identificacion_4_forma_2)
max_lz_4_forma_2                             = str(max_lz_4_forma_2)
nombre_base_de_datos_zona_lz_4_forma_2       = str(nombre_base_de_datos_zona_lz_4_forma_2)
nombre_tabla_lz_4_forma_2                    = str(nombre_tabla_lz_4_forma_2)
nombre_columna_lz_year_4_forma_2             = str(nombre_columna_lz_year_4_forma_2)
igual_year_4_forma_2                         = str(igual_year_4_forma_2)
nombre_columna_lz_month_4_forma_2            = str(nombre_columna_lz_month_4_forma_2)
igual_month_4_forma_2                        = str(igual_month_4_forma_2)

# Forma 3 - consulta_4
nombre_columna_lz_identificacion_4_forma_3   = str(nombre_columna_lz_identificacion_4_forma_3)
max_lz_4_forma_3                             = str(max_lz_4_forma_3)
nombre_base_de_datos_zona_lz_4_forma_3       = str(nombre_base_de_datos_zona_lz_4_forma_3)
nombre_tabla_lz_4_forma_3                    = str(nombre_tabla_lz_4_forma_3)
nombre_columna_lz_year_4_forma_3             = str(nombre_columna_lz_year_4_forma_3)
igual_year_4_forma_3                         = str(igual_year_4_forma_3)
nombre_columna_lz_month_4_forma_3            = str(nombre_columna_lz_month_4_forma_3)
igual_month_4_forma_3                        = str(igual_month_4_forma_3)



########################################################################
#%%

""" Validacion de datos """
# SI cualquiera de las casillas q estan en el Excel 3_macro_variables.xlsx estan vacias
# entonces detener la ejecucion del codigo
# Esto es asi porq 
# para q las consultas SQL funcionen estas variables NO pueden estar vacias ==""

if (# variables de la consulta_1
    nombre_columna_lz_identificacion_1              ==""
    or max_lz_1                                     ==""
    or nombre_nueva_columna_excel_respuesta_1       ==""
    or nombre_base_de_datos_zona_lz_1               ==""
    or nombre_tabla_lz_1                            ==""
    or nombre_columna_lz_year_1                     ==""
    or igual_year_1                                 ==""
    or nombre_columna_lz_corte_1                    ==""
    or igual_corte_1                                ==""
    
    # variables de la consulta_2
    or nombre_columna_lz_identificacion_2           ==""
    or nombre_columna_lz_obligacion_2               ==""
    or nombre_columna_lz_calife_2                   ==""
    or nombre_nueva_columna_excel_respuesta_2       ==""
    or nombre_columna_lz_fdesem_2                   ==""
    or nombre_base_de_datos_zona_lz_2               ==""
    or nombre_tabla_lz_2                            ==""
    or nombre_columna_lz_year_2                     ==""
    or nombre_columna_lz_ingestion_month_2          ==""

    # variables de la consulta_3
    or nombre_columna_lz_identificacion_3           ==""
    or max_lz_3                                     ==""
    or nombre_nueva_columna_excel_respuesta_3       ==""
    or nombre_base_de_datos_zona_lz_3               ==""
    or nombre_tabla_lz_3                            ==""
    or nombre_columna_lz_year_3                     ==""
    or igual_year_3                                 ==""
    or nombre_columna_lz_corte_3                    ==""
    or igual_corte_3                                ==""
    
    # variables de la consulta_4
    # Forma 1 - consulta_4
    or nombre_columna_lz_identificacion_4_forma_1   ==""
    or max_lz_4_forma_1                             ==""
    or nombre_nueva_columna_excel_respuesta_4       ==""
    or nombre_base_de_datos_zona_lz_4_forma_1       ==""
    or nombre_tabla_lz_4_forma_1                    ==""
    or nombre_columna_lz_year_4_forma_1             ==""
    or igual_year_4_forma_1                         ==""
    or nombre_columna_lz_month_4_forma_1            ==""
    or igual_month_4_forma_1                        ==""

    # Forma 2 - consulta_4
    or nombre_columna_lz_identificacion_4_forma_2   ==""
    or max_lz_4_forma_2                             ==""
    or nombre_base_de_datos_zona_lz_4_forma_2       ==""
    or nombre_tabla_lz_4_forma_2                    ==""
    or nombre_columna_lz_year_4_forma_2             ==""
    or igual_year_4_forma_2                         ==""
    or nombre_columna_lz_month_4_forma_2            ==""
    or igual_month_4_forma_2                        ==""

    # Forma 3 - consulta_4
    or nombre_columna_lz_identificacion_4_forma_3   ==""
    or max_lz_4_forma_3                             ==""
    or nombre_base_de_datos_zona_lz_4_forma_3       ==""
    or nombre_tabla_lz_4_forma_3                    ==""
    or nombre_columna_lz_year_4_forma_3             ==""
    or igual_year_4_forma_3                         ==""
    or nombre_columna_lz_month_4_forma_3            ==""
    or igual_month_4_forma_3                        ==""
   ):   
    print("\n")
    print("\n")
    print("Faltan datos por llenar en el Excel llamado 3_macro_variables.xlsx")
    print("verifique que halla llenado todos los datos y despues vuelva a ejecutar el codigo")
    # Llamar funcion sin parametros 
    # para detener la ejecucion del codigo
    detener_la_ejecucion_del_codigo()

########################################################################
#%%

# Lo UNICO q se demora en ejecutarse en todo el codigo son las consultas SQL
# esto pasa por q Python tienen q hacer unas peticiones al servidor de la base de datos (Impala Server)
# Excepto por esto, todo lo q se ejecuta en local es rapido (dentro del computador, sin hacer peticiones)
# Por eso cuando llego a la parte de ejecutar las consultas SQL muestro un mensaje de cargando
# El tiempo aproximado en q se demora en ejecutar es: 80 filas en menos de un minuto

print("\n")
print("\n")

print("█▀▀ ▄▀█ █▀█ █▀▀ ▄▀█ █▄░█ █▀▄ █▀█")
print("█▄▄ █▀█ █▀▄ █▄█ █▀█ █░▀█ █▄▀ █▄█")
print("\n")

print("▒█▀▀▀ █▀▀ █▀▀█ █▀▀ █▀▀█ █▀▀")
print("▒█▀▀▀ ▀▀█ █░░█ █▀▀ █▄▄▀ █▀▀")
print("▒█▄▄▄ ▀▀▀ █▀▀▀ ▀▀▀ ▀░▀▀ ▀▀▀")
print("          █")
print("\n")
print("Ejecutando consultas SQL q estan en sql_consultas_llenar_campos.sql")

""" 
LEER TODOS LOS EXCEL Q ESTAN EN ...\1_insumo
Y CREACION DE LOS EXCEL EN ...\2_respuesta 
AGREGANDO EL RESULTADO DE LAS CONSULTAS SQL EN NUEVAS COLUMNAS 
"""

# Crear DataFrame vacio
# https://www.statology.org/pandas-create-dataframe-with-column-names/
df_insumo = pd.DataFrame()
df_respuesta = pd.DataFrame()

# df_insumo                 = Lee todos los archivos de Excel de la carpeta ...\1_insumo
# df_ejecutar_consulta_X    = Guarda UN SOLO resultado de la consulta 
# df_resultado_consulta_X   = Agrega nuevas filas con los resultados de las consultas de df_ejecutar_consulta_X, 
#                             contiene TODOS los resultados juntos de CADA UNO de los Excel de insumo

# Crear una lista con el orden q quiero darle a las PRIMERAS columnas del df_respuesta
# Esto lo hago FUERA del for elementos_2 in lista_de_rutas_absolutas:
# porq TODOS los Excel q estan dentro de la carpeta ...\2_excel\2_respuesta tienen el mismo orden en las primeras columnas
# el orden es el siguiente:
lista_orden_primeras_columnas_df_respuesta = ["numero_identificacion",               
                                              "numero_obligacion",                    
                                               nombre_nueva_columna_excel_respuesta_1, # = max_altamora_a_corte_de_febrero_20_del_cliente_interna
                                               nombre_nueva_columna_excel_respuesta_2, # = calificacion_al_momento_del_desembolso 
                                               nombre_nueva_columna_excel_respuesta_3, # = max_calificacion_del_cliente_a_corte_de_junio_20
                                               nombre_nueva_columna_excel_respuesta_4, # = max_altamora_a_corte_de_febrero_20_del_cliente_externa
                                              ]
#print(lista_orden_primeras_columnas_df_respuesta)

# Crear un contador
# Esto sirve para recorrer lista_nombre_excel_insumo
i = 0

# Hacer q la variable para guardar la consulta_2 exista
# esto se hace para q NO aparezca el error "variable indefinida"
consulta_2=""

# Leer uno o mas Excel en un solo DataFrame
# En este for creamos un DataFrame para leer TODOS los Excel q estan dentro de la carpeta 
# ...\4_Proyecto_requerimientos_FNG\2_excel\1_insumo

#elementos_2 =  r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\4_Proyecto_requerimientos_FNG\2_excel\1_insumo\1_prueba.xlsx'
#elementos_2 =  r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\4_Proyecto_requerimientos_FNG\2_excel\1_insumo\2_validacion_FNG.xlsx'

for elementos_2 in lista_de_rutas_absolutas: # esto va a leer todos los N Excel de los insumos
                                             # Recorrer los elementos de la lista_de_rutas_absolutas
  #print("\n")  
  #print("elementos_2", elementos_2)

  """ Validacion de datos """  
  try: # Cuando NO se produzca un error entonces leer los Excel q estan dentro de la carpeta ...\2_excel\1_insumo
      df_insumo = pd.read_excel (
                  elementos_2,            # Ruta donde esta guardado el Excel
                  sheet_name = "Base",    # Nombre de la hoja de Excel
                  header = 0,             # Numero de fila donde estan los nombres de las columnas
                  skiprows = 0,           # Desde esta Fila hacia abajo se empieza a leer el Excel

                  # C:\Users\danpined\Anaconda3\lib\site-packages\pandas\io\excel\_base.py:
                  # 1292: FutureWarning: 
                  # Defining usecols with out of bounds indices is deprecated and will raise a ParserError in a future version.
                  # **kwds
                  usecols = "A:XFD",      # Letras de las columnas del Excel a leer
                                          # SIEMPRE se empieza desde la columna A
                                          # y le puse HASTA XFD porq esa es la ultima columna q existe en Excel
                                          # asi cuando se agregan nuevas columnas en los Excel de insumos se leeran automaticamente
                                          # https://excellover.com/aprende-excel/que-es-excel#:~:text=Que%20es%20una%20hoja%20de%20Excel,-Una%20hoja%20de&text=Cada%20una%20de%20las%20columnas,la%20AA%20hasta%20la%20XFD.
                  )
  # Imprimir df_insumo
  # se TIENEN q imprimir TODOS los Excel de los insumos
  #print("df_insumo", df_insumo) 

  except Exception: # Cuando SI se produza un error entonces detener la ejecucion del codigo
      # Llamar funciones sin parametros
      print("\n")
      print("\n")
      imprimir_mensaje_de_error()

      print("\n")
      print("ERROR al leer los Excel que estan dentro de la carpeta ...\\2_excel\\1_insumo", "\n",
            "verifique lo siguiente en TODOS los Excel que estan dentro de la carpeta ...\\2_excel\\1_insumo", "\n",
            "1. La extension de los archivos de Excel tiene que ser .xlsx", "\n",
            "2. Las hojas de Excel donde esta el numero_identificacion y numero_obligacion se tienen que llamar Base (con la letra B en mayuscula y el resto de la palabra en minuscula)", "\n",
            "3. Solo puede haber una sola hoja de Excel por cada insumo que se llame Base. Ejemplo: Si son 10 insumos entonces hay 10 hojas de Excel que se llaman Base", "\n",
            "4. Despues de hacer lo anterior vuelva a ejecutar el codigo otra vez"
           )

      detener_la_ejecucion_del_codigo()


  # Eliminar los espacios en blanco al principio y al final
  # de los nombres de las columnas del DataFrame df_insumo
  # https://stackoverflow.com/questions/43332057/pandas-strip-white-space
  df_insumo.columns = df_insumo.columns.str.strip()
  # Verificar q se hayan eliminado los espacios en blanco
  #print(df_insumo.columns)

  # Llamar funcion sin parametros 
  # para cambiar el nombre de las columnas... 
  # numero_identificacion
  cambiar_nombre_columna_numero_identificacion()
  # y numero_obligacion
  cambiar_nombre_columna_numero_obligacion()
  # Verificar q si se hayan cambiado los nombres de las columnas
  #print("numero_obligacion", "\n", df_insumo["numero_obligacion"])
  #print("numero_identificacion", "\n", df_insumo["numero_identificacion"])

  # Convertir las columnas numero_obligacion y numero_identificacion a tipo int64
  # https://stackoverflow.com/questions/15891038/change-column-type-in-pandas
  """ Validacion de datos """
  try: # Cuando NO se produzca un error entonces SI se convierte la columna a tipo int 64 
      df_insumo["numero_identificacion"] = df_insumo["numero_identificacion"].astype("int64") 
      #print("se ha convertido la columna numero_identificacion a tipo int 64")
      
  except Exception: # Cuando SI se produza un error entonces detener la ejecucion del codigo
      # Llamar funciones sin parametros
      print("\n")
      print("\n")
      imprimir_mensaje_de_error()

      print("\n")
      print("ERROR en la columna numero_identificacion", "\n",
            "verifique lo siguiente en TODOS los Excel que estan dentro de la carpeta ...\\2_excel\\1_insumo", "\n",
           "1. En la columna numero_identificacion solamente puede haber casillas con números, no se puede escribir #N/D ni otra cosa diferente", "\n",
           "2. Los numeros no pueden tener espacios en blanco ni al principio ni al final", "\n",
           "3. Despues de hacer lo anterior vuelva a ejecutar el codigo otra vez"
           )

      detener_la_ejecucion_del_codigo()


  try:
      df_insumo["numero_obligacion"] = df_insumo["numero_obligacion"].astype("int64") 
      #print("se ha convertido la columna numero_obligacion a tipo int 64")
      
  except Exception: 
      print("\n")
      print("\n")
      imprimir_mensaje_de_error()

      print("\n")
      print("ERROR en la columna numero_obligacion", "\n",
            "verifique lo siguiente en TODOS los Excel que estan dentro de la carpeta ...\\2_excel\\1_insumo", "\n",
           "1. En la columna numero_obligacion solamente puede haber casillas con números, no se puede escribir #N/D ni otra cosa diferente", "\n",
           "2. Los numeros no pueden tener espacios en blanco ni al principio ni al final", "\n",
           "3. Despues de hacer lo anterior vuelva a ejecutar el codigo otra vez"
           )

      detener_la_ejecucion_del_codigo()

  # Verificar el tipo de dato de las columnas numero_obligacion y numero_identificacion
  #df_insumo["numero_obligacion"].dtypes
  #df_insumo["numero_identificacion"].dtypes
  
  # Reiniciar indices del DataFrame
  df_insumo = df_insumo.reset_index(drop=True)

  """
  Ejecutar consultas para N filas (identificaciones y obligaciones) 
  de los Excel de insumos
  """
  # Guardar en una variable de tipo sereies TODOS los numero_identificacion
  # y en otra variable los numero_obligacion
  # En cada variable se guarda la columna COMPLETA con el numero_identificacion y numero_obligacion
  numero_identificacion = df_insumo["numero_identificacion"]
  #print("numero_identificacion", "\n", numero_identificacion)
  
  numero_obligacion = df_insumo["numero_obligacion"]
  #print("numero_obligacion", "\n", numero_obligacion)
  
  # Convertir de tipo series a lista
  # https://www.programiz.com/python-programming/methods/built-in/list
  numero_identificacion = list(numero_identificacion)
  #print("numero_identificacion", "\n", numero_identificacion)

  numero_obligacion = list(numero_obligacion)
  #print("numero_obligacion", "\n", numero_obligacion)

  """
  consulta_1

  numero_identificacion
  max_altamora_a_corte_de_febrero_20_del_cliente_interna
  """

  # Reemplazar el valor de una variable dentro de una cadena de texto:
  # https://stackoverflow.com/questions/44462209/how-to-insert-a-variable-value-in-a-string-in-python

  # Las variables tipo string con las consultas SQL 
  # se TIENEN q leer DENTRO del for elementos_2 in lista_de_rutas_absolutas
  # porque los ID van a cambiar entre cada uno de los Excel de insumos q estan en la carpeta ...\2_excel\1_insumo
  consulta_1 ="""
  SELECT {} AS numero_identificacion,
         MAX({}) AS {} 
  FROM {}.{} 
  WHERE (CAST({} AS BIGINT)={}) 
        AND (CAST({} AS BIGINT)={}) 
        AND (CAST({} AS BIGINT)) 
        IN (/*aqui va el numero_identificacion*/{}) 
  GROUP BY 1
  ;
  """.format(# Estas variables de aqui abajo se leen del Excel ...\2_excel\3_macro_variables.xlsx
             # Como en este caso en especifico las variables se llaman ..._1 entonces se leen de la hoja de Excel llamada consulta_1
             # Y asi sucesivamente con los otros: 
             # para variables llamadas ..._2 se lee la hoja de Excel consulta_2
             # "                     " ..._3 "                              "_3
             # "                     " ..._4 "                              "_4
             nombre_columna_lz_identificacion_1,
             max_lz_1,
             nombre_nueva_columna_excel_respuesta_1,
             nombre_base_de_datos_zona_lz_1,
             nombre_tabla_lz_1,
             nombre_columna_lz_year_1,
             igual_year_1,
             nombre_columna_lz_corte_1,
             igual_corte_1,
             nombre_columna_lz_identificacion_1,
             # Y esta ultima variable se lee de los Excel que estan dentro de la carpeta ...\2_excel\1_insumo
             # Esta variable en algunas consultas es numero_identificacion y en otras numero_obligacion
             numero_identificacion
            )

  # Verificar q se haya guardado la consulta
  # y se hayan reemplazado todas las variables
  #print(consulta_1)
  
  # Convertir variable a tipo string
  consulta_1 = str(consulta_1)

  # Eliminar los corchetes [ ] q fueran insertados por la lista
  consulta_1 = consulta_1.replace("[","")
  consulta_1 = consulta_1.replace("]","")
  # Verificar q si se hayan eliminado los corchetes [ ] de la consulta_X
  #print(consulta_1)

  # Captura de errores usando try: except Exception:
  try: # Cuando NO se produzca un error entonces SI se ejecutara la consulta_X
      # Guardar cada uno de los resultados de las consultas en un DataFrame
      df_ejecutar_consulta_1 = pd.read_sql_query(consulta_1, cn)
      #print("\n")
      #print("EJECUTANDO CONSULTA_1", "\n", consulta_1)

      # Verificar q se haya ejecutado la consulta
      #print(df_ejecutar_consulta_1)

  except Exception: # Cuando SI se produza un error entonces detener la ejecucion del codigo
      # Llamar funciones sin parametros
      print("\n")
      print("\n")
      imprimir_mensaje_de_error()

      print("\n")
      print("Se ha producido un error al ejecutar la CONSULTA_1, las posibles causas del error son:", "\n",
            "1. Verifique que este conectado al VPN de Bancolombia", "\n",
            "2. Verifique que haya digitado correctamente los datos en las casillas del Excel 3_macro_variables.xlsx", "\n",
            "3. Verifique que haya escrito correctamente su nombre de usuario y contraseña corporativo", "\n",
            "4. Despues de hacer lo anterior vuelva a ejecutar el codigo otra vez"
            )

      detener_la_ejecucion_del_codigo()

  # Convertir la columna numero_identificacion del df_ejecutar_consulta_X a tipo int64
  #df_ejecutar_consulta_1["numero_identificacion"] = df_ejecutar_consulta_1["numero_identificacion"].astype("int64")
  # Verificar el tipo de dato de la columna
  #print(df_ejecutar_consulta_1["numero_identificacion"].dtypes)

  """ 
  Tipos de JOIN en SQL:
  https://stackoverflow.com/questions/5706437/whats-the-difference-between-inner-join-left-join-right-join-and-full-join

  Hacer un RIGTH JOIN (merge, union) en Python entre...  
  df_ejecutar_consulta_X
  df_insumo

  El resultado del JOIN es el df_respuesta
  """

  df_respuesta = pd.merge(left       = df_ejecutar_consulta_1,      # Tabla 1 de la izquierda
                          right      = df_insumo,                   # Tabla 2 de la derecha
                          how        = "right",                     # how= ... es el tipo de JOIN
                                                                    # En este caso es un RIGHT JOIN porq:
                                                                    # - Necesito todos los registros de df_insumo
                                                                    # - Necesito los numero_identificacion 
                                                                    #   q coincidan en ambos DataFrame (df_ejecutar_consulta_X y df_insumo)                                                           
                          left_on    = ["numero_identificacion"],   # Nombre columna de la tabla izquierda left = ... por la q se unen los datos
                          right_on   = ["numero_identificacion"]               # "                        " derecha  right = ... "                        "
                          )

  #print(df_respuesta)
  
  # Convertir las columnas numero_obligacion y numero_identificacion a tipo int64
  df_respuesta["numero_obligacion"] = df_respuesta["numero_obligacion"].astype("int64")
  df_respuesta["numero_identificacion"] = df_respuesta["numero_identificacion"].astype("int64")
  # Verificar el tipo de dato de las columnas numero_obligacion y numero_identificacion
  #df_respuesta["numero_obligacion"].dtypes
  #df_respuesta["numero_identificacion"].dtypes

  """ 
  Las siguientes veces despues de la primera el RIGHT JOIN se hace asi:

  df_respuesta = pd.merge(left       = df_ejecutar_consulta_X,      
                          right      = df_respuesta,                 
                          how        = "right",                                                                             
                          left_on    = ["numero_..."],
                          right_on   = ["numero_..."]
                          )

  """

  """ 
  consulta_2
  
  numero_obligacion
  calificacion_al_momento_del_desembolso 
  
  NO tengo permisos para la tabla proceso_riesgos.ceniegarc_preliminar
  """

  # Llamar funcion sin parametros 
  # para cambiar el nombre de la columna fecha_desembolso
  #cambiar_nombre_columna_fecha_desembolso()
  # Verificar q si se haya cambiado el nombre de la columna
  #print("fecha_desembolso", "\n", df_insumo["fecha_desembolso"])
  
  # En la consulta_2 dependiendo de la tabla las consultas son diferentes
  
  """
  if-else en Excel
  esto esta en 3_macro_variables.xlsx en la hoja consulta_2
  y es la misma condicion q escribire a continuacion en Python
  
  Funcion SI() (if-else) en Excel
  https://support.microsoft.com/es-es/office/funci%C3%B3n-si-f%C3%B3rmulas-anidadas-y-c%C3%B3mo-evitar-problemas-0b22ff44-f149-44ba-aeb5-4ef99da241c8
  
  Funcion Y() (and) en Excel
  https://support.microsoft.com/es-es/office/y-funci%C3%B3n-y-5f19b2e8-e1df-4408-897a-ce285a19e9d9
  
  =SI(Y(F3="resultados_riesgos",
      G3="ceniegarc_lz"
     ), "tabla DEFINITIVA",

    SI(Y(F3="proceso_riesgos",
         G3="ceniegarc_preliminar"
         ),"tabla PRE-LIMINAR",

       SI(Y(F3="resultados_riesgos",
            G3="ceniegarc_preliminar"
           ),"tabla PRE-LIMINAR",
          "ERROR"
         )
      )
   )
  """

  """Validacion de datos"""
  # Esta es la misma condicion if-else del Excel 3_macro_variables.xlsx pero en Python
  # SI la tabla es DEFINITIVA - resultados_riesgos.ceniegarc_lz
  if(nombre_base_de_datos_zona_lz_2      =="resultados_riesgos"
     and nombre_tabla_lz_2               =="ceniegarc_lz"
    ):
      # Entonces la consulta_2 es:
      # ROW_NUMBER() OVER(PARTITION BY ...) 
      
      # Crear una variable con una cadena de texto (de tipo string) en varias lineas de codigo
      # https://www.delftstack.com/es/howto/python/python-multi-line-string/
      consulta_2 = """
      WITH tabla_temporal AS
      (
        SELECT {} AS numero_identificacion,
               {} AS numero_obligacion,
               {} AS {},
               ROW_NUMBER() OVER(PARTITION BY {}
                                 ORDER BY {} ASC
                                ) AS numero
        FROM {}.{} 
        WHERE (
               (({}*100) + {}) = (SELECT MAX(({}*100) + {})
                                                 FROM {}.{}
                                                )
              )
              AND (CAST({} AS BIGINT) IN (/*aqui va el numero_obligacion*/{}
                                         )
                  )
      )
      SELECT numero_identificacion,
             numero_obligacion,
             {}
      FROM tabla_temporal
      WHERE numero=1
      ;  
      """.format(nombre_columna_lz_identificacion_2,
                 nombre_columna_lz_obligacion_2,
                 nombre_columna_lz_calife_2,
                 nombre_nueva_columna_excel_respuesta_2,
                 nombre_columna_lz_obligacion_2,
                 nombre_columna_lz_fdesem_2,
                 nombre_base_de_datos_zona_lz_2,
                 nombre_tabla_lz_2,
                 nombre_columna_lz_year_2,
                 nombre_columna_lz_ingestion_month_2,
                 nombre_columna_lz_year_2,
                 nombre_columna_lz_ingestion_month_2,
                 nombre_base_de_datos_zona_lz_2,
                 nombre_tabla_lz_2,
                 nombre_columna_lz_obligacion_2,
                 numero_obligacion,
                 nombre_nueva_columna_excel_respuesta_2
                )
      # Verificar q el codigo entre a esta condicion
      #print("\n", "se ha ejecutado el if (resultados_riesgos.ceniegarc_lz)","\n",consulta_2)

  # SI NO la tabla es PRE-LIMINAR - proceso_riesgos.ceniegarc_preliminar
  elif (nombre_base_de_datos_zona_lz_2   =="proceso_riesgos"
        and nombre_tabla_lz_2            =="ceniegarc_preliminar"
       ):
      # Entonces la consulta_2 es:
      # SELECT ...
      consulta_2 = """
      SELECT {} AS numero_identificacion,
             {} AS numero_obligacion,
             {} AS {}
      FROM {}.{} 
      WHERE CAST({} AS BIGINT)
            IN (/*aqui va el numero_obligacion*/{}
               )
      ;
      """.format(nombre_columna_lz_identificacion_2,
                 nombre_columna_lz_obligacion_2,
                 nombre_columna_lz_calife_2,
                 nombre_nueva_columna_excel_respuesta_2,
                 nombre_base_de_datos_zona_lz_2,
                 nombre_tabla_lz_2,
                 nombre_columna_lz_obligacion_2,
                 numero_obligacion
                )
      # Verificar q el codigo entre a esta condicion
      #print("\n", "se ha ejecutado el if (proceso_riesgos.ceniegarc_preliminar)","\n",consulta_2)

  # SI NO la tabla es PRE-LIMINAR - resultados_riesgos.ceniegarc_preliminar
  elif (nombre_base_de_datos_zona_lz_2   =="resultados_riesgos"
        and nombre_tabla_lz_2            =="ceniegarc_preliminar"
       ):
      # Entonces la consulta_2 es:
      # SELECT DISTINCT ...
      consulta_2 = """
      SELECT DISTINCT CAST({} AS BIGINT) AS numero_identificacion,
                      CAST({} AS BIGINT) AS numero_obligacion,
                      {} AS {}
      FROM {}.{}
      WHERE CAST({} AS BIGINT)
            IN (/*aqui va el numero_obligacion*/{}
               )
      ;
      """.format(nombre_columna_lz_identificacion_2,
                 nombre_columna_lz_obligacion_2,
                 nombre_columna_lz_calife_2,
                 nombre_nueva_columna_excel_respuesta_2,
                 nombre_base_de_datos_zona_lz_2,
                 nombre_tabla_lz_2,
                 nombre_columna_lz_obligacion_2,
                 numero_obligacion
                 ) 
      # Verificar q el codigo entre a esta condicion
      #print("\n", "se ha ejecutado el if (resultados_riesgos.ceniegarc_preliminar)","\n",consulta_2)

  # SI NO imprimir un mensaje de error
  # porq el usuario final digito en el Excel los datos incorrectos
  else: #(nombre_base_de_datos_zona_lz_2   =="proceso_riesgos"
        # and nombre_tabla_lz_2            =="ceniegarc_lz"
        #):
      print("\n")
      print("\n")
      print("En el archivo de Excel llamado 3_macro_variables.xlsx en la hoja consulta_2 el nombre de la base de datos (zona) o de la tabla son incorrectos", "\n", "\n", 
            "Recuerde que los nombres de las tablas y bases de datos (zonas) son las siguientes:", "\n",
            "TABLA PRE-LIMINAR", "\n",
            "proceso_riesgos.ceniegarc_preliminar", "\n",
            "resultados_riesgos.ceniegarc_preliminar", "\n", "\n",
            "TABLA DEFINITIVA", "\n",
            "resultados_riesgos.ceniegarc_lz", "\n", "\n",
            "Verifique que la informacion digitada sea correcta y despues vuelva a ejecutar el codigo"
            )
      # Llamar funcion sin parametros 
      # para detener la ejecucion del codigo
      detener_la_ejecucion_del_codigo()

  # Imprimir la consulta_2 despues de evaluar el condicional if-else
  #print(consulta_2)

  consulta_2 = str(consulta_2)
  
  consulta_2 = consulta_2.replace("[","")
  consulta_2 = consulta_2.replace("]","")
  #print(consulta_2)
  
  try:      
      df_ejecutar_consulta_2 = pd.read_sql_query(consulta_2,cn)
      #print("\n")
      #print("EJECUTANDO CONSULTA_2", "\n", consulta_2)
      
      #print(df_ejecutar_consulta_2)

  except Exception:
      # Llamar funciones sin parametros
      print("\n")
      print("\n")
      imprimir_mensaje_de_error()
      
      print("\n")
      print("Se ha producido un error al ejecutar la CONSULTA_2, las posibles causas del error son:", "\n",
      "1. Verifique que este conectado al VPN de Bancolombia", "\n",
      "2. Verifique que haya digitado correctamente los datos en las casillas del Excel 3_macro_variables.xlsx", "\n",
      "3. Verifique que haya escrito correctamente su nombre de usuario y contraseña corporativo", "\n",
      "4. Despues de hacer lo anterior vuelva a ejecutar el codigo otra vez"
      )
      
      detener_la_ejecucion_del_codigo()

  df_ejecutar_consulta_2["numero_identificacion"] = df_ejecutar_consulta_2["numero_identificacion"].astype("int64")
  df_ejecutar_consulta_2["numero_obligacion"] = df_ejecutar_consulta_2["numero_obligacion"].astype("int64")
  #print(df_ejecutar_consulta_2.dtypes)

  # Hacer q NO se repitan las columnas cuando se hace un pd.merge(...) en Python
  # https://www.geeksforgeeks.org/prevent-duplicated-columns-when-joining-two-pandas-dataframes/
  df_respuesta = pd.merge(left       = df_ejecutar_consulta_2,    
                          right      = df_respuesta,        
                          how        = "right",  
                          # Hacer la union (cruce) de tablas 
                          # por el numero_identificacion y numero_obligacion
                          # esto se hace asi para:
                          # 1) q NO creen nuevas columnas (repetidas) de identificacion y obligacion 
                          # 2) porq en todas las formas de hacer la consuta_2 se esta seleccionando la identificacion y obligacion
                          #     SELECT CAST(id AS BIGINT) AS numero_identificacion,
                          #            CAST(obl341 AS BIGINT) AS numero_obligacion,
                          #     ...
                          left_on    = ["numero_identificacion", "numero_obligacion"],
                          right_on   = ["numero_identificacion", "numero_obligacion"]
                        )

  #print(df_respuesta)

  df_respuesta["numero_obligacion"] = df_respuesta["numero_obligacion"].astype("int64")
  df_respuesta["numero_identificacion"] = df_respuesta["numero_identificacion"].astype("int64")
  #print(df_respuesta["numero_obligacion"].dtypes)
  #print(df_respuesta["numero_identificacion"].dtypes)

  """ 
  consulta_3
  
  numero_identificacion
  max_calificacion_del_cliente_a_corte_de_junio_20
  """
  
  consulta_3 ="""
  SELECT CAST({} AS BIGINT) AS numero_identificacion,
         MAX({}) AS {}
  FROM {}.{}
  WHERE (CAST({} AS BIGINT)={}) 
        AND (CAST({} AS BIGINT)={}) 
        AND CAST({} AS BIGINT)
        IN (/*aqui va el numero_identificacion*/{}  
           )
  GROUP BY 1
  ;  
  """.format(nombre_columna_lz_identificacion_3,
             max_lz_3,
             nombre_nueva_columna_excel_respuesta_3,
             nombre_base_de_datos_zona_lz_3,
             nombre_tabla_lz_3,
             nombre_columna_lz_year_3,
             igual_year_3,
             nombre_columna_lz_corte_3,
             igual_corte_3,
             nombre_columna_lz_identificacion_3,
             numero_identificacion
            )

  #print(consulta_3)

  consulta_3 = str(consulta_3)

  consulta_3 = consulta_3.replace("[","")
  consulta_3 = consulta_3.replace("]","")
  #print(consulta_3)

  # Captura de errores usando try: except Exception:
  try:
      df_ejecutar_consulta_3 = pd.read_sql_query(consulta_3, cn)
      #print("\n")
      #print("EJECUTANDO CONSULTA_3", "\n", consulta_3)

      #print(df_ejecutar_consulta_3)

  except Exception:
      print("\n")
      print("\n")
      imprimir_mensaje_de_error()

      print("\n")
      print("Se ha producido un error al ejecutar la CONSULTA_3, las posibles causas del error son:", "\n",
            "1. Verifique que este conectado al VPN de Bancolombia", "\n",
            "2. Verifique que haya digitado correctamente los datos en las casillas del Excel 3_macro_variables.xlsx", "\n",
            "3. Verifique que haya escrito correctamente su nombre de usuario y contraseña corporativo", "\n",
            "4. Despues de hacer lo anterior vuelva a ejecutar el codigo otra vez"
            )

      detener_la_ejecucion_del_codigo()

  df_ejecutar_consulta_3["numero_identificacion"] = df_ejecutar_consulta_3["numero_identificacion"].astype("int64")
  #print(df_ejecutar_consulta_3["numero_identificacion"].dtypes)

  df_respuesta = pd.merge(left       = df_ejecutar_consulta_3,      
                          right      = df_respuesta,                 
                          how        = "right",                                                                             
                          left_on    = ["numero_identificacion"],
                          right_on   = ["numero_identificacion"]
                          )

  #print(df_respuesta)

  df_respuesta["numero_obligacion"] = df_respuesta["numero_obligacion"].astype("int64")
  df_respuesta["numero_identificacion"] = df_respuesta["numero_identificacion"].astype("int64")
  #df_respuesta["numero_obligacion"].dtypes
  #df_respuesta["numero_identificacion"].dtypes

  """ 
  consulta_4
  
  numero_identificacion
  max_altamora_a_corte_de_febrero_20_del_cliente_externa



  La forma correcta de hacer esta consulta es creando una tabla_temporal,
  por eso use WITH tabla_temporal AS (...) SELECT * FROM tabla_temporal
  porque cuando NO haces esto y ejecutas la consulta en Python con el ODBC 
  en esta linea de codigo de Python: 
  df_ejecutar_consulta_4 = pd.read_sql_query(consulta_4, cn)
  
  te da este error:
  columns = [col_desc[0] for col_desc in cursor.description]
  TypeError: 'NoneType' object is not iterable

  Intente solucionarlo asi pero NO funciono:
  https://stackoverflow.com/questions/57866905/typeerror-nonetype-object-is-not-iterable-from-pandas-read-sql

  Pero SI me funciono con la WITH tabla_temporal...
  """

  consulta_4 ="""
  WITH tabla_temporal AS 
  ( 
         /*Forma 1*/
         SELECT CAST({} AS BIGINT) AS numero_identificacion,
                TRIM(CAST(MAX({}) AS STRING)) AS {}
         FROM {}.{}
         WHERE (CAST({} AS BIGINT)={})
               AND (CAST({} AS BIGINT)={}) 
               AND CAST({} AS BIGINT)
               IN (/*aqui va el numero_identificacion*/{})
         GROUP BY 1
         UNION
         /*Forma 2*/
         SELECT CAST({} AS BIGINT) AS numero_identificacion, 
                TRIM(CAST(MAX({}) AS STRING)) AS {}
         FROM {}.{}
         WHERE (CAST({} AS BIGINT)={})
               AND (CAST({} AS BIGINT)={})
               AND CAST({} AS BIGINT)
               IN (/*aqui va el numero_identificacion*/{})
         GROUP BY 1
         UNION
         /*Forma 3*/
         SELECT CAST({} AS BIGINT) AS numero_identificacion, 
                TRIM(CAST(MAX({}) AS STRING)) AS {}
         FROM {}.{}
         WHERE (CAST({} AS BIGINT)={})
               AND (CAST({} AS BIGINT)={})
               AND CAST({} AS BIGINT)
               IN (/*aqui va el numero_identificacion*/{})
         GROUP BY 1
  )
  SELECT numero_identificacion,
        /*max_altamora_a_corte_de_febrero_20_del_cliente_externa,*/
        (
         CASE
             WHEN (CAST({} AS STRING) = '0')
                  THEN 'EL MES NO EXISTE' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'EL MES NO EXISTE'*/

             WHEN (CAST({} AS STRING) = '1')
                  THEN 'MORA 30 DIAS' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 30 DIAS'*/

             WHEN (CAST({} AS STRING) = '2')
                  THEN 'MORA 60 DIAS' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 60 DIAS'*/

             WHEN (CAST({} AS STRING) = '3')
                  THEN 'MORA 90 DIAS' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 90 DIAS'*/

             WHEN (CAST({} AS STRING) = '4')
                  THEN 'MORA 120 DIAS' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 120 DIAS'*/

             WHEN (CAST({} AS STRING) = '5')
                  THEN 'AL DIA' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'AL DIA'*/

             WHEN (CAST({} AS STRING) = '6')
                  THEN 'N/A' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'N/A'*/

             WHEN (CAST({} AS STRING) = '7')
                  THEN 'CADUCIDAD' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'CADUCIDAD'*/

             WHEN (CAST({} AS STRING) = '8')
                  THEN 'N/A' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'N/A'*/

             WHEN (CAST({} AS STRING) = '9')
                  THEN 'SIN INFORMACION' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'SIN INFORMACION'*/

             WHEN (CAST({} AS STRING) = '10')
                  THEN 'MORA 150 DIAS' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 150 DIAS'*/

             WHEN (CAST({} AS STRING) = '11')
                  THEN 'MORA 180 DIAS' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 180 DIAS'*/

             WHEN (CAST({} AS STRING) = '12')
                  THEN 'MORA 210+ DIAS' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 210+ DIAS'*/

             WHEN (CAST({} AS STRING) = '13')
                  THEN 'DUDOSO RECAUDO' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'DUDOSO RECAUDO'*/

             WHEN (CAST({} AS STRING) = '14')
                  THEN 'CARTERA CASTIGADA' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'CARTERA CASTIGADA'*/

             WHEN (CAST({} AS STRING) = '15')
                  THEN 'N/A' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'N/A'*/

             WHEN (CAST({} AS STRING) = '')
                   OR (CAST({} AS STRING) = 'NULL')
                   OR ({} IS NULL)
                  THEN 'NULL' /*max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'NULL'*/

             ELSE {}
         END) AS {}

  FROM tabla_temporal
  """.format(# consulta 4 - forma 1
             nombre_columna_lz_identificacion_4_forma_1,
             max_lz_4_forma_1,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_base_de_datos_zona_lz_4_forma_1,
             nombre_tabla_lz_4_forma_1,
             nombre_columna_lz_year_4_forma_1,
             igual_year_4_forma_1,
             nombre_columna_lz_month_4_forma_1,
             igual_month_4_forma_1,
             nombre_columna_lz_identificacion_4_forma_1,
             numero_identificacion,

             # consulta 4 - forma 2
             nombre_columna_lz_identificacion_4_forma_2,
             max_lz_4_forma_2,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_base_de_datos_zona_lz_4_forma_2,
             nombre_tabla_lz_4_forma_2,
             nombre_columna_lz_year_4_forma_2,
             igual_year_4_forma_2,
             nombre_columna_lz_month_4_forma_2,
             igual_month_4_forma_2,
             nombre_columna_lz_identificacion_4_forma_2,
             numero_identificacion,

             # consulta 4 - forma 3
             nombre_columna_lz_identificacion_4_forma_3,
             max_lz_4_forma_3,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_base_de_datos_zona_lz_4_forma_3,
             nombre_tabla_lz_4_forma_3,
             nombre_columna_lz_year_4_forma_3,
             igual_year_4_forma_3,
             nombre_columna_lz_month_4_forma_3,
             igual_month_4_forma_3,
             nombre_columna_lz_identificacion_4_forma_3,
             numero_identificacion,
             
             # consulta 4 - tabla_temporal
             # SELECT numero_identificacion, CASE WHEN ... ELSE ... END AS ... FROM tabla_temporal
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4,
             nombre_nueva_columna_excel_respuesta_4
            )

  consulta_4 = str(consulta_4)
  
  consulta_4 = consulta_4.replace("[","")
  consulta_4 = consulta_4.replace("]","")

  #print(consulta_4)
  
  try:
      df_ejecutar_consulta_4 = pd.read_sql_query(consulta_4, cn)
      #print("\n")
      #print("EJECUTANDO CONSULTA_4", "\n", consulta_4)

      #print(df_ejecutar_consulta_4)

  except Exception:
      print("\n")
      print("\n")
      imprimir_mensaje_de_error()
      
      print("\n")
      print("Se ha producido un error al ejecutar la CONSULTA_4, las posibles causas del error son:", "\n",
            "1. Verifique que este conectado al VPN de Bancolombia", "\n",
            "2. Verifique que haya digitado correctamente los datos en las casillas del Excel 3_macro_variables.xlsx", "\n",
            "3. Verifique que haya escrito correctamente su nombre de usuario y contraseña corporativo", "\n",
            "4. Despues de hacer lo anterior vuelva a ejecutar el codigo otra vez"
            )

      detener_la_ejecucion_del_codigo()

  df_ejecutar_consulta_4["numero_identificacion"] = df_ejecutar_consulta_4["numero_identificacion"].astype("int64")
  #print(df_ejecutar_consulta_4.dtypes)
  
  # Convertir la columna llamada "max_altamora_a_corte_de_febrero_20_del_cliente_externa" de df_ejecutar_consulta_4 a tipo string
  df_ejecutar_consulta_4[nombre_nueva_columna_excel_respuesta_4] = df_ejecutar_consulta_4[nombre_nueva_columna_excel_respuesta_4].astype("string")
  # Verificar el tipo de dato de la columna "max_altamora_a_corte_de_febrero_20_del_cliente_externa"
  #print(df_ejecutar_consulta_4.dtypes)

  # Eliminar los espacios en blanco al principio y al final de la columna llamada "max_altamora_a_corte_de_febrero_20_del_cliente_externa"
  # https://stackoverflow.com/questions/43332057/pandas-strip-white-space
  df_ejecutar_consulta_4[nombre_nueva_columna_excel_respuesta_4] = df_ejecutar_consulta_4[nombre_nueva_columna_excel_respuesta_4].str.strip()

  df_respuesta = pd.merge(left       = df_ejecutar_consulta_4,    
                          right      = df_respuesta,        
                          how        = "right",                                                               
                          left_on    = ["numero_identificacion"],
                          right_on   = ["numero_identificacion"]
                        )

  #print(df_respuesta)
  
  """ Dar un orden en especifico a las columnas de un DataFrame
      https://www.stackvidhya.com/change-order-of-columns-in-pandas-dataframe/ """
  
  # Inicializar un contador en cero
  j = 0  

  # Recorrer una lista
  for elementos_3 in lista_orden_primeras_columnas_df_respuesta:
      #print(elementos_3)
      
      j = j+1
      #print("j=",j)
      #print("j-1=",j-1)
      
      ordenar_columnas = df_respuesta.pop(elementos_3)
      df_respuesta.insert(j-1, elementos_3, ordenar_columnas)

  """ 
  Crear Excel resultante en la carpeta ...\2_respuesta 


  Exportar DataFrame de Pandas a Excel:
  https://datatofish.com/export-dataframe-to-excel/
  
  Primero ejecuta en CMD este comando para instalar openpyxl
  pip install openpyxl

  Convertir el DataFrame df_insumo a Excel
  y guardarlo en la carpeta ...\4_Proyecto_requerimientos_FNG\2_excel\2_respuesta
  """
  
  # Guardar en una variable la fecha y hora con milisegundos
  # Esto lo hago para q los Excel resultantes de ...\2_respuesta tengan nombres diferentes
  # https://stackoverflow.com/questions/7588511/format-a-datetime-into-a-string-with-milliseconds
  fecha_y_hora = datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f')[:-3]
  #print(fecha_y_hora)
      
  # Ruta donde se guarda el Excel de respuesta
  # RutaDondeSeGuardaElExcel\NombreArchivoExcel.xlsx
  #ruta_guardar_excel = r"C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\4_Proyecto_requerimientos_FNG\2_excel\2_respuesta\respuesta_1.xlsx"
  # Concatenar (unir) rutas
  # Esto es para q el excel de respuesta tenga el nombre del Excel de insumo con la fecha y hora
  #ruta_guardar_excel = ruta_respuesta + fecha_y_hora + lista_nombre_excel_insumo[i]
  i = i + 1
  #print("i=",i)
  #print("i-1=",i-1)
  # ¿Por que lista_nombre_excel_insumo[i-1]? 
  # porq la primera vez q entre al for i=1, i=2... (la primera vez ya es igual a uno)
  # en cambio con i-1=0, i-1=1, i-1=2 (la primera vez es igual a cero)
  # Esto es para q i pueda empezar en 0 
  # porq en Python las posiciones de las listas empiezan en 0
  ruta_guardar_excel = ruta_respuesta + "\\" + fecha_y_hora + "_" + lista_nombre_excel_insumo[i-1]
  #print(ruta_guardar_excel)

  # Guardar Excel resultante en la carpeta ...\2_respuesta
  df_respuesta.to_excel(ruta_guardar_excel, # Ruta donde se guarda el Excel
                        sheet_name="Base",  # nombre de la hoja de Excel
                        index = False       # Si ejecuto esta linea de codigo entonces en el Excel NO se muestran los indices (0, 1, 2, 3...)
                        )
  # Imprimir el Excel (DataFrame) resultante
  print("\n")
  #print("nombre excel respuesta:", lista_nombre_excel_insumo[i-1], "\n", "DataFrame:", "\n", df_insumo)
  print("En la carpeta ...\\2_excel\\2_respuesta se han creado los siguientes Excel con las nuevas columnas añadidas: ", "\n", lista_nombre_excel_insumo[i-1])
  print("\n")

  """ Limpiar datos de las variables y DataFrame """
  # Esto va hacer q cada vez q se vuelva a ejecutar el for
  # se cree con los datos nuevos
  # para q no se mezclen los datos de los insumos
  # y se cree para cada uno de los Excel de insumo un Excel de respuesta
  # https://stackoverflow.com/questions/8237647/clear-variable-in-python
  df_insumo = pd.DataFrame() 
  df_respuesta = pd.DataFrame() 

  df_ejecutar_consulta_1 = pd.DataFrame()
  df_ejecutar_consulta_2 = pd.DataFrame()
  df_ejecutar_consulta_3 = pd.DataFrame()
  df_ejecutar_consulta_4 = pd.DataFrame()

  del numero_identificacion
  del numero_obligacion

########################################################################
#%%

print("\n")
print("\n")
print("Eliminando TODAS las variables, DataFrame, etc")
# Esto va a evitar errores en las siguientes veces (despues de la primera) q se ejecute el codigo
# https://stackoverflow.com/questions/48254689/clearing-user-created-variables-in-python
locals().clear()
globals().clear()

print("\n")
print("\n")
print("▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄")
print("█┌─┐┌─┐┌┬┐┌─┐┬  ┌─┐┌┬┐┌─┐┌┬┐┌─┐█")
print("█│  │ ││││├─┘│  ├┤  │ ├─┤ │││ │█")
print("█└─┘└─┘┴ ┴┴  ┴─┘└─┘ ┴ ┴ ┴─┴┘└─┘█")
print("▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀")

########################################################################
#%%
