"""
Proyecto - automatizacion FNG


1) Todo esto se hace en Python

2) Las consultas SQL las ejecutas desde el Python

3) Leer los Excel q estan en la carpeta ...\1_insumo, se lee la hoja llamda "Base"

4) Ejecutar consultas SQL q estan en "consultas_llenar_campos.sql"  

5) Reemplazar en la consulta 4 los siguientes valores q estan en "catalogo_moras_experian.xlsx":

Consulta 4 - calificacion_externa	Descripción Mora
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
10	                              MORA 150 DIAS
11	                              MORA 180 DIAS
12	                              MORA 210+ DIAS
13	                              DUDOSO RECAUDO
14	                              CARTERA CASTIGADA
15	                              N/A

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

print("Escriba los siguientes datos y despues presione la tecla enter...")
print("Si te equivocas al escribir tus datos el codigo falla, en caso de q eso suceda vuelvelo a ejecutar")

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

import os                                                # CMD / sistema operativo

#os.system('cmd /c "pip install --upgrade --user pip"')   # Actualizar pip
#os.system('cmd /c "pip install openpyxl --user"')        # Exportar DataFrame de Pandas a Excel
#os.system('cmd /c "pip install pandas --user"')
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

import pandas as pd              # Para los DataFrame
from os import scandir, getcwd   # scandir manejo de rutas
                                 # getcwd directorio (carpeta) de trabajo actual
from sparky_bc import Sparky     # Conexion de Python a la base de datos de Bancolombia (LZ - HUE), libreria hecha por el banco
import pyodbc                    # Conexion a bases de datos con el estandar ODBC
import datetime                  # Fecha y hora del sistema operativo
from datetime import datetime



########################################################################
#%%

# Cambiar los nombres de los indices de las columnas de df_insumo
# df = df.rename(columns= {"NombreAntiguoDeLaColumna_1": "NuevoNombreColumna_1", 
#                          "NombreAntiguoDeLaColumna_2": "NuevoNombreColumna_2} 
#                          index={NumeroDeIndice: "NuevoNombreIndice"}
#                          inplace=True)
# Funcion sin parametros
# para cambiar los nombres de las columnas llamadas numero_obligacion y numero_identificacion
# https://stackoverflow.com/questions/11346283/renaming-column-names-in-pandas
def cambiar_nombre_columnas_obligacion_identificacion():
    #print("se ha ejecutado la funcion llamada cambiar_nombre_columnas_obligacion_identificacion()")
    
    diccionario_obligacion_identificacion = { # numero_obligacion = numero de obligacion
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
                                              "obligacion":"numero_obligacion",
                                              "obligación":"numero_obligacion",
                                              "Obligacion":"numero_obligacion",
                                              "Obligación":"numero_obligacion",
                                              "OBLIGACION":"numero_obligacion",
                                              "OBLIGACIÓN":"numero_obligacion",

                                              # numero_identificacion = Número de documento = Número de identificación = Nit = ID 
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

                                              "nit":"numero_identificacion",
                                              "NIT":"numero_identificacion",
                                              "Nit":"numero_identificacion",
                                              }

    # inplace=True hace q se reemplacen los nombres de las columnas y q NO se creen otras columnas duplicadas con los nuevos nombres de los indices
    df_insumo.rename(diccionario_obligacion_identificacion, axis=1, inplace=True)

########################################################################
#%%

# Funcion sin parametros
# para reemplazar en la consulta 4 los valores q estan en "catalogo_moras_experian.xlsx"
# https://stackoverflow.com/questions/11346283/renaming-column-names-in-pandas

def reemplazar_valores_consulta_4_catalogo_moras_experian():
    #print("se ha ejecutado la funcion llamada reemplazar_valores_consulta_4_catalogo_moras_experian()")

    df_ejecutar_consulta_4["calificacion_externa"] = (df_ejecutar_consulta_4["calificacion_externa"].str.replace("0", "EL MES NO EXISTE")
                                                                                                    .str.replace("1", "MORA 30 DIAS")
                                                                                                    .str.replace("2", "MORA 60 DIAS")
                                                                                                    .str.replace("3", "MORA 90 DIAS")
                                                                                                    .str.replace("4", "MORA 120 DIAS")                                                                                                      
                                                                                                    .str.replace("5", "AL DIA")
                                                                                                    .str.replace("6", "N/A")
                                                                                                    .str.replace("7", "CADUCIDAD")
                                                                                                    .str.replace("8", "N/A")
                                                                                                    .str.replace("9", "SIN INFORMACION")
                                                                                                    .str.replace("10", "MORA 150 DIAS")
                                                                                                    .str.replace("11", "MORA 180 DIAS")
                                                                                                    .str.replace("12", "MORA 210+ DIAS")
                                                                                                    .str.replace("13", "DUDOSO RECAUDO")
                                                                                                    .str.replace("14", "CARTERA CASTIGADA")
                                                                                                    .str.replace("15", "N/A")
                                                      )



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

# Convertir variable a tipo string
ruta_insumo = str(ruta_insumo)
ruta_respuesta = str(ruta_respuesta)



########################################################################
#%%

print("\n")
print("\n")
print("Leer Excel de insumo")

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

print("\n")
print("\n")
print("Ejecutando consultas SQL q estan en sql_consultas_llenar_campos.sql")

# Conexion a la base de datos de Bancolombia
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



########################################################################
#%%

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

# Crear un contador
# Esto sirve para recorrer lista_nombre_excel_insumo
i = 0

# Leer uno o mas Excel en un solo DataFrame
# En este for creamos un DataFrame para leer TODOS los Excel q estan dentro de la carpeta 
# ...\4_Proyecto_requerimientos_FNG\2_excel\1_insumo

#elementos_2 =  r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\4_Proyecto_requerimientos_FNG\2_excel\1_insumo\1_identificacion_y_obligacion_repetidos.xlsx'


for elementos_2 in lista_de_rutas_absolutas: # esto va a leer todos los N Excel de los insumos
                                             # Recorrer los elementos de la lista_de_rutas_absolutas
  #print("\n")  
  #print("elementos_2", elementos_2)

  df_insumo = pd.read_excel (
              elementos_2,            # Ruta donde esta guardado el Excel
              sheet_name = "Base",    # Nombre de la hoja de Excel
              header = 0,             # Numero de fila donde estan los nombres de las columnas
              skiprows = 0,           # Desde esta Fila hacia abajo se empieza a leer el Excel

              # C:\Users\danpined\Anaconda3\lib\site-packages\pandas\io\excel\_base.py:
              # 1292: FutureWarning: Defining usecols with out of bounds indices is deprecated and will raise a ParserError in a future version.
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

  # Eliminar los espacios en blanco al principio y al final
  # de los nombres de las columnas
  # https://stackoverflow.com/questions/43332057/pandas-strip-white-space
  df_insumo.columns = df_insumo.columns.str.strip()
  # Verificar q se hayan eliminado los espacios en blanco
  #print(df_insumo.columns)

  # Llamar funcion sin parametros 
  # para cambiar el nombre de las columnas numero_obligacion y numero_identificacion
  cambiar_nombre_columnas_obligacion_identificacion()
  # Verificar q si se hayan cambiado los nombres de las columnas
  #print("numero_obligacion", "\n", df_insumo["numero_obligacion"])
  #print("numero_identificacion", "\n", df_insumo["numero_identificacion"])
    
  # Convertir las columnas numero_obligacion y numero_identificacion a tipo int64
  df_insumo["numero_obligacion"] = df_insumo["numero_obligacion"].astype("int64")
  df_insumo["numero_identificacion"] = df_insumo["numero_identificacion"].astype("int64")
  # Verificar el tipo de dato de las columnas numero_obligacion y numero_identificacion
  #df_insumo["numero_obligacion"].dtypes
  #df_insumo["numero_identificacion"].dtypes
  
  # Reiniciar indices del DataFrame
  df_insumo = df_insumo.reset_index(drop=True)

  """ Ejecutar consultas para N filas (identificaciones y obligaciones) 
      de los Excel de insumos """
  # Guardar en una variable de tipo sereies TODOS los numero_identificacion
  # y en otra variable los numero_obligacion
  # En cada variable se guarda la columna COMPLETA con el numero_identificacion y numero_obligacion
  # SI se repite
  #numero_identificacion = 900039118
  #numero_obligacion = 4200106809
  
  # El registro NO EXISTE (es vacio)
  #numero_identificacion = 0
  #numero_obligacion = 0

  # NO se repite
  #numero_identificacion = 830096237
  #numero_obligacion = 40094054

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

  # Reemplazar el valor de una variable dentro de una cadena de texto:
  # https://stackoverflow.com/questions/44462209/how-to-insert-a-variable-value-in-a-string-in-python
  """ 
  Consulta 1

  numero_identificacion
  max_altmora_a_corte_de_febrero_20_del_cliente_interna
  """  
  consulta_1 = "SELECT id AS numero_identificacion, MAX(altmora) AS max_altmora_a_corte_de_febrero_20_del_cliente_interna FROM resultados_riesgos.ceniegarc_lz WHERE (CAST(year AS BIGINT)=2020) AND (CAST(corte AS BIGINT)=202002) AND CAST(id AS BIGINT) IN (/*aqui va el numero_identificacion*/{}) GROUP BY 1;".format(numero_identificacion)
  # Verificar q se haya guardado la consulta 
  # y se haya reemplazado el numero_identificacion
  #print(consulta_1)
  
  # Convertir a tipo string
  consulta_1 = str(consulta_1)
  # Eliminar los corchetes [ ] q fueran insertados por la lista
  consulta_1 = consulta_1.replace("[","")
  consulta_1 = consulta_1.replace("]","")
  # Verificar q si se hayan eliminado los corchetes [ ] de la consulta_X
  #print(consulta_1)

  # Guardar cada uno de los resultados de las consultas en un DataFrame
  df_ejecutar_consulta_1 = pd.read_sql_query(consulta_1, cn)
  # Verificar q se haya ejecutado la consulta
  #print(df_ejecutar_consulta_1)
  
  # Convertir la columna numero_identificacion del df_ejecutar_consulta_X a tipo int64
  df_ejecutar_consulta_1["numero_identificacion"] = df_ejecutar_consulta_1["numero_identificacion"].astype("int64")
  # Verificar el tipo de dato de la columna
  #df_ejecutar_consulta_1["numero_identificacion"].dtypes
  
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
  Consulta 2
  
  numero_obligacion
  externa_al_momento_desembolso
  
  INCOMPLETO: Falta por verificar q esto funcione
  NO tengo permisos para la tabla proceso_riesgos.ceniegarc_preliminar
  
  EL JOIN EN ESTA CONSULTA 2
  SE HACE POR EL numero_obligacion 
  PORQ ES UN IDENTIFICADOR UNICO (NO SE REPITE)
  """

  """
  consulta_2="SELECT id AS numero_identificacion, obl341 AS numero_obligacion, calife AS externa_al_momento_desembolso FROM proceso_riesgos.ceniegarc_preliminar WHERE /*(CAST(year AS BIGINT)=2021) AND (from_timestamp(fdesem,'yyyyMM') = CAST(corte AS STRING)) AND*/ CAST(obl341 AS BIGINT) IN (/*aqui va el numero_obligacion*/{});".format(numero_obligacion)
  #print(consulta_2)

  consulta_2 = str(consulta_2)
  
  consulta_2 = consulta_2.replace("[","")
  consulta_2 = consulta_2.replace("]","")
  #print(consulta_2)
  
  df_ejecutar_consulta_2 = pd.read_sql_query(consulta_2,cn)
  #print(df_ejecutar_consulta_2)

  df_ejecutar_consulta_2["numero_obligacion"] = df_ejecutar_consulta_2["numero_obligacion"].astype("int64")
  #df_ejecutar_consulta_2["numero_obligacion"].dtypes

  df_respuesta = pd.merge(left       = df_ejecutar_consulta_2,    
                          right      = df_respuesta,        
                          how        = "right",                                                                             
                          left_on    = ["numero_obligacion"],
                          right_on   = ["numero_obligacion"]
                        )

  #print(df_respuesta)
  """

  """ 
  Consulta 3
  
  numero_identificacion
  max_calificacion_del_cliente_a_corte_de_junio_20
  """
  consulta_3="SELECT id AS numero_identificacion, max(calife) AS max_calificacion_del_cliente_a_corte_de_junio_20 FROM resultados_riesgos.ceniegarc_lz WHERE (CAST(year AS BIGINT)=2020) AND (CAST(corte AS BIGINT)=202006) AND cast(id as BIGINT) IN (/*aqui va el numero_identificacion*/{}) GROUP BY 1;".format(numero_identificacion)
  #print(consulta_3)
  
  consulta_3 = str(consulta_3)
  
  consulta_3 = consulta_3.replace("[","")
  consulta_3 = consulta_3.replace("]","")
  #print(consulta_3)
  
  df_ejecutar_consulta_3 = pd.read_sql_query(consulta_3,cn)
  #print(df_ejecutar_consulta_3)

  df_ejecutar_consulta_3["numero_identificacion"] = df_ejecutar_consulta_3["numero_identificacion"].astype("int64")
  #df_ejecutar_consulta_3["numero_identificacion"].dtypes

  df_respuesta = pd.merge(left       = df_ejecutar_consulta_3,    
                          right      = df_respuesta,                 
                          how        = "right",                                                                             
                          left_on    = ["numero_identificacion"],
                          right_on   = ["numero_identificacion"]
                        )

  #print(df_respuesta)

  """ 
  Consulta 4
  
  numero_identificacion
  calificacion_externa
  """
  consulta_4="SELECT num_doc AS numero_identificacion, MAX(mora_0) AS calificacion_externa FROM resultados_preaprobados.buro_pj_detallado_principal WHERE (CAST(ingestion_year AS BIGINT)=2020) AND (CAST(ingestion_month AS BIGINT)=2) AND CAST(num_doc AS BIGINT) IN (/*aqui va el numero_identificacion*/{}) GROUP BY 1;".format(numero_identificacion)
  #print(consulta_4)
  
  consulta_4 = str(consulta_4)
  
  consulta_4 = consulta_4.replace("[","")
  consulta_4 = consulta_4.replace("]","")
  #print(consulta_4)
  
  df_ejecutar_consulta_4 = pd.read_sql_query(consulta_4, cn)
  #print(df_ejecutar_consulta_4)
  
  df_ejecutar_consulta_4["numero_identificacion"] = df_ejecutar_consulta_4["numero_identificacion"].astype("int64")
  #print(df_ejecutar_consulta_4.dtypes)
  
  # Convertir la columna llamada "calificacion_externa" de df_ejecutar_consulta_4 a tipo string
  df_ejecutar_consulta_4["calificacion_externa"] = df_ejecutar_consulta_4["calificacion_externa"].astype("string")
  # Verificar el tipo de dato de la columna "calificacion_externa"
  #print(df_ejecutar_consulta_4.dtypes)

  # Eliminar los espacios en blanco al principio y al final de la columna llamada "calificacion_externa"
  # https://stackoverflow.com/questions/43332057/pandas-strip-white-space
  df_ejecutar_consulta_4["calificacion_externa"] = df_ejecutar_consulta_4["calificacion_externa"].str.strip()

  # Llamar funcion sin parametros
  # para reemplazar en la consulta 4 los valores q estan en "catalogo_moras_experian.xlsx"
  reemplazar_valores_consulta_4_catalogo_moras_experian()
  #print(df_ejecutar_consulta_4["calificacion_externa"])

  df_respuesta = pd.merge(left       = df_ejecutar_consulta_4,    
                          right      = df_respuesta,        
                          how        = "right",                                                               
                          left_on    = ["numero_identificacion"],
                          right_on   = ["numero_identificacion"]
                        )

  #print(df_respuesta)

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
  # porq la primera vez q entre al for i=1, i=2...
  # en cambio con i-1=0, i-1=1, i-1=2
  # Esto es para q i pueda empezar en 0 
  # porq en Python las posiciones de las listas empiezan en 0
  ruta_guardar_excel = ruta_respuesta + "\\" + fecha_y_hora + "_" + lista_nombre_excel_insumo[i-1]
  #print(ruta_guardar_excel)

  # Guardar Excel resultante en la carpeta ...\2_respuesta
  df_respuesta.to_excel(ruta_guardar_excel, # Ruta donde se guarda el Excel
                        sheet_name="base",  # nombre de la hoja de Excel
                        index = False       # Si ejecuto esta linea de codigo entonces en el Excel NO se muestran los indices (0, 1, 2, 3...)
                        )
  # Imprimir el Excel (DataFrame) resultante
  print("\n")
  #print("nombre excel respuesta:", lista_nombre_excel_insumo[i-1], "\n", "DataFrame:", "\n", df_insumo)
  print("En la carpeta ...\\2_excel\\2_respuesta se han creado los siguientes Excel con las nuevas columnas añadidas: ", "\n", lista_nombre_excel_insumo[i-1])

  """ Limpiar datos """
  # Esto va hacer q cada vez q se vuelva a ejecutar el for
  # se cree con los datos nuevos
  # para q no se mezclen los datos de los insumos
  # y se cree para cada uno de los Excel de insumo un Excel de respuesta
  # https://stackoverflow.com/questions/8237647/clear-variable-in-python
  df_insumo = pd.DataFrame() 
  df_respuesta = pd.DataFrame() 

  df_ejecutar_consulta_1 = pd.DataFrame()
  #df_ejecutar_consulta_2 = pd.DataFrame()
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
