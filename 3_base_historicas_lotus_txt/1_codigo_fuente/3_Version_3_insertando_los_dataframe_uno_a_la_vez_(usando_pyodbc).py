"""
Proyecto - base_historicas_lotus_txt


************************************
El codigo para subir los DataFrame (.txt) la tome de esta pagina web
https://towardsdatascience.com/how-i-made-inserts-into-sql-server-100x-faster-with-pyodbc-5a0b5afdba5
************************************


REQUISITOS

1) Este Python solo se va a ejecutar una sola vez 
   para los archivos .txt q me pasaron en la carpeta comprimida, 
   NO se seguira ejecutando en el futuro

2) Personas que me asignaron la tarea:
   Santiago Gonzalez Gil
   Ruben Alonso Guevara Jaramillo

3) Pasar MANUALMENTE los archivos 
   q estan en muchas carpetas en ...\2_archivos\1_original-separados_en_varias_carpetas
   a una sola carpeta en ...\2_archivos\2_en_una_sola_carpeta

4) Los nombres de los archivos .txt son:
   Actual NombreMes.txt      = Toda la Base1.txt --> subir solo uno porq son los mismos
   Historico NombreMes.txt   = Toda la Base2.txt --> subir solo uno porq son los mismos

   aval...                                       --> este NO hay q subirlo a LZ
   Consolidado... .xlsx                          --> este NO hay q subirlo a LZ
   comentarios...                                --> este NO hay q subirlo a LZ
   monto...                                      --> este NO hay q subirlo a LZ

5) Todos las tablas de los .txt tienen la misma estructura 
   y los siguientes nombres de columnas:

   Radicado	
   Identificación del cliente	
   Tipo de identificación	
   Nombre del cliente	
   Región	
   Zona	
   Segmento	
   Sector	
   Actividad económica	
   Código CIIU	
   Gerente	
   Código del gerente	
   Centro de costos	
   Grupo de riesgo	
   Código de riesgo	
   Calificación Superbancaria	
   Calificación Interna actual	
   Fecha de creación	
   Autor	
   Finalidad del crédito	
   Fecha de decisión	
   Nombre de quien aprueba	
   Código de quien aprueba	
   Número acta de comité de crédito	
   LME solicitado	
   LME PIC	
   LME aprobado	
   Flujo Actual	
   Estado Actual	
   Responsable actual	
   Flujo	
   Estado	
   Responsable	
   Fecha de entrada	
   Hora de entrada	
   Fecha de salida	
   Hora de salida	
   Tiempo	
   Vigencia LME

6) Leer los N .txt q estan en ...\2_archivos\2_en_una_sola_carpeta asi:
   https://www.geeksforgeeks.org/how-to-read-text-files-with-pandas/
    
   df = pd.read_csv(f, 
                    encoding='latin-1', 
                    sep='^
                    )

7) En los nombres de las columnas hacer lo siguiente:
   7.1) Eliminar los espacios en blanco al principio y al final 
   7.2) Reemplazar los espacios por guion bajo _
   7.3) Quitar las tildes de las vocales

8) Convertir TODAS las columnas q tengan fecha a tipo string,
   esto se hace para q las fechas queden tal cual como estan en los .txt

9) Crear una tabla vacia en LZ - HUE (base de datos de Bancolombia) 
    con los nombres de las columnas del paso 5)
    la tabla se llama proceso_consumidores.base_historicas_lotus_txt

10) Ir subiendo cada uno de todos los .txt a una sola tabla de LZ,
    esto se hace con SQL

    INSERT INTO table_name
    VALUES (value1, value2, value3, ...);

11) Descargar en Excel la tabla resultante (final) 
    proceso_consumidores.base_historicas_lotus_txt





PENDIENTE
Subir los 88 txt a LZ (cada uno por separado)

"""

########################################################################
#%%

""" 
Guardar nombre de usuario corporativo en una variable
y clave en otra variable
https://www.aluracursos.com/blog/la-diferencia-entre-las-funciones-input-y-raw-input-en-python
"""

#print("Cierre TODOS los archivos q tenga abiertos dentro de la carpeta ...\2_archivos antes de ejecutar el codigo")
#print("Escriba los siguientes datos y despues presione la tecla enter...")
#print("Si te equivocas al escribir tus datos o no cierras los archivos entonces el codigo falla, en caso de q eso suceda vuelvelo a ejecutar")

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

print("\n")
print("\n")
print("importando librerias")

import os, os.path                 # CMD / sistema operativo
from os import scandir             # scandir manejo de rutas
import pandas as pd                # Para los DataFrame
from sparky_bc import Sparky       # Conexion de Python a la base de datos de Bancolombia (LZ - HUE), libreria hecha por el banco
import pyodbc                      # Conexion a bases de datos con el estandar ODBC
import time                        # tiempo
from traceback import format_exc   # Captura de errores try: except Exception:



########################################################################
#%%

# Crear una funcion sin parametros
def imprimir_mensaje_de_error():
      # Guardar en una variable el mensaje de error q muestra Python por consola
      error = format_exc()
      # Imprimir el mensaje de error de Python
      print("Mensaje de error de Python", "\n", error)
      


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

"""
# Funcion sin parametros
# para reemplazar los espacios en blanco de los nombres de las columnas por guion bajo _
# https://stackoverflow.com/questions/59177208/how-to-replace-blank-space-with-on-every-elements-list-python
def reemplazar_espacios_por_guion_bajo_en_nombres_columnas():
    #print("se ha ejecutado la funcion llamada  reemplazar_espacios_por_guion_bajo_en_nombres_columnas()()")
    #df.columns = df.columns.str.replace("","_")
    df.columns = df.columns.str.replace(" ","_")
    df.columns = df.columns.str.replace("  ","_")
"""



########################################################################
#%%

"""
# Funcion sin parametros
# para 1uitar las tildes de las vocales en los nombres de las columnas
def quitar_tildes_en_nombres_columnas():
    #print("se ha ejecutado la funcion llamada quitar_tildes_en_nombres_columnas()")
    df.columns = (df.columns.str.replace("á", "a")
                            .str.replace("Á", "a")

                            .str.replace("é", "e")
                            .str.replace("É", "e")
    
                            .str.replace("í", "i")
                            .str.replace("Í", "i")

                            .str.replace("ó", "o")
                            .str.replace("Ó", "o")
                            
                            .str.replace("ú", "u")
                            .str.replace("Ú", "u")
                 )
"""



########################################################################
#%%

print("\n")
print("\n")
print("Leyendo rutas quemadas")

# Guardar en variables las rutas...
# Donde esta la carpeta codigo fuente

ruta_carpeta_codigo_fuente = str(r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\1_codigo_fuente')
#print(ruta_carpeta_codigo_fuente)

# La carpeta donde estan guardados los txt
ruta_txt = str(r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\2_archivos\2_en_una_sola_carpeta')
#print(ruta_txt)

# Carpeta donde se guarda un DataFrame 
# con los nombres de los txt q se han subido a LZ (base de datos de Bancolombia)
ruta_txt_subidos = str(r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\2_archivos\3_txt_subidos')
#print(ruta_txt_subidos)


########################################################################
#%%

print("\n")
print("\n")
print("Guardando rutas de los .txt q estan en \\2_archivos\\2_en_una_sola_carpeta")

# LA PRIMERA VEZ Q ESTO SE HACE LOS .txt PUEDEN TENER CUALQUIER NOMBRE
# Leer uno o mas archivos q esta en una carpeta pero no se como se llaman los archivos
# ruta relativa (nombre del archivo)
# https://es.stackoverflow.com/questions/24278/c%C3%B3mo-listar-todos-los-archivos-de-una-carpeta-usando-python
# Guardar en una lista el nombre desconocido de los insumos de Excel
lista_nombre_txt = [arch.name 
                    for arch in scandir(ruta_txt) 
                    if arch.is_file()
                   ]
# Se TIENE q imprimir el (o los) nombre(s) de los .txt q estan dentro de \2_archivos\2_en_una_sola_carpeta
#print(lista_nombre_txt)

# Crear una lista vacia
# En esta lista se guardan TODAS las rutas COMPLETAS (absoluta)
# de donde estan los .txt
lista_de_rutas_absolutas = []

# Recorrer los elementos de una lista 
# En este for crearemos una lista con las rutas COMPLETAS (absoluta) de todos los .txt
#lista_de_rutas_absolutas =  r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\2_archivos\2_en_una_sola_carpeta\1A0C16CA Historico Diciembre.txt'
for elementos_1 in lista_nombre_txt: 
    #print("elementos_1", elementos_1)
    #print("lista_nombre_txt", lista_nombre_txt)

    # Concatenar (unir) rutas
    #agregar_ruta = ruta_txt + "\\" + elementos_1
    agregar_ruta = os.path.join(f'{ruta_txt}', f'{elementos_1}')
    #print(agregar_ruta)
    
    # Convertir variable a tipo string
    agregar_ruta = str(agregar_ruta)
    
    # Agregar nuevos elementos a la lista .append()
    # el resultado es una lista con las rutas absolutas (COMPLETAS) donde estan los Excel de insumo
    # lista_de_rutas_absolutas = (ruta_txt) + (lista_nombre_txt)
    lista_de_rutas_absolutas.append(agregar_ruta)

# Imprimir la lista_de_rutas_absolutas
# Esto TIENE q imprimir TODAS las rutas de los .txt
#print(lista_de_rutas_absolutas)

####

# LA SEGUNDA VEZ Q ESTO SE HACE LOS txt SIEMPRE SE VAN A LLAMAR
# 1.txt 2.txt ... hasta n
# Crear un contador
# a medida de q el contador va aumentando se le van dando los nuevos nombres a los txt
i=0
for elementos_2 in lista_de_rutas_absolutas:
    #print(elementos_2)
    
    """ Cambiar nombre de los .txt por numeros """
    # https://stackoverflow.com/questions/2491222/how-to-rename-a-file-using-python
    i = i + 1
    # Concatenar (unir) tipo string con int https://stackoverflow.com/questions/20441035/unsupported-operand-types-for-int-and-str
    nuevo_nombre = ruta_txt + "\\" + str(i) + ".txt" 
    
    #os.rename(NombreAntiguoArchivo.extension, NombreNuevoArchivo.extension)
    os.rename(elementos_2, nuevo_nombre)

# Guardar en una variable el numero total de .txt
numero_total_de_txt = len(lista_nombre_txt)
#print(numero_total_de_txt)

# Vaciar la lista
lista_nombre_txt            = []

# Ahora q le cambie los nombres a los txt 
# ya se q SIEMPRE se van a llamar 1.txt 2.txt 3.txt ...
# sabiendo esto agregare los elementos a la lista_nombre_txt 
# ordenados de menor a mayor (ascendente) 
for i in range(numero_total_de_txt): # para i=0 hasta el numero total de txt
    #print(i)

    agregar_numero_txt_ordenados = (str(i+1)) + ".txt"    
    #print(agregar_numero_txt_ordenados)
    
    lista_nombre_txt.append(agregar_numero_txt_ordenados)

# Se deben imprimir los txt en orden
# 1.txt 2.txt 3.txt ...
#print(lista_nombre_txt)

# Vaciar los datos de la variable y la lista
agregar_ruta                = ""
lista_de_rutas_absolutas    = []

# En este momento ya tengo ordenada la lista_nombre_txt con las rutas relativgas
# en el siguiente for voy a ordenar la lista_de_rutas_absolutas
# de menor a mayor (ascendente) 
for elementos_3 in lista_nombre_txt: 
    #print("elementos_3", elementos_3)
    #print("lista_nombre_txt", lista_nombre_txt)

    agregar_ruta = os.path.join(f'{ruta_txt}', f'{elementos_3}')
    #print(agregar_ruta)

    agregar_ruta = str(agregar_ruta)

    lista_de_rutas_absolutas.append(agregar_ruta)

#print(lista_de_rutas_absolutas)



########################################################################
#%%

""" CONEXION A LA BASE DE DATOS DE BANCOLOMBIA (LZ - HUE) """

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
    
    cursor = cn.cursor()
    # Verificar conexion a la base de datos
    #print(cn)
    #print(cursor)

    print("\n")
    print("\n")
    print("Conectando a la base de datos de Bancolombia LZ - HUE USANDO cn pyodbc")

except Exception:
    # Llamar funciones sin parametros
    print("\n")
    print("\n")
    imprimir_mensaje_de_error()
    
    print("\n")
    print("ERROR - cn = pyodbc.connect - en la conexion a la base de datos de Bancolombia LZ - HUE", "\n",
          "posibles razones del error:", "\n",
          "1. No ha activado el VPN de Bancolombia", "\n",
          "2. LZ esta caido", "\n",
          "3. Digito mal su usuario o contraseña"
         )

    #detener_la_ejecucion_del_codigo()


try:
    sp = Sparky(nombre_usuario,          # nombre de usuario de la base de datos
                'IMPALA_PROD',           # Nombre de la fuente de datos = DSN = Data Source Name
                clave,                   # contraseña de la base de datos
                hostname='sbmdeblze003', # nombre del Host
                remote=True              # como es igual a =True entonces la conexion a Sparky SI se hace de forma remota
               )
    # Verificar conexion a la base de datos
    #print(sp)

    hp=sp.helper

    print("\n")
    print("\n")
    print("Conectando a la base de datos de Bancolombia LZ - HUE USANDO sp sparky")

except Exception:
    # Llamar funciones sin parametros
    print("\n")
    print("\n")
    imprimir_mensaje_de_error()
    
    print("\n")
    print("ERROR - sp = Sparky - en la conexion a la base de datos de Bancolombia LZ - HUE", "\n",
          "posibles razones del error:", "\n",
          "1. No ha activado el VPN de Bancolombia", "\n",
          "2. LZ esta caido", "\n",
          "3. Digito mal su usuario o contraseña"
         )

    #detener_la_ejecucion_del_codigo()



########################################################################
#%%

# En este DataFrame df_leer_txt se leeran los .txt
# Crear un DataFrame q en un principio estara vacio pero despues lo hire llenando conforme voy leyendo los .txt
# TODOS los .txt tienen la misma extructura y nombres de columnas
nombre_columnas_tabla_txt = ["radicado",	
                             "identificacion_del_cliente",
                             "tipo_de_identificacion",
                             "nombre_del_cliente",
                             "region",
                             "zona",
                             "segmento",
                             "sector",
                             "actividad_economica",
                             "codigo_CIIU",
                             "gerente",
                             "codigo_del_gerente",
                             "centro_de_costos",
                             "grupo_de_riesgo",
                             "codigo_de_riesgo",
                             "calificacion_superbancaria",
                             "calificacion_interna_actual",
                             "fecha_de_creacion",
                             "autor",
                             "finalidad_del_credito",
                             "fecha_de_decision",
                             "nombre_de_quien_aprueba",
                             "codigo_de_quien_aprueba",
                             "numero_acta_de_comite_de_credito",
                             "LME_solicitado",
                             "LME_PIC",
                             "LME_aprobado",
                             "flujo_actual",
                             "estado_actual",
                             "responsable_actual",
                             "flujo",
                             "estado",
                             "responsable",
                             "fecha_de_entrada",
                             "hora_de_entrada",
                             "fecha_de_salida",
                             "hora_de_salida",
                             "tiempo",
                             "vigencia_LME" 
                            ]

df_leer_txt = pd.DataFrame(columns = nombre_columnas_tabla_txt) # Darle nombres a las columnas del DataFrame
# Verificar q se haya creado el DataFrame vacio con los nombres de las columnas
#print(df_leer_txt)



########################################################################
#%%

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
print("Leyendo los .txt q estan dentro de la carpeta ...\\2_archivos\\2_en_una_sola_carpeta")
print("Insertando datos en tabla proceso_generadores.base_historicas_lotus_txt_...")

# Crear una lista q en un principio esta vacia
# pero despues le ire agregando los nombres de los DataFrame q fueron subidos a LZ
lista_nombre_txt_subidos = []

# Crear un contador
# esto lo hago para recorrer la lista_nombre_txt
# y asi guardar los nombres de los .txt q se han subido a LZ
j=0

# Crear un acumulador
# para contar el numero total de filas (registros) de todos los DataFrame .txt 
contar_filas = 0

# Leer uno o mas .txt en un solo DataFrame
# En este for creamos un DataFrame para leer TODOS los .txt q estan dentro de la carpeta 
# ...\2_archivos\2_en_una_sola_carpeta

MY_TABLE = 'proceso_consumidores.base_historicas_lotus_txt_definitivo'

# Guardar cada una de las consultas sql en una variable tipo string
sql_drop_table_definitivo="""
DROP TABLE IF EXISTS proceso_consumidores.base_historicas_lotus_txt_definitivo PURGE
"""
#print(sql_drop_table_definitivo)

sql_create_table_definitivo="""
CREATE TABLE IF NOT EXISTS proceso_consumidores.base_historicas_lotus_txt_definitivo (
    radicado STRING,	
    identificacion_del_cliente STRING,
    tipo_de_identificacion STRING,
    nombre_del_cliente STRING,
    region STRING,
    zona STRING,
    segmento STRING,
    sector STRING,
    actividad_economica STRING,
    codigo_CIIU STRING,
    gerente STRING,
    codigo_del_gerente STRING,
    centro_de_costos STRING,
    grupo_de_riesgo STRING,
    codigo_de_riesgo STRING,
    calificacion_superbancaria STRING,
    calificacion_interna_actual STRING,
    fecha_de_creacion STRING,
    autor STRING,
    finalidad_del_credito STRING,
    fecha_de_decision STRING,
    nombre_de_quien_aprueba STRING,
    codigo_de_quien_aprueba STRING,
    numero_acta_de_comite_de_credito STRING,
    LME_solicitado STRING,
    LME_PIC STRING,
    LME_aprobado STRING,
    flujo_actual STRING,
    estado_actual STRING,
    responsable_actual STRING,
    flujo STRING,
    estado STRING,
    responsable STRING,
    fecha_de_entrada STRING,
    hora_de_entrada STRING,
    fecha_de_salida STRING,
    hora_de_salida STRING,
    tiempo STRING,
    vigencia_LME STRING
)
"""
#print(sql_create_table_definitivo)

# Ejecutar consultas SQL desde Python
cursor.execute(sql_drop_table_definitivo) # eliminar tabla definitiva
cn.commit() 

time.sleep(20)

cursor.execute(sql_create_table_definitivo) # crear tabla definitiva
cn.commit() 

#elementos_4 =  r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\2_archivos\2_en_una_sola_carpeta\1A0C16CA Historico Diciembre.txt'
#elementos_4 =  r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\2_archivos\2_en_una_sola_carpeta\prueba.txt'
#elementos_4 =  r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\2_archivos\2_en_una_sola_carpeta\1.txt'

for elementos_4 in lista_de_rutas_absolutas: # esto va a leer todos los N .txt
                                             # Recorrer los elementos de la lista_de_rutas_absolutas
  #print("\n")  
  #print("elementos_4", elementos_4)

  # Aumentar contador
  j = j+1
  #print(j)

  """ guardar los nombres de los .txt (DataFrame) q se subieron a LZ """
  guardar_nombre_txt_subido = str(lista_nombre_txt[j-1])
  #print(guardar_nombre_txt_subido)

  lista_nombre_txt_subidos.append(guardar_nombre_txt_subido)
  #print(lista_nombre_txt_subidos)


  """ Leer los N .txt """
  try:# Cuando NO se produzca un error entonces leer los .txt q estan dentro de la carpeta ...\2_archivos\2_en_una_sola_carpeta
      # https://www.geeksforgeeks.org/how-to-read-text-files-with-pandas/

      df_leer_txt = pd.read_csv(elementos_4,                        # Ruta donde esta guardado los .txt
                       
                                sep="^",                            # Separador: Caracter por el cual se separan los datos de la tabla
                       
                                header = None,                      # Numero de fila donde estan los nombres de las columnas
                                                                    # en este caso como = None entonces los nombres de las columnas NO se leen de los .txt
                                                     
                                names=nombre_columnas_tabla_txt,    # Nombre de las columnas del DataFrame
                                                                    # en este caso como es igual a una lista =nombre_columnas_tabla entonces le estoy dando un nombre en especifico a las columnas
                                                     
                                #index_col=False,                   # Indice de las filas del DataFrame
                                                                    # en este caso como = False entonces los indices de las filas son 0, 1, 2...
                                                                    # https://stackoverflow.com/questions/12960574/pandas-read-csv-index-col-none-not-working-with-delimiters-at-the-end-of-each-li
                                      
                                                        
                                dtype=str,                          # Tipo de dato del DataFrame
                                                                    # en este caso es string =str

                                skiprows=1,                         # Desde esta Fila hacia abajo se empieza a leer los .txt
                       
                                encoding='latin-1',
                                #encoding="UTF-8",                   # Formato de codificacion
                                                                     # Esto hace q el dataframe pueda leer TODO tipo de caracteres: tildes, mayusculas, minusculas, numeros, caracteres especiales, etc...
                                 error_bad_lines=False                      # ignorar errores al leer el DataFrame                  
                               )

      # Imprimir df_leer_txt
      # se TIENEN q imprimir TODOS los .txt
      #print("df_leer_txt", df_leer_txt) 

  except Exception: # Cuando SI se produza un error entonces detener la ejecucion del codigo
      # Llamar funciones sin parametros
      print("\n")
      print("\n")
      print("ERROR al leer el .txt (DataFrame) llamado", guardar_nombre_txt_subido)
      imprimir_mensaje_de_error()

      #detener_la_ejecucion_del_codigo()


  # Reiniciar indices del DataFrame
  df_leer_txt = df_leer_txt.reset_index(drop=True)
  #print(df_leer_txt.index)
  
  # Contar el numero total de filas de todos los DataFrame
  # https://stackoverflow.com/questions/15943769/how-do-i-get-the-row-count-of-a-pandas-dataframe
  contar_filas = contar_filas + df_leer_txt[df_leer_txt.columns[0]].count()
  #print(contar_filas)

  """ subir los .txt a LZ - HUE """
  
  #insert_to_tmp_tbl_stmt = f"""
  #INSERT INTO {MY_TABLE} (
  ##          radicado,	
  #          identificacion_del_cliente,
  #          tipo_de_identificacion,
  #          nombre_del_cliente,
  #          region,
  #          zona,
  #          segmento,
  #          sector,
  #          actividad_economica,
  #          codigo_CIIU,
  #          gerente,
  #          codigo_del_gerente,
  #          centro_de_costos,
  #          grupo_de_riesgo,
  #          codigo_de_riesgo,
  #          calificacion_superbancaria,
  #          calificacion_interna_actual,
  #          fecha_de_creacion,
  #          autor,
  #          finalidad_del_credito,
  #          fecha_de_decision,
  #          nombre_de_quien_aprueba,
  #          codigo_de_quien_aprueba,
  #          numero_acta_de_comite_de_credito,
  #          LME_solicitado,
  #          LME_PIC,
  #          LME_aprobado,
  #          flujo_actual,
  #          estado_actual,
  #          responsable_actual,
  #          flujo,
  #          estado,
  #          responsable,
  #          fecha_de_entrada,
  #          hora_de_entrada,
  #          fecha_de_salida,
  #          hora_de_salida,
  #          tiempo,
  #          vigencia_LME
  #)  
  #VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  #"""
  
  #print(insert_to_tmp_tbl_stmt)


  #cursor = cn.cursor()
  #print(cursor)
      
  # Esto aumenta la velocidad de la subida de los DataFrame a la base de datos
  #cursor.fast_executemany = True
  #print(cursor.fast_executemany)
      
  #convertir_df_a_lista = df_leer_txt.values.tolist()
  #print(convertir_df_a_lista)

  #cursor.executemany(insert_to_tmp_tbl_stmt, df_leer_txt.values.tolist())  
  #print(f'{len(df_leer_txt)} filas insertadas en la tabla {MY_TABLE}')
      
  # Confirmar de q si se ejecute la consulta
  # https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlconnection-commit.html
  #cursor.commit()    
      
  # Cerrar conexion a la base de datos 
  # https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-close.html                            
  #cn.close()
  #cursor.close()

  #time.sleep(10)
  
  """ OTRO INTENTO """

  convertir_df_a_lista = df_leer_txt.values.tolist()
  #print(convertir_df_a_lista)
    
  #posiciones_lista = len(convertir_df_a_lista)
  #print(posiciones_lista)
  
  insert_to_tmp_tbl_stmt = """
  INSERT INTO proceso_consumidores.base_historicas_lotus_txt_definitivo (
            radicado,	
            identificacion_del_cliente,
            tipo_de_identificacion,
            nombre_del_cliente,
            region,
            zona,
            segmento,
            sector,
            actividad_economica,
            codigo_CIIU,
            gerente,
            codigo_del_gerente,
            centro_de_costos,
            grupo_de_riesgo,
            codigo_de_riesgo,
            calificacion_superbancaria,
            calificacion_interna_actual,
            fecha_de_creacion,
            autor,
            finalidad_del_credito,
            fecha_de_decision,
            nombre_de_quien_aprueba,
            codigo_de_quien_aprueba,
            numero_acta_de_comite_de_credito,
            LME_solicitado,
            LME_PIC,
            LME_aprobado,
            flujo_actual,
            estado_actual,
            responsable_actual,
            flujo,
            estado,
            responsable,
            fecha_de_entrada,
            hora_de_entrada,
            fecha_de_salida,
            hora_de_salida,
            tiempo,
            vigencia_LME
  )  
  VALUES {}
  """.format(convertir_df_a_lista)
  
  insert_to_tmp_tbl_stmt = (insert_to_tmp_tbl_stmt.replace("[", "(")
                                                  .replace("]", ")")
                                                  .replace("nan", "'nan'")
                           )
  
  #insert_to_tmp_tbl_stmt = str(insert_to_tmp_tbl_stmt)
  
  hp.ejecutar_consulta(insert_to_tmp_tbl_stmt)
   
  # Esto aumenta la velocidad de la subida de los DataFrame a la base de datos
  #cursor.fast_executemany = True
  
  #cursor.execute(insert_to_tmp_tbl_stmt)
  
  #cn.commit()
  
  #cursor = cn.cursor()
  #print(cursor)
  
  time.sleep(30)
  
  """ Limpiar datos de las variables y DataFrame """
  df_leer_txt = pd.DataFrame()
  df_leer_txt = pd.DataFrame(columns = nombre_columnas_tabla_txt)

  guardar_nombre_txt_subido          =""
  
  # Variables con los nombres de las columnas de los .txt
  radicado                           = ""
  identificacion_del_cliente         = ""
  tipo_de_identificacion             = ""
  nombre_del_cliente                 = ""
  region                             = ""
  zona                               = ""
  segmento                           = ""
  sector                             = ""
  actividad_economica                = ""
  codigo_CIIU                        = ""
  gerente                            = ""
  codigo_del_gerente                 = ""
  centro_de_costos                   = ""
  grupo_de_riesgo                    = ""
  codigo_de_riesgo                   = ""
  calificacion_superbancaria         = ""
  calificacion_interna_actual        = ""
  fecha_de_creacion                  = ""
  autor                              = ""
  finalidad_del_credito              = ""
  fecha_de_decision                  = ""
  nombre_de_quien_aprueba            = ""
  codigo_de_quien_aprueba            = "" 
  numero_acta_de_comite_de_credito   = ""
  LME_solicitado                     = ""
  LME_PIC                            = ""
  LME_aprobado                       = ""
  flujo_actual                       = ""
  estado_actual                      = ""
  responsable_actual                 = ""
  flujo                              = ""
  estado                             = ""
  responsable                        = ""
  fecha_de_entrada                   = ""
  hora_de_entrada                    = ""
  fecha_de_salida                    = ""
  hora_de_salida                     = ""
  tiempo                             = ""
  vigencia_LME                       = ""
  


#print(lista_nombre_txt_subidos)


# En este DataFrame df_txt_subidos
# guardare los nombres de los txt q se han subido a LZ (base de datos de Bancolombia)
# Crear un DataFrame q en un principio estara vacio
nombre_columnas_txt_subidos = ["nombre_txt_subido_a_lz"]

# Llenar df_txt_subidos con los nombres de los txt q se subieron a LZ
df_txt_subidos = pd.DataFrame(lista_nombre_txt_subidos, columns = nombre_columnas_txt_subidos)

ruta_guardar_nombre_txt_subidos = ruta_txt_subidos + "\\" + "nombre_txt_subido_a_lz.xlsx"
#print(ruta_guardar_nombre_txt_subidos)

# Exportar DataFrame a Excel .xlsx
# Las siguientes veces desps de la primera 
# q se ejecute el codigo se va a sobreescribir (reemplazar) el Excel
# esto sucede porq el Excel siempre tiene el mismo nombre q es nombre_txt_subido_a_lz.xlsx
df_txt_subidos.to_excel(ruta_guardar_nombre_txt_subidos, # Ruta donde se guarda el Excel
                        sheet_name="nombre_txt_subido_a_lz",  # nombre de la hoja de Excel
                        index = False       # Si ejecuto esta linea de codigo entonces en el Excel NO se muestran los indices (0, 1, 2, 3...)
                       )



########################################################################
#%%

print("\n")
print("\n")
print("▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄")
print("█┌─┐┌─┐┌┬┐┌─┐┬  ┌─┐┌┬┐┌─┐┌┬┐┌─┐█")
print("█│  │ ││││├─┘│  ├┤  │ ├─┤ │││ │█")
print("█└─┘└─┘┴ ┴┴  ┴─┘└─┘ ┴ ┴ ┴─┴┘└─┘█")
print("▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀")

print("numero de .txt (DataFrame) subidos a LZ (base de datos de Bancolombia): ", j)

print("numero total de filas de todos los .txt (DataFrame): ", contar_filas)

########################################################################
#%%
