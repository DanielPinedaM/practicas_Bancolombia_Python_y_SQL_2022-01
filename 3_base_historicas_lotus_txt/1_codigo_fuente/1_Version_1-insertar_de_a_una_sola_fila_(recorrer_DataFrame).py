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
import pandas as pd                # Para los DataFrame
from os import scandir, getcwd     # scandir manejo de rutas
                                   # getcwd directorio (carpeta) de trabajo actual
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
ruta_carpeta_codigo_fuente = r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\1_codigo_fuente'

# La carpeta donde estan guardados los txt
ruta_txt = r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\2_archivos\2_en_una_sola_carpeta'

# Carpeta donde se guarda un DataFrame 
# con los nombres de los txt q se han subido a LZ (base de datos de Bancolombia)
ruta_txt_subidos = r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\2_archivos\3_txt_subidos'


# Convertir variable a tipo string
ruta_carpeta_codigo_fuente = str(ruta_carpeta_codigo_fuente)
ruta_txt = str(ruta_txt)
ruta_txt_subidos = str(ruta_txt_subidos)



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

    #detener_la_ejecucion_del_codigo()


"""
try:
    sp = Sparky(nombre_usuario,          # nombre de usuario de la base de datos
                'IMPALA_PROD',           # Nombre de la fuente de datos = DSN = Data Source Name
                clave,                   # contraseña de la base de datos
                hostname='sbmdeblze003', # nombre del Host
                remote=True              # como es igual a =True entonces la conexion a Sparky SI se hace de forma remota
               )
    # Verificar conexion a la base de datos
    #print(sp)

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
"""



########################################################################
#%%

print("\n")
print("\n")
print("Leyendo los .txt q estan dentro de la carpeta ...\\2_archivos\\2_en_una_sola_carpeta")

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
#print(df)



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


#elementos_4 =  r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\2_archivos\2_en_una_sola_carpeta\1A0C16CA Historico Diciembre.txt'
#elementos_4 =  r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\2_archivos\2_en_una_sola_carpeta\prueba.txt'
#elementos_4 =  r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\2_archivos\2_en_una_sola_carpeta\1.txt'

for elementos_4 in lista_de_rutas_absolutas: # esto va a leer todos los N .txt
                                             # Recorrer los elementos de la lista_de_rutas_absolutas
  #print("\n")  
  #print("elementos_4", elementos_4)
  
  # Aumentar contador
  j = j+1

  """ Leer los N .txt """
  try:# Cuando NO se produzca un error entonces leer los .txt q estan dentro de la carpeta ...\2_archivos\2_en_una_sola_carpeta
      # https://www.geeksforgeeks.org/how-to-read-text-files-with-pandas/
      
      """
      INCOMPLETO - ERROR
      se esta perdiendo informacion cuando leo el df
      
      decorators.py:311: ParserWarning: Length of header or names does not match length of data. This leads to a loss of data with index_col=False.
        return func(*args, **kwargs)
      """
      
      df_leer_txt = pd.read_csv(elementos_4,                        # Ruta donde esta guardado los .txt
                       
                                sep="^",                            # Separador: Caracter por el cual se separan los datos de la tabla
                       
                                header = None,                      # Numero de fila donde estan los nombres de las columnas
                                                                    # en este caso como = None entonces los nombres de las columnas NO se leen de los .txt
                                                     
                                names=nombre_columnas_tabla_txt,    # Nombre de las columnas del DataFrame
                                                                    # en este caso como es igual a una lista =nombre_columnas_tabla entonces le estoy dando un nombre en especifico a las columnas
                                                     
                                index_col=False,                   # Indice de las filas del DataFrame
                                                                    # en este caso como = False entonces los indices de las filas son 0, 1, 2...
                                                                    # https://stackoverflow.com/questions/12960574/pandas-read-csv-index-col-none-not-working-with-delimiters-at-the-end-of-each-li
                                      
                                                        
                                dtype=str,                          # Tipo de dato del DataFrame
                                                                    # en este caso es string =str

                                skiprows=1,                         # Desde esta Fila hacia abajo se empieza a leer los .txt
                       
                                encoding='latin-1',                 # Formato de codificacion
                               )

      # Imprimir df_leer_txt
      # se TIENEN q imprimir TODOS los .txt
      #print("df_leer_txt", df_leer_txt) 

  except Exception: # Cuando SI se produza un error entonces detener la ejecucion del codigo
      # Llamar funciones sin parametros
      print("\n")
      print("\n")
      print("ERROR al leer los .txt")
      imprimir_mensaje_de_error()

      #detener_la_ejecucion_del_codigo()

  # Reiniciar indices del DataFrame
  df_leer_txt = df_leer_txt.reset_index(drop=True)
  
  # Contar el numero total de filas de todos los DataFrame
  # https://stackoverflow.com/questions/15943769/how-do-i-get-the-row-count-of-a-pandas-dataframe
  contar_filas = contar_filas + df_leer_txt[df_leer_txt.columns[0]].count()
  #print(contar_filas)

  """ subir los .txt a LZ - HUE """  
  
  """ INCOMPLETO """
  

  # Recorrer (iterar) las filas de un DataFrame
  # https://stackoverflow.com/questions/16476924/how-to-iterate-over-rows-in-a-dataframe-in-pandas
  for index, row in df_leer_txt.iterrows(): # esto ejecutara la consulta para las N filas de los .txt
      # Guardar en variables cada uno de los datos de las filas
      
      """
      radicado = 0
      
      radicado = list(df_leer_txt["radicado"].values)
      print(radicado)
           
      prueba="INSERT ... {}".format(radicado)
      """

      radicado                           = str(row["radicado"])
      identificacion_del_cliente         = str(row["identificacion_del_cliente"])
      tipo_de_identificacion             = str(row["tipo_de_identificacion"])
      nombre_del_cliente                 = str(row["nombre_del_cliente"])
      region                             = str(row["region"])
      zona                               = str(row["zona"])
      segmento                           = str(row["segmento"])
      sector                             = str(row["sector"])
      actividad_economica                = str(row["actividad_economica"])
      codigo_CIIU                        = str(row["codigo_CIIU"])
      gerente                            = str(row["gerente"])
      codigo_del_gerente                 = str(row["codigo_del_gerente"])
      centro_de_costos                   = str(row["centro_de_costos"])
      grupo_de_riesgo                    = str(row["grupo_de_riesgo"])
      codigo_de_riesgo                   = str(row["codigo_de_riesgo"])
      calificacion_superbancaria         = str(row["calificacion_superbancaria"])
      calificacion_interna_actual        = str(row["calificacion_interna_actual"])
      fecha_de_creacion                  = str(row["fecha_de_creacion"])
      autor                              = str(row["autor"])
      finalidad_del_credito              = str(row["finalidad_del_credito"])
      fecha_de_decision                  = str(row["fecha_de_decision"])
      nombre_de_quien_aprueba            = str(row["nombre_de_quien_aprueba"])
      codigo_de_quien_aprueba            = str(row["codigo_de_quien_aprueba"])
      numero_acta_de_comite_de_credito   = str(row["numero_acta_de_comite_de_credito"])
      LME_solicitado                     = str(row["LME_solicitado"])
      LME_PIC                            = str(row["LME_PIC"])
      LME_aprobado                       = str(row["LME_aprobado"])
      flujo_actual                       = str(row["flujo_actual"])
      estado_actual                      = str(row["estado_actual"])
      responsable_actual                 = str(row["responsable_actual"])
      flujo                              = str(row["flujo"])
      estado                             = str(row["estado"])
      responsable                        = str(row["responsable"])
      fecha_de_entrada                   = str(row["fecha_de_entrada"])
      hora_de_entrada                    = str(row["hora_de_entrada"])
      fecha_de_salida                    = str(row["fecha_de_salida"])
      hora_de_salida                     = str(row["hora_de_salida"])
      tiempo                             = str(row["tiempo"])
      vigencia_LME                       = str(row["vigencia_LME"])

      # Consulta para insertar los datos de los .txt en LZ
      # ¿Como insertar datos en Impala HUE SQL desde Python usando Pyodbc?
      # https://datatofish.com/insert-sql-server-python/
      consulta_sql ="""
      INSERT INTO proceso_consumidores.base_historicas_lotus_txt (
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
                  VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'
                         )
      ;
      """.format(radicado,
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

      #print(consulta_sql)
    
      cursor.execute(consulta_sql)
      #cursor.fast_executemany = True
      cn.commit() 
  
      #sp.subir_df(df_resultado_paso_1,'proceso_riesgos.tabla_1_cafe', modo="overwrite")



  """ guardar los nombres de los .txt q se subieron a LZ """
  guardar_nombre_txt_subido = lista_nombre_txt[j-1]

  guardar_nombre_txt_subido = str(guardar_nombre_txt_subido)

  lista_nombre_txt_subidos.append(guardar_nombre_txt_subido)
 
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

print("numero total de filas de todos los DataFrame")
print(contar_filas)

########################################################################
#%%
