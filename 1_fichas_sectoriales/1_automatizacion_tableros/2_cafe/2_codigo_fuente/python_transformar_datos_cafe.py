""" 
WEB SCRAPING, TRANSFORMACIONES A DataFrame y SUBIR DataFrame A LA BASE DE DATOS LZ (HUE)

PASOS DEL CÓDIGO:
1) Leer los datos de las paginas web que estan en Transformaciones.xlsx 
    y guardarlos en un DataFrame
    (se guarda un DataFrame por cada pagina web - paso).

2) Transformar datos (cambiar nombres a columnas, hacer operaciones matematicas con las casillas, etc.)

3) Unir todos los DataFrame haciendo un UNION ALL por la fecha.

4) En la LZ a Zona de Procesos se suben los DataFrame por separado:
   proceso_riesgos.tabla_1_cafe
   proceso_riesgos.tabla_2_cafe
   proceso_riesgos.tabla_3_cafe
   proceso_riesgos.tabla_4_y_5_cafe
   El df_resultado_paso_6 ya esta en la LZ - HUE (base de datos de Bancolombia) - resultados_canales.tdc_trm_hist
   proceso_riesgos.tabla_7_cafe
   proceso_riesgos.tabla_8_cafe
   proceso_riesgos.tabla_9_cafe
    
5) En la LZ a Zona de Resultados se sube un solo DataFrame que es la union de todos los DataFrame del paso 4)
   proceso_riesgos.tbl_ficha_sector_cafe
    
6) Cuando termine avisarle a Daniel Felipe Ramirez Cadavid, Sara Patricia Castro Duque y Jose Julian Mercado Garcia

subo una sola tabla a zona de procesos y despues pido permisos para q me la pasen a zona reultados

Donde diga "IMPORTANTE" ahi puse notas q PUEDEN (no siempre) hacer q el codigo falle   
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

print("Digite su NOMBRE DE USUARIO corporativo: ", end="")
nombre_usuario = input()

print("Digite su CONTRASEÑA: ", end="")
clave = input()

# Convertir las variables a tipo string
# https://www.geeksforgeeks.org/python-str-function/
nombre_usuario = str(nombre_usuario)
clave = str(clave)
# Imprimir el tipo de dato de las variable
# Tienen q ser tipo string
#type(nombre_usuario)
#type(clave)
#Out[...]: str



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

os.system('cmd /c "pip install --upgrade --user pip"')   # Actualizar pip
os.system('cmd /c "pip install openpyxl --user"')        # Exportar DataFrame de Pandas a Excel
os.system('cmd /c "pip install selenium --user"')
os.system('cmd /c "pip install pandas --user"')
os.system('cmd /c "pip install glob2 --user"')
# Conexion de Python a la base de datos de Bancolombia (LZ - HUE)
os.system('cmd /c "python -m pip install sparky-bc -i https://artifactory.apps.bancolombia.com/api/pypi/pypi-bancolombia/simple --trusted-host artifactory.apps.bancolombia.com --user"') 
os.system('cmd /c "python -m pip install Pysftp -i https://artifactory.apps.bancolombia.com/api/pypi/pypi-bancolombia/simple --trusted-host artifactory.apps.bancolombia.com --user"') 

##############
#%%

""" Importar Librerias """

from selenium import webdriver   # chromedriver.exe
#import time                     # Tiempo
import pandas as pd              # Para los DataFrame
from sparky_bc import Sparky     # Conexion de Python a la base de datos de Bancolombia (LZ - HUE), libreria hecha por el banco
import sys
from os import scandir, getcwd   # scandir manejo de rutas
                                 # getcwd directorio (carpeta) de trabajo actual
import glob


########################################################################
#%%

"""
Rutas de los Excel y de chromedriver.exe

obtener (guardar) en una variable el directorio actual
sin importar las carpetas anteriores en las q este  
q es donde esta python_transformar_datos_cafe.py

la ruta es la siguiente:
 ...\1_automatizacion_tableros\2_cafe\2_codigo_fuente

https://www.geeksforgeeks.org/python-os-getcwd-method/

IMPORTANTE:
- para q esto funcione la terminal (directorio de trabajo / working directory) 
  TIENE q estar situada en la ruta donde esta el archivo python_transformar_datos_cafe.py 
  NO puede estar en C:\\Users\\NombreUsuario

- Para q funcione en VS Code hacer lo siguiente:
  - Dependiendo de la forma en como abres VS Code es probable q el directorio de trabajo sea C:\\Users\\NombreUsuario
  - Abrir el explorador de archivos de Windows
  - Buscar la carpeta en donde tienes guardado el archivo llamado python_transformar_datos_cafe.py
  - Click derecho
  - Abrir con Code
  - Verifica q el CMD este situado en la ruta donde guardaste el python_transformar_datos_cafe.py

- Para q funcione en Spyder hay q hacer lo siguiente:
  - Hay q cambiar el directorio de trabajo de Spyder, 
    para ello hacer este video: https://www.youtube.com/watch?v=KWK5Qma-jFQ
  - Spyder por defecto inicia la terminal en C:\\Users\\NombreUsuario
  - Click en el icono de la carpeta en la parte superior derecha Browse a working directory
  - Se abre una ventana con el explorador de archivos
  - Buscar la carpeta en donde tienes guardado el codigo llamado python_transformar_datos_cafe.py
  - En mi caso en especifico, la ruta es 
    C:\\Users\\danpined\\OneDrive - Grupo Bancolombia\\4_fichas_sectoriales\\1_automatizacion_tableros\\2_cafe\\2_codigo_fuente
  - Click en Seleccionar Carpeta
  - Se cambiara la ruta en el cuadro de texto de la parte superior derecha
"""

#ruta_carpeta_codigo_fuente = r'C:\Users\danpined\OneDrive - Grupo Bancolombia\4_fichas_sectoriales\1_automatizacion_tableros\2_cafe\2_codigo_fuente'

ruta_carpeta_codigo_fuente = os.getcwd()
# Imprimir la ruta donde esta python_transformar_datos_cafe.py
# Se TIENE q imprimir la siguiente ruta:
# ...\1_automatizacion_tableros\2_cafe\2_codigo_fuente
#print(ruta_carpeta_codigo_fuente)

##############
#%%

# Devolverme entre directorios (carpetas)
#devolver = os.path.abspath(ruta_carpeta_codigo_fuente + "../")

# Ruta donde tengo el ejecutable del chromedriver.exe
# de aqui puedes descargarlo: https://chromedriver.chromium.org/downloads
#ruta_exe = r'C:\chromedriver.exe'
ruta_exe = os.path.abspath(ruta_carpeta_codigo_fuente + "../../../"  + "chromedriver.exe")
# Imprimir ruta donde esta el chromedriver.exe
# Se TIENE q imprimir la siguiente ruta:
# ...\1_automatizacion_tableros\chromedriver.exe
#print(ruta_exe)

"""

paso 1 - eliminar archivos de la ruta

archivos_delete = glob.glob(os.path.join(f'{descarga}','*.xls'))

    for f in archivos_delete:
        os.remove(f)


paso 2 - descargar el archivo de web scraping en la ruta


paso 3 - leer el nuevo archivo, para leer el arhivo utilizamos
https://es.stackoverflow.com/questions/24278/c%C3%B3mo-listar-todos-los-archivos-de-una-carpeta-usando-python

archivo = [arch.name for arch in scandir(descarga) if arch.is_file()]


paso 4 -, unir la ruta mas el nombre el archivo

ruta_archivo_carga = os.path.join(f'{descarga}',archivo[0])


funcion para llamar al web driver, pasandole el parametro de la ruta donde se van a descargar los archivos

def driver_google(ruta_descarga):
    separador = os.path.sep
    ruta_driver = os.path.join(separador.join(os.path.dirname(os.path.abspath(__file__)).split(separador)[:-1]),'chromedriver.exe')
    chromeOptions = Options()
    chromeOptions.add_experimental_option("prefs", {"download.default_directory" :f"{ruta_descarga}",})
    #llamado al driver con la nueva ruta de descargas
    driver =  webdriver.Chrome(executable_path = f'{ruta_driver}', chrome_options= chromeOptions)
    return driver

llamar a la fucnion
donde descarga seria la variable con la ruta donde quedarion los archivos

driver =  driver_google(descarga)
"""

########################################################################
""" Cambiar la ruta predeterminada de descarga de los archivos """


########################################################################
#%%

"""
"DESCARGAR LOS ARCHIVOS DE EXCEL NECESARIOS (web scraping)"
options = webdriver.ChromeOptions()
prefs = {"download.default_directory" : ruta_exe}
options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(executable_path=ruta_exe,chrome_options=options) # Esto va abrir una nueva ventana en el navegador predeterminado del sistema operativo
"""

# Enviar una peticion GET (leer)
# Esto abrira una ventana de Windows para descargar los archivos de Excel
# IMPORTANTE: Los archivos se deben guardar en la carpeta Descargas

"""
# Precios-área-y-producción-de-café-3.xlsx 
driver.get('https://federaciondecafeteros.org/app/uploads/2020/01/Precios-%C3%A1rea-y-producci%C3%B3n-de-caf%C3%A9-3.xlsx')
# Exportaciones.xlsx
driver.get('https://federaciondecafeteros.org/app/uploads/2020/01/Exportaciones.xlsx ')
# Datos históricos Futuros café C EE.UU.csv - ESTE ARCHIVO FALTA - INCOMPLETO https://es.investing.com/commodities/us-coffee-c-historical-data

# IMPORTANTE: este link de descarga cambia dependiendo del ultimo mes en q se actualizaron los datos, Ejemplo: ...destino_economico_ ene22, feb22...
# anexo_ipp_procedencias_especializadas_segun_destino_economico_feb22.xls
driver.get('https://www.dane.gov.co/files/investigaciones/boletines/ipp/anexo_ipp_procedencias_especializadas_segun_destino_economico_feb22.xls')


#time.sleep(5)
#driver.close()
"""

########################################################################
#%%

"""
#################### PASO 1 - Link pagina web:   https://federaciondecafeteros.org/wp/estadisticas-cafeteras/

                              Archivo:           Precios-área-y-producción-de-café-3.xlsx
                              Hoja: 8.           Área cult. según tecnifi 
                              
####################

Para este PASO 1 tengo 5 DataFrame:
1) df_tradicional_paso_1             = La tecnificacion tradicional es desde C hasta Q
2) df_envejecido_paso_1              = "              " envejecido  "      " R "   " AF
3) df_joven_paso_1                   = "              " joven       "      " AG "  " AU
4) df_temporal                       = Usado para copiar los datos de los DataFrame 1) hasta 3) y transformarlos (modificar los datos)
5) df_resultado_paso_1               = A este DataFrame le voy agregando los datos transformados de los DataFrame anteriores 1) hasta 4), este es el DataFrame q tiene el resultado final de los datos del paso 1

(24 departamentos contando cantidad_total) * (45 fechas desde 2007 hasta 2021) = 1080 registros (filas de df_resultado_paso_1)    
"""

print("\n")
print("\n")
print("PASO 1")

"Ruta Paso 1"

#ruta_precios_area = r'C:\Users\danpined\Downloads\Precios-área-y-producción-de-café-3.xlsx'

ruta_precios_area = os.path.abspath(ruta_carpeta_codigo_fuente + "../../" + "/1_descargas_excel_original_sin_transformar/1_precios_area_y_produccion_de_cafe")
# Imprimir ruta_precios_area
# Se TIENE q imprimir la siguiente ruta:
# ...\1_automatizacion_tableros\2_cafe\1_descargas_excel_original_sin_transformar\1_precios_area_y_produccion_de_cafe
#print(ruta_precios_area)

# Leer un archivo q esta en una carpeta pero no se como se llama
# ruta relativa (nombre del archivo)
# https://es.stackoverflow.com/questions/24278/c%C3%B3mo-listar-todos-los-archivos-de-una-carpeta-usando-python
# Guardar el nombre desconocido del archivo de Excel
nombre_excel_precios_area = [arch.name for arch in scandir(ruta_precios_area) if arch.is_file()]
# Imprimir el nombre del excel q esta dentro de ruta_precios_area
# Se TIENE q imprimir algo muy similar a esto: ['Precios-área-y-producción-de-café-3.xlsx']
#print(nombre_excel_precios_area)

# Convertir las variables a tipo string
# https://www.geeksforgeeks.org/python-str-function/
ruta_precios_area = str(ruta_precios_area)
nombre_excel_precios_area = str(nombre_excel_precios_area)

# Eliminar caracteres de la variable nombre_excel_precios_area
nombre_excel_precios_area = nombre_excel_precios_area.replace("[","")
nombre_excel_precios_area = nombre_excel_precios_area.replace("]","")
nombre_excel_precios_area = nombre_excel_precios_area.replace("'","")
# Imprimir nombre_excel_precios_area
# Se debieron eliminar los caracteres innecesarios ['']
#print(nombre_excel_precios_area)

# ruta_precios_area     = ruta COMPLETA (absoluta) donde esta el Excel Precios-área-y-producción-de-café-3.xlsx
#                         ...\1_automatizacion_tableros\2_cafe\1_descargas_excel_original_sin_transformar\1_precios_area_y_produccion_de_cafe\Precios-área-y-producción-de-café-3.xlsx                        

# ruta_precios_area_2   = ruta (relativa) de la carpeta 1_precios_area_y_produccion_de_cafe SIN el nombre del Excel
#                         ...\1_automatizacion_tableros\2_cafe\1_descargas_excel_original_sin_transformar\1_precios_area_y_produccion_de_cafe
ruta_precios_area_2 = ruta_precios_area

# Concatenar (unir) lo siguiente: (ruta_precios_area) + (nombre_excel_precios_area)
# Esto da como resultado la ruta COMPLETA (absoluta) donde esta Precios-área-y-producción-de-café-3.xlsx
ruta_precios_area = ruta_precios_area + "\\" + nombre_excel_precios_area
# Imprimir la ruta completa (absoluta) donde se encuentra Precios-área-y-producción-de-café-3.xlsx
# Se TIENE q imprimir la siguiente ruta (el nombre del Excel puede cambiar un poco, pero deberia llamarse similar):
# ...\1_automatizacion_tableros\2_cafe\1_descargas_excel_original_sin_transformar\1_precios_area_y_produccion_de_cafe\Precios-área-y-producción-de-café-3.xlsx
#print(ruta_precios_area)

# Convertir variable a tipo string
ruta_precios_area = str(ruta_precios_area)
ruta_precios_area_2 = str(ruta_precios_area_2)

##############
#%%

"Eliminar Excel Paso 1"
# Eliminar todos los archivos (Excel) q estan dentro de una carpeta

#eliminar_archivos = glob.glob(os.path.join(f'{ruta_precios_area_2}','*'))

#for f in eliminar_archivos:
    #print("f \n", f)
    #print("\n")
    #print("eliminar_archivos \n", eliminar_archivos)
#    os.remove(f)

##############
#%%

"Web Scraping paso 1"


##############
#%%

"DataFrames paso 1"

"1) df_tradicional_paso_1"
df_tradicional_paso_1 = pd.read_excel (
    ruta_precios_area, # Ruta donde esta guardado el Excel
    sheet_name = "8. Área cult. según tecnifi", # Nombre de la hoja de Excel
    header = 0, # Numero de fila donde estan los nombres de las columnas
    skiprows = 6, # Desde esta Fila hacia abajo se empieza a leer el Excel
    usecols = "C:Q", # Letras de las columnas del Excel a leer, en este caso es desde la C hasta la Q
                     # IMPORTANTE: Cuando se agrega un nuevo año esto cambia
    )

# Eliminar un indice de filas del DataFrame en especifico
# Estos numeros (indices) de filas tienen datos q no me interesan
df_tradicional_paso_1 = df_tradicional_paso_1.drop(labels=[24, 25, 26, 27], axis=0)

# Reemplazar todos los valores q sean NULL por 0
df_tradicional_paso_1.fillna(0, inplace=True)

# Convertir TODOS los datos a float64 
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.astype.html
df_tradicional_paso_1 = df_tradicional_paso_1.astype(float)

# Imprimir el tipo de dato de las columnas del DataFrame
#df_tradicional_paso_1.dtypes 

# Multiplicar todos los numeros del DataFrame por 1000 y aproximarlos a 5 decimales
# Cuando NO los aproximaba a 5 decimales round(...) se eliminaba la parte decimal del numero, y esto es un error, ya q debe conservarse todo el decimal 
df_tradicional_paso_1 = round(1000 * df_tradicional_paso_1, 5)

# Cuando se lee el DataFrame en los nombres de las columnas (años) hay unos asteriscos: 2010* 2011* ...
# Entonces como solamente son ALGUNOS (NO todos) años los q tienen este asterisco
# Primero vamos hacer q todos los años tengan este asterisco:
# https://stackoverflow.com/questions/34049618/how-to-add-a-suffix-or-prefix-to-each-column-name
df_tradicional_paso_1 = df_tradicional_paso_1.add_suffix("*")

# Ahora q todos tienen el asterisco, vamos a eliminar el asterisco de todos los nombres de las columnas (años)
# https://www.geeksforgeeks.org/pandas-remove-special-characters-from-column-names/
df_tradicional_paso_1.columns = df_tradicional_paso_1.columns.str.replace("*","")

# Imprimir los nombres de las columnas del DataFrame
# se debieron eliminar los asteriscos de los años
#df_tradicional_paso_1.columns

# Convertir los nombres de las columnas a tipo fecha
# año-mes-dia hora-minuto-segundo (todo en numeros)
df_tradicional_paso_1.columns = df_tradicional_paso_1.columns.astype("datetime64[ns]")

# Imprimir los nombres de las columnas del DataFrame
# los nombres de las columnas tienen q estar en formato fecha dtype='datetime64[ns]'
#df_tradicional_paso_1.columns

# En un principio los indices de las filas son numeros 0, 1... 23
# Cambiar los indices de las filas por los nombres de los departamentos
# Como los departamentos SIEMPRE son los mismos entonces los puedo agregar manualmente al DataFrame
# Estoy tratando total_nacional como si fuera un departamento
# Son 24 departamentos contanto el total_nacional
# df = df.rename(columns= {"NombreAntiguoDeLaColumna": "NuevoNombreColumna", 
#                          "NombreAntiguoDeLaColumna": "NuevoNombreColumna"}, 
#                          index={NumeroDeIndice: "NuevoNombreIndice"}
#                          inplace=True)
# https://note.nkmk.me/en/python-pandas-dataframe-rename

diccionario_departamentos_paso_1 = { 0: "Antioquia", # Cada numero de indice corresponde a un departamento
                                     1: "Arauca",
                                     2: "Bolivar",
                                     3: "Boyaca",
                                     4: "Caldas",
                                     5: "Caqueta",
                                     6: "Casanare",
                                     7: "Cauca",
                                     8: "Cesar",
                                     9: "Choco", 
                                     10: "Cundinamarca",
                                     11: "Huila",
                                     12: "Guajira",
                                     13: "Magdalena",
                                     14: "Meta",
                                     15: "Nariño",
                                     16: "N. Santander",
                                     17: "Putumayo",
                                     18: "Quindio",
                                     19: "Risaralda",
                                     20: "Santander",
                                     21: "Tolima",
                                     22: "Valle",
                                     23: "total_nacional"
                                   }

df_tradicional_paso_1.rename(index = diccionario_departamentos_paso_1, inplace=True)

# Imprimir los nombres de las filas (indices) del DataFrame
# Se tienen q imprimir los departamentos
#df_tradicional_paso_1.index



"2) df_envejecido_paso_1"
df_envejecido_paso_1 = pd.read_excel (
    ruta_precios_area, 
    sheet_name = "8. Área cult. según tecnifi",
    header = 0,
    skiprows = 6,
    usecols = "R:AF",
    )

df_envejecido_paso_1 = df_envejecido_paso_1.drop(labels=[24, 25, 26, 27], axis=0)

df_envejecido_paso_1.fillna(0, inplace=True)

df_envejecido_paso_1 = df_envejecido_paso_1.astype(float)
#df_envejecido_paso_1.dtypes 

df_envejecido_paso_1 = round(1000 * df_envejecido_paso_1, 5)

# https://stackoverflow.com/questions/37001787/remove-ends-of-string-entries-in-pandas-dataframe-column
 # Eliminar * y .
df_envejecido_paso_1.columns = df_envejecido_paso_1.columns.str.replace('[*,.]','')  
# En este caso el ultimo caracter es un 1 por lo q las fechas quedan asi 20071, 20081... 20211
# Eliminar el ultimo caracter sin importar cual sea 
df_envejecido_paso_1.columns = df_envejecido_paso_1.columns.map(lambda x: str(x)[:-1]) 
#df_envejecido_paso_1.columns

df_envejecido_paso_1.columns = df_envejecido_paso_1.columns.astype("datetime64[ns]")
#df_envejecido_paso_1.columns

df_envejecido_paso_1.rename(index = diccionario_departamentos_paso_1, inplace=True)
#df_envejecido_paso_1.index

"3) df_joven_paso_1"
df_joven_paso_1 = pd.read_excel (
    ruta_precios_area, 
    sheet_name = "8. Área cult. según tecnifi",
    header = 0,
    skiprows = 6,
    usecols = "AG:AU",
    )

df_joven_paso_1 = df_joven_paso_1.drop(labels=[24, 25, 26, 27], axis=0)

df_joven_paso_1.fillna(0, inplace=True)

df_joven_paso_1 = df_joven_paso_1.astype(float)
#df_joven_paso_1.dtypes 

df_joven_paso_1 = round(1000 * df_joven_paso_1, 5)

df_joven_paso_1.columns = df_joven_paso_1.columns.str.replace('[*,.]','')  
df_joven_paso_1.columns = df_joven_paso_1.columns.map(lambda x: str(x)[:-1]) 

df_joven_paso_1.columns = df_joven_paso_1.columns.astype("datetime64[ns]")
#df_joven_paso_1.columns

df_joven_paso_1.rename(index = diccionario_departamentos_paso_1, inplace=True)
#df_joven_paso_1.index

"4) df_temporal"
df_temporal = pd.DataFrame() # Crear DataFrame vacio, esto tambien sirve para vaciar el DataFrame

"5) df_resultado_paso_1"
# En este DataFrame estara el resultado final de las transformaciones del PASO 1
# Crear un DataFrame q en un principio estara vacio pero despues lo hire llenando conforme voy transformando los datos
nombre_columnas_paso_1 = ["fecha", "departamento", "tecnificacion", "cantidad_anual_ha", "unidad_ha"]
df_resultado_paso_1 = pd.DataFrame(columns = nombre_columnas_paso_1) # Darle nombres a las columnas del DataFrame resultante


""" Agregar datos transformados a df_resultado_paso_1 """

# Crear una lista con los nombres de los departamentos
# Los departamentos SIEMPRE son los mismos, aqui estamos contando total_nacional como si fuera un departamento
lista_departamentos_paso_1 = [
"Antioquia", "Arauca", "Bolivar", "Boyaca", "Caldas", "Caqueta",
"Casanare", "Cauca", "Cesar", "Choco", "Cundinamarca", "Huila",
"Guajira", "Magdalena", "Meta", "Nariño", "N. Santander", "Putumayo",
"Quindio", "Risaralda", "Santander", "Tolima", "Valle", "total_nacional"
]

for i in range(0, len(df_tradicional_paso_1)): # Para i = 0,1,2... 23 / son 24 departamentos contando el total nacional
    #print(i)
    #print(" ")
    #print(df_tradicional_paso_1.iloc[i])
    
    # .iloc[fila, columna] sirve para seleccionar filas y columnas de un DataFrame
    # https://www.delftstack.com/es/howto/python-pandas/pandas-loc-vs-iloc-python/
    # esto df_tradicional_paso_1.iloc[i] me da como resultado cada una de las fechas y cantidad_anual_ha
    # Guardo la fecha y cantidad_anual_ha en una variable de tipo series
    fecha_y_cantidad_anual_ha_paso_1 = df_tradicional_paso_1.iloc[i]
    # Convierto el resultado a un DataFrame
    df_temporal = pd.DataFrame(fecha_y_cantidad_anual_ha_paso_1)
    # La fecha es un indice, convertir el indice en columna
    df_temporal = df_temporal.reset_index()
    
    # La columna q se llama "index"              cambiarle el nombre a "fecha"
    # "                   " como el departamento "                   " "cantidad_anual_ha" (para esto usamos la lista_departamentos_paso_1)
    df_temporal = df_temporal.rename(columns={"index": "fecha",
                                              lista_departamentos_paso_1[i]: "cantidad_anual_ha"} # lista_departamentos_paso_1 en la posicion i se reemplaza por el nombre de cada uno de los departamentos
                                     )

    df_temporal = df_temporal.assign(departamento = lista_departamentos_paso_1[i], # El primer departamento es Antioquia, el segundo Arauca y asi sucesivamente hasta llegar a total_nacional
                                     tecnificacion = "tradicional", # Dependiendo del DataFrame se cual es la tecnificacion
                                                                    # Como en este caso estoy usando el df_tradicional_paso_1 para llenar los datos de df_temporal
                                                                    # entonces la tecnificacion es tradicional
                                     unidad_ha = "ha" # para el paso 1 los datos de la columna "unidad_..." SIEMPRE son "ha" 
                                     )
    
    # Cambiar el orden de las columnas de df_temporal
    # El orden en q deben estar las columnas es:
    # nombre_columnas_paso_1 = ["fecha", "departamento", "tecnificacion", "cantidad_anual_ha", "unidad_ha"]
    df_temporal = df_temporal.reindex(columns = nombre_columnas_paso_1)
    
    # .append() inserta nuevas filas
    # https://www.delftstack.com/es/howto/python-pandas/how-to-add-one-row-to-pandas-dataframe/#el-m%25C3%25A9todo-.append-de-dataframe-para-a%25C3%25B1adir-una-fila
    # copiar los datos de df_temporal a df_resultado_paso_1
    # esto da el resultado final de los datos transformados del paso 1
    # ignore_index = ... Reiniciar los indices cuando se copian los datos del DataFrame
    df_resultado_paso_1 = df_resultado_paso_1.append(df_temporal, ignore_index=True)

df_temporal = pd.DataFrame() # Vaciar los datos del DataFrame temporal

for i in range(0, len(df_envejecido_paso_1)): 
    fecha_y_cantidad_anual_ha_paso_1 = df_envejecido_paso_1.iloc[i]
    df_temporal = pd.DataFrame(fecha_y_cantidad_anual_ha_paso_1)
    df_temporal = df_temporal.reset_index()
    df_temporal = df_temporal.rename(columns={"index": "fecha",
                                              lista_departamentos_paso_1[i]: "cantidad_anual_ha"}
                                     )
    df_temporal = df_temporal.assign(departamento = lista_departamentos_paso_1[i], 
                                     tecnificacion = "envejecido",
                                     unidad_ha = "ha")
    df_temporal = df_temporal.reindex(columns = nombre_columnas_paso_1)
    df_resultado_paso_1 = df_resultado_paso_1.append(df_temporal, ignore_index=True)

df_temporal = pd.DataFrame()

for i in range(0, len(df_joven_paso_1)): 
    fecha_y_cantidad_anual_ha_paso_1 = df_joven_paso_1.iloc[i]
    df_temporal = pd.DataFrame(fecha_y_cantidad_anual_ha_paso_1)
    df_temporal = df_temporal.reset_index()
    df_temporal = df_temporal.rename(columns={"index": "fecha",
                                              lista_departamentos_paso_1[i]: "cantidad_anual_ha"}
                                     )
    df_temporal = df_temporal.assign(departamento = lista_departamentos_paso_1[i], 
                                     tecnificacion = "joven",
                                     unidad_ha = "ha")
    df_temporal = df_temporal.reindex(columns = nombre_columnas_paso_1)
    df_resultado_paso_1 = df_resultado_paso_1.append(df_temporal, ignore_index=True)

df_temporal = pd.DataFrame()

#df_resultado_paso_1.dtypes



##################################################################################################################################
#%%

"""
#################### PASO 2 - Link pagina web:  https://federaciondecafeteros.org/wp/estadisticas-cafeteras/

                              Son 2 archivos (hojas)
                              1) Archivo:          Precios-área-y-producción-de-café-3.xlsx
                                 Hoja:             9. Producción mensual
                                 Nombre Columna:   Producción = produccion_mensual_sacos
                              
                              2) Archivo:          Exportaciones.xlsx
                                 Hoja:             1. Total_Volumen
                                 Nombre Columna:   Total Exportaciones = exportacion_mensual_sacos
                                 
                                 
####################

Para este PASO 2 tengo 3 DataFrame:
1) df_produccion_mensual_paso_2     = Contiene los datos del archivo 1)
2) df_total_volumen_paso_2          = "                            " 2)
3) df_resultado_paso_2              = Es la union (merge / INNER JOIN) de los DataFrame anteriores 1) y 2) usando la fecha, este es el DataFrame q tiene el resultado final de los datos del paso 2    
"""

print("\n")
print("\n")
print("PASO 2")

"1) df_produccion_mensual_paso_2"
df_produccion_mensual_paso_2 = pd.read_excel (
    ruta_precios_area, # La hoja 9. Producción mensual esta en el archivo Precios-área-y-producción-de-café-3.xlsx 
    sheet_name = "9. Producción mensual",
    header = None, # Esto hara q los indices de la columna del DataFrame sean los mismos q los numeros de columna donde estan los datos del Excel
    skiprows = 654, # Los datos se empiezan a leer desde Enero del 2010 hasta el ultimo mes actual al q estamos hoy
                    # IMPORTANTE: es algo probable q este numero cambie
    usecols = "D, E", 
    )

# Cambiar los nombres de las columnas
# df.set_axis(lista_con_nombre_de_las_columnas, axis=1)
# axis=1 significa q se modifican los indices de las columnas
# https://datascienceparichay.com/article/pandas-rename-column-names/
df_produccion_mensual_paso_2 = df_produccion_mensual_paso_2.set_axis(["fecha", "produccion_mensual_sacos"], axis=1)

# Imprimir los nombres de las columnas del DataFrame
#df_produccion_mensual_paso_2.columns

# Eliminar TODAS las filas del DataFrame q en la columna llamada produccion_mensual_sacos estan vacias (NULL)
# df.dropna(subset = ["nombre_columna"], inplace=True)
# https://www.kite.com/python/answers/how-to-drop-empty-rows-from-a-pandas-dataframe-in-python
df_produccion_mensual_paso_2.dropna(subset = ["produccion_mensual_sacos"], inplace=True)

# Multiplicar todos los datos de la columna llamada produccion_mensual_sacos por 1000
df_produccion_mensual_paso_2["produccion_mensual_sacos"] = 1000 * df_produccion_mensual_paso_2["produccion_mensual_sacos"]

# Imprimir la columna llamada produccion_mensual_sacos
#df_produccion_mensual_paso_2["produccion_mensual_sacos"]

# En la columna llamada fecha eliminar la hora y solamente dejar la fecha 
#df_produccion_mensual_paso_2["fecha"] = pd.to_datetime(df_produccion_mensual_paso_2["fecha"]).dt.date

# Convertir a tipo fecha
# año-mes-dia hora-minuto-segundo (todo en numeros)
df_produccion_mensual_paso_2["fecha"] = df_produccion_mensual_paso_2["fecha"].astype("datetime64[ns]")

# Imprimir el tipo de dato de las columnas del DataFrame
#df_produccion_mensual_paso_2.dtypes

##############
#%%

#ruta_exportaciones = r'C:\Users\danpined\Downloads\Exportaciones.xlsx'

ruta_exportaciones = os.path.abspath(ruta_carpeta_codigo_fuente + "../../" + "/1_descargas_excel_original_sin_transformar/2_exportaciones")
# Imprimir ruta_exportaciones
# Se TIENE q imprimir la siguiente ruta:
# ...\1_automatizacion_tableros\2_cafe\1_descargas_excel_original_sin_transformar\2_exportaciones
#print(ruta_exportaciones)

# Leer un archivo q esta en una carpeta pero no se como se llama
# ruta relativa (nombre del archivo)
# https://es.stackoverflow.com/questions/24278/c%C3%B3mo-listar-todos-los-archivos-de-una-carpeta-usando-python
# Guardar el nombre desconocido del archivo de Excel
nombre_excel_exportaciones = [arch.name for arch in scandir(ruta_exportaciones) if arch.is_file()]
# Imprimir el nombre del Excel q esta dentro de ruta_exportaciones
# Se TIENE q imprimir algo muy similar a esto: ['Exportaciones.xlsx']
#print(nombre_excel_exportaciones)

# Convertir las variables a tipo string
# https://www.geeksforgeeks.org/python-str-function/
ruta_exportaciones = str(ruta_exportaciones)
nombre_excel_exportaciones = str(nombre_excel_exportaciones)

# Eliminar caracteres de la variable nombre_excel_exportaciones
nombre_excel_exportaciones = nombre_excel_exportaciones.replace("[","")
nombre_excel_exportaciones = nombre_excel_exportaciones.replace("]","")
nombre_excel_exportaciones = nombre_excel_exportaciones.replace("'","")
# Imprimir nombre_excel_exportaciones
# Se debieron eliminar los caracteres innecesarios ['']
#print(nombre_excel_exportaciones)

# Concatenar (unir) lo siguiente: (ruta_exportaciones) + (nombre_excel_exportaciones)
# Esto da como resultado la ruta COMPLETA (absoluta) donde esta Exportaciones.xlsx
ruta_exportaciones = ruta_exportaciones + "\\" + nombre_excel_exportaciones
# Imprimir la ruta completa (absoluta) donde se encuentra Exportaciones.xlsx
# Se TIENE q imprimir la siguiente ruta (el nombre del Excel puede cambiar un poco, pero deberia llamarse similar):
# ...\1_automatizacion_tableros\2_cafe\1_descargas_excel_original_sin_transformar\2_exportaciones\Exportaciones.xlsx
#print(ruta_exportaciones)

# Convertir variable a tipo string
ruta_exportaciones = str(ruta_exportaciones)

"2) df_total_volumen_paso_2"
df_total_volumen_paso_2 = pd.read_excel (
    ruta_exportaciones, # La hoja 1. Total_Volumen esta en el archivo Exportaciones.xlsx
    sheet_name = "1. Total_Volumen", 
    header = None,
    skiprows = 631, # IMPORTANTE: es algo probable q este numero cambie
    usecols = "D, E",
    )

df_total_volumen_paso_2 = df_total_volumen_paso_2.set_axis(["fecha", "exportacion_mensual_sacos"], axis=1)
#df_total_volumen_paso_2.columns

df_total_volumen_paso_2.dropna(subset = ["exportacion_mensual_sacos"], inplace=True)

# Debes configurar el Spyder para q los numeros NO se vean en notacion cientifica:
# - Abrir un DataFrame en el explorador de variables (en este caso abrir df_total_volumen_paso_2)
# - Se abre una ventana
# - Click en la parte inferior izquierda donde dice "Format"
# - Escribir esto %.15g
# - Click en OK
# https://stackoverflow.com/questions/67344408/spyder-variable-explorer-disable-scientific-notation
df_total_volumen_paso_2["exportacion_mensual_sacos"] = df_total_volumen_paso_2["exportacion_mensual_sacos"] * 1000
#df_total_volumen_paso_2["exportacion_mensual_sacos"]

df_total_volumen_paso_2["exportacion_mensual_sacos"] = df_total_volumen_paso_2["exportacion_mensual_sacos"].astype("int64")
#df_total_volumen_paso_2.dtypes

df_total_volumen_paso_2["fecha"] = df_total_volumen_paso_2["fecha"].astype("datetime64[ns]")
#df_total_volumen_paso_2.dtypes

"3) df_resultado_paso_2"
# En este DataFrame estara el resultado final de las transformaciones del PASO 2
# Crear un DataFrame q en un principio estara vacio pero despues lo llenare uniendo (merge) los DataFrame 1) df_produccion_mensual_paso_2 y 2) df_total_volumen_paso_2 de acuerdo a la fecha
nombre_columnas_paso_2 = ["fecha", "produccion_mensual_sacos", "exportacion_mensual_sacos"] # Aqui falta la columna llamada unidad q la crearemos mas abajo despues del merge
df_resultado_paso_2 = pd.DataFrame(columns = nombre_columnas_paso_2)

""" 
Tipos de JOIN en SQL:
https://stackoverflow.com/questions/5706437/whats-the-difference-between-inner-join-left-join-right-join-and-full-join

El equivalente a este pd.merge(...) de aqui abajo en SQL es:
SELECT t1.fecha,
       t1.exportacion_mensual_sacos,
       t2.produccion_mensual_sacos
FROM exportacion_mensual_sacos t1
INNER JOIN produccion_mensual_sacos t2 ON t1.fecha = t2.fecha

Agregar datos transformados a df_resultado_paso_2 
"""

df_resultado_paso_2 = pd.merge(left       = df_produccion_mensual_paso_2,  # Columna q esta a la izquierda
                               right      = df_total_volumen_paso_2,       # "                 " derecha
                               how        = "inner",                # how= ... es el tipo de JOIN (union)
                                                                    # En este caso como las FECHAS de ambas tablas son las MISMAS entonces es un INNER JOIN                                                              
                               left_on    = ["fecha"],              # Nombre columna de la tabla izquierda left = ... por la q se unen los datos
                               right_on   = ["fecha"]               # "                        " derecha  right = ... "                        "
                               )

"""
# Verificar q el INNER JOIN este bien hecho, para ello vamos hacer lo siguiente:
# SI la suma de la columna produccion_mensual_sacos es la misma en... 
if ( (sum(df_produccion_mensual_paso_2["produccion_mensual_sacos"]) ==      # df_produccion_mensual_paso_2
      sum(df_resultado_paso_2["produccion_mensual_sacos"])           # df_resultado_paso_2
      )
      and                                                            # Y ademas
                                                                     # la suma de la columna exportacion_mensual_sacos es la misma en...
      (sum(df_total_volumen_paso_2["exportacion_mensual_sacos"]) ==         # df_total_volumen_paso_2
       sum(df_resultado_paso_2["exportacion_mensual_sacos"])         # df_resultado_paso_2
      ) 
   ): # ENTONCES el INNER JOIN esta BIEN hecho
    print("\n", "el INNER JOIN entre df_produccion_mensual_paso_2 y df_total_volumen_paso_2 es CORRECTO")
else: # SINO el INNER JOIN esta MAL hecho
    print("\n", "ERROR al hacer INNER JOIN entre df_produccion_mensual_paso_2 y df_total_volumen_paso_2")
"""

# Crear columna unidad
# Para el paso 2 todos los datos de la columna "unidad_..." SIEMPRE son "Saco 60 kg" 
df_resultado_paso_2 = df_resultado_paso_2.assign(unidad_produccion_y_exportacion_mensual_sacos = "Saco 60 kg")



##################################################################################################################################
#%%

"""
#################### PASO 3 - Link Pagina Web:    https://federaciondecafeteros.org/wp/estadisticas-cafeteras/

                              Archivo:            Exportaciones.xlsx
                              Hoja:               10. Exportador_Destino
                              Columnas:           E = País destino                = pais_destino
                                                  F = Nombre exportador           = exportador
                                                  H = Sacos de 60 Kg. Exportados  = cantidad_sacos_exportacion
####################

Para este PASO 3 tengo 1 DataFrame:
1) df_resultado_paso_3 = Este es el DataFrame q tiene el resultado final de los datos del paso 3
"""

print("\n")
print("\n")
print("PASO 3")

"1) df_resultado_paso_3"
df_resultado_paso_3 = pd.read_excel (
    ruta_exportaciones,
    sheet_name = "10. Exportador_Destino",
    header = None, 
    skiprows = 8,
    usecols = "C, D, E, F, H", 
    )

# Cambiar los nombres de las columnas
# df.set_axis(lista_con_nombre_de_las_columnas, axis=1)
# axis=1 significa q se modifican los indices de las columnas
# https://datascienceparichay.com/article/pandas-rename-column-names/
df_resultado_paso_3 = df_resultado_paso_3.set_axis(["anio", "mes", "pais_destino", "exportador", "cantidad_sacos_exportacion"], axis=1)
# Imprimir los nombres de las columnas
#df_resultado_paso_3.columns

# En las ultimas filas del df_resultado_paso_3 han quedado unos datos q no me importan
# Eliminar TODAS las filas del DataFrame q en la columna llamada pais_destino estan vacias (NULL)
# IMPORTANTE: si en una fila falta el pais_destino entonces se elimina la fila, por lo tanto para q esto funcione en toda la columna pais_destino NO puede existir ninguna casilla NULL
# df.dropna(subset = ["nombre_columna"], inplace=True)
# https://www.kite.com/python/answers/how-to-drop-empty-rows-from-a-pandas-dataframe-in-python
df_resultado_paso_3.dropna(subset = ["pais_destino"], inplace=True)

# ¿Como selecccionar varias columnas de un DataFrame? https://www.geeksforgeeks.org/how-to-select-multiple-columns-in-a-pandas-dataframe/
# Convertir los tipos de datos del DataFrame
# Para poder concatenar las columnas año y mes, estas tienen q ser tipo string
# Convertir las columnas año y mes a string
# https://stackoverflow.com/questions/54655705/python-is-adding-point-zero-at-end-post-number-to-string
df_resultado_paso_3[["anio", "mes"]] = df_resultado_paso_3[["anio", "mes"]].astype("int64").astype("string") # primero convierto anio y mes a entero y despues a string, asi no se va a agregar un .0 al final
df_resultado_paso_3[["pais_destino", "exportador"]] = df_resultado_paso_3[["pais_destino", "exportador"]].astype("string")
#df_resultado_paso_3["cantidad_sacos_exportacion"] = df_resultado_paso_3["cantidad_sacos_exportacion"].astype("int64")
# Imprimir el tipo de dato de las columnas del DataFrame
#df_resultado_paso_3.dtypes

# Crear una nueva columna llamada "fecha"
# la cual se hace concatenando las columnas "anio" y "mes"
# IMPORTANTE si existen casillas NULL en "anio" y "mes" esto va a dar error (porq estarias concatenando casillas NULL)
# https://stackoverflow.com/questions/19377969/combine-two-columns-of-text-in-pandas-dataframe
df_resultado_paso_3["fecha"] = df_resultado_paso_3[["anio", "mes"]].agg("-".join, axis=1)
# Imprimir la columna "fecha"
#df_resultado_paso_3["fecha"]

# Convertir a tipo fecha
# año-mes-dia hora-minuto-segundo (todo en numeros)
df_resultado_paso_3["fecha"] = df_resultado_paso_3["fecha"].astype("datetime64[ns]")
# Imprimir el tipo de dato de las columnas del DataFrame
#df_resultado_paso_3.dtypes

# Eliminar las columnas "anio" y "mes" por completo
# https://stackoverflow.com/questions/13411544/delete-a-column-from-a-pandas-dataframe
del df_resultado_paso_3["anio"]
del df_resultado_paso_3["mes"]
# Imprimir las columnas de df_resultado_paso_3 - como eliminaste "anio" y "mes" entonces NO se deben imprimir
#df_resultado_paso_3.columns

# Crear columna unidad
# Para el paso 3 los datos de la columna "unidad_..." SIEMPRE son "Saco 60 Kg" 
df_resultado_paso_3 = df_resultado_paso_3.assign(unidad_cantidad_sacos_exportacion = "Saco 60 Kg")

# Cambiar el orden de las columnas de df_resultado_paso_3
# El orden en q deben estar las columnas es:
nombre_columnas_paso_3 = ["fecha", "pais_destino", "exportador", "cantidad_sacos_exportacion", "unidad_cantidad_sacos_exportacion"]
df_resultado_paso_3 = df_resultado_paso_3.reindex(columns = nombre_columnas_paso_3)



##################################################################################################################################
#%%

"""
#################### PASO 4 y 5 -  Links Paginas Web: https://federaciondecafeteros.org/wp/estadisticas-cafeteras/
                                                      https://es.investing.com/commodities/us-coffee-c-historical-data

                                   Son 2 archivos:
                                   1) Archivo:        Precios-área-y-producción-de-café-3.xlsx
                                      Hoja:           6. Precio OIC Mensual

                                      Todas estas columnas en el Excel se llaman "Promedio ponderado"
                                      Letra columna:  B = fecha
                                                      C = precio_OIC
                                                      F = precio_suaves_col
                                                      I = precio_otros_suaves
                                                      L = precio_naturales_brasil
                                                      O = precio_robustas

                                  2) Archivo:        Datos históricos Futuros café C EE.UU..csv
                                     Hoja:           Datos históricos Futuros café C
                                     
                                     Letra columna:  A = fecha
                                                     B = Último = precio_internacional

Para calcular prima_precio_colombia hay q restar estas dos columnas:
prima_precio_colombia = precios_suaves_col – precio_internacional
####################

Para este PASO 4 tengo 2 DataFrame:
    
1) df_resultado_paso_4_y_5 = Este es el DataFrame q tiene el resultado final de los datos del pasos 4 y 5
2) df_precio_internacional_paso_5 = Lee la columna precio_internacional y la agrega dentro del df_resultado_paso_4_y_5
"""

print("\n")
print("\n")
print("PASOS 4 y 5")

"1) df_resultado_paso_4_y_5"
df_resultado_paso_4_y_5 = pd.read_excel (
    ruta_precios_area, 
    sheet_name = "6. Precio OIC Mensual",
    header = None,
    skiprows = 199, # IMPORTANTE: es algo probable q este numero cambie skiprows = 199
                    # SOLAMENTE se toman los datos desde 2016-01-01 hasta el ultimo registro q es la fecha del mes en la q estamos hoy actualmente
                    # es decir, eliminar los registros inferiores a 2016-01-01
                    # 2016-01-01 NO se elimina
    usecols = "B, C, F, I, L, O", 
    )

# Cambiar los nombres de las columnas
# df.set_axis(lista_con_nombre_de_las_columnas, axis=1)
# axis=1 significa q se modifican los indices de las columnas
# https://datascienceparichay.com/article/pandas-rename-column-names/
nombre_columnas_paso_4 = ["fecha", "precio_OIC", "precio_suaves_col", "precio_otros_suaves", "precio_naturales_brasil", "precio_robustas"] # Aqui falta la columna unidad q la añadiremos despues
df_resultado_paso_4_y_5 = df_resultado_paso_4_y_5.set_axis(nombre_columnas_paso_4, axis=1)
# Imprimir los nombres de las columnas
#df_resultado_paso_4_y_5.columns

# Convertir a tipo fecha
# año-mes-dia hora-minuto-segundo (todo en numeros)
df_resultado_paso_4_y_5["fecha"] = df_resultado_paso_4_y_5["fecha"].astype("datetime64[ns]")
# Imprimir el tipo de dato de las columnas del DataFrame
#df_resultado_paso_4_y_5.dtypes

# Crear columna unidad
# Para el paso 4 todos los datos de la columna "unidad_..." SIEMPRE son "CC USD / lb" 
df_resultado_paso_4_y_5 = df_resultado_paso_4_y_5.assign(unidad_precio_suaves_naturales_robustas_prima = "CC USD / lb")


##############
#%%

#ruta_datos_historicos = r'C:\Users\danpined\Downloads\Datos históricos Futuros café C EE.UU..csv'

ruta_datos_historicos = os.path.abspath(ruta_carpeta_codigo_fuente + "../../" + "/1_descargas_excel_original_sin_transformar/3_datos_historicos_futuros_cafe")
# Imprimir ruta_datos_historicos
# Se TIENE q imprimir la siguiente ruta:
# ...\1_automatizacion_tableros\2_cafe\1_descargas_excel_original_sin_transformar\3_datos_historicos_futuros_cafe
#print(ruta_datos_historicos)

# Leer un archivo q esta en una carpeta pero no se como se llama
# ruta relativa (nombre del archivo)
# https://es.stackoverflow.com/questions/24278/c%C3%B3mo-listar-todos-los-archivos-de-una-carpeta-usando-python
# Guardar el nombre desconocido del archivo de Excel
nombre_excel_datos_historicos = [arch.name for arch in scandir(ruta_datos_historicos) if arch.is_file()]
# Imprimir el nombre del Excel q esta dentro de ruta_datos_historicos
# Se TIENE q imprimir algo muy similar a esto: ['Datos históricos Futuros café C EE.UU..csv']
#print(nombre_excel_datos_historicos)

# Convertir las variables a tipo string
# https://www.geeksforgeeks.org/python-str-function/
ruta_datos_historicos = str(ruta_datos_historicos)
nombre_excel_datos_historicos = str(nombre_excel_datos_historicos)

# Eliminar caracteres de la variable nombre_excel_datos_historicos
nombre_excel_datos_historicos = nombre_excel_datos_historicos.replace("[","")
nombre_excel_datos_historicos = nombre_excel_datos_historicos.replace("]","")
nombre_excel_datos_historicos = nombre_excel_datos_historicos.replace("'","")
# Imprimir nombre_excel_datos_historicos
# Se debieron eliminar los caracteres innecesarios ['']
#print(nombre_excel_datos_historicos)

# Concatenar (unir) lo siguiente: (ruta_datos_historicos) + (nombre_excel_datos_historicos)
# Esto da como resultado la ruta COMPLETA (absoluta) donde esta Datos históricos Futuros café C EE.UU..csv
ruta_datos_historicos = ruta_datos_historicos + "\\" + nombre_excel_datos_historicos
# Imprimir la ruta completa (absoluta) donde se encuentra Datos históricos Futuros café C EE.UU..csv
# Se TIENE q imprimir la siguiente ruta (el nombre del Excel puede cambiar un poco, pero deberia llamarse similar):
# ...\1_automatizacion_tableros\2_cafe\1_descargas_excel_original_sin_transformar\3_datos_historicos_futuros_cafe\Datos históricos Futuros café C EE.UU..csv
#print(ruta_datos_historicos)

# Convertir variable a tipo string
ruta_datos_historicos = str(ruta_datos_historicos)

"2) df_precio_internacional_paso_5"
# ¿Como leer un .csv en Python - Pandas?
# https://towardsdatascience.com/how-to-read-csv-file-using-pandas-ab1f5e7e7b58
df_precio_internacional_paso_5 = pd.read_csv (
    ruta_datos_historicos, # Ruta donde esta guardado el .csv
    header = 0, # Numero de fila donde estan los nombres de las columnas
    skiprows = 0, # Desde esta Fila hacia abajo se empieza a leer el Excel
    usecols=["Fecha", "Último"], # SOLAMENTE leer las columnas llamadas Fecha y Último, eliminar las otras columnas}
    )

# Cambiar los nombres de las columnas
# df.set_axis(lista_con_nombre_de_las_columnas, axis=1)
# axis=1 significa q se modifican los indices de las columnas
# https://datascienceparichay.com/article/pandas-rename-column-names/
df_precio_internacional_paso_5 = df_precio_internacional_paso_5.set_axis(["fecha", "precio_internacional"], axis=1)
# Imprimir los nombres de las columnas del DataFrame
#df_precio_internacional_paso_5.columns

# Convertir columna "fecha" a string
df_precio_internacional_paso_5["fecha"] = df_precio_internacional_paso_5["fecha"].astype("string") 
# Verificamos q si se haya convertido a string
#df_precio_internacional_paso_5.dtypes

# Los meses estan en español (ene, feb... dic), cambiarlos a ingles (jan, feb... dec)
df_precio_internacional_paso_5["fecha"] = (df_precio_internacional_paso_5["fecha"].str.replace("ene", "january")
                                                                                  .str.replace("feb", "february")
                                                                                  .str.replace("mar", "march")
                                                                                  .str.replace("abr", "april")
                                                                                  #.str.replace("may", "may")
                                                                                  .str.replace("jun", "june")
                                                                                  .str.replace("jul", "july")
                                                                                  .str.replace("ago", "august")
                                                                                  .str.replace("sep", "september")
                                                                                  .str.replace("oct", "october")
                                                                                  .str.replace("nov", "november")
                                                                                  .str.replace("dic", "december")
                                           )
# Imprimir columna fecha
#df_precio_internacional_paso_5["fecha"]
#0       january-22
#1      december-21
#...
#72      january-16

# Agregar "01-" al principio de las fechas
# Esto es asi porq en las fechas siempre se toma el primer dia 01 del mes
# La columna fecha (hasta el momento) tiene las fechas sin el 01 al principio. 
concatenar_fecha_paso_5 = "01-" + df_precio_internacional_paso_5["fecha"]
# Imprimir la variable concatenar_fecha_paso_5 
#concatenar_fecha_paso_5
#0       01-january-22
#1      01-december-21
#...
#72      01-january-16

# Meter en la columna fecha del df_precio_internacional_paso_5 las fechas con el 01- al principio
df_precio_internacional_paso_5["fecha"] = concatenar_fecha_paso_5
#Imprimir columna fecha
#df_precio_internacional_paso_5["fecha"]
#0       01-january-22
#1      01-december-21
#...
#72      01-january-16

# Convertir la columna llamada "fecha" a tipo fecha datetime64[D]
# https://stackoverflow.com/questions/55275660/pandas-column-astype-error-typeerror-cannot-cast-index-to-dtype-datetime64d
df_precio_internacional_paso_5["fecha"] = pd.to_datetime(df_precio_internacional_paso_5["fecha"]).values.astype('datetime64[D]')
# tipo de dato de la columna fecha - la columna fecha tiene q ser tipo fecha datetime64[ns]
#df_precio_internacional_paso_5.dtypes
#fecha                   datetime64[ns]
#precio_internacional            object
#dtype: object

# Imprimir columna fecha, debe estar con este formato: año-mes-dia hora-minuto-segundo (todo en numeros)
#df_precio_internacional_paso_5["fecha"]
#0    2022-01-01
#1    2021-12-01
#...
#72   2016-01-01


# Convertir columna precio_internacional a string
df_precio_internacional_paso_5["precio_internacional"] = df_precio_internacional_paso_5["precio_internacional"].astype("string") 
#Verificar q si se haya convertido a string
#df_precio_internacional_paso_5.dtypes
#fecha                   datetime64[ns]
#precio_internacional            string
#dtype: object

# En la columna precio_internacional los numeros decimales estan con comas. Ejemplo: 235,10 ... 116,35
# Reemplazar las comas por puntos. Ejemplo: 235.10 ... 116.35
df_precio_internacional_paso_5["precio_internacional"] = (df_precio_internacional_paso_5["precio_internacional"].str.replace(",", "."))
# Verificar q si se hayan cambiado las comas por los puntos
#df_precio_internacional_paso_5["precio_internacional"]
#0     235.10
#1     226.10
#...
#72    116.35

# Convertir la columna llamada "precio_internacional" a float64
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.astype.html
df_precio_internacional_paso_5["precio_internacional"] = df_precio_internacional_paso_5["precio_internacional"].astype(float)
# Imprimir el tipo de dato de las columnas
#df_precio_internacional_paso_5.dtypes
# Se tiene q imprimir esto:
#fecha                   datetime64[ns]
#precio_internacional           float64
#dtype: object

# Ordenar los DataFrame de acuerdo a las fechas
# desde la fecha mas antigua (2016-01-01) hasta la mas reciente (2022-01-01)
# .sort_values(...) es para ordenar datos
# by="NombreColumnaPorLaQseOrdenanLosDatos"
# ascending=True de menor a mayor (ascendentemente)
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sort_values.html
df_precio_internacional_paso_5 = df_precio_internacional_paso_5.sort_values(by="fecha", ascending=True)
df_resultado_paso_4_y_5 = df_resultado_paso_4_y_5.sort_values(by="fecha", ascending=True)

# Reiniciar los indices de las filas
df_precio_internacional_paso_5 = df_precio_internacional_paso_5.reset_index(drop=True)  
df_resultado_paso_4_y_5 = df_resultado_paso_4_y_5.reset_index(drop=True) 

# Calcular...
# prima_precio_colombia = precio_suaves_col – precio_internacional
prima_precio_colombia_paso_4_y_5 = df_resultado_paso_4_y_5["precio_suaves_col"] - df_precio_internacional_paso_5["precio_internacional"]

# Vamos a agregar prima_precio_colombia_paso_4_y_5 al DataFrame df_resultado_paso_4_y_5
# Agregar una nueva columna a un DataFrame existente
# https://www.geeksforgeeks.org/python-pandas-dataframe-assign/
df_resultado_paso_4_y_5 = df_resultado_paso_4_y_5.assign(prima_precio_colombia = prima_precio_colombia_paso_4_y_5)
#Verificar q si se haya creado la columna prima_precio_colombia
#df_resultado_paso_4_y_5["prima_precio_colombia"]
#0     18.922381
#1     24.516667
#...
#72    59.830952

# Agregar columna precio_internacional al DataFrame df_resultado_paso_4_y_5
df_resultado_paso_4_y_5 = df_resultado_paso_4_y_5.assign(precio_internacional = df_precio_internacional_paso_5["precio_internacional"])
#Verificar q si se haya creado la columna
#df_resultado_paso_4_y_5["precio_internacional"]
#0     116.35
#1     112.65
#...
#72    235.10

# Ordenar las columnas del DataFrame df_resultado_paso_4_y_5
# El orden en q deben estar las columnas es:
nombre_columnas_paso_4_y_5 = ["fecha", "precio_OIC", "precio_suaves_col", "precio_internacional", "prima_precio_colombia", "precio_otros_suaves", "precio_naturales_brasil", "precio_robustas", "unidad_precio_suaves_naturales_robustas_prima"]  
df_resultado_paso_4_y_5 = df_resultado_paso_4_y_5.reindex(columns = nombre_columnas_paso_4_y_5)
# Verificar q si se haya cambiado el orden de las columnas
#df_resultado_paso_4_y_5.columns



##################################################################################################################################
#%%

"""
#################### PASO 5 - link Pagina Web:   https://es.investing.com/commodities/us-coffee-c-historical-data
 
                              Archivo:           Datos históricos Futuros café C EE.UU..csv    
                              Hoja:              Datos históricos Futuros café C
                            
                              Letra columna:     A = fecha
                                                 B = Último = 

####################

OUTLOOK:      https://office.live.com/start/Outlook.aspx?omkt=es-CL
Correo:       webscraping3@outlook.es
Clave:        12345678a#1

INVESTING:    https://es.investing.com/commodities/us-coffee-c-historical-data
Correo:       webscraping3@outlook.es
Clave:        12345678ab#1

####################

Para este PASO 5 tengo X DataFrame:
1) df_resultado_paso_5 = Este es el DataFrame q tiene el resultado final de los datos del paso 5
"""

"""
#Iniciar driver
    options = webdriver.ChromeOptions()
    # options.add_argument("no-sandbox")
    prefs = {'download.default_directory' : f'{ruta}\downloads\investing'}
    options.add_experimental_option('prefs', prefs)
    # options.headless = True
    #Abre el navegador
    driver = webdriver.Chrome(executable_path='./chromedriver', options=options)
    driver.maximize_window()
    #Hacer que el driver entre a la pagina
    driver.get('https://www.investing.com')
    sleep(10)
    #Cerrar el banner inicial
    # driver.find_element_by_xpath('/html/body/div[6]/div[2]/i').click()
    #Iniciar sesion en la cuenta
    driver.find_element_by_css_selector("a[class*='login']").click()
    driver.find_element_by_id('loginFormUser_email').send_keys('jjulian951@hotmail.com')
    driver.find_element_by_id('loginForm_password').send_keys('Investing12.')
    driver.find_element_by_xpath("//div[@id='loginEmailSigning']//following-sibling::a[@class='newButton orange']").click()
    #Hacer que el driver entre a la pagina
    driver.get(pdto)
    #Cambia periodicidad a semanal
    select = Select(driver.find_element_by_id("data_interval"))
    select.select_by_value("Weekly")
    #Selecciona el intervalo de fechas que se descargara
    driver.find_element_by_id("widgetFieldDateRange").click()
    #Ingresa la fecha de inicio
    driver.find_element_by_id("startDate").clear()
    driver.find_element_by_id("startDate").send_keys("01/02/2022")
    #Ingresa la fecha de finalizacion
    driver.find_element_by_id("endDate").clear()
    driver.find_element_by_id("endDate").send_keys("01/02/2022")
    #Guarda los cambios de fecha en la pagina
    driver.find_element_by_id("applyBtn").click()
    #Descarga el archivo que resulta de la consulta
    driver.find_element_by_xpath("/html/body/div[5]/section/div[8]/div[4]/div/a").click()
    sleep(5)
    #Cerrar ventana actual
    # cwh = driver.current_window_handle
    # driver.switch_to.window(cwh)
    # driver.close()
    #Cerrar el driver despues de descargar el archivo
    driver.quit()
"""



##################################################################################################################################
#%%

"""
#################### PASO 6 - Este paso NO hay q hacerlo porq 
                              ya esta en la base de datos de Bancolombia
                              resultados_canales.tdc_trm_hist
                              
                              Nombre base de datos (zona):   resultados_canales
                              Nombre tabla:                  tdc_trm_hist
####################
"""

print("\n")
print("\n")
print("PASO 6 esta en resultados_canales.tdc_trm_hist")

##################################################################################################################################
#%%

"""
#################### PASO 7 - Página Web:     https://federaciondecafeteros.org/wp/estadisticas-cafeteras/

                              Archivo:        Precios-área-y-producción-de-café-3.xlsx   
                              Hoja:           2. Precio Interno Mensual
                            
                              Letra columna:  D = Mes             = fecha
                                              E = Precio interno  = precio_interno
####################

Para este PASO 7 tengo 1 DataFrame:
1) df_resultado_paso_7 = Este es el DataFrame q tiene el resultado final de los datos del paso 7
"""

print("\n")
print("\n")
print("PASO 7")

"1) df_resultado_paso_7"
df_resultado_paso_7 = pd.read_excel (
    ruta_precios_area, 
    sheet_name = "2. Precio Interno Mensual",
    header = None,
    skiprows = 870, # IMPORTANTE: es algo probable q este numero cambie skiprows = 870
                    # SOLAMENTE se toman los datos desde 2016-01-01 hasta el ultimo registro q es la fecha del mes en la q estamos hoy actualmente
                    # es decir, eliminar los registros inferiores a 2016-01-01
                    # 2016-01-01 NO se elimina
    usecols = "D, E", 
    )

# Cambiar los nombres de las columnas
# df.set_axis(lista_con_nombre_de_las_columnas, axis=1)
# axis=1 significa q se modifican los indices de las columnas
# https://datascienceparichay.com/article/pandas-rename-column-names/
nombre_columnas_paso_7 = ["fecha", "precio_interno"] # Aqui falta la columna unidad q la añadiremos despues
df_resultado_paso_7 = df_resultado_paso_7.set_axis(nombre_columnas_paso_7, axis=1)
# Imprimir los nombres de las columnas
#df_resultado_paso_7.columns

# Convertir a tipo fecha
# año-mes-dia hora-minuto-segundo (todo en numeros)
df_resultado_paso_7["fecha"] = df_resultado_paso_7["fecha"].astype("datetime64[ns]")
# Imprimir el tipo de dato de las columnas del DataFrame
#df_resultado_paso_7.dtypes

# Crear columna unidad
# Para el paso 7 todos los datos de la columna "unidad_..." SIEMPRE son "Pesos / Carga 125KG" 
df_resultado_paso_7 = df_resultado_paso_7.assign(unidad_precio_interno = "Pesos / Carga 125KG")

##################################################################################################################################
#%%

"""
#################### PASO 8 - Link Página Web:    https://www.dane.gov.co/index.php/estadisticas-por-tema/precios-y-costos/indice-de-precios-del-productor-ipp

                              Archivo:            anexo_ipp_procedencias_especializadas_segun_destino_economico.xls 
                              Hoja:               CI PYC
                             
                              Columnas:           fecha =
                                                  Son las FECHAS q estan desde
                                                  la columna AO (enero 2018) y fila 7
                                                  hasta la ultima fecha actual - columna CL (febrero 2022) 
                                              
                                                  IPP_cafe = 
                                                  Son los NUMEROS q van desde 
                                                  la columna AO (enero 2018) y fila 228 
                                                  hasta la  ultima fecha actual - columna CL (febrero 2022) 
                                                  SOLAMENTE se toma una sola fila

(141 filas q van desde la 228 hasta 368) * (50 meses / columnas desde Enero 2018 hasta Febrero 2022) = 7050 registros (filas de df_resultado_paso_8)
                                              
####################

el codigo de este paso 8 es MUY similar al del paso 1

Para este PASO 8 tengo 3 DataFrame:
1) df_IPP_cafe_fecha_paso_8     = Contiene los numeros q van en la columna IPP_cafe y las fechas desde Enero 2018 hasta el ultimo mes en el q estamos actualmente
2) df_temporal                  = Usado para copiar los datos de los DataFrame 1) anterior y transformarlos (modificar los datos)
3) df_resultado_paso_8          = Este es el DataFrame q tiene el resultado final de los datos del paso 8
"""

print("\n")
print("\n")
print("PASO 8")

#ruta_anexo_ipp = r'C:\Users\danpined\OneDrive - Grupo Bancolombia\4_fichas_sectoriales\1_automatizacion_tableros\2_cafe\1_descargas_excel_original_sin_transformar\4_anexo_ipp\anexo_ipp.xls'

ruta_anexo_ipp = os.path.abspath(ruta_carpeta_codigo_fuente + "../../" + "/1_descargas_excel_original_sin_transformar/4_anexo_ipp")
# Imprimir ruta_anexo_ipp 
# Se TIENE q imprimir la siguiente ruta:
# ...\1_automatizacion_tableros\2_cafe\1_descargas_excel_original_sin_transformar\4_anexo_ipp
#print(ruta_anexo_ipp)

# Leer un archivo q esta en una carpeta pero no se como se llama
# ruta relativa (nombre del archivo)
# https://es.stackoverflow.com/questions/24278/c%C3%B3mo-listar-todos-los-archivos-de-una-carpeta-usando-python
# Guardar el nombre desconocido del archivo de Excel
nombre_excel_anexo_ipp = [arch.name for arch in scandir(ruta_anexo_ipp) if arch.is_file()]
# Imprimir el nombre del excel q esta dentro de ruta_anexo_ipp
# Se TIENE q imprimir algo muy similar a esto: ['anexo_ipp.xls']
#print(nombre_excel_anexo_ipp)

# Convertir las variables a tipo string
# https://www.geeksforgeeks.org/python-str-function/
ruta_anexo_ipp = str(ruta_anexo_ipp)
nombre_excel_anexo_ipp = str(nombre_excel_anexo_ipp)

# Eliminar caracteres de la variable nombre_excel_anexo_ipp
nombre_excel_anexo_ipp = nombre_excel_anexo_ipp.replace("[","")
nombre_excel_anexo_ipp = nombre_excel_anexo_ipp.replace("]","")
nombre_excel_anexo_ipp = nombre_excel_anexo_ipp.replace("'","")
# Imprimir nombre_excel_anexo_ipp
# Se debieron eliminar los caracteres innecesarios ['']
#print(nombre_excel_anexo_ipp)

# Concatenar (unir) lo siguiente: (ruta_anexo_ipp) + (nombre_excel_anexo_ipp)
# Esto da como resultado la ruta COMPLETA (absoluta) donde esta anexo_ipp.xls
ruta_anexo_ipp = ruta_anexo_ipp + "\\" + nombre_excel_anexo_ipp
# Imprimir la ruta completa (absoluta) donde se encuentra anexo_ipp.xls
# Se TIENE q imprimir la siguiente ruta (el nombre del Excel puede cambiar un poco, pero deberia llamarse similar):
# ...\1_automatizacion_tableros\2_cafe\1_descargas_excel_original_sin_transformar\4_anexo_ipp\anexo_ipp.xls
#print(ruta_anexo_ipp)

# Convertir variable a tipo string
ruta_anexo_ipp = str(ruta_anexo_ipp)

##############
#%%

# IMPORTANTE: 
# el archivo se descarga con la fecha, Ejemplo: anexo_ipp_procedencias_especializadas_segun_destino_economico_feb22.xls
# entonces para q funcione en la carpeta Descargas debo borrar la fecha q esta al final del nombre del archivo
# q en este ejemplo habria q borrar el feb22
# entonces asi el archivo siempre se llamaria igual: anexo_ipp_procedencias_especializadas_segun_destino_economico.xls
"1) df_IPP_cafe_fechas_paso_8"
df_IPP_cafe_fecha_paso_8 = pd.read_excel (
    ruta_anexo_ipp, # Ruta donde esta guardado el Excel
    sheet_name = "CI PYC", # Nombre de la hoja de Excel
    header = 0, # Numero de fila donde estan los nombres de las columnas
    skiprows = 6, # Desde esta Fila hacia abajo se empieza a leer el Excel
    usecols = "AO:XFD", # Letras de las columnas del Excel a leer
                        # SIEMPRE se empieza DESDE AO (enero 2018)
                        # y le puse HASTA XFD porq esa es la ultima columna q existe en Excel
                        # asi cuando se agregan nuevas fechas se leeran automaticamente
                        #https://excellover.com/aprende-excel/que-es-excel#:~:text=Que%20es%20una%20hoja%20de%20Excel,-Una%20hoja%20de&text=Cada%20una%20de%20las%20columnas,la%20AA%20hasta%20la%20XFD.
    )

# Eliminar un indice de filas del DataFrame en especifico
# Estos numeros (indices) de filas tienen datos q no me interesan
df_IPP_cafe_fecha_paso_8 = df_IPP_cafe_fecha_paso_8.drop(labels=[361, 362, 363, 364, 365, 366], axis=0)

# Reemplazar todos los valores q sean NULL por 0
df_IPP_cafe_fecha_paso_8.fillna(0, inplace=True)

# Convertir TODOS los datos a float64 
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.astype.html
df_IPP_cafe_fecha_paso_8 = df_IPP_cafe_fecha_paso_8.astype(float)
# Imprimir el tipo de dato de las columnas del DataFrame - los numeros tienen q ser de tipo float64
#df_IPP_cafe_fecha_paso_8.dtypes 

# Cuando se lee el DataFrame SOLAMENTE el ultimo mes SIEMPRE va tener un "pr" al final de la fecha, ejemplo: feb-22 pr
# Entonces necesitamos...
# https://www.geeksforgeeks.org/pandas-remove-special-characters-from-column-names/
# seleccionar el ultimo nombre de la columna df_IPP_cafe_fecha_paso_8.columns[-1]
# y eliminar los caracteres (letras) "pr" .replace("pr","")
# el nombre borrando el "pr" lo guardamos en una variable llamada borrar_pr_paso_8
# Ejemplo: es decir q en borrar_pr_paso_8 se guarda 01-feb-22
borrar_pr_paso_8 = df_IPP_cafe_fecha_paso_8.columns[-1].replace(" pr",'') # borrar_pr_paso_8 = feb-22 pr
borrar_pr_paso_8 = "01-" + borrar_pr_paso_8 # borrar_pr_paso_8 = 01-feb-22
                                            # Concatenar: al principio de la fecha debe ir un 01 para poderlo convertir a tipo fecha
                                            # como SIEMPRE se toman las fechas desde el primer dia 01 entonces esto esta BUENO  
# Imprimir la variable borrar_pr_paso_8
#borrar_pr_paso_8

# y el nombre de la ultima columna q SI tiene el "pr" lo guardamos en una variable llamada fecha_con_pr_paso_8
# Ejemplo: es decir q en fecha_con_pr_paso_8 se guarda feb-22 pr
fecha_con_pr_paso_8 = df_IPP_cafe_fecha_paso_8.columns[-1] # fecha_con_pr_paso_8 = "feb-22 pr"
# Imprimir la variable fecha_con_pr_paso_8
#fecha_con_pr_paso_8
# Vamos a reemplazar el nombre de la ultima columna .rename(columns={...})
# para ello necesitamos decirle como se llama la columna cuando SI tiene el "pr" al final fecha_con_pr_paso_8
# y despues cuando se la borramos borrar_pr_paso_8 
df_IPP_cafe_fecha_paso_8 = df_IPP_cafe_fecha_paso_8.rename(columns={fecha_con_pr_paso_8: borrar_pr_paso_8})
# Imprimir el nombre de la ultima columna - se debio borrar el "pr" al final del nombre de la ultima columna y agregado un 01 al principio de la fecha. Ejemplo: "01-feb-22"
#df_IPP_cafe_fecha_paso_8.columns[-1]

# Convertir los nombres de las columnas a tipo fecha
# año-mes-dia hora-minuto-segundo (todo en numeros)
# https://stackoverflow.com/questions/55275660/pandas-column-astype-error-typeerror-cannot-cast-index-to-dtype-datetime64d
df_IPP_cafe_fecha_paso_8.columns = pd.to_datetime(df_IPP_cafe_fecha_paso_8.columns).values.astype('datetime64[D]')
# Imprimir el tipo de dato de los NOMBRES de las columnas - tienen q ser tipo fecha dtype='datetime64[ns]'
#df_IPP_cafe_fecha_paso_8.columns


""" Agregar datos transformados a df_resultado_paso_8 """

"2) df_temporal" # este DataFrame ya fue creado en el PASO 1
# guardar SOLAMENTE la fila 220
# el resto de las filas q NO sean la 220 se eliminan 
# IMPORTANTE: este numero de fila puede cambiar y puede causar error
# https://www.delftstack.com/es/howto/python-pandas/pandas-loc-vs-iloc-python/
fecha_y_IPP_cafe_paso_8 = df_IPP_cafe_fecha_paso_8.iloc[220]

# Convierto el resultado a un DataFrame
df_temporal = pd.DataFrame(fecha_y_IPP_cafe_paso_8)
# La fecha es un indice, convertir el indice en columna
df_temporal = df_temporal.reset_index()
# Restablecer los indices de los nombres de las columnas
# Asi hare q las columnas se llamen 0 y 1
# https://stackoverflow.com/questions/42284617/reset-column-index-in-pandas-to-0-1-2-3
df_temporal = df_temporal.T.reset_index(drop=True).T    

# La columna q se llama "0" cambiarle el nombre a "fecha"
# "                   " "1" "                   " "IPP_cafe"         
df_temporal = df_temporal.rename(columns={0: "fecha",
                                          1: "IPP_cafe"}
                                 )
# Imprimir df_temporal
# se debe imprimir desde la fecha 2018-01-01 
# hasta la ultima fecha del mes q este en el Excel
#df_temporal
#index   fecha    IPP_cafe
#0  2018-01-01      104.14
#1  2018-02-01      104.72
#2  2018-03-01      103.33
#3  2018-04-01      101.78
#...
#49 2022-02-01      217.91

"3) df_resultado_paso_8"
# En este DataFrame estara el resultado final de las transformaciones del PASO 8
# Crear un DataFrame q en un principio estara vacio pero despues lo hire llenando conforme voy transformando los datos
nombre_columnas_paso_8 = ["fecha", "IPP_cafe"] # Aqui falta la columna "unidad..." q la crearemos despues
df_resultado_paso_8 = pd.DataFrame(columns = nombre_columnas_paso_8) # Darle nombres a las columnas del DataFrame resultante

# .append() inserta nuevas filas
# https://www.delftstack.com/es/howto/python-pandas/how-to-add-one-row-to-pandas-dataframe/#el-m%25C3%25A9todo-.append-de-dataframe-para-a%25C3%25B1adir-una-fila
# copiar los datos de df_temporal a df_resultado_paso_8
# esto da el resultado final de los datos transformados del paso 8
# ignore_index = ... Reiniciar los indices cuando se copian los datos del DataFrame
df_resultado_paso_8 = df_resultado_paso_8.append(df_temporal, ignore_index=True)

# Vaciar los datos del DataFrame temporal
df_temporal = pd.DataFrame() 

# Convertir la columna llamada "fecha" a tipo fecha datetime64[D]
# año-mes-dia hora-minuto-segundo (todo en numeros)
# https://stackoverflow.com/questions/55275660/pandas-column-astype-error-typeerror-cannot-cast-index-to-dtype-datetime64d
df_resultado_paso_8["fecha"] = pd.to_datetime(df_resultado_paso_8["fecha"]).values.astype('datetime64[D]')

# Convertir la columna llamada "IPP_cafe" a float64
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.astype.html
df_resultado_paso_8["IPP_cafe"] = df_resultado_paso_8["IPP_cafe"].astype(float)

# Crear columna unidad
# Para el paso 8 todos los datos de la columna "unidad_..." SIEMPRE son "N/A" 
df_resultado_paso_8 = df_resultado_paso_8.assign(unidad_IPP_cafe = "N/A")

# Convertir la columna llamada "unidad_IPP_cafe" a string
df_resultado_paso_8["unidad_IPP_cafe"] = df_resultado_paso_8["unidad_IPP_cafe"].astype("string") 

# Imprimir el tipo de dato de las columnas del DataFrame
#df_resultado_paso_8.dtypes 
# se debe imprimir esto:
#Out[...]: 
#fecha              datetime64[ns]
#IPP_cafe                  float64
#unidad_IPP_cafe            string
#dtype: object

##################################################################################################################################
#%%

"""
INCOMPLETO (IMPORTANTE) - a esto le falta el Web Scraping

#################### PASO 9 - Link pagina web:   https://www.dane.gov.co/index.php/estadisticas-por-tema/precios-y-costos/indice-de-precios-al-consumidor-ipc/ipc-historico#base-2018

                              Archivo:           anexo_ipc_ene18.xlsx, ... anexo_ipc_feb22.xlsx
                                                 Son muchos archivos, por cada mes hay un Excel
                              Hoja:              ......
                             
                              Columnas:          fecha =
                                                 Desde Enero 2018 hasta el ultimo mes en q estamos actualmente

####################

Para este PASO 9 tengo 1 DataFrame:
1) df_resultado_paso_9          = Este es el DataFrame q tiene el resultado final de los datos del paso 9
"""

print("\n")
print("\n")
print("PASO 9")

##############
#%%

#ruta_IPC_cafe = r'C:\Users\danpined\Downloads\IPC_cafe.xlsx'

ruta_IPC_cafe = os.path.abspath(ruta_carpeta_codigo_fuente + "../../" + "/1_descargas_excel_original_sin_transformar/5_anexo_ipc/1_excel_desde_enero_2018_hasta_febrero_2022/datos_unidos.xlsx")
# Imprimir la ruta_IPC_cafe
# La ruta TIENE q ser:
# ...\1_automatizacion_tableros\2_cafe\1_descargas_excel_original_sin_transformar\5_anexo_ipc\1_excel_desde_enero_2018_hasta_febrero_2022\datos_unidos.xlsx
#print(ruta_IPC_cafe)

"""INCOMPLETO - aqui falta leer todos los archivos (web scraping) q estan en..."""
# ...\1_automatizacion_tableros\2_cafe\1_descargas_excel_original_sin_transformar\5_anexo_ipc\2_excel_desde_enero_2018_hasta_ultima_fecha_actual

"1) df_resultado_paso_9"
df_resultado_paso_9 = pd.read_excel (
    ruta_IPC_cafe, # Ruta donde esta guardado el Excel
    sheet_name = "Hoja1", # Nombre de la hoja de Excel
    header = 0, # Numero de fila donde estan los nombres de las columnas
    skiprows = 0, # Desde esta Fila hacia abajo se empieza a leer el Excel
    usecols = "A,B", # Letras de las columnas del Excel a leer, en este caso son la A y B
    )

# Convertir a tipo fecha
# año-mes-dia hora-minuto-segundo (todo en numeros)
df_resultado_paso_9["fecha"] = df_resultado_paso_9["fecha"].astype("datetime64[ns]")

# Convertir la columna llamada "IPP_cafe" a float64
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.astype.html
df_resultado_paso_9["IPC_cafe"] = df_resultado_paso_9["IPC_cafe"].astype(float)

# Crear columna unidad
# Para el paso 9 todos los datos de la columna "unidad_..." SIEMPRE son "N/A" 
df_resultado_paso_9 = df_resultado_paso_9.assign(unidad_IPC_cafe = "N/A")
# Convertir la columna llamada "unidad_IPC_cafe" a string
df_resultado_paso_9["IPC_cafe"] = df_resultado_paso_9["IPC_cafe"].astype(float)

# Imprimir el tipo de dato de las columnas del DataFrame
#df_resultado_paso_9.dtypes
# se tienen q imprimir los tipos de datos asi:
#fecha              datetime64[ns]
#IPC_cafe                  float64
#unidad_IPC_cafe            object
#dtype: object


"""
def frutoprocesado():
    #ruta para descargar achivo
    descarga = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ProduccionFrutoTotal")
    #cambiar ruta de descarga de archivo
    chromeOptions = Options()
    chromeOptions.add_experimental_option("prefs", {"download.default_directory" :f"{descarga}",})
    #llamado al driver con la nueva ruta de descargas
    driver =  webdriver.Chrome(executable_path = f'{ruta_driver}', chrome_options= chromeOptions)
    #obtener ultimo año en pagina para descargar y crear variablde de año y año -1
    driver.get('http://sispaweb.fedepalma.org/sispaweb/default.aspx?Control=Reportes/rep_evolucionanualfrutopro&Sec=65')
    driver.find_element_by_xpath('//*[@id="ddlAno"]/div/button/span[1]')
    ultimo_y = driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlAno"]/option[2]')
    ultimo_y= int(ultimo_y.text)
    penultimo_y = ultimo_y - 1
    archivos_delete = []
    archivos_delete.append(ultimo_y)
    archivos_delete.append(penultimo_y)
    #borrar achivos (este proceso se hace porque dependiendo del momento de la descarga la info puede estar desactualizada)
    archivos = glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)), "ProduccionFrutoTotal",'*.xls')) #lista con todos los archivos en carpeta
    for x in range(0,len(archivos_delete)):
        i = archivos_delete[x]
        a = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ProduccionFrutoTotal",f'ProduccionTotal_{i}.xls')
        for y in range(0,len(archivos)):
            f = archivos[y]
            if f != a:
                pass
            else:
                os.remove(f)
    driver.get(f'http://sispaweb.fedepalma.org/sispaweb/Reportes/generarArchivoPrecios.aspx?idSec=65&anio={penultimo_y}')
    time.sleep(3)
    driver.find_element_by_xpath('//*[@id="btnGenerar"]').click()
    time.sleep(3)
    driver.get(f'http://sispaweb.fedepalma.org/sispaweb/Reportes/generarArchivoPrecios.aspx?idSec=65&anio={ultimo_y}')
    time.sleep(3)
    driver.find_element_by_xpath('//*[@id="btnGenerar"]').click()
    time.sleep(3)
    driver.close()
"""

##################################################################################################################################
#%%

print("\n")
print("\n")
print("Ordenar DataFrames desde la fecha mas antigua hasta la mas reciente")

# Ordenar los DataFrame de acuerdo a las fechas
# desde la fecha mas antigua hasta la mas reciente
# .sort_values(...) es para ordenar datos
# by="NombreColumnaPorLaQseOrdenanLosDatos"
# ascending=True de menor a mayor (ascendentemente)
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sort_values.html
df_resultado_paso_1 = df_resultado_paso_1.sort_values(by="fecha", ascending=True)
df_resultado_paso_2 = df_resultado_paso_2.sort_values(by="fecha", ascending=True)
df_resultado_paso_3 = df_resultado_paso_3.sort_values(by="fecha", ascending=True)
df_resultado_paso_4_y_5 = df_resultado_paso_4_y_5.sort_values(by="fecha", ascending=True)
"El df_resultado_paso_6 ya esta en la LZ - HUE (base de datos de Bancolombia) - resultados_canales.tdc_trm_hist"
#df_resultado_paso_6 = df_resultado_paso_6.sort_values(by="fecha", ascending=True)
df_resultado_paso_7 = df_resultado_paso_7.sort_values(by="fecha", ascending=True)
df_resultado_paso_8 = df_resultado_paso_8.sort_values(by="fecha", ascending=True)
df_resultado_paso_9 = df_resultado_paso_9.sort_values(by="fecha", ascending=True)

# Reiniciar los indices de las filas
df_resultado_paso_1 = df_resultado_paso_1.reset_index(drop=True) 
df_resultado_paso_2 = df_resultado_paso_2.reset_index(drop=True)
df_resultado_paso_3 = df_resultado_paso_3.reset_index(drop=True)
df_resultado_paso_4_y_5 = df_resultado_paso_4_y_5.reset_index(drop=True)
"El df_resultado_paso_6 ya esta en la LZ - HUE (base de datos de Bancolombia) - resultados_canales.tdc_trm_hist"
#df_resultado_paso_6 = df_resultado_paso_6.reset_index(drop=True)
df_resultado_paso_7 = df_resultado_paso_7.reset_index(drop=True)
df_resultado_paso_8 = df_resultado_paso_8.reset_index(drop=True)
df_resultado_paso_9 = df_resultado_paso_9.reset_index(drop=True)



##################################################################################################################################
#%%

" ETL: Unir todas los DataFrame df_resultado_paso_... de acuerdo a la fecha "
                                                                                                           
"""
Exportar DataFrame de Pandas a Excel:
https://datatofish.com/export-dataframe-to-excel/

Primero ejecuta en CMD este comando para instalar openpyxl
pip install openpyxl

Convertir el DataFrame df_final_ETL_unir_tablas
q tiene todas las tablas juntas a Excel
y guardarlo en el computador
"""

#ruta_guardar_excel = r"C:\Users\danpined\OneDrive - Grupo Bancolombia\4_FichasSectoriales\2_TableroFichaSectorCafe\3_ETL(UnirTablas)\tabla_final_ETL_unir_tablas.xlsx" # RutaDondeSeGuardaElExcel\NombreArchivoExcel.xlsx
#nombre_hoja_excel = "ETL_tablas_unidas_nueva_columna"

#df_final_ETL_unir_tablas.to_excel(ruta_guardar_excel,
#                                  sheet_name=nombre_hoja_excel,
#                                  #index = False # Si ejecuto esta linea de codigo entonces en el Excel NO se muestran los indices
#                                  )



##################################################################################################################################
#%%

print("\n")
print("\n")
print("Subir DataFrame a la base de datos de Bancolombia (LZ - HUE)")

# Conexion a la base de datos de Bancolombia
#sp = Sparky('danpined','IMPALA_PROD', hostname='sbmdeblze003', remote=True)
sp = Sparky(nombre_usuario,'IMPALA_PROD', clave, hostname='sbmdeblze003', remote=True)

# Subir DataFrame
sp.subir_df(df_resultado_paso_1,'proceso_riesgos.tabla_1_cafe', modo="overwrite")
sp.subir_df(df_resultado_paso_2,'proceso_riesgos.tabla_2_cafe', modo="overwrite")
sp.subir_df(df_resultado_paso_3,'proceso_riesgos.tabla_3_cafe', modo="overwrite")
sp.subir_df(df_resultado_paso_4_y_5,'proceso_riesgos.tabla_4_y_5_cafe', modo="overwrite")
#El df_resultado_paso_6 ya esta en la LZ - HUE (base de datos de Bancolombia) - resultados_canales.tdc_trm_hist
sp.subir_df(df_resultado_paso_7,'proceso_riesgos.tabla_7_cafe', modo="overwrite")
sp.subir_df(df_resultado_paso_8,'proceso_riesgos.tabla_8_cafe', modo="overwrite")
sp.subir_df(df_resultado_paso_9,'proceso_riesgos.tabla_9_cafe', modo="overwrite")


##################################################################################################################################
#%%

print("\n")
print("\n")
print("Eliminando TODAS las variables, DataFrame, etc")
# Esto va a evitar errores en las siguientes veces (despues de la primera) q se ejecute el codigo
# https://stackoverflow.com/questions/48254689/clearing-user-created-variables-in-python
locals().clear()
globals().clear()

##################################################################################################################################
