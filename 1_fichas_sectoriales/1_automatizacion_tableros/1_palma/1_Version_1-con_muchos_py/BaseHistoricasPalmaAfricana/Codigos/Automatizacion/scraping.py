# Importar librerias
import unittest
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as ECO
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import time
import pyodbc
import pandas as pd
from sparky_bc import Sparky

##############################################################

'DESCARGA DEL ARCHIVO DE AREA SEMBRADA DE PALMA ACEITE'
'******** DESARROLLO Y PRODUCCION DE SIEMBRA *********'
fechaInicio=1994
fechaActual=2020                            #SE CAMBIA EN ENERO DEL 2022 A 2021
username='danpined'                         #USUARIO
rutaArchivo=r'C:\Users\danpined\Downloads'  #RUTA DONDE SE DESCARGARA EL ARCHIVO
ruta_exe=r'C:\chromedriver.exe'             #RUTA DE CHROMEDRIVER
Base_LZ='proceso_generadores'               #NOMBRE DE LA BASE EN LA LZ
NombreArchivo1='AreaDesarrolloProduccion'   #NOMBRE DE LA TABLA EN LA LZ

##############################################################

'*** DESCARGANDO ARCHIVO AUTOMATICAMENTE ***'
crhome_driver= webdriver.Chrome(ruta_exe)
crhome_driver.maximize_window() #MAXIMIZAR PANTALLA
crhome_driver.get(f'http://sispaweb.fedepalma.org/sispaweb/Reportes/generarArchivoNuevasSiembras.aspx?idSec=2&fecini={fechaInicio}&fecfin={fechaActual}') #LINK DE DESCARGA DEL ARCHIVO
path8='//*[@id="btnGenerar"]'
boton8= crhome_driver.find_element_by_xpath(path8).click()
time.sleep(5)

##############################################################

'*** LECTURA ARCHIVO Y SUBIR AUTOMATICAMENTE A LA LZ***'
Insumo1= pd.read_html(rutaArchivo +f'\AreaDesarrolloProduccion_{fechaInicio}-{fechaActual}.xls',encoding='utf-8')[0]
Insumo1.columns = Insumo1.iloc[0]
#Insumo1.reset_index(inplace=True, drop=False)   #REINICIANDO INDICES
Insumo1=Insumo1.drop([0],axis=0)
Insumo1=Insumo1.rename(columns={"ANIO":"Fecha"}) #RENOMBRANDO COLUMNA
print(Insumo1)
Insumo1.to_excel('BaseHistoricaAreaDesarrolloProducion.xlsx', index=False) #GUARDA EL DATAFRAME FINAL

##############################################################

print("▄ ╔═════════════════╗")
print("█═╣ INICIANDO CICLO ║")
print("█═╩═════════════════╝")

'***** SUBIR EXCEL CON SPARKY *****'

'*********************************************'
'****** FUNCION SUBIENDO ARCHIVO A LA LZ *****'
'*********************************************'
def SubirSparky(RutaArchivo,NombreArchivo):
    conexion=ConexionPyODBC()
    cursorElimina = conexion.cursor()
    cursorElimina.execute(f"DROP TABLE IF EXISTS {Base_LZ}.{NombreArchivo}")
    cursorElimina.close()
    print("█ ╔════════════════════════════╗")
    print("█═╣ALMACENANDO ARCHIVO DE EXCEL║")
    print("█ ╩════════════════════════════╝")
    sp.subir_df(RutaArchivo, f"{Base_LZ}.{NombreArchivo}")
    #sp.subir_df(rutaArchivo, f"{Base_LZ}.{NombreArchivo}")
    print("█ ╔══════════════════════╗")
    print("█═╣ ALMACENADO CON EXITO ║")
    print("█═╩══════════════════════╝")

##############################################################

print("▄ ╔═════════════════╗")
print("█═╣   BIENVENIDO    ║")
print("█═╩═════════════════╝")

'*********************************************'
'***** CONEXION A BASE DE DATOS PYODBC *****'
'*********************************************'
def ConexionPyODBC():
    try:
        CONN_STR = 'DSN=IMPALA_PROD'
        conexion = pyodbc.connect(CONN_STR, autocommit=True)
        print('Conexion Exitosa')
    except:
        print('Error al intentar la conexion')
    return conexion

##############################################################

'*********************************************'
'***** CONEXION A BASE DE DATOS SPARKY *******'
'*********************************************'
sp = Sparky(username,'IMPALA_PROD')
print(sp)

##############################################################



##############################################################

#LLAMADO FUNCION PARA SUBIR LOS ARCHIVOS CSV A BASE DE DATOS
SubirSparky(Insumo1,NombreArchivo1)
