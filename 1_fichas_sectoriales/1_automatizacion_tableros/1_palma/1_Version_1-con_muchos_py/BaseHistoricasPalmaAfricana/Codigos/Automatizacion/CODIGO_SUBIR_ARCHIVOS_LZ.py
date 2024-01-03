import pandas as pd
import pyodbc
from sparky_bc import Sparky

'HISTORICO DE LA PRODUCCION DE ACEITE DE PALMA POR ZONA'
'***********************************************************'
'****************** MODIIFICAR SOLAMENTE AQUI **************'
'***********************************************************'
username='cesmaldo'               #USUARIO DE LZ
Base_LZ='proceso_generadores'     #NOMBRE DE LA BASE EN LA LZ
NombreArchivo1='IPC'  #Nombre para subirlo a la LZ en proceso_consumidores

'**** RUTA Y NOMBRE DEL ARCHIVO A SUBIR ****'
ruta=r'C:\Users\cesmaldo\Documents\CESAR MALDONADO\PYTHON AUTOMATIZACION\AUTOMATIZACION SECTORIAL\IPC' #RUTA DE LA CARPETA QUE CONTIENE EL EXCEL
NombreExcel='\AUTOMATIZACION_IPC.xlsx'   #NOMBRE DEL ARCHIVO DE EXCEL primero slash y luego el nombre. ejem: \Nombre.xlsx


Insumo1= pd.read_excel(ruta+f'{NombreExcel}')  #LECTURA DEL ARCHIVO DE EXCEL
'***********************************************************'


'***************************************************************'
'****************** SUBIENDO ARCHIVO DE EXCEL ******************'
'***************************************************************'
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
'*********************************************'

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
    print("█ ╔══════════════════════╗")
    print("█═╣ ALMACENADO CON EXITO ║")
    print("█═╩══════════════════════╝")

'*********************************************'
'***** CONEXION A BASE DE DATOS SPARKY *******'
'*********************************************'
sp = Sparky(username,'IMPALA_PROD')
'*********************************************'
print("▄ ╔═════════════════╗")
print("█═╣ INICIANDO CICLO ║")
print("█═╩═════════════════╝")
'*********************************************'
'***** SUBIR EXCEL CON SPARKY *****'
'*********************************************'
'**********************************************'
#LLAMADO FUNCION PARA SUBIR LOS ARCHIVOS A BASE DE DATOS
SubirSparky(Insumo1,NombreArchivo1)
