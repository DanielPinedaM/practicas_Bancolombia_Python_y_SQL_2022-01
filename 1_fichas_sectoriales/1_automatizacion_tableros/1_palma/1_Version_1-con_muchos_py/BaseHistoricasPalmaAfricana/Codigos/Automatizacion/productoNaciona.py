import pyodbc
import pandas as pd
from sparky_bc import Sparky

ruta1=r'C:\Users\cesmaldo\Documents\CESAR MALDONADO\PYTHON AUTOMATIZACION\AUTOMATIZACION SECTORIAL\PROYECCIONES NACIONALES DE LOS PRODUCTOS'
ruta=r'\Agroindustria e Insumos en Cifras Octubre (Por Favor DESCARGAR).xlsm'
username='cesmaldo'
#MODIFICAR SI ES NECESARIO LA BASE O EL NOMBRE DE LA TABLA
NombreBase= 'proceso_generadores'
NombreArchivo1='ProyeccionPreciosNacionales'

data= pd.read_excel(ruta1+ruta,sheet_name='Proyecciones y precios ',skiprows =41,  nrows=21, usecols = 'E:I')
data= pd.DataFrame(data)


data = data.drop(data.columns[[1]], axis='columns')

data=data.rename(columns={'Unnamed: 4':'Productos Nacionales'}) #RENOMBRANDO COLUMNA
data.insert(1,'Fecha','2021')


print(data)
data.to_excel('PROYECION_PreciosNacionesxSector.xlsx',index=False)  #ARCHIVO FINAL



'** SUBIENDO A LA LZ A PROCESOS_CONSUMIDORES **'
'*********************************************'
'***** CONEXION A BASE DE DATOS SPARKY *****'
sp=Sparky(username,'IMPALA_PROD')
'*********************************************'
print("▄ ╔═════════════════╗")
print("█═╣ INICIANDO CICLO ║")
print("█═╩═════════════════╝")
'***************************************'
'*** CONEXION A BASE DE DATOS PYODBC ***'
'***************************************'
def ConexionPyODBC():
    try:
        CONN_STR = 'DSN=IMPALA_PROD'
        conexion = pyodbc.connect(CONN_STR, autocommit=True)
        print('Conexion Exitosa')
    except:
        print('Error al intentar la conexion')
    return conexion

'**********************************************'
'****** FUNCION SUBIENDO ARCHIVO A LA LZ ******'
'**********************************************'


Insumo1= data
def SubirSparky(RutaArchivo,NombreArchivo,NombreBase):
    conexion=ConexionPyODBC()
    cursorElimina = conexion.cursor()
    cursorElimina.execute(f"DROP TABLE IF EXISTS {NombreBase}.{NombreArchivo}")
    cursorElimina.close()
    print("█ ╔════════════════════════════╗")
    print("█═╣ALMACENANDO ARCHIVO DE EXCEL║")
    print("█ ╩════════════════════════════╝")
    sp.subir_df(RutaArchivo, f"{NombreBase}.{NombreArchivo}")
    print("█ ╔══════════════════════╗")
    print("█═╣ ALMACENADO CON EXITO ║")
    print("█═╩══════════════════════╝")

'*********************************************'
'**********************************************'
#LLAMADO FUNCION PARA SUBIR LOS ARCHIVOS CSV A BASE DE DATOS
SubirSparky(Insumo1,NombreArchivo1,NombreBase)







