import pyodbc
import pandas as pd
from sparky_bc import Sparky

'PROYECCION INTERNACIONAL DE PRODUCTOS'

'********************************'
'MODIFICAR UNICAMENTE AQUI ABAJO'
'********************************'
ruta1=r'C:\Users\cesmaldo\Documents\CESAR MALDONADO\PYTHON AUTOMATIZACION\AUTOMATIZACION SECTORIAL\PROYECCIONES INTERNACIONALES DE LOS PRODUCTOS'
ruta=r'\Agroindustria e Insumos en Cifras Octubre (Por Favor DESCARGAR).xlsm'
username='cesmaldo'
#MODIFICAR SI ES NECESARIO LA BASE O EL NOMBRE DE LA TABLA
NombreBase= 'proceso_generadores'
NombreArchivo1='ProyeccionPreciosInternacional'
'********************************'

'LECTURA DEL ARCHIVO'
dt= pd.read_excel(ruta1+ruta,sheet_name='Proyecciones y precios ',skiprows =70,  nrows=10, usecols = 'C:J')
dt= pd.DataFrame(dt)

dt=dt.rename(columns={'Producto ':'Productos Internacionales'}) #RENOMBRANDO COLUMNA
print(dt)
dt.to_excel('PROYECION_PreciosInternacionalesxSector.xlsx',index=False)  #ARCHIVO FINAL



Insumo1= dt
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
