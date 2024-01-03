import pyodbc
import pandas as pd
from sparky_bc import Sparky

'***** PRECIO DEL BIODIESEL ******'
'************************************************'
'********* MODIFICAR SOLAMENTE AQUI ABAJO *******'
'************************************************'
#MODIFICAR SI ES NECESARIO LA BASE O EL NOMBRE DE LA TABLA
username='cesmaldo'
ruta1=r'C:\Users\cesmaldo\Documents\CESAR MALDONADO\PYTHON AUTOMATIZACION\AUTOMATIZACION SECTORIAL\PRECIO BIODISEL'
ruta=r'\Federacion_Nacional_Biocombustibles.xls'
NombreBase= 'proceso_generadores'
NombreArchivo1='BioCombustibles_sec'
'************************************************'


'LECTURA DEL ARCHIVO'
data= pd.read_html(ruta1+ruta)[0]
data= pd.DataFrame(data)
data['Fecha'] = pd.to_datetime(data['Fecha'])

print(data)
data.to_excel('PRECIO_BIO_DIESEL.xlsx',index=False)  #ARCHIVO FINAL

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

'** SUBIENDO A LA LZ A PROCESOS_CONSUMIDORES **'

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