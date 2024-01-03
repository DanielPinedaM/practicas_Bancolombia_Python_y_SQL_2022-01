import pyodbc
import pandas as pd
from sparky_bc import Sparky

ruta1= r'C:\Users\cesmaldo\Documents\CESAR MALDONADO\PYTHON AUTOMATIZACION\AUTOMATIZACION SECTORIAL\DEMANDA DE ENERGIA'
ruta=r'\Demanda de Energía Sector Agro.xlsx'
usuario='cesmaldo'
#MODIFICAR SI ES NECESARIO LA BASE O EL NOMBRE DE LA TABLA EN LA LZ
NombreBase= 'proceso_generadores'
NombreArchivo1='DeMandaEnergia'



data= pd.read_excel(ruta1+ruta,sheet_name='DATA',skiprows =0,usecols = 'A:F')  #LECTURA DEL ARCHIVO DEMANDA DE ENERGIA

df1=data.loc[(data['Sub Actividad']=='ELABORACIÓN DE ACEITES Y GRASAS DE ORIGEN VEGETAL Y ANIMAL')] #FILTRO
print(df1)

df1.to_excel('AUTOMATIZACION_DemandaDeEnergia.xlsx', index=False) #ARCHIVO FINA - EXPORTAR



'** SUBIENDO A LA LZ A PROCESOS_CONSUMIDORES **'
Insumo1= df1
'*********************************************'
'***** CONEXION A BASE DE DATOS SPARKY *****'
sp=Sparky(usuario,'IMPALA_PROD')
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