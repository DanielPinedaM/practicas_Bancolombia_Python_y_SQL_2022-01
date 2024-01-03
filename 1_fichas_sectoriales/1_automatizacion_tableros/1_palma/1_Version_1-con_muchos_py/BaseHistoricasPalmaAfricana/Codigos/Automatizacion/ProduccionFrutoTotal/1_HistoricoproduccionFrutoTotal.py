import html5lib          #pip install html5lib
import pandas as pd
import pyodbc
from sparky_bc import Sparky

'****** Producción de fruto de palma por zona ******'
'**** HISTORICO DE PRODUCCION DE FRUTO DE PALMA TOTAL ****'
'*** MODIFICAR SI ES NECESARIO EN CADA ACTUALIZACION ***'
FechaInicio= 1994  #FECHA DEL EXCEL MAS ANTIGUO
FechaActual= 2021  #FECHA DEL EXCEL MAS RECIENTE
ruta=r'C:\Users\cesmaldo\Documents\CESAR MALDONADO\PYTHON AUTOMATIZACION\AUTOMATIZACION SECTORIAL\ProduccionFrutoTotal'
username='cesmaldo'
Base_LZ='proceso_generadores'
NombreArchivo1=f'ProduccionFrutosec' #Nombre para subirlo a la LZ en proceso_consumidores
'******************************************************************'

df1= pd.read_html(ruta+ f'\ProduccionTotal_{FechaInicio}.xls',index_col=0)[0]  #LECTURA
produccionTotal=pd.concat([df1])   #CONCATENO EL PRIMER REGISTRO 1994 CON EL ENCABEZADO


'******************************************************'
'****** CICLO PARA CONCATENAR LOS DEMAS ARCHIVOS ******'
'******************************************************'
df={}
vector=[]
for i in range(FechaInicio+1,FechaActual+1):
    z=f'W{i}'
    vector.append(z)
    #print(z)
    df[z]= pd.read_html(ruta+ f'\ProduccionTotal_{i}.xls',index_col=0,skiprows=1)[0]
    produccionTotal=produccionTotal.append(pd.concat([df[f'W{i}']]))
    #print(df1)

produccionTotal.columns = produccionTotal.iloc[0]

produccionTotal.reset_index(inplace=True, drop=False)
produccionTotal=produccionTotal.drop([0],axis=0)

produccionTotal=produccionTotal.rename(columns={0:'Fecha'})
print(produccionTotal.columns)
print(produccionTotal)  #IMPRIME BASE FINAL

produccionTotal.to_excel('BaseHistoricaProduccionFrutoTotal.xlsx', index=False) #GUARDA EL DATAFRAME FINAL
'******************************************************************'
'******************************************************************'


'********************************************'
'*************** SUBIR A LA LZ **************'
Insumo1= produccionTotal
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