import pandas as pd
import numpy as np
import xlwt    #Escribir en el cdm --> pip install xlwt
import pyodbc
from sparky_bc import Sparky

#LECTURA DE BASE CONSOLIDADA MES
'***************************************************'
'***** MODIFICAR UNICAMENTE LA RUTA DEL ARCHIVO ****'
'***************************************************'
#MODIFICAR SI ES NECESARIO LA BASE O EL NOMBRE DE LA TABLA EN LA LZ
usuario='cesmaldo'
NombreBase= 'proceso_generadores'
NombreArchivo1='IndiceProduccionEMMET'
ruta=r'C:\Users\cesmaldo\Documents\CESAR MALDONADO\PYTHON AUTOMATIZACION\AUTOMATIZACION SECTORIAL\INDICE DE VENTAS Y PRODUCCION EMMET'
nombreArchivo=r'\anexos_nacional_emmet_septiembre_2021.xlsx'
'***************************************************'

'***************************************************'
'********** LECTURA DEL ARCHIVO Y FILTRO **********'
'***************************************************'
datos = pd.read_excel(ruta+nombreArchivo,sheet_name='9. Enlace legal hasta 2014',skiprows =9,usecols = 'B:G')
dt =pd.DataFrame(datos)
df=dt.loc[(dt['Clases industriales']=='Elaboración de aceites y grasas de origen vegetal y animal')]
df=df.rename(columns={'AÑO':'Fecha','MES':'Meses'})
df=df.rename(columns={'Producción \nNominal':'Produccion_Nominal','Producción \nreal':'Produccion_Real','Ventas\nNominales':'Ventas_Nominales'})  #cambiar las otras producciones

df.reset_index(inplace=True, drop=True)
print(df.columns.values)

df.to_excel('BASE_EMMET_GRUPO_SECTORIAL.xlsx',index=False)
'***************************************************'

'** SUBIENDO A LA LZ A PROCESOS_CONSUMIDORES **'
Insumo1= df
print(Insumo1)
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

