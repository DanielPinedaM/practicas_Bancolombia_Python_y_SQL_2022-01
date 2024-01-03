import pandas as pd
import pyodbc
from sparky_bc import Sparky

'*****************************************'
' ******MODIFICAR UNICAMENTE AQUÍ ********'
'*****************************************'
ruta=r'C:\Users\cesmaldo\Documents\CESAR MALDONADO\PYTHON AUTOMATIZACION\AUTOMATIZACION SECTORIAL\VentasMensuales_AceitePalma_PorIndustria'
FechaInicio=2010
FechaActual=2021
username='cesmaldo'
Base_LZ='proceso_generadores'
NombreArchivo1='VentasAceite_Industriasec' #Nombre para subirlo a la LZ en proceso_consumidores
'*****************************************'

def ultimaFecha(df):
    idx=df.index
    idx
    cont=0
    for i in range(0,len(idx)):
            if(idx[i]=='Ene'):
                print(idx[i])
                cont=1        
            else:
                if(cont==0):
                    print('Eliminar',idx[i])
                    df=df.drop(idx[i])

                if(cont==1):
                    print(idx[i])
                    #break
    return df


'CODIGO HISTORICO DE VENTAS MENSUAL DE ACEITE DE PALMA POR INDUSTRIA'
df1={}
vector=[]
ventasTotal=pd.DataFrame()

for i in range(FechaInicio,FechaActual+1):
    z=f'W{i}'
    vector.append(z)
    print(z)
    df1[z]=pd.read_excel(ruta+f'\VentasMensualesAceitePalmaPorIndustria_{i}.xls',index_col=0, skiprows = 13,  nrows=7, usecols = 'E:AF')
    df1[z]= df1[z].drop(df1[z].columns[[0,2,3,5,6,8,10,12,14,16,18,20,22,24,25]], axis='columns')
    df1[z]=df1[z].iloc[1:7,[0,1,2,3,4,5,6,7,8,9,10,11]]
    df1[z]=df1[z].iloc[0:13].T
    df1[z].insert(0,'Fecha',f"{i}")
    
    
    if z== f'W{FechaActual}':
        df=ultimaFecha(df1[z])
        ventasTotal=ventasTotal.append(pd.concat([df]))
    else:
        ventasTotal=ventasTotal.append(pd.concat([df1[z]]))
            


ventasTotal=ventasTotal.fillna(0)  #RELLENA LOS CAMPOS VACIOS EN CERO
ventasTotal.reset_index(inplace=True, drop=False)
ventasTotal=ventasTotal.rename(columns={'index':'Meses'})
print(ventasTotal)
#ventasTotal=ventasTotal.drop_duplicates()  #ELIMINA LAS FILAS DUPLICADOS

ventasTotal.to_excel('BaseHistoricaVentasAceite_Industria.xlsx',index=False)


Insumo1=ventasTotal

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
