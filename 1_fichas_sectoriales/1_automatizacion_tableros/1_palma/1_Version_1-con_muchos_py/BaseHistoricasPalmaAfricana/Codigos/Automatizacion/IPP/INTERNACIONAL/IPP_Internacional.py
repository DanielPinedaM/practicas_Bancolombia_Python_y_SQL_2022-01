import pyodbc
import pandas as pd
from sparky_bc import Sparky

'IPP INTERNACIONAL'

'**************** MODIFICAR SOLAMENTE AQUI ABAJO ********'
'********************************************************'
ruta1=r'C:\Users\cesmaldo\Documents\CESAR MALDONADO\PYTHON AUTOMATIZACION\AUTOMATIZACION SECTORIAL\IPP\INTERNACIONAL'
ruta=r'\anexo_ipp_oct21.xlsx'  #NOMBRE DEL ARCHIVO

username='cesmaldo'
ano=21       #AÑO DE INGESTA DEL ARCHIVO
mesActual=9  #MES ACTUAL INGESTA DEL ARCHIVO
#MODIFICAR SI ES NECESARIO LA BASE O EL NOMBRE DE LA TABLA
NombreBase= 'proceso_generadores'  #NOMBRE DE LA BASE EN LA LZ
NombreArchivo1='ipp_internacional'  #NOMBRE DE LA TABLA EN LA LZ

'********************************************************'




vector=['ene','feb','mar','abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']

'LECTURA DEL ARCHIVO'
data= pd.read_excel(ruta1+ruta,sheet_name='4.1',skiprows=5)
data= pd.DataFrame(data)

#FILTRO
df1=data.loc[(data['DESCRIPTIVA']=='Aceites vegetales, refinados')]
df2=data.loc[(data['DESCRIPTIVA']=='Margarina y preparaciones similares')]
frames = [df1, df2]
result = pd.concat(frames)

vectorFechas=[]
cont=0
contInd=0
indice=[]
for i in (result.columns):
    #print(i,'indice', cont)
    
    if i==f'{vector[contInd]}-{ano} (pr)*':
        vectorFecha=f"{vector[contInd]}-{ano}"
        vectorFechas.append(vectorFecha)
        #print('indice',cont)
        indice.append(cont)
        contInd=contInd+1 
    cont=cont+1



result =result.drop(result.columns[[indice]], axis='columns')
result =result.drop(result.columns[[0,1]], axis='columns')
#ELIMINAR COLUMNAS CON NaN
result = result.dropna(1)


result=result.T
result.columns = result.iloc[0]


result.reset_index(inplace=True, drop=False)
result=result.drop([0],axis=0)
result=result.rename(columns={"index":"Fecha"})


j=0
for i in range(73,73+mesActual):
    #print(i)

    print(result.iloc[i,[0]])
    result.iloc[i,[0]]= vectorFechas[j]
    print(result.iloc[i,[0]])
    j=j+1

print(result)
result.to_excel('AUTOMATIZACION_IPP_INTERNNACIONAL.xlsx', index=False) #ARCHIVO FINAL





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


Insumo1= result
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





