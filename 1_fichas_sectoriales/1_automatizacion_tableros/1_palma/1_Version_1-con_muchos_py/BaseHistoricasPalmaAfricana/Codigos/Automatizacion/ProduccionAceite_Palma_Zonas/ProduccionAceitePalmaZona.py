import pandas as pd
import pyodbc
from sparky_bc import Sparky

'HISTORICO DE LA PRODUCCION DE ACEITE DE PALMA POR ZONA'
'***********************************************************'
'****************** MODIIFICAR SOLAMENTE AQUI **************'
'***********************************************************'
FechaInicio=2010   #FECHA FIJA
FechaActual=2021   #FECHA ACTUAL QUE CONTIENE EL ARCHIVO
username='cesmaldo'
Base_LZ='proceso_generadores'   #NOMBRE DE LA BASE EN LA LZ
NombreArchivo1='ProduccionAceiteZonasec' #Nombre para subirlo a la LZ en proceso_consumidores
ruta=r'C:\Users\cesmaldo\Documents\CESAR MALDONADO\PYTHON AUTOMATIZACION\AUTOMATIZACION SECTORIAL\ProduccionAceite_Palma_Zonas'
'***********************************************************'



Meses=['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']
VectorFecha=[]

for i in range(FechaInicio,FechaActual+1):
    #print(i)
    VectorFecha.append(i)
    
print(VectorFecha)


'FUNCION PARA EL LLENADO DE LOS MESES DE LOS AÑOS ANTERIORES'
def Mercado(df1,cont,VectorFecha):
    tamMeses=12
    dft=pd.DataFrame()
    df_MercadoT=pd.DataFrame()
    Meses=['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']
    for j in range(0,tamMeses):
        df_Mercado= df1.iloc[0:4,[j]]
        df_Mercado.insert(0,'Mes',f"{Meses[j]}")
        M=df1.columns[j]
        df_Mercado=df_Mercado.rename(columns={f"{M}":"Produccion"}) #RENOMBRANDO COLUMNA
        df_MercadoT=pd.concat([df_Mercado])
        dft=dft.append(df_MercadoT)
    
    dft.insert(0,'Fecha',VectorFecha[cont-1])
       
    

    #print('esto es dft',dft)
    #print('esto es dft',dft)
    return dft

'FUNCION PARA EL ULTIMO AÑO'
def MercadoUlt(df1):
    tamMeses=12
    dft=pd.DataFrame()
    df_MercadoT=pd.DataFrame()
    Meses=['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']
    for j in range(0,tamMeses):
        df_Mercado= df1.iloc[0:4,[j]]
        df_Mercado.insert(0,'Mes',f"{Meses[j-3]}")
        M=df1.columns[j]
        df_Mercado=df_Mercado.rename(columns={f"{M}":"Produccion"}) #RENOMBRANDO COLUMNA
        df_MercadoT=pd.concat([df_Mercado])
        dft=dft.append(df_MercadoT)
    
    dft.insert(0,'Fecha','2021') 
    dft.iloc[0:12,[0]] =2020
    
     
    #print('esto es dft',dft)
    #print('esto es dft',dft)
    return dft


'LECTURA Y LLAMADO DE FUNCIONES'
df1={}
dftotal=pd.DataFrame()
cont=0
for i in range(FechaInicio,FechaActual+1):
    print(i)

    if i==FechaActual:
        df1[i]= pd.read_excel(ruta+ f'\Reporte_{i}.xls',index_col=0, skiprows = 19,  nrows=5, usecols = 'B:AM')
        df1[i] = df1[i].drop(df1[i].columns[[0,1,2,3,4,6,7,9,10,12,13,15,16,18,20,22,24,25,26,28,29,31,32,34,35]], axis='columns')
        df_MercadoI= df1[i].iloc[1:5,[0,1,2,3,4,5,6,7,8,9,10,11]]
        #LLAMADO DE LA FUNCION MERCADOINTERNO
        #cont=cont+1
        dft=MercadoUlt(df_MercadoI)
        dftotal=dftotal.append(dft)
            
    else:
        df1[i]= pd.read_excel(ruta+ f'\Reporte_{i}.xls',index_col=0, skiprows = 19,  nrows=5, usecols = 'B:AM')
        df1[i] = df1[i].drop(df1[i].columns[[0,1,2,3,4,6,7,9,10,12,13,15,16,18,20,22,24,25,26,28,29,31,32,34,35]], axis='columns')
        df_MercadoI= df1[i].iloc[1:5,[0,1,2,3,4,5,6,7,8,9,10,11]]
        #LLAMADO DE LA FUNCION MERCADOINTERNO
        cont=cont+1
        #print(df_MercadoI)
        dft=Mercado(df_MercadoI,cont,VectorFecha)

        dftotal=dftotal.append(dft)

dftotal=dftotal.drop_duplicates()  #ELIMINA LAS FILAS DUPLICADOS    
dftotal.reset_index(inplace=True, drop=False)
dftotal=dftotal.rename(columns={'index':'Zonas'})

print(dftotal)

dftotal.to_excel('ProduccionDeAceiteDePalmaPorZona_FINAL.xlsx',index=False) #NOMBRE CON EL CUAL SE EXPORTA
#print('cont',cont)

'***************************************************************'
'****************** SUBIENDO ARCHIVO DE EXCEL ******************'
'***************************************************************'
Insumo1=dftotal
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
