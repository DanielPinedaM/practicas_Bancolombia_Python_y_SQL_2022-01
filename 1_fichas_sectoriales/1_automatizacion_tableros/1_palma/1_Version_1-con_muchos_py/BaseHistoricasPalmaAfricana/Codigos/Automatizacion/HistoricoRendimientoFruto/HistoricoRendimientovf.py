import pandas as pd
import pyodbc
from sparky_bc import Sparky
import html5lib  #pip install 


'*******************************'
'** MODIFICAR SI ES NECESARIO **'
'*******************************'
ruta=r'C:\Users\cesmaldo\Documents\CESAR MALDONADO\PYTHON AUTOMATIZACION\AUTOMATIZACION SECTORIAL\HistoricoRendimientoFruto' #RUTA CARPETA DE LOS ARCHIVOS DE RENDIMIENTOS
username='cesmaldo'                  #USUARIO DE LZ
Base_LZ='proceso_generadores'        #BASE EN LA LZ
NombreArchivo1='RendimientoFrutosec' #NOMBRE TABLA DE LZ
numReporte=6             #NUMERO MAXIMO DE ARCHIVOS DE EXCEL O DE REPORTES
FechaInicio= 1994                    #SIEMPRE FIJA
FechaActual= 2020
'*******************************'



numFecha=FechaActual+1
def Lectura(ruta,num,cont):
    produccionTotal=pd.DataFrame()
    #print('hola2')
    df1= pd.read_excel(ruta+ f'\Reporte{num}.xls', skiprows = 19,  nrows=4, usecols = 'D:M')
        
    df1 = df1.drop(df1.columns[[1,2,3,6]], axis='columns')
    df2= df1.iloc[:,[0,1]]
    df2=df2.rename(columns={f'{FechaInicio+cont}':'Rendimiento'})
    df2.insert(1,"Fecha",f'{FechaInicio+cont}')
    produccionTotal=produccionTotal.append(pd.concat([df2]))

    #2
    df2= df1.iloc[:,[0,2]]
    df2=df2.rename(columns={f'{FechaInicio+(cont+1)}':'Rendimiento'})
    df2.insert(1,"Fecha",f'{FechaInicio+(cont+1)}')
    produccionTotal=produccionTotal.append(pd.concat([df2]))
    
    #3
    df3= df1.iloc[:,[0,3]]
    df3=df3.rename(columns={f'{FechaInicio+(cont+2)}':'Rendimiento'})
    df3.insert(1,"Fecha",f'{FechaInicio+(cont+2)}')
    produccionTotal=produccionTotal.append(pd.concat([df3]))

    #4
    df4= df1.iloc[:,[0,4]]
    df4=df4.rename(columns={f'{FechaInicio+(cont+3)}':'Rendimiento'})
    df4.insert(1,"Fecha",f'{FechaInicio+(cont+3)}')
    produccionTotal=produccionTotal.append(pd.concat([df4]))
            
    #5
    df5= df1.iloc[:,[0,5]]
    df5=df5.rename(columns={f'{FechaInicio+(cont+4)}':'Rendimiento'})
    df5.insert(1,"Fecha",f'{FechaInicio+(cont+4)}')
    produccionTotal=produccionTotal.append(pd.concat([df5]))
    cont=cont+5
    #print(cont)

    return produccionTotal,cont

cont=0
dft=pd.DataFrame()
df_T=pd.DataFrame()
produccionfinal=pd.DataFrame()

'****************************************'
'** LECTURA Y MODIFICACION DE ARCHIVOS **'
'****************************************'

for i in range(1,numReporte+1):
    if i==numReporte:
        print(cont)
        print('fecha indice cont: ',FechaInicio+(cont))
        print('Diferencia cont: ',FechaActual-(FechaInicio+(cont)))
        t=FechaInicio+(cont)  #Fecha actual según contador de fechas de los archivos
        j=FechaActual-(FechaInicio+(cont)) #Diferencia con la ultima fecha del ultimo archivo
        print(5-j)
        x=4-j
        print('Fecha correcta',t-x)
        cont=cont-x
        print('nuevo cont',cont)
        
        produccion,cont=Lectura(ruta,i,cont)
        print('contador',cont)
        print('NumReporte',i)
        print('ESTO ES PRODUCCION',produccion)
        df_T=pd.concat([produccion])
        dft=dft.append(df_T)
    else:
        produccion,cont=Lectura(ruta,i,cont)
        print('contador',cont)
        print('NumReporte',i)
        print(produccion)
        #produccionfinal=produccionfinal.append(produccion)
        df_T=pd.concat([produccion])
        dft=dft.append(df_T)


print('\n\n ********* TABLA FINAL **********\n\n')
#print(dft)
print('esto es produccion final',dft)
#ELIMINANDO DUPLICADOS
dft = dft.drop_duplicates()
dft.to_excel('BaseHistoricaRendimientoFrutosTotal.xlsx',index=False) #GUARDAR ARCHIVO DE EXCEL

'***************************************************************'
'****************** SUBIENDO ARCHIVO DE EXCEL ******************'
'***************************************************************'
Insumo1=dft
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