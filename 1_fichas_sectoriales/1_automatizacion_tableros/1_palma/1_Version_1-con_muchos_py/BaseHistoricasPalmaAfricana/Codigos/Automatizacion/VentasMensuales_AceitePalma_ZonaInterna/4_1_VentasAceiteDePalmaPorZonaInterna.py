import pandas as pd

'HISTORICO DE LA PRODUCCION DE ACEITE DE PALMA POR ZONA'

FechaInicio=2010
FechaActual=2021

ruta=r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_FichasSectoriales\1_Tablero sectorial\BaseHistoricasPalmaAfricana\Codigos\AutomatizacionSectorial\VentasMensuales_AceitePalma_ZonaInterna' #RUTA DE LA CARPETA


'CODIGO HISTORICO'
df1={}
vector=[]
produccionMecardoInternoTotal=pd.DataFrame()
produccionMecardoExportacionTotal=pd.DataFrame()

df_MercadoInterno=pd.DataFrame()

for i in range(FechaInicio,FechaActual):
    z=f'W{i}'
    vector.append(z)
    print(z)
    df1[z]= pd.read_excel(ruta+f'\ReporteVentasAceite_{i}.xls',index_col=0, skiprows = 12,  nrows=8, usecols = 'E:AI')
    df1[z] = df1[z].drop(df1[z].columns[[0,1,3,4,6,7,9,10,12,14,16,18,20,22,24,26,27,28]], axis='columns')
    #print(df1[z])
    
    'MERCADO INTERNO'
    df_MercadoInterno= df1[z].iloc[1:6,[0,1,2,3,4,5,6,7,8,9,10,11]]
    df_MercadoInterno=df_MercadoInterno.iloc[0:13].T
    df_MercadoInterno.insert(0,'Fecha',f"{i}")
    
    'MERCADO DE EXPORTACION'
    df_MercadoExportacion=df1[z].iloc[7:8,[0,1,2,3,4,5,6,7,8,9,10,11]]
    df_MercadoExportacion=df_MercadoExportacion.iloc[0:13].T
    df_MercadoExportacion.insert(0,'Fecha',f"{i}")
    #df_MercadoExportacion
    
    
    produccionMecardoInternoTotal=produccionMecardoInternoTotal.append(pd.concat([df_MercadoInterno]))
    produccionMecardoExportacionTotal=produccionMecardoExportacionTotal.append(pd.concat([df_MercadoExportacion]))


#dftotal=dftotal.drop_duplicates()  #ELIMINA LAS FILAS DUPLICADOS    
produccionMecardoInternoTotal.reset_index(inplace=True, drop=False)
produccionMecardoInternoTotal=produccionMecardoInternoTotal.rename(columns={'index':'Meses'})
print(produccionMecardoInternoTotal)

produccionMecardoExportacionTotal.reset_index(inplace=True, drop=False)
produccionMecardoExportacionTotal=produccionMecardoExportacionTotal.rename(columns={'index':'Meses'})

print('MERCADO INTERNO 1',produccionMecardoInternoTotal)
produccionMecardoInternoTotal.to_excel('BaseHistoricaVentasMecardoInterno_PorZona2020.xlsx')
produccionMecardoExportacionTotal.to_excel('BaseHistoricaVentasMecardoExportacion_PorZona2020.xlsx')



'****************************************************************************'
'****************************** PARA DICIEMBRE *********************************'
'*************************** PARA 2021 DICIEMBRE *********************************'
'****************************************************************************'
fecha=2021
#NOTA: modificar el df2.drop --> depende mucho de la estructura del excel
df2= pd.read_excel(ruta+f'\ReporteVentasAceite_{FechaActual}.xls',index_col=0, skiprows = 12,  nrows=8, usecols = 'E:AI')
df2 = df2.drop(df2.columns[[0,1,3,4,6,7,9,10,12,14,16,18,20,22,23,24,25]], axis='columns')
#df1

df_MercadoInterno2= df2.iloc[1:6,[0,1,2,3,4,5,6,7,8]]
df_MercadoInterno2=df_MercadoInterno2.iloc[0:13].T
df_MercadoInterno2.insert(0,'Fecha',f"{fecha}")
#df_MercadoInterno2

df_MercadoExportacion=df2.iloc[7:8,[0,1,2,3,4,5,6,7,8]]
df_MercadoExportacion=df_MercadoExportacion.iloc[0:13].T
df_MercadoExportacion.insert(0,'Fecha',f"{fecha}")
#df_MercadoExportacion

#print(df1)
print('MERCADO INTERNO2',df_MercadoInterno2)
print(df_MercadoExportacion)


#df_MercadoInterno2.reset_index(inplace=True, drop=False)
#df_MercadoInterno2=produccionMecardoExportacionTotal.rename(columns={'index':'Meses'})
#df_MercadoExportacion.reset_index(inplace=True, drop=False)
#df_MercadoExportacion=produccionMecardoExportacionTotal.rename(columns={'index':'Meses'})




df_MercadoInterno2.to_excel('BaseHistoricaVentasMecardoInterno_PorZona2021.xlsx')
df_MercadoExportacion.to_excel('BaseHistoricaVentasMecardoExportacion_PorZona2021.xlsx')

'****************************************************************************'







'''
'LLENADO DE UN SOLO AÑO '
FechaManual=2010
df1= pd.read_excel(ruta+f'\ReporteVentasAceite_{FechaInicio}.xls',index_col=0, skiprows = 12,  nrows=8, usecols = 'E:AI')
df1 = df1.drop(df1.columns[[0,1,3,4,6,7,9,10,12,14,16,18,20,22,24,26,27,28]], axis='columns')
#df1
df_MercadoInterno= df1.iloc[1:6,[0,1,2,3,4,5,6,7,8,9,10,11]]
df_MercadoInterno=df_MercadoInterno.iloc[0:13].T
df_MercadoInterno.insert(0,'Fecha',f"{FechaManual}")
df_MercadoInterno

df_MercadoExportacion=df1.iloc[7:8,[0,1,2,3,4,5,6,7,8,9,10,11]]
df_MercadoExportacion=df_MercadoExportacion.iloc[0:13].T
df_MercadoExportacion.insert(0,'Fecha',f"{FechaManual}")
df_MercadoExportacion

#print(df1)
print(df_MercadoInterno)
print(df_MercadoExportacion)
'''