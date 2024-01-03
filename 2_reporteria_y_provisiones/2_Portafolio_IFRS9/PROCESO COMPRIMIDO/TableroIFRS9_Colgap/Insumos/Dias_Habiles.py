# -*- coding: utf-8 -*-
"""
Created on Fri Aug 14 09:57:00 2020

@author: cesherna
"""



import pandas as pd
import numpy as np
import sys

import pyodbc
import pandas as pd



trusted_cert = "D:/libs/cacerts"

#CONN_STR = "Driver=Cloudera ODBC Driver for Impala;Host=impala.bancolombia.corp;"            "Port=21050;AuthMech=1;SSL=1;KrbRealm=BANCOLOMBIA.CORP; "            "KrbFQDN=impala.bancolombia.corp;KrbServiceName=impala;"            "TrustedCerts={trusted_cert}".format(trusted_cert = trusted_cert)
CONN_STR='DSN=IMPALA_PROD'
cn = pyodbc.connect(CONN_STR, autocommit = True )
cursor = cn.cursor()

def as_pandas(cursor):
    names = [metadata[0] for metadata in cursor.description]
    return pd.DataFrame([dict(zip(names, row)) for row in cursor], columns=names)


def EjecuteSql(query_sql):
    CONN_STR='DSN=IMPALA_PROD'
    cn = pyodbc.connect(CONN_STR, autocommit = True )
    cursor = cn.cursor()
    cursor.execute(query_sql)

def Base_Festivos():
    dfCalendario = pd.read_excel('Z:/Tablero_IFRS9_COLGAP/Insumos/Calendario.xlsx',sheet_name='calendario')
    dfFestivos = pd.read_excel('Z:/Tablero_IFRS9_COLGAP/Insumos/Calendario.xlsx',sheet_name='Festivos')
    dfConsolidacion = pd.merge( dfCalendario,dfFestivos,  how='left', left_on=['DIA','MES'], right_on = ['DIA','MES'])
    dfConsolidacion=dfConsolidacion.fillna('NO_FESTIVO')
    cols = dfConsolidacion.select_dtypes(include=[np.object]).columns
    dfConsolidacion[cols] = dfConsolidacion[cols].apply(lambda x: x.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8'))
    
    ### SUBIRLO A LA LZ ###
    from sparky_bc import Sparky
    sp=Sparky('cesherna')
    sp.subir_df(dfConsolidacion,"proceso.fecha_base_maestra")
    
    
    def Query_Base_fechas():        
        Query_1_Generar_Base_fechas=""" create table proceso.base_dias stored as parquet as 
        with dias_habiles_sin_festivos_sin_domingo as (
        
        select 
        fecha
        ,ROW_NUMBER() OVER (PARTITION BY  mes,ano ORDER BY fecha asc) AS  dia_habil_no_dom_fest
        from proceso.fecha_base_maestra
        where nombre='NO_FESTIVO' and nombre_dia !='domingo'
        order by fecha asc
        ),
        
        dias_habiles_sin_festivos_sin_domingo_sin_sabados as (
        
        select 
        fecha
        ,nombre_dia
        ,ROW_NUMBER() OVER (PARTITION BY  mes,ano ORDER BY fecha asc) AS  dia_habil_no_dom_sab_fest
        from proceso.fecha_base_maestra
        where nombre='NO_FESTIVO' and nombre_dia !='domingo' and nombre_dia !='sabado'
        order by fecha asc
        )
        
        
        select 
        t1.fecha,
        t1.nombre_mes,
        t1.nombre_dia,
        t1.dia,
        t1.mes,
        t1.ano,
        t1.nombre as estado_dia,
        t2.dia_habil_no_dom_fest,
        t3.dia_habil_no_dom_sab_fest
        
        from proceso.fecha_base_maestra t1
        left join dias_habiles_sin_festivos_sin_domingo t2 on t1.fecha=t2.fecha
        left join dias_habiles_sin_festivos_sin_domingo_sin_sabados  t3 on t1.fecha=t3.fecha"""
        EjecuteSql(Query_1_Generar_Base_fechas)
    
    Query_Base_fechas()

Base_Festivos()



#dfCalendario = pd.read_excel('Z:/Tablero_IFRS9_COLGAP/Insumos/Calendario.xlsx',sheet_name='calendario')
#dfFestivos = pd.read_excel('Z:/Tablero_IFRS9_COLGAP/Insumos/Calendario.xlsx',sheet_name='Festivos')


#dfConsolidacion = pd.merge( dfCalendario,dfFestivos,  how='left', left_on=['DIA','MES'], right_on = ['DIA','MES'])
 
#dfConsolidacion=dfConsolidacion.fillna('NO_FESTIVO')

#cols = dfConsolidacion.select_dtypes(include=[np.object]).columns
#dfConsolidacion[cols] = dfConsolidacion[cols].apply(lambda x: x.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8'))


   
#list_date = dfConsolidacion.dtypes == 'datetime64[ns]'
#list_date = list_date.reset_index()
#list_date = list_date[list_date[0]==True]['index']
#for dato in list_date:
#    dfConsolidacion[dato] = dfConsolidacion[dato].dt.strftime("%Y%m%d").astype(int)


#hp=conn_lz()
#hp.fromPandasDF(dfConsolidacion,"Fechas_Base_Maestra",serverOpts)

#from sparky_bc import Sparky
#sp=Sparky('cesherna')
#sp.subir_df(dfConsolidacion,"proceso.fecha_base_maestra")

#sp.subir_excel("D:\subir LZ\DESEMBOLSOS_20200818.xlsx", "proceso.DESEMBOLSOS_20200818_")
#sp.subir_df(dfConsolidacion,"proceso.fecha_base_maestra")

