import pandas as pd
import numpy as np
import sys
import pyodbc
import csv
import time
from sparky_bc import Sparky

# stdout_handler = logging.StreamHandler(sys.stdout)
####################################################################################################################################
#PROGRAMA PARA AUTOMATIZAR RECUPERACIONES


nombre_tabla_proceso="proceso_riesgos.base_consolidada_final_gian"
anio="2022"
mes="01" 
fecha = "ene-22"
trusted_cert = "C:/Certificados/cacerts"

salida_consolidada = pd.DataFrame()

uta_leasing="input/enero/leasing_202201.xls"
ruta_banco="input/enero/banco_202201.xlsx"
ruta_sufi="input/enero/sufi_202201.xls"
ruta_hipotecaria="input/enero/hipotecario_202201.xlsx"
# ruta_ventas_2="input/octubre/venta2_202110.xlsx"

salida="output/Base Consolidada/base_consolidada_"+fecha+".xlsx"




# now_fecha =datetime.now()
# now = now_fecha.strftime("%d_%m_%Y_%H_%M_%S")
# output_file_handler = logging.FileHandler("output"+now+".log")

####################################################################################################################################
#Funcion para consultas en la base de datos
def runQuery(query):
    CONN_STR='DSN=IMPALA_PROD'
    cn = pyodbc.connect(CONN_STR, autocommit = True )
    cursor = cn.cursor()
    cursor.execute(query)

#Funcion para consultas en la base de datos guardando la informacion en un dataframe
def runQueryPandas(query):
    CONN_STR='DSN=IMPALA_PROD'
    cn = pyodbc.connect(CONN_STR, autocommit = True )
    data = pd.read_sql_query(query,cn)
    return data

def subirLZ(tabla,ruta,hoja):
    CONN_STR='DSN=IMPALA_PROD'
    cn = pyodbc.connect(CONN_STR, autocommit = True )
    cursor = cn.cursor()
    data=pd.read_excel(ruta, sheet_name=hoja)
    sp=Sparky('gimoreno', "IMPALA_PROD")
#     sp = Sparky( username = 'gimoreno', logger = { 'path': "C:/Users/gimoreno/Documents/Procesos mios/Python Proyects/Recuperaciones/logs" }  )
    sp.subir_df(data,tabla)


def poblarQuery(anio,mes,tabla):
     query_master="""
    CREATE TABLE PROCESO.RECUPERACIONES_CAPA STORED AS PARQUET AS 

WITH 

CAPA AS (
SELECT * FROM  (SELECT 
          ROW_NUMBER() OVER(PARTITION BY oblig,num_doc
                         ORDER BY (ingestion_year*100+ingestion_month) DESC) AS numero,
          num_doc,
          SUBSTR(oblig, 3, LENGTH(oblig)-1)AS oblig,
          producto_agrupado AS PRODUCTO_AGRUPADO,
          banca AS banca,
          COD_CLASE AS MODalidad,
          desc_segm AS desc_segm,
          vic_ccial AS vic_ccial

   FROM resultados_riesgos.master_credit_risk
)  BASE 
WHERE numero=1)
SELECT X.BASE,
       """+anio+""" AS ANHO, 
       """+mes+""" AS MES, 
       X.ID,
       X.OBL17,
       V.PRODUCTO_AGRUPADO AS PRODUCTO_AJUSTADO,
       V.BANCA AS BANCA_AJUSTADA,
       v.vic_ccial,
       V.MODALIDAD,
       V.DESC_SEGM AS SGTO_AJUSTADO,
       X.MONTO,
       X.aplicativo,
       X.amcdty,
       X.cncdbi
FROM """+tabla+""" X
LEFT JOIN CAPA V ON CAST(X.OBL17 AS DOUBLE)=CAST(V.oblig AS DOUBLE)
AND CAST(X.ID AS BIGINT)=CAST(V.num_doc AS BIGINT) 

"""

# CRUZAR POR ID

     query_master_2="""CREATE TABLE PROCESO.RECUPERACIONES_CENIE STORED AS PARQUET AS 
WITH 

CENIE AS (
 SELECT * FROM  (SELECT ROW_NUMBER() OVER(PARTITION BY obl341,id
                         ORDER BY (ingestion_year*100+ingestion_month) DESC) AS numero,
       id,
       obl341,
       pcons AS pcons,
       banca AS banca,
       vic_ccial AS vic_ccial,
       clf AS modalidad,
       segdesc AS segdesc
   FROM resultados_riesgos.ceniegarc_lz) BASE WHERE numero=1)
SELECT x.base,
       """+anio+""" AS anho, 
       """+mes+""" AS mes, 
       x.id,
       x.obl17,
       v.pcons AS producto_ajustado,
       v.banca AS banca_ajustada,
       v.vic_ccial AS vic_ccial,
       v.modalidad,
       v.segdesc AS sgto_ajustado,
       x.monto,
       X.aplicativo,
       X.amcdty,
       X.cncdbi
FROM PROCESO.RECUPERACIONES_CAPA X
LEFT JOIN CENIE V ON CAST(X.OBL17 AS DOUBLE)=CAST(V.OBL341 AS DOUBLE)
AND CAST(X.ID AS DOUBLE)=CAST(V.ID AS DOUBLE)
WHERE X.PRODUCTO_AJUSTADO IS NULL  ;

"""
     query_ceniegarc="""CREATE TABLE proceso.informe_recu_2020102_v2 STORED AS PARQUET AS 

WITH 

BASE AS
  (SELECT *
   FROM PROCESO.RECUPERACIONES_CAPA
   WHERE PRODUCTO_AJUSTADO IS NOT NULL
   
   UNION ALL 
   SELECT *
   FROM PROCESO.RECUPERACIONES_CENIE
   )
   
SELECT base,
       anho,
       mes,
       id,
       lpad(cast(obl17 AS string),
            17,
            '0') AS obl17,
       CASE
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '37781%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '40998%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '4491%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '45130%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '53037%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '5491%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '53069%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '10000%' THEN 'LEASING'
           WHEN producto_ajustado IN ('Cartera Consumo',
                                      'Vehículo',
                                      'LIBRE INVERSION')
                AND modalidad = '2' THEN 'LIBRE INVERSION'
           WHEN producto_ajustado IN ('Cartera Comercial',
                                      'Vehículo',
                                      'CARTERA ORDINARIA')
                AND modalidad = '1' THEN 'CARTERA ORDINARIA'
           WHEN producto_ajustado IN ('Credipago virtual') THEN 'CREDIPAGO'
           WHEN producto_ajustado IN ('Ex-Empleado Libranza',
                                      'Libranza',
                                      'LIBRANZA')
                AND modalidad = '2' THEN 'LIBRANZA'
           WHEN producto_ajustado IN ('Tarjeta de Crédito',
                                      'TARJETA DE CREDITO') THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IN ('CCT SUFI',
                                      'CPE CORTO PLAZO SUFI',
                                      'CPE LARGO PLAZO SUFI',
                                      'MOTOS GAMA BAJA SUFI',
                                      'COTIDIANIDAD') THEN 'COTIDIANIDAD'
           WHEN producto_ajustado IN ('MOVILIDAD',
                                      'VEHÍCULOS SUFI') THEN 'MOVILIDAD'
           ELSE upper(producto_ajustado)
       END AS producto_ajustado,
       CASE
           WHEN banca_ajustada = 'Personas y Pyme' THEN 'NEPYP'
           WHEN banca_ajustada = 'PPyE' THEN 'NEPYP'
           WHEN banca_ajustada = 'Empresarial' THEN 'NEPYP'
           WHEN banca_ajustada IN ('BEG',
                                   'VEG')
                AND sgto_ajustado = 'EMPRESARIAL' THEN 'NEPYP'
           WHEN banca_ajustada IN ('BEG',
                                   'VEG')
                AND sgto_ajustado != 'EMPRESARIAL' THEN 'NCORP'
           ELSE banca_ajustada
       END AS banca_ajustada,
       CASE
           WHEN vic_ccial IS NULL
                AND banca_ajustada = 'Personas y Pyme'
                AND sgto_ajustado NOT IN ('GOBIERNO DE RED',
                                          'PYMES',
                                          'NEGOCIOS E INDEPEND',
                                          'MICROFINANZAS',
                                          'MI NEGOCIO',
                                          'MICROPYME') THEN 'PERSONAS'
           WHEN vic_ccial IS NULL
                AND banca_ajustada = 'Personas y Pyme'
                AND sgto_ajustado NOT IN ('GOBIERNO DE RED',
                                          'PYMES',
                                          'NEGOCIOS E INDEPEND',
                                          'MICROFINANZAS',
                                          'MI NEGOCIO',
                                          'MICROPYMES') THEN 'PYMES'
           ELSE upper(vic_ccial)
       END AS vic_ccial,
       modalidad,
       CASE
           WHEN sgto_ajustado IN ('NEGOCIOS E INDEPEND',
                                  'MICROFINANZAS',
                                  'MI NEGOCIO',
                                  'MICROPYME') THEN 'NEGOCIOS & INDEPEND'
           WHEN sgto_ajustado IN ('PLUS',
                                  'PERSONAL PLUS') THEN 'PLUS'
           ELSE UPPER(sgto_Ajustado)
       END AS sgto_ajustado,
       monto,
       aplicativo,
       amcdty,
       cncdbi
FROM base ;

"""
     runQuery(query_master)
     print("queri master")
     runQuery(query_master_2)
     print("queri master 2")
     runQuery(query_ceniegarc)
     print("queri master cenie")

def llenarHipotecario(df):
     global salida_consolidada
     global ruta_hipotecaria
     hipo_df_sinull=df[df["base"] == "HIPOTECARIO" ]
     hipo_df=hipo_df_sinull[hipo_df_sinull['modalidad'].isnull()]
     hipo_df_conul=hipo_df_sinull[hipo_df_sinull['banca_ajustada'].isnull()==False]
     hipo_original=pd.read_excel(ruta_hipotecaria,sheet_name='Pagos Cartera Castigada')
     for index in hipo_df.index:
          for index_original in hipo_original.index:
               if hipo_df["id"][index] == hipo_original["Número de identificación del cliente"][index_original] and hipo_original["Clasificación de Cartera"][index_original] =="COMER":
                    hipo_df.loc[index,"modalidad"] = 1
               else:
                    hipo_df.loc[index,"modalidad"] = 3

          if hipo_df["modalidad"][index] == 1:
               hipo_df.loc[index,"producto_ajustado"] = "OTROS HIPOTECARIO"
               hipo_df.loc[index,"sgto_ajustado"] = "NEGOCIOS & INDEPEND"
               hipo_df.loc[index,"vic_ccial"] = "PYMES"
          else:
               hipo_df.loc[index,"producto_ajustado"] = "HIPOTECARIO VIVIENDA"
               hipo_df.loc[index,"sgto_ajustado"] = "PERSONAL"
               hipo_df.loc[index,"vic_ccial"] = "PERSONAS"
          hipo_df.loc[index,"banca_ajustada"] = "NEPYP"
               
     salida_consolidada=pd.concat([salida_consolidada,hipo_df])
     salida_consolidada=pd.concat([salida_consolidada,hipo_df_conul])

def llenarBanco(df):
     global salida_consolidada
     global ruta_banco
     d = {'cod': ['5','9','G','4','M','6','3','2','1','7','8','C','A','B','S'], 'segm': ["PYMES","NEGOCIOS & INDEPEND","GOBIERNO DE RED","PERSONAL","PLUS","PREFERENCIAL","CORPORATIVO","EMPRESARIAL","GOBIERNO","INTERNACIONAL","INSTITUCIONES FINANCIERAS","CONSTRUCTOR CORPORATIVO","CONSTRUCTOR EMPRESARIAL","CONSTRUCTOR PYME","SOCIAL"]}
     sgto_df=pd.DataFrame(data=d)
     print(sgto_df)
     banco_df=df[df["base"] == "BANCO"]
     for index in banco_df.index:
          if banco_df.loc[index,"aplicativo"] == "L" or banco_df.loc[index,"aplicativo"] == "E":
               if banco_df.loc[index,"amcdty"] == 1:
                    if pd.isnull(banco_df.loc[index,"modalidad"]):
                         banco_df.loc[index,"modalidad"]  = 2
                    if pd.isnull(banco_df.loc[index,"producto_ajustado"]):
                         banco_df.loc[index,"producto_ajustado"] = "LIBRE INVERSION"
               elif banco_df.loc[index,"amcdty"] == 2:
                    if pd.isnull(banco_df.loc[index,"modalidad"]):
                         banco_df.loc[index,"modalidad"]  = 1
                    if pd.isnull(banco_df.loc[index,"producto_ajustado"]):
                         banco_df.loc[index,"producto_ajustado"] = "CARTERA ORDINARIA"
               else:
                    if pd.isnull(banco_df.loc[index,"producto_ajustado"]):
                         banco_df.loc[index,"producto_ajustado"] = "CARTERA MICROCREDITO"
                    if pd.isnull(banco_df.loc[index,"modalidad"]):
                         banco_df.loc[index,"modalidad"]  = 4
          elif banco_df.loc[index,"aplicativo"] == "K" or banco_df.loc[index,"aplicativo"] == "M" or banco_df.loc[index,"aplicativo"] == "V": 
               if pd.isnull(banco_df.loc[index,"modalidad"]):
                    banco_df.loc[index,"modalidad"]  = banco_df.loc[index,"amcdty"]
               if pd.isnull(banco_df.loc[index,"producto_ajustado"]):
                    banco_df.loc[index,"producto_ajustado"] = "TARJETA DE CREDITO"
          elif banco_df.loc[index,"aplicativo"] == "D":
               if pd.isnull(banco_df.loc[index,"modalidad"]):
                    banco_df.loc[index,"modalidad"]  = banco_df.loc[index,"amcdty"]
               if pd.isnull(banco_df.loc[index,"producto_ajustado"]):
                    banco_df.loc[index,"producto_ajustado"] = "SOBREGIROS"

          for index_sgto in sgto_df.index:
               cod_item=sgto_df.loc[index_sgto,"cod"]
               if banco_df["cncdbi"][index] == cod_item:
                    banco_df.loc[index,"sgto_ajustado"] = sgto_df.loc[index_sgto,"segm"]
                    if banco_df.loc[index,"sgto_ajustado"] == "PERSONAL" or banco_df.loc[index,"sgto_ajustado"] == "PLUS" or banco_df.loc[index,"sgto_ajustado"] == "PREFERENCIAL" or banco_df.loc[index,"sgto_ajustado"] == "SOCIAL":
                         if pd.isnull(banco_df.loc[index,"vic_ccial"]):
                              banco_df.loc[index,"vic_ccial"] = "PERSONAS"
                         if pd.isnull(banco_df.loc[index,"banca_ajustada"]):
                              banco_df.loc[index,"banca_ajustada"] = "NEPYP"

                    elif banco_df.loc[index,"sgto_ajustado"] == "CONSTRUCTOR CORPORATIVO" or banco_df.loc[index,"sgto_ajustado"] == "CONSTRUCTOR EMPRESARIAL" or banco_df.loc[index,"sgto_ajustado"] == "CONSTRUCTOR PYME" :
                         if pd.isnull(banco_df.loc[index,"vic_ccial"]):
                              banco_df.loc[index,"vic_ccial"] = "INMOBILIARIO Y CONSTRUCTOR"
                         if pd.isnull(banco_df.loc[index,"banca_ajustada"]):
                              banco_df.loc[index,"banca_ajustada"] = "NCORP"

                    elif banco_df.loc[index,"sgto_ajustado"] == "GOBIERNO" or banco_df.loc[index,"sgto_ajustado"] == "INSTITUCIONES FINANCIERAS":
                         if pd.isnull(banco_df.loc[index,"vic_ccial"]):
                              banco_df.loc[index,"vic_ccial"] = "GOBIERNO, SERVICIOS FINANCIEROS, SALUD Y EDUCACION"
                         if pd.isnull(banco_df.loc[index,"banca_ajustada"]):
                              banco_df.loc[index,"banca_ajustada"] = "NCORP" 

                    elif banco_df.loc[index,"sgto_ajustado"] == "CORPORATIVO" or banco_df.loc[index,"sgto_ajustado"] == "FINANCIERA":
                         if pd.isnull(banco_df.loc[index,"vic_ccial"]):
                              banco_df.loc[index,"vic_ccial"] = "CORPORATIVO_AJUSTAR"
                         if pd.isnull(banco_df.loc[index,"banca_ajustada"]):
                              banco_df.loc[index,"banca_ajustada"] = "NCORP" 

                    elif banco_df.loc[index,"sgto_ajustado"] == "EMPRESARIAL":
                         if pd.isnull(banco_df.loc[index,"vic_ccial"]):
                              banco_df.loc[index,"vic_ccial"] = "EMPRESARIAL"
                         if pd.isnull(banco_df.loc[index,"banca_ajustada"]):
                              banco_df.loc[index,"banca_ajustada"] = "NEPYP"

                    # elif banco_df.loc[index,"sgto_ajustado"] == "NEGOCIOS & INDEPEND":
                    #      if pd.isnull(banco_df.loc[index,"vic_ccial"]):
                    #           banco_df.loc[index,"vic_ccial"] = "NEGOCIOS & INDEPEND"
                    #      if pd.isnull(banco_df.loc[index,"banca_ajustada"]):
                    #           banco_df.loc[index,"banca_ajustada"] = "NEPYP"
                    else:
                         if pd.isnull(banco_df.loc[index,"vic_ccial"]):
                              banco_df.loc[index,"vic_ccial"] = "PYMES"
                         if pd.isnull(banco_df.loc[index,"banca_ajustada"]):
                              banco_df.loc[index,"banca_ajustada"] = "NEPYP"

                    
     salida_consolidada=pd.concat([salida_consolidada,banco_df])
     banco_df.to_excel("output/base_banco_completa.xlsx",index=False)
     sgto_df.to_excel("output/sgto.xlsx",index=False)          
     # amcdty = Para cambiar

     # cncdbi = Para segmento

def llenarLeasing(df):
     global salida_consolidada
     global ruta_leasing
     leasing_df=df[df["base"] == "LEASING" ]
     salida_consolidada=pd.concat([salida_consolidada,leasing_df])

def llenarSufi(df):
     global salida_consolidada
     global ruta_sufi
     sufi_df=df[df["base"] == "SUFI" ]
     salida_consolidada=pd.concat([salida_consolidada,sufi_df])

def validarAnterior():
     query=""""WITH base_inicial AS
  (SELECT base,
          obl17,
          sum(monto) AS monto
   FROM proceso.informe_recu_2020102_v2
   GROUP BY 1,
            2),
     base_2021 AS
  (SELECT base,
          obl17,
          sum(monto) AS monto
   FROM resultados_riesgos.detalle_recuperaciones
   WHERE CAST(anho AS INT)=2021
   GROUP BY 1,
            2),
     previa AS
  (SELECT t1.base AS base,
          t1.obl17 AS obl17,
          t1.monto AS monto_2021,
          t2.monto AS monto_reportado
   FROM base_2021 t1
   INNER JOIN base_inicial t2 ON CAST(t1.obl17 AS int)= cast(t2.obl17 AS int)),
     previa2 as
  (SELECT base, obl17, monto_2021, monto_reportado, monto_2021 + monto_reportado AS monto_total
   FROM PREVIA)
SELECT *
FROM previa2
WHERE monto_total > 100"""
     df=runQueryPandas(query)
     df.to_excel("output/Base Consolidada Completa/reversiones_positivas.xlsx",index=False)



if __name__ == "__main__":
     runQuery("DROP TABLE IF EXISTS "+nombre_tabla_proceso+"")
     print("ejecutado sin fallas")
     runQuery("DROP TABLE IF EXISTS proceso.informe_recu_2020102_v2")
     print("ejecutado sin fallas")
     runQuery("DROP TABLE IF EXISTS proceso.RECUPERACIONES_CENIE")
     print("ejecutado sin fallas")
     runQuery("DROP TABLE IF EXISTS PROCESO.RECUPERACIONES_CAPA")
     runQuery("""SELECT DISTINCT ingestion_month FROM resultados_riesgos.detalle_recuperaciones WHERE ingestion_year=2022""")
     print("ejecutado sin fallas el select")
     subirLZ(nombre_tabla_proceso,salida,"Sheet1")
     poblarQuery(anio,mes,nombre_tabla_proceso)
     

     df=runQueryPandas("SELECT * FROM proceso.informe_recu_2020102_v2" )
     print("base lista ")
     df.to_excel("output/Base Consolidada Vacia/base_final_vacia_"+fecha+".xlsx",index=False)
     llenarHipotecario(df)
     llenarBanco(df)
     llenarLeasing(df)
     llenarSufi(df)
     salida_consolidada.to_excel("output/Base Consolidada Completa/base_final_completa_"+fecha+".xlsx",index=False)
     validarAnterior()


     ######

     #CONSULTA LEASING =  select id,obl341,contrato from resultaI

     #DICAI, PABLO ARBOLEDA