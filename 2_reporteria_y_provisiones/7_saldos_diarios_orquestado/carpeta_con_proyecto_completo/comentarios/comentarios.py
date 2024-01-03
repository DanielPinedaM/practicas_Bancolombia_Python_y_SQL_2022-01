from sparky_bc import Sparky
import pandas as pd
import numpy as np
import sys
import pyodbc
import csv
import time
import os
import getpass


# PROCESO PARA RECUPERACIONES

# trusted_cert = "C:\Certificados\cacerts"
# ruta_comentarios= r"comentarios.xlsx"
# CONN_STR='DSN=IMPALA_PROD'
# cn = pyodbc.connect(CONN_STR, autocommit = True )
# cursor = cn.cursor()
# data_informe=pd.read_excel(ruta_comentarios, sheet_name='Hoja1')
# sp=Sparky('gimoreno',"IMPALA_PROD",hostname="sbmdeblze003.bancolombia.corp")
# sp.subir_df(data_informe,"proceso.comentarios_gian")


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

if __name__ == "__main__":
    runQuery("""DROP TABLE IF EXISTS proceso.comentarios_gian PURGE""")
    trusted_cert = "C:\Certificados\cacerts"
    ruta_comentarios= r"comentarios.xlsx"
    CONN_STR='DSN=IMPALA_PROD'
    cn = pyodbc.connect(CONN_STR, autocommit = True )
    cursor = cn.cursor()
    data_informe=pd.read_excel(ruta_comentarios, sheet_name='Hoja1')
    sp=Sparky('gimoreno',"IMPALA_PROD",hostname="sbmdeblze003.bancolombia.corp")
    sp.subir_df(data_informe,"proceso.comentarios_gian")
    runQuery("""DROP TABLE IF EXISTS proceso_riesgos.comentarios_gian PURGE""")
    runQuery("""
    CREATE TABLE proceso_riesgos.comentarios_gian  STORED AS PARQUET AS 
WITH 

ORIGINAL AS (
    SELECT ROW_NUMBER() OVER(PARTITION BY num_doc
                         ORDER BY (fecha_actualizacion) DESC) AS numero,
    CAST(num_doc AS VARCHAR(15)) AS id,
    nombre AS cliente,
    gestion,
    fecha_actualizacion,
    ingestion_year,
    ingestion_month,
    ingestion_day
    FROM proceso.comentarios_gian)
    SELECT * FROM ORIGINAL WHERE numero=1
    """)