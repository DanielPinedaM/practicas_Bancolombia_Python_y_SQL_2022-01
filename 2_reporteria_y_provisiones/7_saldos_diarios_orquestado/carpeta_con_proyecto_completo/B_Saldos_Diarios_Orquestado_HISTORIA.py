# -*- coding: utf-8 -*-
"""

@author: vaosorio, dulondon 

Proceso ejecución reporte "Saldos Diarios"

"""
#%%
from orquestador.funcs_master import init_logger, impala, ejecucion_spark
from orquestador.impala_manager import ImpalaExecutionManager
import pandas as pd
from sparky_bc import Sparky
from datetime import datetime
from datetime import date
from openpyxl import Workbook
import win32com.client as win32
import os
import getpass
import pandas as pd


#%%
####################### FLUJO DEL PROGRAMA ####################################
if __name__ == "__main__":

    # Parámetros del ejecutor
    # Se debe especificar, además del DSN de conexión:
    #   - la cantidad de reintentos ante fallos: n_retry (int)
    #   - el tiempo de espera entre reintentos: t_espera (int)
    # Opcionalmente:
    #   - se pueden especificar destinatarios de correo 
    #     para envío de logs al finalizar ejecución: emails (list<string>)
    #   - activar la ejecución de sonidos durante la ejecución: play_sound (bool)
    params_ejecutor = {
            'dsn': 'IMPALA_PROD',
            'n_retry': 20,
            't_espera': 10,
            'emails': None,
            'play_sound': False
            }

    log_path = ""
    
    logger = init_logger(log_path) # Logs de ejecución

    # Opción 1: Pass en archivo
    # f = open("D:/Temp/p.txt", "r")
    # usr = f.readline().replace("\n", "")
    # pass_usr = f.readline()

    usr=getpass.getuser()
    pass_usr=os.environ['PWD']

    sp = Sparky(usr,"IMPALA_PROD", False, pass_usr)
    hp = sp.helper


    # Objeto de tipo manager que ejecuta las sentencias en impala
    impala_manager = ImpalaExecutionManager(logger, 
                                            dsn = params_ejecutor['dsn'], 
                                            n_retry = params_ejecutor['n_retry'],
                                            t_espera = params_ejecutor['t_espera'],
                                            emails = params_ejecutor['emails'],
                                            play_sound = params_ejecutor['play_sound'])

    # Parámetros de la ejecución (opcional, depende de los parámetros puestos en el SQL)

    ## ## SQL que calcula las fechas del reporte -> Se recomienda activar si no se quieren validar pruebas previas y posteriores.
    ## params_ejecucion = {}
    ## impala('1_ingestiones.sql', impala_manager, params_ejecucion)

    ### El archivo 1_ingestiones.sql para calcular las fechas a continuación, se ejecuta en el programa unitest/pre-test/tes_pre_SD.py:
    ### Se crea DF para asignar variables con fechas reporte
    ## asigna_df_sd = impala_manager.tabla_impala_to_df('proceso.fechas_saldos')
    ## fecha_corte = asigna_df_sd.iloc[0]['fecha_corte'] # Es equivalente a fecha_corte = asigna_df_sd.loc[0,'fecha_corte']
    ## fecha_corte_ant = asigna_df_sd.iloc[0]['fecha_corte_ant']
    ## fecha_corte_nmb = asigna_df_sd.iloc[0]['fecha_corte_nmb']

    ## asigna_df_cn = impala_manager.tabla_impala_to_df('proceso.fechas_cierre')
    ## fecha_cierre = asigna_df_cn.iloc[0]['fecha_corte']

    ## asigna_df_trm = impala_manager.tabla_impala_to_df('proceso.fechas_trm')
    ## fecha_trm = asigna_df_cn.iloc[0]['fecha_corte']

    ## ult_fecha_vic_ccial=hp.obtener_ultima_ingestion("resultados_riesgos.erm_vic_ccial")
    ## fecha_vic_ccial=str((ult_fecha_vic_ccial['year']))+"-"+str((ult_fecha_vic_ccial['month'])).zfill(2)+"-"+str((ult_fecha_vic_ccial['day'])).zfill(2)

    ## ult_fecha_matriz =hp.obtener_ultima_ingestion("resultados_riesgos.erm_matriz_corporativa")
    ## fecha_matriz=str((ult_fecha_matriz['year']))+"-"+str((ult_fecha_matriz['month'])).zfill(2)+"-"+str((ult_fecha_matriz['day'])).zfill(2)

    ## ult_fecha_receta =hp.obtener_ultima_ingestion("resultados_riesgos.asignacion_productos")
    ## fecha_receta=str((ult_fecha_receta['year']))+"-"+str((ult_fecha_receta['month'])).zfill(2)+"-"+str((ult_fecha_receta['day'])).zfill(2)

    ## ult_fecha_planta =hp.obtener_ultima_ingestion("resultados_serv_para_los_clientes.planta_comercial_pgc")
    ## fecha_planta=str(int((ult_fecha_planta['year'])))+"-"+str(int((ult_fecha_planta['month']))).zfill(2)+"-"+str(int((ult_fecha_planta['day']))).zfill(2)

    ## ult_fecha_alivios =hp.obtener_ultima_ingestion("resultados_riesgos.base_alivios_covid")
    ## fecha_alivios=str((ult_fecha_alivios['year']))+"-"+str((ult_fecha_alivios['month'])).zfill(2)+"-"+str((ult_fecha_alivios['day'])).zfill(2)

    # Importa tabla de Parámetros Saldos Diarios - Catálogos que se usarán en el proceso
    file_path = os.getcwd()+'\\Parametros_Saldos_Diarios.csv' 
    sp.subir_csv(file_path,'proceso_riesgos.sd_parametros', sep=';', modo ='overwrite' )

    ## Importa tabla de Parámetros Saldos Diarios - Rangos de variación tolerables para cada aplicativo
    ## file_path = os.getcwd()+'\\unitest\\test\\rng_var_sld_apl.csv' 
    ## sp.subir_csv(file_path,'proceso.saldos_diarios_rangos_apl', sep=';', modo ='overwrite' )
    df = pd.read_excel('historico.xlsx',sheet_name="Hoja1") # can also index sheet by name or fetch all sheets
    
    fechas_corte = df['fechas_corte'].tolist()
    print("La fechas_corte es: "+fechas_corte)

    fechas_corte_nmb = df['fechas_corte_nmb'].tolist()

    fechas_corte_ant = df['fechas_corte_ant'].tolist()

    fechas_cierre = df['fechas_cierre'].tolist()

    fechas_trm = df['fechas_trm'].tolist()

    fechas_vic_ccial = df['fechas_vic_ccial'].tolist()

    fechas_matriz = df['fechas_matriz'].tolist()

    fechas_receta = df['fechas_receta'].tolist()

    fechas_planta = df['fechas_planta'].tolist()

    fechas_alivios = df['fechas_alivios'].tolist()

    for fecha_corte, fecha_corte_nmb, fecha_corte_ant, fecha_cierre, fecha_trm, fecha_vic_ccial, fecha_matriz, fecha_receta, fecha_planta, fecha_alivios in zip(fechas_corte,fechas_corte_nmb, fechas_corte_ant, fechas_cierre, fechas_trm, fechas_vic_ccial, fechas_matriz, fechas_receta, fechas_planta, fechas_alivios):
        params_ejecucion = {
                                'processZone': 'proceso',
                                'processZone2': 'proceso_riesgos',
                                'ResultZone':  'resultados_riesgos',
                                'FECHA_CORTE': fecha_corte,
                                'FECHA_CORTE_NMB': fecha_corte_nmb,
                                'FECHA_CORTE_ANT': fecha_corte_ant,
                                'FECHA_CIERRE': fecha_cierre,
                                'FECHA_TRM': fecha_trm,
                                'FECHA_VIC_CCIAL': fecha_vic_ccial,
                                'FECHA_MATRIZ': fecha_matriz,
                                'FECHA_RECETA': fecha_receta,
                                'FECHA_PLANTA': fecha_planta,
                                'FECHA_ALIVIOS': fecha_alivios
                                }
        print(params_ejecucion)
        
    # Ejecutando archivos con queries
        impala('2_insumos_saldos.sql', impala_manager, params_ejecucion)
        impala('3_consulta_master.sql', impala_manager, params_ejecucion)
        impala('4_query_saldos.sql', impala_manager, params_ejecucion) ## Detalle para Reporte
        impala('5_reportes.sql', impala_manager, params_ejecucion) ## Agrupación para Reporte
        impala('6_crea_resultado.sql', impala_manager, params_ejecucion)
        impala('7_depuracion.sql', impala_manager, params_ejecucion)
        ## impala('unitest/test/test_pre_apl.sql', impala_manager, params_ejecucion) ## Cálculo para Pruebas Unitarias

       
    # Ejecutando consultas directamente (no archivos)
#     dropSql = "drop table if exists {0} purge".format(tablaNueva)
#     createSql = "create table {0} like {1}".format(tablaNueva, tablaOriginal)
#     impala_manager.exec_query(dropSql)
#     impala_manager.exec_query(createSql)

#     Manejo específico de logs
#     print("Se crea la tabla {0} exitosamente".format(tablaNueva))
#     impala_manager.print_log("Se crea la tabla {0} exitosamente".format(tablaNueva))

#     Contando registros
#     cnt_cli_nvos = impala_manager.conteo_tabla('proceso_riesgos.aec')
#     print('La tabla {0} tiene {1} registros'.format('proceso_riesgos.aec', cnt_cli_nvos))
