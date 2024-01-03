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
            'dsn': 'impala_prod',
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

    # ## SQL que calcula las fechas del reporte -> Se recomienda activar si no se quieren validar pruebas previas y posteriores.
    # params_ejecucion = {}
    # impala('1_ingestiones.sql', impala_manager, params_ejecucion)

    ### El archivo 1_ingestiones.sql para calcular las fechas a continuación, se ejecuta en el programa unitest/pre-test/tes_pre_SD.py:
    ## Se crea DF para asignar variables con fechas reporte


###########################################################################################################################

    # asigna_df_sd = impala_manager.tabla_impala_to_df('proceso.fechas_saldos')
    # fecha_corte = asigna_df_sd.iloc[0]['fecha_corte'] # Es equivalente a fecha_corte = asigna_df_sd.loc[0,'fecha_corte']
    # fecha_corte_ant = asigna_df_sd.iloc[0]['fecha_corte_ant']
    # fecha_corte_nmb = asigna_df_sd.iloc[0]['fecha_corte_nmb']

    # asigna_df_cn = impala_manager.tabla_impala_to_df('proceso.fechas_cierre')
    # fecha_cierre = asigna_df_cn.iloc[0]['fecha_corte']

    # asigna_df_trm = impala_manager.tabla_impala_to_df('proceso.fechas_trm')
    # fecha_trm = asigna_df_cn.iloc[0]['fecha_corte']

    ult_fecha_vic_ccial=hp.obtener_ultima_ingestion("resultados_riesgos.erm_vic_ccial")
    fecha_vic_ccial=str((ult_fecha_vic_ccial['year']))+"-"+str((ult_fecha_vic_ccial['month'])).zfill(2)+"-"+str((ult_fecha_vic_ccial['day'])).zfill(2)

    ult_fecha_matriz =hp.obtener_ultima_ingestion("resultados_riesgos.erm_matriz_corporativa")
    fecha_matriz=str((ult_fecha_matriz['year']))+"-"+str((ult_fecha_matriz['month'])).zfill(2)+"-"+str((ult_fecha_matriz['day'])).zfill(2)

    ult_fecha_receta =hp.obtener_ultima_ingestion("resultados_riesgos.asignacion_productos")
    fecha_receta=str((ult_fecha_receta['year']))+"-"+str((ult_fecha_receta['month'])).zfill(2)+"-"+str((ult_fecha_receta['day'])).zfill(2)

    ult_fecha_planta =hp.obtener_ultima_ingestion("resultados_serv_para_los_clientes.planta_comercial_pgc")
    fecha_planta=str(int((ult_fecha_planta['year'])))+"-"+str(int((ult_fecha_planta['month']))).zfill(2)+"-"+str(int((ult_fecha_planta['day']))).zfill(2)

    ult_fecha_alivios =hp.obtener_ultima_ingestion("resultados_riesgos.base_alivios_covid")
    fecha_alivios=str((ult_fecha_alivios['year']))+"-"+str((ult_fecha_alivios['month'])).zfill(2)+"-"+str((ult_fecha_alivios['day'])).zfill(2)
    
    # fecha_corte = '2021-06-27'
    # fecha_corte_nmb = '20210627'
    # fecha_corte_ant = '2021-06-24'
    # fecha_cierre = '2021-05-31' #'2021-03-31'
    # fecha_trm = '2021-05-01'

    # fecha_corte = '2021-12-09'
    # fecha_corte_nmb = '20211209'
    # fecha_corte_ant = '2021-12-08'
    # fecha_cierre = '2021-10-31'
    # fecha_trm = '2021-10-01'
    
    fecha_corte = '2022-05-12'
    fecha_corte_nmb = '20220512'
    fecha_corte_ant = '2022-05-11'
    fecha_cierre = '2022-04-30'
    fecha_trm = '2022-04-01'

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

    # Importa tabla de Parámetros Saldos Diarios - Catálogos que se usarán en el proceso
    file_path = os.getcwd()+'\\Parametros_Saldos_Diarios.csv' 
    sp.subir_csv(file_path,'proceso_riesgos.sd_parametros', sep=';', modo ='overwrite' )

    # Importa tabla de Parámetros Saldos Diarios - Rangos de variación tolerables para cada aplicativo
    file_path = os.getcwd()+'\\unitest\\test\\rng_var_sld_apl.csv' 
    sp.subir_csv(file_path,'proceso.saldos_diarios_rangos_apl', sep=';', modo ='overwrite' )

        
    # # Ejecutando archivos con queries
    impala('2_insumos_saldos.sql', impala_manager, params_ejecucion)
    impala('3_consulta_master.sql', impala_manager, params_ejecucion)
    impala('4_query_saldos.sql', impala_manager, params_ejecucion) ## Detalle para Reporte
    impala('5_reportes.sql', impala_manager, params_ejecucion) ## Agrupación para Reporte
    impala('unitest/test/test_pre_apl.sql', impala_manager, params_ejecucion) ## Cálculo para Pruebas Unitarias

       
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
