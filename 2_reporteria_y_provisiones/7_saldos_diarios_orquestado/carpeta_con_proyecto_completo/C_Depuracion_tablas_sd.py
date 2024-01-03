# -*- coding: utf-8 -*-
"""

@author: vaosorio, dulondon 

Proceso depuracion tablas temporales del reporte "Saldos Diarios"

"""
#%%
from orquestador.funcs_master import init_logger, impala, ejecucion_spark
from orquestador.impala_manager import ImpalaExecutionManager
# import pandas as pd
from sparky_bc import Sparky
# from datetime import datetime
# from datetime import date
# from openpyxl import Workbook
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
            'dsn': 'IMPALA_PROD',
            'n_retry': 20,
            't_espera': 10,
            'emails': None,
            'play_sound': False
            }

    log_path = ""
    
    logger = init_logger(log_path) # Logs de ejecución

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

    
    ### El archivo 1_ingestiones.sql para calcular las fechas a continuación, se ejecuta en el programa unitest/pre-test/tes_pre_SD.py:
    asigna_df_sd = impala_manager.tabla_impala_to_df('proceso.fechas_saldos')
    fecha_corte_nmb = asigna_df_sd.iloc[0]['fecha_corte_nmb']

    # fecha_corte_nmb = '20210627'

    params_ejecucion = {
                            'processZone': 'proceso',
                            'processZone2': 'proceso_riesgos',
                            'ResultZone':  'resultados_riesgos',
                            'FECHA_CORTE_NMB': fecha_corte_nmb
                            }
    print(params_ejecucion)

    ## Abrir archivo de Excel para que se actualicen conexiones ODBC
    xl = win32.Dispatch('Excel.Application')
    xl.Application.visible = True #change to True if you are desired to make Excel visible

    file_path = os.getcwd()+'\\reporte\\SALDOS DIARIOS EJECUCION ACTUAL.xlsm' 

    wb = xl.Workbooks.Open(file_path)


    # # Ejecutando archivos con queries
    impala('6_crea_resultado.sql', impala_manager, params_ejecucion)
    impala('7_depuracion.sql', impala_manager, params_ejecucion)

   
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
