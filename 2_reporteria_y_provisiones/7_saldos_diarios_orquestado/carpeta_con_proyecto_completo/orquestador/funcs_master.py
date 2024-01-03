 # -*- coding: utf-8 -*-
"""
Created on Wed Feb 22 14:34:34 2017

@author: caranvel, warboled
"""
import os
import logging
import sys
from datetime import datetime

def init_logger(log_path=None):
    """Inicializa el logger
    
    Parámetros: 
        log_path (str): [Opcional] Ruta donde se almacena el logger. 
                        Si es vacío, el log se establece usando sys.stdout como búffer de salida

    Retorna:
        Objeto logger que controla los registros en el archivo resultante

    """
    
    # Creación del logger
    logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    logger.setLevel(logging.DEBUG)
    
    if log_path is None:        
        # Establece stdout como búffer de salida
        hdlr = logging.StreamHandler(sys.stdout)
        hdlr.setLevel(logging.DEBUG)
        hdlr.setFormatter(formatter)
        
        logger.addHandler(hdlr)
        logger.setLevel(logging.INFO)
    else:
        # Creación del logger
        log_name= 'log'
        namelog =   log_name  + '_' + str(datetime.now().strftime("%Y%m%d_%H%M%S")) + '.log'
        
        # Creación de directorio de logs
        if log_path.endswith('logs') or log_path.endswith('logs/'):
            directory = log_path
        else:
            directory = os.path.join(log_path, 'logs/')           

        if not os.path.exists(directory): os.makedirs(directory)
    
        # Creación de  archivo de logs y formateo
        hdlr = logging.FileHandler(os.path.join(directory, namelog), 'w', 'utf-8')
        hdlr.setFormatter(formatter)

        logger.addHandler(hdlr)
        logger.setLevel(logging.INFO)
    
    return logger
#%%
def impala(sql_file_path, manager, params):
    """Ejecuta un archivo sql con sentencian sql a través del manager
    
       Parámetros: 
       sql_file_path --  sql_file_path: ruta del archivo con los queries
       manager       --  objeto que ejecuta las sentencias en impala
       params        --  parametros de entrada para el manager, contiene la zona y fechas
    """
    # Configuración del manager
    manager.add_sql_sequence(sql_file_path, params)
    
    # Ejecución de todo el sql y test
    manager.execute()

#%%
#se define la funcion que ejecuta el codigo de Spark que es un .jar
def ejecucion_spark(spark_cmd, logger, username=None, password=None):
    """
        Funcion auxiliar para ejecutar sentencias a traves de spark
        
        Parámetros:
            username  -- Usuario de red para conexion con Home impala
            password  -- Contraseña de red para conexion con Home impala
            spark_cmd -- Comando de spark a ser ejecutado. Ej.: spark2-submit --driver-memory 10G --class Maestro /home/caranvel/prueba.jar
        
    """
    logger.info("Inicia Ejecución Spark")
    if os.name == 'nt':
        import paramiko
        nbytes = 4096
        hostname = 'sbmdeblze003'
        port = 22
        client = paramiko.Transport((hostname, port))    
        client.connect(username=username, password=password)
        client.set_keepalive(5)
        stdout_data = []
        stderr_data = []
        print("Conexión paramiko")
        session = client.open_channel(kind='session')
        session.exec_command("echo " + password + " | kinit " + username.lower())
        session = client.open_channel(kind='session')
        session.exec_command(spark_cmd)
        print("Ejecutando spark")
        while True:
            if session.recv_ready():
                stdout_data.append(session.recv(nbytes))
                print(session.recv(nbytes))
                logger.info(session.recv(nbytes))
            if session.recv_stderr_ready():
                stderr_data.append(session.recv_stderr(nbytes))
                print(session.recv_stderr(nbytes))
                logger.error(session.recv_stderr(nbytes)) 
            if session.exit_status_ready():
                break
    else:
        q=os.popen(spark_cmd, shell = True)
        err=q.close()
        if err:
            logger.error("Error en ejecución Spark")
        for i in q:
            logger.error(i)
    logger.info("Finaliza Ejecusión de Spark")