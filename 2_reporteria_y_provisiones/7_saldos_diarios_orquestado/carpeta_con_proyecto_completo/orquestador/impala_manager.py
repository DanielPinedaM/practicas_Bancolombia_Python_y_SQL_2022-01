# -*- coding: utf-8 -*-
"""
@author: jufherna, caranvel, warboled
"""

import time
import collections
import pyodbc
import os
import winsound as ws
import re
#import funcs
import pandas as pd
from importlib import reload

################ MANEJADOR DE CONEXIÓN CON IMPALA #############################

# Creación de tupla de query
Query = collections.namedtuple('Impala', ['typeq', 'tname', 'query'])

class ImpalaExecutionManager:
    """ Esta clase establece una conexión con Impala y controla las ejecuciones
        haciendo uso de Log y manejo de excepciones con reintentos.
        Opcionalmente también envía un correo al finalizar con el .log
        También opcionalmente ejecuta sonidos en cada ejecución
    """
    def __init__(self, logger, dsn='IMPALA_PROD', 
                server_host='impala.bancolombia.corp', server_port=21050, 
                n_retry=100, t_espera = 30, emails=None, play_sound=False):
        # Variables de conexión
        self._cnx = None
        self._server_host = server_host
        self._server_port = server_port
        self._dsn = dsn

        # Deques con las sentencias de Impala
        self._exec_querys = collections.deque()
        self._untested_tables = collections.deque()
        
        # Variables de reinicio de ejecución
        self._t_espera = t_espera
        self._n_retry = n_retry
        self._internal_executions = 0
        self._executing_process = False
        
        # Logger interno
        self._logger = logger

        # Email
        self._email = emails

        # Ejecutar sonido
        self._play_sound = play_sound

    def exec_query(self, query):
        """Metodo publico que ejecuta una sentencia en Impala
        
           Retorna el tiempo de ejecucion y el cursor
           
           Parámetros:
           query -- Sentencia a ser ejecutada en el motor Impala a travéz de pyodbc

           Excepciones:
           General -- se capturan en la fucion _process_exception y se reporta en el log
           
        """
        try:
            if self._cnx is None: 
                self._cnx = self._connect() # Conecta si no se ha hecho
            
            query_split = query.strip().split()
            if query_split[0].lower().strip() in ['select','with']:
                results = pd.read_sql(query,self._cnx)
                return results
            else:        
                cur = self._cnx.cursor()
                results = cur.execute(query)
                cur.close()
                return True
            
        except Exception as err: 
            self._process_exception(err)
            return None

    def execute(self):
        """Metodo publico que para la ejecion de una serie de sentencias en impala
            
           Parametros:
           
        """
        self._internal_executions = 0
        self._execute()

    def conteo_tabla(self, nombre_tabla):
        """Cuenta la cantidad de registros de una tabla
        
        Parámetros:
            nombre_tabla (str):  Nombre de la tabla en la LZ (incluir base de datos si es necesario)

        Retorna:
            Entero (int) con cantidad de registros en tabla especificada
        
        Raise error: Si no encuentra la tabla
        """
        conteo_df = self.exec_query(""" select count(*) as conteo
                            from %s;""" % nombre_tabla)
        return conteo_df['conteo'][0]

    def descarga_tabla_impala(self, nombre_tabla, ruta_archivo, nombre_archivo):
        """Descarga una tabla de impala en un directorio específico

        Parámetros:
            nombre_tabla (str):  Nombre de la tabla en la LZ (incluir base de datos si es necesario)
            ruta_archivo (str): Directorio donde se almacenará el archivo a exportar
            nombre_archivo(str): Nombre del archivo como desea ser exportado. Incluir la extensión.
        """

        tabla_impala = self.tabla_impala_to_df(nombre_tabla)
        file_path = os.path.join(ruta_archivo, nombre_archivo)
        tabla_impala.to_csv(file_path, header=None, index=False)

    def tabla_impala_to_df(self, nombre_tabla):
        """Descarga una tabla de Impala dejándola en un data frame

        Parámetros:
            nombre_tabla (str):  Nombre de la tabla en la LZ (incluir base de datos si es necesario)

        Retorna:
            Dataframe de Pandas con la tabla
        """
        if self._cnx is None: 
            self._cnx = self._connect() # Conecta si no se ha hecho
            
        print("Exporta a dataframe ====> " + nombre_tabla)
        sql = "select * from {0}".format(nombre_tabla)
        return pd.read_sql(sql, self._cnx)


    def print_log(self, message):
        """Método público que realiza las impresiones de los mensajes de logs tanto
           en consola de ejecucion como en el archivo
            
           Parametros:
           message -- Mensaje que se quiere mostrar y guardar con respecto a la ejecucion
                      en algún punto de esta
           
        """
        self._logger.info(message)

    def connect(self):
        """Método público que retorna un objeto de conexión ODBC que puede ser útil para realizar pruebas.
        
           Excepciones:
            
        """
        self.print_log("ExecutionManager: Inicio de la conexión con Impala.")

        try:  
            self._cnx = pyodbc.connect("DSN=%s" % self._dsn, autocommit = True )
            return  self._cnx

        except Exception as err: 
            self._process_exception(err)
            return None
    

    def add_sql_sequence(self, file_path, params):
        """Crea un objeto de la clase SQLArray que lee un archivo y adiciona a una lista ligada
           las sentencias sql que este contenga
           
           Parámetros:
           file_path -- ruta del archivo con las sentencias sql
           params -- parametros que requieren las sentencias sql en el archivo de entrada
           
        """
        querys = SQLArray(file_path, params)

        if self._exec_querys is None or len(self._exec_querys) == 0:
            self._exec_querys = collections.deque(querys[:])
        else:
            self._exec_querys = collections.deque( list(self._exec_querys) + querys[:] )
            
    def test_insert_maestros(self, maestros, params, params_ctrl_mensual):
        """Calcúla para las tablas del transaccional la difernecia entre la cantidad de
           registros entrante y la calculada en el ultimo mes y reporta el warning segun las
           condiciones definidas
           
           Parámetros:
           maestros            -- diccionario con las tablas a evaluar
           params              -- parametros para la zona de almacenmaiento y la fecha
           params_ctrl_mensual -- diccionario con los intervalos para el control mensual
           
           Excepciones:
           General -- Fallo en el proceso de consulta a maestros
           
        """
        self.print_log("ExecutionManager: Comienzo el test de insert para maestro.")
        res = {}
        res_array = []
        try:
            if self._cnx is None: 
                self._cnx = self.connect()
            print('Calculando: ')
            for table in maestros.keys():
                print('.... ' + table)                
                query_maestro = """with regs_x_fechas as
                                   (
                                           select count(*) as rgtros, f_corte 
                                           from {0}.{1}
                                           group by f_corte
                                   ) select rgtros from regs_x_fechas
                                     where f_corte = (select max(f_corte) from regs_x_fechas);
                                """.format(params['ZONA_FIN'],maestros[table])
                                    
                query_mensual = """select count(*) as rgtros, f_corte 
                                   from {0}.{1}
                                   group by f_corte
                                   order by f_corte""".format(params['ZONA_TEMP'],maestros[table]  + "_mes")
            
                df_maestro = pd.read_sql(query_maestro,self._cnx)
                df_mes = pd.read_sql(query_mensual,self._cnx)
                
                if abs(df_mes['rgtros'][0] -  df_maestro['rgtros'][0]) < params_ctrl_mensual[table][0] or abs(
                       df_mes['rgtros'][0] -  df_maestro['rgtros'][0]) > params_ctrl_mensual[table][1] :
                        res_array.append('X(')
                        res_array.append(df_mes['f_corte'][0])
                else:
                        res_array.append('=)')
                        res_array.append(df_mes['f_corte'][0])
                if len(df_mes.index) > 1:
                    i = 1
                    while( i < len(df_mes.index)):
                        if abs(df_mes['rgtros'][i] -  df_mes['rgtros'][i-1]) < params_ctrl_mensual[table][0] or abs(
                            df_mes['rgtros'][i] -  df_mes['rgtros'][i-1]) > params_ctrl_mensual[table][1] :
                            res_array.append('X(')
                            res_array.append(df_mes['f_corte'][i])
                        else:
                            res_array.append('=)')
                            res_array.append(df_mes['f_corte'][i])
                        i += 1
                res[table] = res_array
                res_array = []
                #print('.',end = '')
            print('\n')
            for r in res.keys():
                print("| {tabla} |".format( tabla=r.ljust(15, ' ') ), end  = '', flush = True)
                self._logger.info("{tabla}".format( tabla=r.ljust(15, ' ') ))
                if len(res[r]) > 1:
                    ra = 0
                    while(ra < len(res[r])):
                        if ra > 0:
                            print("""{esp} | {fecha} | {resultado}"""
                                  .format(esp = '  ----->'.ljust(17, ' '),fecha = res[r][ra+1], 
                                          resultado = res[r][ra]))
                            self._logger.info("----> fecha: {fech}, resultado: {res}"
                                              .format(fech = res[r][ra+1], res = res[r][ra]))
                        else:
                            print(""" {fecha} | {resultado}"""
                                  .format(fecha = res[r][ra+1], resultado = res[r][ra]))
                            self._logger.info("----> fecha: {fech}, resultado: {res}"
                                              .format(fech = res[r][ra+1], res = res[r][ra]))
                        if res[r][ra] == 'X(':
                            self._logger.warn("""ExecutionManager: Tabla = {tabla}, El numero de datos a insertar supera el intervalo historico
                                              """.format(tabla=table))
                        ra += 2
                else:
                    print(" {fecha} | {resultado}"
                          .format(fecha = res[r][1], resultado = res[r][0]))
                    self._logger.info("----> fecha: {fech}, resultado: {res}"
                                      .format(fech = res[r][1], res = res[r][0]))
                    if res_array[0] == 'X(':
                        self._logger.warn("""ExecutionManager: Tabla = {tabla}, El numero de datos a insertar supera el intervalo historico
                                          """.format(tabla=table))
        except Exception as ex: 
            print("ERROR!: " + str(ex))
            self._logger.error(str(ex))
            

    def test_tables(self, tables = None):
        """Retorna el tamaño (count) de las tablas que se pasan como parámetro. En una estructura de diccionario.
           Si no se pasa una lista de tablas, se testearán las que hallan sido creadas con el manager.
        
           Parámetros:
           tables -- lista con las tablas a evaluar

           Excepciones:
            General -- falla en la conexión y ejecución de comandos con imapala
        """
        # TODO: Se pueden agregar mas opciones de comprobación como número de registros esperado por tabla análisis de duplicados.
        if tables is None:
            tables = self._untested_tables

        if len(tables) == 0:
            self.print_log("ExecutionManager: No hay tablas para hacer test.")

        self.print_log("ExecutionManager: Comienzo el test de tablas.")
        # Comienzo del test de tablas
        test_t = tables

        try:
            if self._cnx is None: 
                self._cnx = self._connect()
                
            print("| {tabla} | {conteo} | {tiempo} |"
            .format( tabla="Tabla".ljust(50, ' '), tiempo="Tiempo".ljust(8, ' '), conteo = "Conteo".ljust(10, ' ')))

            for table in test_t:
                print("| {tabla} |"
                    .format( tabla=table.ljust(50, ' ') ), end  = '', flush = True)

                
                query = "select count(*) as conteo from %s;" % table

                start = time.time()
                res = pd.read_sql(query,self._cnx)
                end = time.time()

                tiempo = self._time_transform(end-start)
                
                print(""" {conteo} | {tiempo} |"""
                      .format(tiempo = tiempo.ljust(8, ' '), conteo = str(res['conteo'][0]).ljust(10, ' ')))
    
                if res['conteo'][0] == 0:
                    self._logger.warn("ExecutionManager: Tabla = {tabla}, conteo = {conteo}, tiempo = {tiempo}"
                        .format(tabla=table, tiempo = tiempo , conteo = str(res['conteo'][0])))
                else:
                    self._logger.info("ExecutionManager: Tabla = {tabla}, conteo = {conteo}, tiempo = {tiempo}"
                        .format(tabla=table, tiempo = tiempo , conteo = str(res['conteo'][0])))
        
        except Exception as ex: 
            print("ERROR!: " + str(ex))
            self._logger.error(str(ex))

    def _execute(self):
        """Ejecuta una serie de sentencias sql que han sido agregadas al manager.
        
           Parámetros:
        
           Excepciones:
           General -- se capturan en la función _process_exception y se reporta en el log
        """
        n_exec_querys = len(self._exec_querys)
        reload( pyodbc )
        # Control sobre cantidad de querys a ser ejecutadas
        if n_exec_querys == 0:
            self.print_log('ExecutionManager: No hay sentencias a ser ejecutadas')
            return
       
        self._executing_process = True

        try:
                
                
            self.print_log('ExecutionManager: Hilo de ejecución de sentencias Impala')
            self.print_log("Sentencias: %i" % n_exec_querys)
            
            self._cnx = self._connect() # Nueva conexión a impala

            execution_querys = list(self._exec_querys)
                
            print("| {n_query} | {operacion} | {tabla} | {tiempo} |".format( n_query = "n_query".ljust(7, ' ') , 
                    operacion="Operación".ljust(10, ' '), tabla="Tabla".ljust(50, ' '), tiempo="Tiempo".ljust(8, ' ')))
                
            n = 1
            # Bucle sobre los queries
            for query_t in execution_querys:
                    print("| {n_q} | {operacion} | {tabla} | "
                        .format( n_q = (str(n) + "/" + str(n_exec_querys)).rjust(7, ' '), 
                        operacion = query_t.typeq.ljust(10, ' '), tabla = query_t.tname.ljust(50, ' ') ), 
                        end  = '', flush = True)
                    
                    time_q = self._executeQuery(query_t.query)
                    tiempo = str(self._time_transform(time_q)) # Transformación de formato tiempo
                    
                    self._exec_querys.remove(query_t)
                    
                    print("{tiempo} |".format(tiempo = tiempo.ljust(8, ' ')))
                    self._logger.info("Operacion: {oper} | Tabla: {table} | Tiempo: {tiempo}"
                        .format(oper = query_t.typeq, table = query_t.tname, tiempo = tiempo))

                    if self._play_sound:
                        ws.PlaySound('orquestador/audio/MarioSheetMusicCoinSound.wav', ws.SND_FILENAME)
                    n+=1

                    # Se guardan las tablas a ser probadas más adelante.
                    if query_t.typeq.lower() in ('create', 'insert', 'refresh'):
                        self._untested_tables.append(query_t.tname)
                    
            # Ejecutados correctamente
            self._executing_process = False
            self.print_log("ExecutionManager: Ejecución exitosa de todas las sentencias.")
            if self._play_sound:
                ws.PlaySound('orquestador/audio/MarioSheetMusicFlagpoleFanfa.wav', ws.SND_FILENAME)

            # Envio email de alerta
            if self._email is not None:
                    self._send_email(asunto = "[ExecutionManager] Ejecución Finalizó con éxito.", 
                        body = "Se ejecutaron %i querys.\n\n" % n_exec_querys)
        except Exception as ex:
            self._process_exception(ex)

    
    def _connect(self):
        """ Metodo privado que invoca la conexion con impala
        """
        return self.connect()

    def _send_email(self, asunto, body):
        """ Metodo privado que envia un email con el resumen de la ejecucion
          
          Parámetros:
          asunto -- corresponde al asunto del correo
          body   -- corresponde al mensaje o contenido del correo
        """
        print("ExecutionManager: Enviando email de información.")
        email_destino = self._email
        if hasattr(self._logger.handlers[0], 'baseFilename'):
            adjunto = self._logger.handlers[0].baseFilename
        else:
            adjunto = None
            
        # Caracteres de escape para el body
        escaped = body.translate(str.maketrans({'"':  r"'"}))
        escaped = escaped.replace("\\n", "\n")
        
        if adjunto is not None:
            self.send_email(email_destino=email_destino, asunto = asunto, cuerpo = escaped, adjuntos = [adjunto])

    def _time_transform(self, segundos):
        """Convierte un valor de la clase DateTime en formato HHMMss

            Parametros:
            segundos -- valor a transformar
        """
        horas = int(segundos/3600)
        minutos = int((segundos - horas*3600)/60)
        segundos = segundos - horas*3600 - minutos*60

        return "{H}h{M}m{S}s".format(H=str(horas).rjust(2, '0'), M=str(minutos).rjust(2, '0'), S=str(segundos).rjust(2, '0'))

    def _executeQuery(self, query):
        """ Ejecuta una sentencia SQL en impala

            Parametros:
            query -- dentencia a ejeutar

            Excepciones:
                -- Falla en la conexión con impala
                -- Falla en la ejecución de la sentencia en impala
        """
        reload( pyodbc )
        try:
            cur = self._cnx.cursor()

            time_i = [time.clock()] # Tiempo comienzo
            cur.execute(query)
            time_i.append(time.clock()) # Tiempo fin consulta
            
            cur.close()

            return round(time_i[1] - time_i[0])
        except: raise

    def _resume_execution(self, tiempo_espera):
        # Manejo si se cae LZ en un create, añadir un drop al principio
        if len(list(self._exec_querys)) > 0:
            breaking_q = self._exec_querys[0] # Primer query a ser ejecutado
            if breaking_q.typeq.lower() == "create":
                create_q = Query("drop", breaking_q.tname, "drop table if exists %s purge" % breaking_q.tname)
                self._exec_querys.appendleft(create_q)
        else:
            print("WARNING. Deque esta vacio")
                
        # Reinicio del hilo de ejecución
        self.print_log("ExecutionManager: Reiniciando hilo de ejecución. (%is)" % (tiempo_espera))
        self._internal_executions += 1
        time.sleep(tiempo_espera)
        self._execute()

    def _process_exception(self, err):
        """
        Procesa las excepciones en tiempo de ejecución (Manejador de excepciones)
         y levanta el error si este no se encuentra mapeado.

        Parámetros:
        err -- error que se captura en la ejecucion
        """
        # Ignorar los errores si no está en proceso de ejecución de sentencias
        if self._executing_process == False:
            pass

        print(err)
        self._logger.error(err)

        # PYODBC Errors
        if type(err) == pyodbc.OperationalError and err.args[0] == '08S01': # Error de Conexión
            # Control de reintentos
            if self._internal_executions >= self._n_retry:
                self.print_log('ExecutionManager: Se alcanzó el número máximo de reintentos')
                return
            
            print("ExecutionManager: Error de conexión, reiniciando ejecución.")
            self._logger.error("ExecutionManager: Error de conexión, reiniciando ejecución.")
            self._resume_execution(self._t_espera)
            
        elif type(err) == pyodbc.Error and err.args[0] == 'HY000': # Error de ODBC
            # Formateo del error
            error = re.search('^.*\(\d+\)(.*)\(\d+\).*$', str(err))
            error_s = error.group(1).replace('\\n','\n').strip()
            print('\n')

            resume_ex_errors = {
                "General error"                             : "Error: Inesperado ",
                "Error while executing a query"             : "Error: Ejecutando sentencia SQL",
                "Runtime Error:"                            : "Error: Ejecutando sentencia SQL",
                "AnalysisException: Table already exists"   : "Error: Tabla ya existe, se realiza un drop",
                "Impala Thrift API: connect() failed"       : "Error: Falla en conectividad"
            }

            stop_ex_errors = {
                "AuthorizationException: User"            : "Error de autorización de usuario",           # Error de autorización de usuario
                "AnalysisException: Syntax error"         : "Error de sintaxis en el archivo SQL",        # Error de sintaxis en sql
                "AnalysisException: Could not resolve"    : "Error: No encuentra columna en tabla",       # Error por no encontrar columna
                "AnalysisException: Column/field reference is ambiguous"  : "Error: Columna ambigua",     # Error por no encontrar columna
                "AnalysisException: Operands have unequal": "Error: Tipos de datos incompatibles",        # Error por tipos de datos incompatibles
                "AnalysisException: operands of type"     : "Error: Tipos de datos incomparables",        # Error por tipos de datos incomparables
                "AnalysisException: cannot combine SELECT DISTINCT" : "Error: No se puede usar DISTINCT con funciones analíticas",        # Error por Distinct y función analítica
                "AnalysisException: Duplicate column name": "Error: Nombre de columna repetido",          # Error por nombres de columna repetidos
                "AnalysisException: Table does not exist" : "Error: Tabla no existe",                     # Error por tabla no existente
                "AnalysisException: No matching function with signature": "Error: Nombre de columna repetido", # Error por tipos de datos en función
                "SSL_connect: certificate"                : "Error con el  certificado SSL",              # Error de certificado SSL
                "SSL_CTX_load_verify_locations"           : "Error ruta certificado SSL"                  # Error de path certificado SSL
            }
            
            # Errores que no permiten continuar
            for err_m in stop_ex_errors:
                if err_m in error_s:
                    log = "ExecutionManager: %s. No se puede continuar." % (stop_ex_errors[err_m])
                    print(log)
                    self._logger.error(log)
                    
                    # Envio email de alerta
                    if self._email is not None:
                        self._send_email(asunto = "[ExecutionManager] Ejecución fallo", 
                            body = "Se encontró un error que no permite continuar.\n\n" + str(err))

                    if self._play_sound:
                        ws.PlaySound('orquestador/audio/MarioSheetMusicGameOverSoun.wav', ws.SND_FILENAME)
                    return
            
            # Errores que permiten reanudar la ejecución
            for err_m in resume_ex_errors:
                if err_m in error_s:
                    # Control de reintentos
                    if self._internal_executions >= self._n_retry:
                        self.print_log('ExecutionManager: Se alcanzó el número máximo de reintentos')

                        # Envio email de alerta
                        if self._email is not None:
                            self._send_email(asunto = "[ExecutionManager] Ejecución fallo", 
                                body = "Se alcanzó el número máximo de reintentos. \n\n" + str(err))

                        return
                    else:
                        log = "ExecutionManager: %s. Se reinicia ejecución." % (resume_ex_errors[err_m])
                        print(resume_ex_errors[err_m])
                        self._resume_execution(self._t_espera)
                        return

            # Error no mapeado
            log = "[ExecutionManager] Error no mapeado. Continúa el flujo del error"
            print(log)
            self._logger.error(log)

            # Envio email de alerta
            if self._email is not None:
                self._send_email(asunto = "[ExecutionManager] Ejecución fallo", 
                    body = "Se encontró error no mapeado. \n\n" + str(err))


            raise err

        else: # Error desconocido
            log = "ExecutionManager: Se ha generado un error desconocido. Continúa el flujo del error."
            print(log)
            self._logger.error(log)
            if self._play_sound:
                ws.PlaySound('orquestador/audio/MarioSheetMusicGameOverSoun.wav', ws.SND_FILENAME)
            
            # Envio email de alerta
            if self._email is not None:
                self._send_email(asunto = "[ExecutionManager] Ejecución fallo", 
                    body = "Se encontró error desconocido. \n\n" + str(err))


            raise err
            
    def send_email(self,email_destino, asunto, cuerpo, adjuntos = [], prioridad = 1):
        """
        send_email: Envía un correo automático utilizando el cliente local de outlook,
        esto se hace por medio de un script de PowerShell, cuyos parámetros se pasan
        desde Popen()
        
        Inputs:
            - email_destino: Remitentes a los que debe llegar el correo (list)
            - asunto: Asunto del mensaje (str)
            - cuerpo: Contenido del mensaje (str)
            - adjuntos: Archivos a enviar (list)
            - prioridad: Prioridad del mensaje (Default: 1)
        """
        
        emails = ",".join(email_destino)
        adj = '|'.join(adjuntos)
        
        import subprocess
        process = subprocess.Popen(['powershell.exe', 
                                    'orquestador/sendEmail.ps1', 
                                    '-mails', '"%s"'        % emails,
                                    '-subject', '"%s"'      % asunto,  
                                    '-body', '"%s"'         % cuerpo, 
                                    '-importance', '%i'     % prioridad, 
                                    '-attachments', '"%s"'  % adj], 
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        out, err = process.communicate()
        return [out, err]

    def __len__(self):
        return len(self._exec_querys)
    


################# EXTRACCIÓN DE SENTENCIAS SQL ################################

# Se definen las expresiones regulares que impulsan la ejecucion sql
int_rx = re.compile( '^(?:0*$|(?!0)[-+]?[0-9]*$)' )
createt_rx = '(create)(\s*)(table)(\s*)((if)(\s*)(not)(\s*)(exists)(\s*))?(\w)+\.(\w)+[^;]+(;)'
createt_ext = '(create)(\s*)(external)(\s*)(table)(\s*)((if)(\s*)(not)(\s*)(exists)(\s*))?(\w)+\.(\w)+[^;]+(;)'
compute_stats_rx = '(compute)(\s*)(stats)(\s*)((\w)+\.)?(\w)+[^;]+(;)'
table_rx ='(\s*)?(\w)+\.(\w)+(\s*)' 
dropt_rx = '(drop)(\s*)(table)(\s*)((if)(\s*)(exists)(\s*))?((\w)+\.)?(\w)+(\s*)(purge)(\s*)?(;)'
truncatet_rx = '(truncate)(\s*)(table)(\s*)(\w)+\.(\w)+(\s*)?(;)'
altert_rx = '(alter)(\s*)(table)(\s*)(\w)+\.(\w)+[^;]+(;)'
insert_rx = '(insert)(\s*)(into)(\s*)?((\s*)(table)(\s*))?(\w)+\.(\w)+(\s*)(values)?[^;]+(;)'
sample_control_rx = '(\-)(\-)(sample_control)(\-)(\-)'
invalidate_m = '(invalidate)(\s*)(metadata)(\s*)(\w)+\.(\w)+[^;]+(;)'
refresh = '(refresh)(\s*)(\w)+\.(\w)+[^;]+(;)'
t_TRASH = "create|table|=|;|\s+|,|\t|\r"
double_rx = re.compile( r'^[-+]?([0-9]*\.[0-9]*((e|E)[-+]?[0-9]+)?'+
        r'|NaN|inf|Infinity)$')
date_rx = re.compile( r'^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$' );
time_rx = re.compile( r'[0-9][0-9]:[0-9][0-9](:[0-9][0-9])?' );
datetime_rx = re.compile(r'^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9](T| ).+')
datetime_rx2 = re.compile(r'\w\w\w \w\w\w \d\d \d\d:\d\d:\d\d \w\w\w \d\d\d\d')

class SQLArray:
    def __init__(self, file_path, params = None):
        """Constructor de la ckase SQLArray
           
           Paramáetros:
           file_path -- Archivo con un conjunto de sentencias SQL
           params    -- diccionario con parametros que se llaman desde el file_path

           Excepciones:
           General -- Alfun error en la lectura del file_path 
        """
        self._querys = self._get_queries_list(file_path,params)
        
    def _get_queries_list(self, file_path, params):
        """Retorna la lista de querys en un archivo de impala ingresado.

           Devuelve en una lista las tuplas correspondientes a los querys en 
           formato \n
           Query(typeq, tname, query)

           Excepciones:
           IOERROR: Normalmente se dispara al no encontrar el archivo en la ruta dada o no tener permisos de lectura sobre este.
        
        """
        # Acceso al archivo sql
        try:
            f = open( file_path, 'r', encoding = 'utf8')
            content = f.read()
            f.close()
        except:
            raise

        clean_content = self._clean_sql(content, params)

        # Búsqueda de expresiones regulares en el archivo sql
        it = re.finditer('|'.join([ insert_rx, createt_rx, createt_ext, dropt_rx, truncatet_rx, altert_rx,
                                    invalidate_m, refresh, compute_stats_rx]),  clean_content, re.IGNORECASE) 

        queries = list()
        # Iteración sobre los elementos de los query
        for item in it:
            itg = item.group()
            tname = re.search(table_rx, itg).group()
            typeq = itg.partition(' ')[0]
            queries.append(Query(typeq.lower(), tname.strip(), itg )) # Query(typeq, tname, query)

        return queries

    def _clean_sql(self, content, params):
        """
        Elimina los comentarios de un archivo sql
        """
        # Comentarios simples
        content_simple = re.sub(r"(?:--((?:.*?\r??)*)\n)+", "", content )
        
        # Comentarios complejos
        content_complex = re.sub(r"(?:/\*((?:.*?\r?\n?)*)\*/)+", "", content_simple )
        
        content_complex = content_complex.format(**params)

        return content_complex

    def __len__(self):
        """Elimina los comentarios de un archivo sql
        """
        return len(self._querys)

    def __getitem__(self, position):
        """ Retorna el query en la posición dada  (permite slicing sobre querys)
        """
        return self._querys[position]
