"""
Este Python sirve para subir a la base de datos de Bancolombia (LZ - HUE)
el .txt q causo un error al  subir todos los .txt

el .txt malo es este:
C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\2_archivos\3.2_txt_malo\1B110EDA Base Actual.txt
"""

########################################################################
#%%

print("\n")
print("\n")
print("importando librerias")

import pandas as pd                 # Para los DataFrame (df)
from sparky_bc import Sparky        # Conexion de Python a la base de datos de Bancolombia (LZ - HUE), libreria hecha por el banco

########################################################################
#%%

print("\n")
print("\n")
print("leyendo rutas quemadas")

ruta_txt_malo = str(r'C:\Users\danpined\OneDrive - Grupo Bancolombia\5_ReporteriaYprovisiones\6_base_historicas_lotus_txt\2_archivos\3.2_txt_malo\1B110EDA Base Actual.txt')
#print(ruta_txt_malo)

########################################################################
#%%

print("\n")
print("\n")
print("Conectando a la base de datos de Bancolombia LZ - HUE USANDO sp sparky")

nombre_usuario = "danpined"
clave = "Vancoconecta#2"

sp = Sparky(nombre_usuario,            # nombre de usuario de la base de datos
            'IMPALA_PROD',             # Nombre de la fuente de datos = DSN = Data Source Name
            clave,                     # contraseña de la base de datos
            hostname='sbmdeblze003',   # nombre del Host
            remote=True                # como es igual a =True entonces la conexion a Sparky SI se hace de forma remota
           )

# Verificar conexion a la base de datos
#print(sp)

########################################################################
#%%

nombre_columnas_tabla_txt = ["radicado",	
                             "identificacion_del_cliente",
                             "tipo_de_identificacion",
                             "nombre_del_cliente",
                             "region",
                             "zona",
                             "segmento",
                             "sector",
                             "actividad_economica",
                             "codigo_CIIU",
                             "gerente",
                             "codigo_del_gerente",
                             "centro_de_costos",
                             "grupo_de_riesgo",
                             "codigo_de_riesgo",
                             "calificacion_superbancaria",
                             "calificacion_interna_actual",
                             "fecha_de_creacion",
                             "autor",
                             "finalidad_del_credito",
                             "fecha_de_decision",
                             "nombre_de_quien_aprueba",
                             "codigo_de_quien_aprueba",
                             "numero_acta_de_comite_de_credito",
                             "LME_solicitado",
                             "LME_PIC",
                             "LME_aprobado",
                             "flujo_actual",
                             "estado_actual",
                             "responsable_actual",
                             "flujo",
                             "estado",
                             "responsable",
                             "fecha_de_entrada",
                             "hora_de_entrada",
                             "fecha_de_salida",
                             "hora_de_salida",
                             "tiempo",
                             "vigencia_LME" 
                            ]

df_txt_malo = pd.DataFrame(columns = nombre_columnas_tabla_txt) # Darle nombres a las columnas del DataFrame
# Verificar q se haya creado el DataFrame vacio con los nombres de las columnas
#print(df_txt_malo)


########################################################################
#%%

df_txt_malo = pd.read_csv(ruta_txt_malo,                      # Ruta donde esta guardado los .txt
                       
                          sep='\t',                           # Separador: Caracter por el cual se separan los datos de la tabla
                                                              # en este caso en especifico sep='\t' significa "datos separados por la tecla tabulador"
                                                              # https://stackoverflow.com/questions/22116482/what-does-print-sep-t-mean
                       
                          header = None,                      # Numero de fila donde estan los nombres de las columnas
                                                              # en este caso como = None entonces los nombres de las columnas NO se leen de los .txt
                                                     
                          names=nombre_columnas_tabla_txt,    # Nombre de las columnas del DataFrame
                                                              # en este caso como es igual a una lista =nombre_columnas_tabla entonces le estoy dando un nombre en especifico a las columnas
                                                     
                          index_col=False,                    # Indice de las filas del DataFrame
                                                              # en este caso como = False entonces los indices de las filas son 0, 1, 2...
                                                              # https://stackoverflow.com/questions/12960574/pandas-read-csv-index-col-none-not-working-with-delimiters-at-the-end-of-each-li
                                      
                                                        
                          dtype=str,                          # Tipo de dato del DataFrame
                                                              # en este caso es string =str

                          skiprows=1,                         # Desde esta Fila hacia abajo se empieza a leer los .txt
                       
                          encoding='latin-1',                 # Formato de codificacion
                        )

# Imprimir df_txt_malo
# se TIENEN q imprimir el .txt llamado 1B110EDA Base Actual.txt
#print("df_txt_malo", df_txt_malo) 

########################################################################
#%%

sp.subir_df(df_txt_malo,                       # Nombre del DataFrame q se va a subir a la base de datos
            'base_historicas_lotus_txt_tmp',   # Nombre tabla
            zona ='proceso_consumidores',      # Nombre base de datos
            modo ='overwrite'                  # 'overwrite' sobreescribe (reemplaza) los datos
           )

########################################################################
#%%
