import os
from os import scandir
import glob
import time
import re
import json
import pandas as pd
import getpass
from sparky_bc import Sparky
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

#Función importar archivo json de configuraciones
def load_config_json( conf_path ):
    """Carga un archivo json en un diccionario de python
    recibe como parámetro la ruta de un archivo json
    """
    if conf_path is None or conf_path == '':
        raise RuntimeError('Archivo de Configuración no puede ser Nulo')
    
    with open( conf_path,  encoding='utf-8'  ) as f_in :
        json_str = f_in.read()
        return json.loads( json_str )

#VARIABLES
ruta_config = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
config = load_config_json( ruta_config  )
ruta_en_cifras = config["ruta_en_cifras"]
usuario_inexmundi = config["usuario_inexmundi"]
pass_inexmundi = config["pass_inexmundi"]
rango_proyecciones = config["rango_proyecciones"]
ejecucion = config["ejecucion"]
params = config["params"]
zona = params['zona']
#el siguiente diccionario se utiliza para las fuciones 4 y 5 el cual el rango es variable dependiendo del mes
dicc_range_4 = {"Enero":"B:G,AM:AN",
            "Febrero": "B:G,AJ:AN",
            "Marzo": "B:G,AG:AN",
            "Abril": "B:G,AD:AN",
            "Mayo": "B:G,Z:AN",
            "Junio": "B:G,X:AN",
            "Julio": "B:G,V:AN",
            "Agosto": "B:G,T:AN",
            "Septiembre":"B:G,Q:AN",
            "Octubre": "B:G,N:AN",
            "Noviembre":"B:G,K:AN",
            "Diciembre": "B:AN"
            }

dicc_range_5 = {"Enero":"B:G,AI:AK",
            "Febrero": "B:G,AE:AK",
            "Marzo": "B:G,AC:AK",
            "Abril": "B:G,AA:AK",
            "Mayo": "B:G,Y:AK",
            "Junio": "B:G,W:AK",
            "Julio": "B:G,U:AK",
            "Agosto": "B:G,S:AK",
            "Septiembre":"B:G,Q:AK",
            "Octubre": "B:G,N:AK",
            "Noviembre":"B:G,K:AK",
            "Diciembre": "B:AK"
            }


#SPARKY
#usr=getpass.getuser()
#pass_usr=os.environ['PWD']
#dsn='IMPALA_PROD'
#sp = Sparky(usr, dsn, False, pass_usr)
sp = Sparky(username= getpass.getuser(), 
            password= os.environ.get('PWD'), 
            dsn='IMPALA_PROD', 
            hostname='sbmdeblze003', 
            remote=True)

def driver_google(ruta_descarga):

    separador = os.path.sep
    ruta_driver = os.path.join(separador.join(os.path.dirname(os.path.abspath(__file__)).split(separador)[:-1]),'chromedriver.exe')
    chromeOptions = Options()
    chromeOptions.add_experimental_option("prefs", {"download.default_directory" :f"{ruta_descarga}",})
    #llamado al driver con la nueva ruta de descargas
    driver =  webdriver.Chrome(executable_path = f'{ruta_driver}', chrome_options= chromeOptions)
    return driver

def ejecutar_sql(archivo, parametro):
    sp.helper.ejecutar_archivo(os.path.join(os.path.dirname(os.path.abspath(__file__)),f'{archivo}' ), parametro)

def palma_aceita_1():
    print('Inicia Ejecucion Funcion Palma_aceite_1 \n')
    #ruta para descargar achivo
    descarga = os.path.join(os.path.dirname(os.path.abspath(__file__)), "01_Palma_Aceite")
    #listar los archivos de extension xls para borrarlos
    archivos_delete = glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)), "01_Palma_Aceite",'*.xls'))
    for f in archivos_delete:
        os.remove(f)
    #llamado a la funcion driver_google con la nueva ruta de descargas
    driver =  driver_google(descarga)
    #abrir pagina de formulario y obtener el valor para el  año final.
    driver.get('http://sispaweb.fedepalma.org/sispaweb/default.aspx?Control=Reportes/rep_areadesarrolloproduccion&Sec=2')
    time.sleep(3)
    a_fin = driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlAnoFin"]/option[2]')
    a_fin= a_fin.text
    #abrir formulario con datos y  descargar archivo
    driver.get(f'http://sispaweb.fedepalma.org/sispaweb/Reportes/generarArchivoNuevasSiembras.aspx?idSec=2&fecini=2010&fecfin={a_fin}')
    time.sleep(3)
    driver.find_element_by_xpath('//*[@id="btnGenerar"]').click()
    time.sleep(3)
    driver.close()
    #leer df
    archivo = [arch.name for arch in scandir(descarga) if arch.is_file()]
    ruta_archivo_carga = os.path.join(f'{descarga}',archivo[0])
    dfs  = pd.read_html(ruta_archivo_carga, index_col= 0, header=0, encoding='utf-8')
    #renombrar columnas
    df = dfs[0].reset_index().rename(columns={"ANIO":"Fecha"})
    #subir a la lz
    sp.subir_df(df,f'{zona}.areadesarrolloproduccion_plm', modo="overwrite")
    print('\n Finaliza Ejecucion Funcion Palma_aceite_1 \n')

def frutoprocesado_2():
    print('Inicia Ejecucion Funcion frutoprocesado_2 \n')
    #ruta para descargar achivo
    descarga = os.path.join(os.path.dirname(os.path.abspath(__file__)), "02_ProduccionFrutoTotal")
    #llamado a la funcion driver_google con la nueva ruta de descargas 
    driver = driver_google(descarga)
    #obtener ultimo año en pagina para descargar y crear variablde de año y año -1
    driver.get('http://sispaweb.fedepalma.org/sispaweb/default.aspx?Control=Reportes/rep_evolucionanualfrutopro&Sec=65')
    driver.find_element_by_xpath('//*[@id="ddlAno"]/div/button/span[1]')
    ultimo_y = driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlAno"]/option[2]')
    ultimo_y= int(ultimo_y.text)
    penultimo_y = ultimo_y - 1
    archivos_delete = []
    archivos_delete.append(ultimo_y)
    archivos_delete.append(penultimo_y)
    #borrar achivos (este proceso se hace porque dependiendo del momento de la descarga la info puede estar desactualizada)
    #archivos = glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)), "02_ProduccionFrutoTotal",'*.xls')) #lista con todos los archivos en carpeta
    archivos = glob.glob(os.path.join(f'{descarga}','*.xls')) #lista con todos los archivos en carpeta
    for x in range(0,len(archivos_delete)):
        i = archivos_delete[x]
        a = os.path.join(os.path.dirname(os.path.abspath(__file__)), "02_ProduccionFrutoTotal",f'ProduccionTotal_{i}.xls')
        for y in range(0,len(archivos)):
            f = archivos[y]
            if f != a:
                pass
            else:
                os.remove(f)
    driver.get(f'http://sispaweb.fedepalma.org/sispaweb/Reportes/generarArchivoPrecios.aspx?idSec=65&anio={penultimo_y}')
    time.sleep(3)
    driver.find_element_by_xpath('//*[@id="btnGenerar"]').click()
    time.sleep(3)
    driver.get(f'http://sispaweb.fedepalma.org/sispaweb/Reportes/generarArchivoPrecios.aspx?idSec=65&anio={ultimo_y}')
    time.sleep(3)
    driver.find_element_by_xpath('//*[@id="btnGenerar"]').click()
    time.sleep(3)
    driver.close()
    #leer archivos
    lista_archivos = []
    archivos = [arch.name for arch in scandir(descarga) if arch.is_file()]
    for filename in archivos:
        ruta = os.path.join(f'{descarga}', f'{filename}')
        data = pd.read_html(ruta, index_col= 0, header=0)
        lista_archivos.append(data[0].reset_index())
    #consolidar en un solo dataframe
    df = pd.concat(lista_archivos)
    df = df.rename(columns={'ANIO':'fecha'})
    #subir a la lz
    sp.subir_df(df,f'{zona}.ProduccionFrutosec_plm', modo="overwrite")
    print('\n Finaliza Ejecucion Funcion frutoprocesado_2 \n')
    
def produccion_fruto_3():
    print('Inicia Ejecucion Funcion produccion_fruto_3 \n')
    #ruta para descargar achivo
    descarga = os.path.join(os.path.dirname(os.path.abspath(__file__)), "03_HistoricoRendimientoFruto")
    if ejecucion == 'AUTOMATICA':
        #llamado a la funcion driver_google con la nueva ruta de descargas 
        driver = driver_google(descarga)
        driver.get('http://sispaweb.fedepalma.org/sispaweb/default.aspx?Control=Reportes/rep_evolucionanualrendimientosfruto&Sec=69')
        driver.find_element_by_xpath('//*[@id="ddlAno"]/div/button/span[1]')
        ultimo_y = driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlAno"]/option[2]') #validar si sirve
        '''
        falta abrir la pagina con el año, para realizar la descarga pero fedepalma caido, para obtener esa url

        Para el historico, se me ocurre tener un archivo hisotrico con los datos y unir con la descarga y eliminar repetidos,
        exportar el dataframe y borrar los archivos existentes
        '''
    else:
        #leer archivos en carpeta
        df_list = []
        archivos = [arch.name for arch in scandir(descarga) if arch.is_file()]
        for x in range(0, len(archivos)):
            a = archivos[x]
            df = pd.read_excel(os.path.join(f'{descarga}',f'{a}'), skiprows = 19,  nrows=4, usecols = 'D:M')
            df_list.append(df)
        #concatenar dataframe
        df = pd.concat(df_list)
        columnas = df.columns.values[1:]
        #transponer tabla
        df = pd.pivot_table(df,columns='Zonas', values=columnas).rename_axis('fecha').reset_index()
        #elimnar vacios
        df = df[~df['fecha'].str.contains('Unnamed')]
        #crear estructura para subir tabla a la lz
        columnas_2 = df.columns.values[1:]
        df_zonas = []
        for x in range(0,len(columnas_2)):
            a = columnas_2[x]
            df_2 =  df[['fecha', f'{a}']]
            df_2 = df_2.rename(columns={f'{a}':'rendimiento'})
            df_2['zonas'] = f'{a}'
            df_zonas.append(df_2)
        df_final = pd.concat(df_zonas)
        #eliminar duplicados
        df_final = df_final.drop_duplicates(['fecha','zonas'],keep='last')
        print(df_final)
    sp.subir_df(df_final,f'{zona}.RendimientoFrutosec_plm2', modo="overwrite")
    print('\n Finaliza Ejecucion Funcion produccion_fruto_3 \n')

def pdccionaceite_zonas_4():
    print('Inicia Ejecucion Funcion pdccionaceite_zonas_4 \n')
    #ruta para descargar achivo
    descarga = os.path.join(os.path.dirname(os.path.abspath(__file__)), "04_ProduccionAceite_Palma_Zonas")
    #llamado a la funcion driver_google con la nueva ruta de descargas 
    driver = driver_google(descarga)
    driver.get('http://sispaweb.fedepalma.org/sispaweb/default.aspx?Control=Reportes%2frep_evolucionmensualfrutopro&Sec=66')
    time.sleep(10)
    #hallar ultimo y penultimo año
    ultimo_y = driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlAno"]/option[2]')
    time.sleep(5)
    ultimo_y= int(ultimo_y.text)
    penultimo_y = ultimo_y - 1
    #hallar ultimo  mes para penultimo_y
    driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlAno"]/option[3]').click()
    time.sleep(50)
    n_meses = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    meses_p_y = []
    meses_u_y = []
    for m in range(0,len(n_meses)):
        numero = n_meses[m]
        try:
            mes = driver.find_element_by_xpath(f'//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlMes"]/option[{numero}]')
            mes = (mes.text)
            meses_p_y.append(mes)
            print(mes)
        except:
            break
    mes_p_y = meses_p_y[-1]
    #hallar ultimo  mes para ultimo_y
    driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlAno"]/option[2]').click()
    time.sleep(50)
    for m in range(0,len(n_meses)):
        numero = n_meses[m]
        try:
            mes = driver.find_element_by_xpath(f'//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlMes"]/option[{numero}]')
            mes = (mes.text)
            meses_u_y.append(mes)
            print(mes)
        except:
            break
    mes_u_y = meses_u_y[-1]
    #borrar archivos
    archivos_delete = []
    archivos_delete.append(ultimo_y)
    archivos_delete.append(penultimo_y)
    archivos = glob.glob(os.path.join(f'{descarga}','*.xls')) #lista con todos los archivos en carpeta

    for x in range(0,len(archivos_delete)):
        i = archivos_delete[x]
        a = os.path.join(f'{descarga}',f'Reporte_{i}.xls')
        for y in range(0,len(archivos)):
            f = archivos[y]
            if f != a:
                pass
            else:
                os.remove(f)

    #descargar archivo ultimo año
    #seleccionar excel
    driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlTipo"]/option[2]').click()
    time.sleep(10)
    #seleccionar mes
    vr_u_y = (meses_u_y.index(mes_u_y)) + 1 
    driver.find_element_by_xpath(f'//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlMes"]/option[{vr_u_y}]').click()
    time.sleep(10)
    #seleccionar producto
    driver.find_element_by_xpath(f'//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlProducto"]/option[3]').click()
    time.sleep(5)
    # descargar
    driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_btnBuscar"]').click()
    time.sleep(50)
    nombre_u_y = 'Reporte_'+ f'{ultimo_y}'+'.xls'
    old_name_u_y = os.path.join(f'{descarga}','Reporte.xls')
    new_name_u_y = os.path.join(f'{descarga}',f'{nombre_u_y}')
    os.rename(old_name_u_y,new_name_u_y)

    #descargar penultimo año

    #seleccionar año
    driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlAno"]/option[3]').click()
    time.sleep(40)
    #seleccionar mes
    vr_p_y = (meses_p_y.index(mes_p_y)) + 1 
    driver.find_element_by_xpath(f'//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlMes"]/option[{vr_p_y}]').click()
    time.sleep(20)
    # descargar
    driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_btnBuscar"]').click()
    time.sleep(40)
    nombre_p_y = 'Reporte_'+ f'{penultimo_y}'+'.xls'
    old_name_p_y = os.path.join(f'{descarga}','Reporte.xls')
    new_name_p_y = os.path.join(f'{descarga}',f'{nombre_p_y}')
    os.rename(old_name_p_y,new_name_p_y)
    driver.close()
    
    #leer archivos y subir a la lz
    archivos = [arch.name for arch in scandir(descarga) if arch.is_file()]
    df_list = []
    for x in range(0, len(archivos)):
        a = archivos[x]
        if a != nombre_u_y:
            anio = re.findall(r'[0-9]+',f"{a}")
            anio = int(anio[0])
            df = pd.read_excel(os.path.join(f'{descarga}',f'{a}'),index_col=0, skiprows = 19,  nrows=5, usecols = 'B:AM').rename_axis('zona').reset_index()
            df = df.drop([0],axis=0)
            columnas = df.columns.values[1:]
            dfs = pd.pivot_table(df,columns='zona', values=columnas).rename_axis('mes').reset_index()
            dfs = dfs[~dfs['mes'].str.contains('Unnamed')]
            dfs['fecha'] = anio
            df_list.append(dfs)
        else:
            anio = re.findall(r'[0-9]+',f"{a}")
            anio = int(anio[0])
            rango_columnas = dicc_range_4[mes_u_y]
            df = pd.read_excel(os.path.join(f'{descarga}',f'{a}'),index_col=0, skiprows = 19,  nrows=5, usecols = f'{rango_columnas}').rename_axis('zona').reset_index()
            df = df.drop([0],axis=0)
            columnas = df.columns.values[1:]
            dfs = pd.pivot_table(df,columns='zona', values=columnas).rename_axis('mes').reset_index()
            dfs = dfs[~dfs['mes'].str.contains('Unnamed')]
            dfs['fecha'] = anio
            df_list.append(dfs)
    
    df = pd.concat(df_list)
    print(df)
    columnas_2 = df.columns.values[1:-1]
    df_zonas = []

    for x in range(0,len(columnas_2)):
        a = columnas_2[x]
        df_2 =  df[['fecha', 'mes', f'{a}']]
        df_2 = df_2.rename(columns={f'{a}':'produccion'})
        df_2['zonas'] = f'{a}'
        df_zonas.append(df_2)
    
    df_final = pd.concat(df_zonas)
    print(f'\nDataFrame final \n {df_final}')
    sp.subir_df(df_final,f'{zona}.ProduccionAceiteZonasec_plm', modo="overwrite")
    print('\n Finaliza Ejecucion Funcion pdccionaceite_zonas_4 \n')
        
def vtaaceite_zona_5():
    print('Inicia Ejecucion Funcion vtaaceite_zona_5 \n')
    #ruta para descargar achivo
    descarga = os.path.join(os.path.dirname(os.path.abspath(__file__)), "05_VentasMensuales_AceitePalma_ZonaInterna")
    #llamado a la funcion driver_google con la nueva ruta de descargas 
    driver = driver_google(descarga)
    driver.get('http://sispaweb.fedepalma.org/sispaweb/default.aspx?Control=Reportes/rep_distribucionventascompradordetmensual&Sec=78')
    time.sleep(10)
     #hallar ultimo y penultimo año
    ultimo_y = driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlAno"]/option[2]')
    time.sleep(5)
    ultimo_y= int(ultimo_y.text)
    penultimo_y = ultimo_y - 1
    #hallar ultimo  mes para penultimo_y
    driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlAno"]/option[3]').click()
    time.sleep(40)
    n_meses = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    meses_p_y = []
    meses_u_y = []
    for m in range(0,len(n_meses)):
        numero = n_meses[m]
        try:
            mes = driver.find_element_by_xpath(f'//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlMes"]/option[{numero}]')
            mes = (mes.text)
            meses_p_y.append(mes)
            print(mes)
        except:
            break
    mes_p_y = meses_p_y[-1]
    #hallar ultimo  mes para ultimo_y
    driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlAno"]/option[2]').click()
    time.sleep(40)
    for m in range(0,len(n_meses)):
        numero = n_meses[m]
        try:
            mes = driver.find_element_by_xpath(f'//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlMes"]/option[{numero}]')
            mes = (mes.text)
            meses_u_y.append(mes)
            print(mes)
        except:
            break
    mes_u_y = meses_u_y[-1]
    #borrar archivos
    archivos_delete = []
    archivos_delete.append(ultimo_y)
    archivos_delete.append(penultimo_y)
    archivos = glob.glob(os.path.join(f'{descarga}','*.xls')) #lista con todos los archivos en carpeta
    for x in range(0,len(archivos_delete)):
        i = archivos_delete[x]
        a = os.path.join(f'{descarga}',f'ReporteVentasAceite_{i}.xls')
        for y in range(0,len(archivos)):
            f = archivos[y]
            if f != a:
                pass
            else:
                os.remove(f)

    #descargar archivo ultimo año
    #seleccionar excel
    driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlTipo"]/option[2]').click()
    time.sleep(10)
    #seleccionar mes
    vr_u_y = (meses_u_y.index(mes_u_y)) + 1 
    driver.find_element_by_xpath(f'//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlMes"]/option[{vr_u_y}]').click()
    time.sleep(10)
    #seleccionar producto
    driver.find_element_by_xpath(f'//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlProducto"]/option[2]').click()
    time.sleep(5)
    #seleccionar variable
    driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlVariable"]/option[2]').click()
    # descargar
    driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_btnBuscar"]').click()
    time.sleep(50)
    nombre_u_y = 'ReporteVentasAceite_'+ f'{ultimo_y}'+'.xls'
    old_name_u_y = os.path.join(f'{descarga}','Reporte.xls')
    new_name_u_y = os.path.join(f'{descarga}',f'{nombre_u_y}')
    os.rename(old_name_u_y,new_name_u_y)
    

    #descargar penultimo año

    #seleccionar año
    driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlAno"]/option[3]').click()
    time.sleep(40)
    #seleccionar mes
    vr_p_y = (meses_p_y.index(mes_p_y)) + 1 
    driver.find_element_by_xpath(f'//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_ddlMes"]/option[{vr_p_y}]').click()
    time.sleep(20)
    # descargar
    driver.find_element_by_xpath('//*[@id="_ctl0__ctl0_ContenidoPrincipal_ContenidoPrincipal__ctl0_btnBuscar"]').click()
    time.sleep(40)
    nombre_p_y = 'ReporteVentasAceite_'+ f'{penultimo_y}'+'.xls'
    old_name_p_y = os.path.join(f'{descarga}','Reporte.xls')
    new_name_p_y = os.path.join(f'{descarga}',f'{nombre_p_y}')
    os.rename(old_name_p_y,new_name_p_y)
    driver.close()

     #leer archivos y subir a la lz
    archivos = [arch.name for arch in scandir(descarga) if arch.is_file()]
    df_list = []
    for x in range(0, len(archivos)):
        a = archivos[x]
        if a != nombre_u_y:
            anio = re.findall(r'[0-9]+',f"{a}")
            anio = int(anio[0])
            print(anio)
            df = pd.read_excel(os.path.join(f'{descarga}',f'{a}'),index_col=0, skiprows = 12,  nrows=9, usecols = 'B:Ak').\
                rename_axis('index').reset_index().rename(columns={'Unnamed: 4':'zona'})
            df = df.drop([0],axis=0)
            df = df[df.zona.notnull()]
            #df = df.drop(df.columns[[0,1,2,4,5,7,8,10,11,13,14,16,18,20,22,24,26,28,30,31,32,34,35]],axis='columns')
            df = df.drop(df.columns[[0]],axis='columns')
            columnas = df.columns.values
            dfs = pd.pivot_table(df,columns='zona', values=columnas).rename_axis('mes').reset_index()
            dfs = dfs[~dfs['mes'].str.contains('Unnamed')]
            dfs['fecha'] = anio
            df_list.append(dfs)
            print(dfs)
        else:
            anio = re.findall(r'[0-9]+',f"{a}")
            anio = int(anio[0])
            print(anio)
            rango_columnas = dicc_range_5[mes_u_y]
            df = pd.read_excel(os.path.join(f'{descarga}',f'{a}'),index_col=0, skiprows = 12,  nrows=9, usecols = F'{rango_columnas}').\
                rename_axis('index').reset_index().rename(columns={'Unnamed: 4':'zona'})
            df = df.drop([0],axis=0)    
            df = df[df.zona.notnull()]
            df = df.drop(df.columns[[0]],axis='columns')
            columnas = df.columns.values
            dfs = pd.pivot_table(df,columns='zona', values=columnas).rename_axis('meses').reset_index()
            dfs = dfs[~dfs['meses'].str.contains('Unnamed')]
            dfs['fecha'] = anio
            df_list.append(dfs)
    
    #concatenar dataframes    
    df_final = pd.concat(df_list)
    print(f'\nDataFrame final \n {df_final}')
    sp.subir_df(df_final,f'{zona}.Mercado_interno_exportacion_plm', modo="overwrite")
    print('\n Finaliza Ejecucion Funcion vtaaceite_zona_5 \n')

def anexo_total_nal_6():
    print('Inicia Ejecucion Funcion anexo_total_nal_6 \n')
    #ruta para descargar achivo
    descarga = os.path.join(os.path.dirname(os.path.abspath(__file__)), "06_Indice de Ventas y produccion Emmet")
    #listar los archivos de extension xls para borrarlos
    archivos = glob.glob(os.path.join(f'{descarga}','*.xlsx'))
    for f in archivos:
        os.remove(f)
    #llamado a la funcion driver_google con la nueva ruta de descargas 
    driver = driver_google(descarga)
    driver.maximize_window()
    #abrir pagina Dane y descargar archivo
    driver.get('https://www.dane.gov.co/index.php/estadisticas-por-tema/industria/encuesta-mensual-manufacturera-con-enfoque-territorial-emmet')
    time.sleep(10)
    #el siguiente try-except se hace ya que al hacer click una sola vez en la direccion genera error en python que no encuentra la ruta.
    try:
        driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div/article/section/div[2]/div/div/table[1]/tbody/tr/td/table/tbody/tr[3]/td[4]/a').click()
    except:
        time.sleep(3)
        driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div/article/section/div[2]/div/div/table[1]/tbody/tr/td/table/tbody/tr[3]/td[4]/a').click()
        time.sleep(3)
    driver.close()
    #leer archivo renombrar columnas y filtrar por Elaboración de aceites y grasas de origen vegetal y animal
    archivo = [arch.name for arch in scandir(descarga) if arch.is_file()]
    dfs = pd.read_excel(os.path.join(f'{descarga}',f'{archivo[0]}'), sheet_name='9. Enlace legal hasta 2014',skiprows =9,usecols = 'B:H').drop([0,1], axis=0).rename(columns=\
                       {'AÑO':'Fecha','MES':'Meses','Clases industriales':'clases_industriales', 'Producción \nNominal':'Produccion_Nominal','Producción \nreal':'Produccion_Real',\
                       'Ventas\nNominales':'Ventas_Nominales','Ventas\nreales':'Ventas_Reales'})
    df = dfs[dfs.clases_industriales.isin(['Elaboración de aceites y grasas de origen vegetal y animal'])]
    #subir archivo 
    sp.subir_df(df,f'{zona}.IndiceProduccionEMMET_plm', modo="overwrite")
    print('\n Finaliza Ejecucion Funcion anexo_total_nal_6 \n')

def demanda_energia_7():
    print('Inicia Ejecucion Funcion demanda_energia_7 \n')
    #ruta para descargar achivo
    descarga = os.path.join(os.path.dirname(os.path.abspath(__file__)), "07_Demanda de Energia")
    #llamado a la funcion driver_google con la nueva ruta de descargas 
    driver = driver_google(descarga)
    driver.maximize_window()
    #ingresar a la pagina de sinergox, esta ruta organiza los elementos descendentemente
    driver.get('https://sinergox.xm.com.co/dmnd/Paginas/Historicos/Historicos.aspx#InplviewHash8dba930a-8ae6-48f6-8d6a-f206c89dc868=SortField%3DModified-SortDir%3DDesc')
    #driver.set_page_load_timeout(25)
    time.sleep(10)
    #Crear la lista que contiene los elementos en la clase con los nombres de los archivos 
    value_lista = []
    value_lista = driver.find_elements_by_class_name("itx")
    #creacion de la lista con los nombres de los archivos, y  ciclo para extraerlos de la lista anterior
    lista = []
    for elem in value_lista:
        p = elem.find_elements_by_tag_name("a")
        #print(p[0].text)
        lista.append(p[0].text)
    #hallar los indices, de los elmentos de lista[], que cumplen con el patron (expresion regular), del nombre del archivo a descargar
    lista_index = []
    for x in range(0,len(lista)):
        valor = lista[x]
        patron = re.match('Demanda_Comercial_No_Regulada_Por_CIIU_TRI[_0-9*]*',f'{valor}')
        print(patron)
        if patron == None:
            pass
        else:
            lista_index.append(lista.index(valor))
    #La pagina se abrio en orden descendente, se halla el menor valor de los indices extraidos en el paso anterior, y ese es el archivo a descargar
    menor = lista_index[0]
    posicion = 0
    for x in range(0, len(lista_index)):
        if lista_index[x] < menor:
            menor = lista_index[x]
    
    archivo_descarga = lista[menor]
    #abrir pagina con el archivo a descargar
    driver.get(f'https://sinergox.xm.com.co/dmnd/Histricos/{archivo_descarga}.xlsx')
    time.sleep(30)
    #Cerrar pagina
    driver.close()
    #LEER ARCHIVOS
    #archivo con historia
    df_historico = pd.read_excel(os.path.join(f'{descarga}','Demanda de Energía Sector Agro Industria_Manufactura.xlsx'), sheet_name='DATA')
    #arhivo descargado y filtrarlo por aceites
    dfs = pd.read_excel(os.path.join(f'{descarga}',f'{archivo_descarga}.xlsx'), skiprows =3).rename(columns={'Sub Actividad':'Sub_Actividad','Demanda Comercial kWh':'DeComkWh'})
    df = dfs[dfs.Sub_Actividad.isin(['CULTIVO DE PALMA PARA ACEITE (PALMA AFRICANA) Y OTROS FRUTOS OLEAGINOSOS',\
                                 'ELABORACIÓN DE ACEITES Y GRASAS DE ORIGEN VEGETAL Y ANIMAL'])]
    #concatenar bases y quitar duplicados (en caso tal que se ejecute y este el mismo archivo)
    union_df = pd.concat([df_historico,df])
    union_df_sin_duplicados = union_df.drop_duplicates(['Fecha','CIIU','Sub_Actividad'],keep='last')
    #elimiar archivos, para dejar el nuevo con la historia
    archivos = glob.glob(os.path.join(f'{descarga}','*.xlsx'))
    for f in archivos:
        os.remove(f)

    #guardar nuevo archivo historico    
    union_df_sin_duplicados.to_excel(os.path.join(f'{descarga}','Demanda de Energía Sector Agro Industria_Manufactura.xlsx'),engine='openpyxl', sheet_name='DATA', index=False)
    sp.subir_df(union_df_sin_duplicados,f'{zona}.DeMandaEnergia_plm', modo="overwrite")
    print('\n Finaliza Ejecucion Funcion demanda_energia_7 \n')

def agroindustria_encifras_8_10_15_21_22():
    print('Inicia Ejecucion Funcion agroindustria_encifras_8_10_15_21_22 \n')
    #listar archivos que se encuentran en la ruta encifras
    lista = [arch.name for arch in scandir(ruta_en_cifras) if arch.is_file()]
    lista_index = []
    for x in range(0,len(lista)):
        valor = lista[x]
        patron = re.match("^Agroindustria e Insumos en Cifras[\w*\s*\D*]*",f"{valor}",flags=re.I)
        if patron == None:
            pass
        else:
            lista_index.append(lista.index(valor))
    #grabar en la variable archivo el que cumple con el patron
    archivo = lista[lista_index[0]]
    ruta_archivo = ruta_en_cifras +"/" + archivo
    #leer archivos
    #punto 8
    df_8 = pd.read_excel(f'{ruta_archivo}', sheet_name='DATOS PALMA Y ACEITES', usecols='AH,AK', skiprows =2).rename(columns=\
                       {'Fecha.4':'fecha','Precio Biodiesel':'precio_biodiesel'}).drop([0],axis=0) #falta el precio precio_diesel_mezcla
    #subir a la lz punto 8
    sp.subir_df(df_8,f'{zona}.BioCombustibles_sec_plm', modo="overwrite")
    #punto 9 y 10
    dfs_9 = pd.read_excel(f'{ruta_archivo}', sheet_name='DATOS PALMA Y ACEITES', usecols='AA:AC', skiprows =2).rename(columns=\
                         {'Fecha.2':'fecha','Precio Nacional del Aceite Crudo de Palma':'precio_nacional_del_aceite_crudo_de_palma','Precio Nacional de Fruto de Palma Africana':'Precio_Nacional_de_Fruto_de_Palma_Africana'}).drop([0],axis=0)
    #df_9 = dfs_9.assign(cambio = dfs_9.precio.pct_change(periods=1))
    #subir a la lz punto 
    sp.subir_df(dfs_9,f'{zona}.PrecioFrutoyAceiteDePalma_plm', modo="overwrite")
    '''
    #punto 10
    df_10 = pd.read_excel(f'{ruta_archivo}', sheet_name='DATOS PALMA Y ACEITES', usecols='AA,AC', skiprows =2).rename(columns=\
                        {'Fecha.2':'fecha','Precio Nacional de Fruto de Palma Africana':'precio_nacional_del_aceite_crudo_de_palma'}).drop([0],axis=0)
    #subir a la lz punto 10
    sp.subir_df(df_10,f'{zona}.PrecioFrutoyAceiteDePalma_plm', modo="overwrite")
    '''
    #punto 15
    df_15 = pd.read_excel(f'{ruta_archivo}', sheet_name='Proyecciones y precios ', usecols=F'{rango_proyecciones}', skiprows =70,nrows=10).rename(columns={'Producto ':'productos_internacionales'}) #para renombrar la columna esta tiene un espacio, si lo quitan en la base hay que quitarlo en el codigo
    columnas = df_15.columns.values[2:]
    df_proyecciones = []
    for x in range(0,len(columnas)):
            a = columnas[x]
            df_2 =  df_15[['productos_internacionales', 'Unidades', a]]
            df_2 = df_2.rename(columns={a:'precio_proyectado_producto'})
            df_2['anio'] = a
            df_proyecciones.append(df_2)

    df_final = pd.concat(df_proyecciones)
    #subir a la lz punto 15
    sp.subir_df(df_final,f'{zona}.ProyeccionPreciosInternacional_plm', modo="overwrite")
    #punto 21 y 22
    df_21 = pd.read_excel(f'{ruta_archivo}', sheet_name='IPC', usecols='B,W,Y', skiprows =4).rename(columns=\
                        {'Aceites comestibls':'aceites_comestibls','Margarinas y grasas (animales y vegetales)':'margarinas_y_grasas__animales_y_vegetales_'})
    #subir a la lz punto 21 y 22
    sp.subir_df(df_21,f'{zona}.IPC_plm', modo="overwrite")
    print('\n Finaliza Ejecucion Funcion agroindustria_encifras_8_10_15_21_22 \n')

def precio_int_aceite_palma_12():
    print('Inicia Ejecucion Funcion precio_int_aceite_palma_12 \n')
    #ruta para descargar achivo
    descarga = os.path.join(os.path.dirname(os.path.abspath(__file__)), "12_PRECIO Internacional ACEITE DE PALMA")
    #listar los archivos de extension xls para borrarlos
    archivos_delete = glob.glob(os.path.join(f'{descarga}','*.xls'))
    for f in archivos_delete:
        os.remove(f)
    #llamado a la funcion driver_google con la nueva ruta de descargas 
    driver = driver_google(descarga)
    driver.maximize_window()
    driver.get('https://www.indexmundi.com/members/login.aspx?ReturnUrl=%2fcommodities%2f%3fcommodity%3dpalm-oil%26months%3d120')
    time.sleep(3)
    driver.find_element_by_xpath('//*[@id="Login1_UserName"]').send_keys(f'{usuario_inexmundi}')
    driver.find_element_by_xpath('//*[@id="Login1_Password"]').send_keys(f'{pass_inexmundi}')
    driver.find_element_by_xpath('//*[@id="Login1_LoginButton"]').click()
    time.sleep(3)
    driver.get('https://www.indexmundi.com/commodities/?commodity=palm-oil&months=120&format=excel')
    time.sleep(10)
    driver.close()
    #leer archivo
    archivo = [arch.name for arch in scandir(descarga) if arch.is_file()]
    ruta_archivo_carga = os.path.join(f'{descarga}',archivo[0])
    dfs = pd.read_html(ruta_archivo_carga, index_col= 0, header=0, encoding='utf-8')
    df = dfs[0].reset_index().rename(columns={'Price':'precio','Change':'cambio'})
    df['fecha'] = pd.to_datetime(df['Month'])
    #subir a la lz
    sp.subir_df(df,f'{zona}.Precio_AceiteDePalmaCruda_plm', modo="overwrite")
    print('\n Finaliza Ejecucion Funcion precio_int_aceite_palma_12 \n')

def precio_int_aceite_soya_13():
    print('Inicia Ejecucion Funcion precio_int_aceite_soya_13 \n')
    #ruta para descargar achivo
    descarga = os.path.join(os.path.dirname(os.path.abspath(__file__)), "13_PRECIO Internacional ACEITE DE SOYA")
    #listar los archivos de extension xls para borrarlos
    archivos_delete = glob.glob(os.path.join(f'{descarga}','*.xls'))
    for f in archivos_delete:
        os.remove(f)
    #llamado a la funcion driver_google con la nueva ruta de descargas 
    driver = driver_google(descarga)
    driver.maximize_window()
    driver.get('https://www.indexmundi.com/members/login.aspx?ReturnUrl=%2fcommodities%2f%3fcommodity%3dpalm-oil%26months%3d120')
    time.sleep(3)
    driver.find_element_by_xpath('//*[@id="Login1_UserName"]').send_keys(f'{usuario_inexmundi}')
    driver.find_element_by_xpath('//*[@id="Login1_Password"]').send_keys(f'{pass_inexmundi}')
    driver.find_element_by_xpath('//*[@id="Login1_LoginButton"]').click()
    time.sleep(3)
    driver.get('https://www.indexmundi.com/commodities/?commodity=soybean-oil&months=120&format=excel')
    time.sleep(10)
    driver.close()
    #leer archivo
    archivo = [arch.name for arch in scandir(descarga) if arch.is_file()]
    ruta_archivo_carga = os.path.join(f'{descarga}',archivo[0])
    dfs = pd.read_html(ruta_archivo_carga, index_col= 0, header=0, encoding='utf-8')
    df = dfs[0].reset_index().rename(columns={'Price':'precio_aceite_soya','Change':'cambio'})
    df['fecha'] = pd.to_datetime(df['Month'])
    #subir a la lz
    sp.subir_df(df,f'{zona}.Precio_AceiteDeSoya_plm', modo="overwrite")
    print('\n Finaliza Ejecucion Funcion precio_int_aceite_soya_13 \n')
    
def precio_int_petroleo_14():
    print('Inicia Ejecucion Funcion precio_int_petroleo_14 \n')
    #ruta para descargar achivo
    descarga = os.path.join(os.path.dirname(os.path.abspath(__file__)), "14_PRECIO Internacional PETROLEO")
    #listar los archivos de extension xls para borrarlos
    archivos_delete = glob.glob(os.path.join(f'{descarga}','*.xls'))
    for f in archivos_delete:
        os.remove(f)
    #llamado a la funcion driver_google con la nueva ruta de descargas 
    driver = driver_google(descarga)
    driver.maximize_window()
    driver.get('https://www.indexmundi.com/members/login.aspx?ReturnUrl=%2fcommodities%2f%3fcommodity%3dpalm-oil%26months%3d120')
    time.sleep(3)
    driver.find_element_by_xpath('//*[@id="Login1_UserName"]').send_keys(f'{usuario_inexmundi}')
    driver.find_element_by_xpath('//*[@id="Login1_Password"]').send_keys(f'{pass_inexmundi}')
    driver.find_element_by_xpath('//*[@id="Login1_LoginButton"]').click()
    time.sleep(3)
    driver.get('https://www.indexmundi.com/commodities/?commodity=crude-oil-brent&months=120&format=excel')
    time.sleep(10)
    driver.close()
    #leer archivo
    archivo = [arch.name for arch in scandir(descarga) if arch.is_file()]
    ruta_archivo_carga = os.path.join(f'{descarga}',archivo[0])
    dfs = pd.read_html(ruta_archivo_carga, index_col= 0, header=0, encoding='utf-8')
    df = dfs[0].reset_index().rename(columns={'Price':'precio_petroleo','Change':'cambio'})
    df['fecha'] = pd.to_datetime(df['Month'])
    #subir a la lz
    sp.subir_df(df,f'{zona}.Precio_DelPetroleo_plm', modo="overwrite")
    print('\n Finaliza Ejecucion Funcion precio_int_petroleo_14 \n')
    
def ipp_aceites_16_20():
    print('Inicia Ejecucion Funcion ipp_aceites_16_20 \n')
    #ruta para descargar achivo
    descarga = os.path.join(os.path.dirname(os.path.abspath(__file__)), "16_20 IPP")
    #listar los archivos de extension xls para borrarlos
    archivos_delete = glob.glob(os.path.join(f'{descarga}','*.xlsx'))
    for f in archivos_delete:
        os.remove(f)
    #llamado a la funcion driver_google con la nueva ruta de descargas 
    driver = driver_google(descarga)
    driver.maximize_window()
    driver.get('https://www.dane.gov.co/index.php/estadisticas-por-tema/precios-y-costos/indice-de-precios-del-productor-ipp')
    time.sleep(5)
    try:
        driver.find_element_by_xpath('//*[@id="t3-content"]/div/article/section/table[1]/tbody/tr/td/table[1]/tbody/tr/td[2]/div/a/strong').click()
    except:
        driver.find_element_by_xpath('//*[@id="t3-content"]/div/article/section/table[1]/tbody/tr/td/table[1]/tbody/tr/td[2]/div/a/strong').click()
        time.sleep(5)
    driver.close()
    #leer archivo
    archivo = [arch.name for arch in scandir(descarga) if arch.is_file()]
    ruta_archivo = os.path.join(f'{descarga}',archivo[0])
    #punto 16,18,19
    dfs_16 = pd.read_excel(ruta_archivo, sheet_name='1.1',skiprows=5).drop(['NIVEL', 'CODIGO'], axis=1)
    dfs_16 = dfs_16[dfs_16.DESCRIPTIVA.isin(['Aceite de palma, crudo', 'Aceites vegetales, refinados', 'Margarina y preparaciones similares'])]
    columnas_16 = dfs_16.columns.values[1:]
    df_16 = pd.pivot_table(dfs_16, columns='DESCRIPTIVA', values=columnas_16)
    df_16 = df_16.rename_axis('DESCRIPTIVA').reset_index().rename(columns=\
                            {'Aceite de palma, crudo':'aceite_de_palma_crudo','Aceites vegetales, refinados':'aceites_vegetales_refinados','Margarina y preparaciones similares':'margarina_y_preparaciones_similares'})
    df_final_16 = df_16[~df_16['DESCRIPTIVA'].str.contains('definitivos')]
    df_final_16['fecha'] = df_final_16['DESCRIPTIVA'].str[:6]
    #subir a la lz punto 16,18,19
    #subir a la lz
    sp.subir_df(df_final_16,f'{zona}.ipp_nacional_plm', modo="overwrite")

    #punto 17,20
    dfs_17 = pd.read_excel(ruta_archivo, sheet_name='4.1',skiprows=5).drop(['NIVEL', 'CODIGO'], axis=1)
    dfs_17 = dfs_17[dfs_17.DESCRIPTIVA.isin(['Aceites vegetales, refinados', 'Margarina y preparaciones similares'])]
    columnas_17 = dfs_17.columns.values[1:]
    dfs_17 = pd.pivot_table(dfs_17, columns='DESCRIPTIVA', values=columnas_17)
    dfs_17 = dfs_17.rename_axis('DESCRIPTIVA').reset_index().rename(columns={'Aceites vegetales, refinados':'aceites_vegetales_refinados','Margarina y preparaciones similares':'margarina_y_preparaciones_similares'})
    df_final_17 = dfs_17[~dfs_17['DESCRIPTIVA'].str.contains('definitivos')]
    df_final_17['fecha'] = df_final_17['DESCRIPTIVA'].str[:6]
    #subir a la lz punto 17,20
    #subir a la lz
    sp.subir_df(df_final_17,f'{zona}.ipp_internacional_plm', modo="overwrite")
    print('\n Finaliza Ejecucion Funcion ipp_aceites_16_20 \n')

def ventas_por_industria_23():
    print('Inicia Ejecucion Funcion ventas_por_industria_23 \n')
    descarga = os.path.join(os.path.dirname(os.path.abspath(__file__)), "23_VentasMensuales_AceitePalma_PorIndustria")
    ruta_archivo = os.path.join(f'{descarga}', 'BaseHistoricaVentasAceite_Industria.xlsx')
    df = pd.read_excel(f'{ruta_archivo}').rename(columns={'Empresas Tradicionales':'Empresas_Tradicionales','Industriales alimentos concentrados':'Industriales_alimentos_concentrados',\
                    'Industriales jaboneros':'Industriales_jaboneros','Otros Industriales':'Otros_Industriales','Industriales de aceites y grasas':'Industriales_de_aceites_y_grasas'})
    sp.subir_df(df,f'{zona}.VentasAceite_Industriasec_plm', modo="overwrite")
    print('\n Finaliza Ejecucion Funcion ventas_por_industria_23 \n')


if __name__ == "__main__":
    t_inicio = time.time() #inicio de programa
    palma_aceita_1()
    frutoprocesado_2()
    produccion_fruto_3()
    pdccionaceite_zonas_4()
    vtaaceite_zona_5()
    anexo_total_nal_6()
    demanda_energia_7()
    agroindustria_encifras_8_10_15_21_22()
    precio_int_aceite_palma_12()
    precio_int_aceite_soya_13()
    precio_int_petroleo_14()
    ipp_aceites_16_20()
    ventas_por_industria_23()
    ejecutar_sql('ETL_PLM_v2.sql',params)
    t_fin = time.time()
    duracion = ((t_fin - t_inicio)/60)
    print(f'La ficha Palma ha finalizado exitosamente con una duración de {duracion} minutos')
    
