# -*- coding: utf-8 -*-
"""
Created on Mon Aug 24 13:57:57 2020

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
    cursor = cn.cursor()
    cursor.execute(query_sql)
    
def query_delete_querys(tablas_borrado):
    for i in range(0,len(tablas_borrado)):
        query="""drop table if exists """+tablas_borrado[i]+""" purge"""
        EjecuteSql(query)
        print(query)
    

tablas_borrado=['proceso.master_customer','proceso.master']


def Borrado_Tablas(tablas_borrado):
    query_delete_querys(tablas_borrado)
    
    print(" ╔═════════════════════════════════════════════╗")
    print(" ║   BORRANDO TABLAS EN CASO DE QUE EXISTAN    ║")
    print(" ╩═════════════════════════════════════════════╝")
    

def Base_Festivos():
    dfCalendario = pd.read_excel('Y:/29.Tablero_IFRS9_COLGAP_CAH/Insumos/Calendario.xlsx',sheet_name='calendario')
    dfFestivos = pd.read_excel('Y:/29.Tablero_IFRS9_COLGAP_CAH/Insumos/Calendario.xlsx',sheet_name='Festivos')
    dfConsolidacion = pd.merge( dfCalendario,dfFestivos,  how='left', left_on=['DIA','MES'], right_on = ['DIA','MES'])
    dfConsolidacion=dfConsolidacion.fillna('NO_FESTIVO')
    cols = dfConsolidacion.select_dtypes(include=[np.object]).columns
    dfConsolidacion[cols] = dfConsolidacion[cols].apply(lambda x: x.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8'))
    
    ### SUBIRLO A LA LZ ###
    from sparky_bc import Sparky
    sp=Sparky('cesherna', 'IMPALA_PROD')
    sp.subir_df(dfConsolidacion,"proceso.fecha_base_maestra")
    
    
    def Query_Base_fechas():        
        Query_1_Generar_Base_fechas=""" create table proceso_riesgos.base_dias stored as parquet as 
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
        ),
        ultimo_dia_mes as (
        
        select 
        fecha
        ,nombre_dia
        ,YEAR(date_add(add_months(trunc(fecha,'MM'),1),-1))*10000+MONTH(date_add(add_months(trunc(fecha,'MM'),1),-1))*100+DAY(date_add(add_months(trunc(fecha,'MM'),1),-1)) as ultimo_dia_mes
        from proceso.fecha_base_maestra
        order by fecha desc
        
        ),pre_base as (
        
        select 
        t1.fecha,
        t1.nombre_mes,
        t1.nombre_dia,
        t1.dia,
        t1.mes,
        t1.ano,
        t1.nombre as estado_dia,
        t2.dia_habil_no_dom_fest,
        t3.dia_habil_no_dom_sab_fest,
        t4.ultimo_dia_mes
        
        from proceso.fecha_base_maestra t1
        left join dias_habiles_sin_festivos_sin_domingo t2 on t1.fecha=t2.fecha
        left join dias_habiles_sin_festivos_sin_domingo_sin_sabados  t3 on t1.fecha=t3.fecha
        left join ultimo_dia_mes  t4 on t1.fecha=t4.fecha
        
        ), pre_base_final AS ( 
        select 
        fecha,
        nombre_mes,
        nombre_dia,
        dia,
        mes,
        ano,
        estado_dia,
        dia_habil_no_dom_fest,
        dia_habil_no_dom_sab_fest,
        DATE_ADD(CAST(CONCAT(SUBSTRING(cast(ultimo_dia_mes as string),1,4),"-",SUBSTRING(cast(ultimo_dia_mes as string),5,2),"-",SUBSTRING(cast(ultimo_dia_mes as string),7,2)) AS TIMESTAMP),-0) fecha_ultimo_dia 
    
        
        from pre_base
        
        )
        
        
        select 
        fecha,
        nombre_mes,
        nombre_dia,
        dia,
        mes,
        ano,
        estado_dia,
        dia_habil_no_dom_fest,
        dia_habil_no_dom_sab_fest,
        (CASE WHEN year(fecha_ultimo_dia) = year(fecha) and month(fecha_ultimo_dia) = month(fecha) and day(fecha_ultimo_dia) = day(fecha) THEN 1 
        ELSE 0 END) AS fecha_ultimo_dia
        from pre_base_final
"""
        EjecuteSql(Query_1_Generar_Base_fechas)
        
        print(" ╔═════════════════════════════════════════════╗")
        print(" ║ CREACION EXITOSA PARA BASE FECHAS           ║")
        print(" ╩═════════════════════════════════════════════╝") 
    
    
    Query_Base_fechas()

  

def Query_Base_GIS():        
    Query_1_Generar_Base_GIS=""" 
        
        
CREATE TABLE proceso.base_GIS stored AS parquet AS

WITH base AS
  ( SELECT *,
           row_number() over (partition BY tipo_doc, num_doc
                              ORDER BY ingestion_year DESC , ingestion_month DESC , ingestion_day DESC) AS rn
   FROM resultados_cap_analit_y_gob_de_inf.geo__dir_clientes)
SELECT tipo_doc,
       num_doc,
       ciudad,
       departamento,
       (CASE 
        WHEN DEPARTAMENTO= 'CUNDINAMARCA' THEN 'DEPARTAMENTO DE CUNDINAMARCA, COLOMBIA'
        WHEN DEPARTAMENTO= 'CESAR' THEN 'DEPARTAMENTO DEL CESAR, COLOMBIA'
        WHEN DEPARTAMENTO= 'NARI#O' THEN 'DEPARTAMENTO DE NARIÑO, COLOMBIA'
        WHEN DEPARTAMENTO= 'BOLIVAR' THEN 'DEPARTAMENTO DEL BOLIVAR, COLOMBIA'
        WHEN DEPARTAMENTO= 'ATLANTICO' THEN 'DEPARTAMENTO DEL ATLANTICO, COLOMBIA'
        WHEN DEPARTAMENTO= 'PUTUMAYO' THEN 'DEPARTAMENTO DEL PUTUMAYO, COLOMBIA'
        WHEN DEPARTAMENTO= 'CALDAS' THEN 'DEPARTAMENTO DEL CALDAS, COLOMBIA'
        WHEN DEPARTAMENTO= 'LA GUAJIRA' THEN 'DEPARTAMENTO DEL GUAJIRA, COLOMBIA'
        WHEN DEPARTAMENTO= 'VALLE'  THEN 'DEPARTAMENTO DEL VALLE DEL CAUCA, COLOMBIA'
        WHEN DEPARTAMENTO= 'ARAUCA' THEN 'DEPARTAMENTO DEL ARAUCA, COLOMBIA'
        WHEN DEPARTAMENTO= 'SANTANDER' THEN 'DEPARTAMENTO DEL SANTANDER, COLOMBIA'
        WHEN DEPARTAMENTO= 'QUINDIO' THEN 'DEPARTAMENTO DEL QUINDIO, COLOMBIA'
        WHEN DEPARTAMENTO= 'SUCRE' THEN 'DEPARTAMENTO DEL SUCRE, COLOMBIA'
        WHEN DEPARTAMENTO= 'NARIÑO' THEN 'DEPARTAMENTO DE NARIÑO, COLOMBIA'
        WHEN DEPARTAMENTO= 'HUILA' THEN 'DEPARTAMENTO DEL HUILA, COLOMBIA'
        WHEN DEPARTAMENTO= 'CHOCO' THEN 'DEPARTAMENTO DEL CHOCO, COLOMBIA'
        WHEN DEPARTAMENTO= 'GUAVIARE' THEN 'DEPARTAMENTO DE GUAVIARE, COLOMBIA'
        WHEN DEPARTAMENTO= 'VICHADA' THEN 'DEPARTAMENTO DEL VICHADA, COLOMBIA'
        WHEN DEPARTAMENTO= 'META' THEN 'DEPARTAMENTO DEL META, COLOMBIA'
        WHEN DEPARTAMENTO= 'RISARALDA' THEN 'DEPARTAMENTO DEL RISARALDA, COLOMBIA'
        WHEN DEPARTAMENTO= 'CASANARE' THEN 'DEPARTAMENTO DE CASANARE, COLOMBIA'
        WHEN DEPARTAMENTO= 'CORDOBA' THEN 'DEPARTAMENTO DEL CORDOBA, COLOMBIA'
        WHEN DEPARTAMENTO= 'BOYACA' THEN 'DEPARTAMENTO DEL BOYACA, COLOMBIA'
        WHEN DEPARTAMENTO= 'NORTE DE SANTANDER' THEN 'DEPARTAMENTO DEL NORTE DE SANTANDER, COLOMBIA'
        WHEN DEPARTAMENTO= 'TOLIMA' THEN 'DEPARTAMENTO DEL TOLIMA, COLOMBIA'
        WHEN DEPARTAMENTO= 'CAUCA' THEN 'DEPARTAMENTO DEL CAUCA, COLOMBIA'
        WHEN DEPARTAMENTO= 'AMAZONAS' THEN 'DEPARTAMENTO DEL AMAZONAS, COLOMBIA'
        WHEN DEPARTAMENTO= 'MAGDALENA' THEN 'DEPARTAMENTO DEL MAGDALENA, COLOMBIA'
        WHEN DEPARTAMENTO= 'CAQUETA' THEN 'DEPARTAMENTO DEL CAQUETA, COLOMBIA'
        WHEN DEPARTAMENTO= 'ANTIOQUIA' THEN 'DEPARTAMENTO DEL ANTIOQUIA, COLOMBIA'
        ELSE DEPARTAMENTO END)  AS LOCATIONS

FROM base
WHERE rn = 1    
"""
    EjecuteSql(Query_1_Generar_Base_GIS)
        
    print(" ╔═════════════════════════════════════════════╗")
    print(" ║ CREACION EXITOSA PARA BASE GIS              ║")
    print(" ╩═════════════════════════════════════════════╝") 
    
    



def Master_Customer(year,month):
    Query_Master_Customer= """
    Create table proceso.master_customer stored as parquet as 
    select * from (
            select ROW_NUMBER() OVER (PARTITION BY  num_doc  ORDER BY f_ult_mantenimiento DESC) AS  rn
            ,cast(num_doc as bigint) as num_doc
            ,cod_gte
            ,cod_ciiu
            ,REGION_GTE
            ,ZONA_GTE
            ,tipo_cli
            ,sector
            ,f_ult_mantenimiento
            ,genero_cli
            
    from resultados_vspc_clientes.master_customer_data
    where  year="""+year+""" and month="""+str(int(month)+1)+""" and ingestion_day=(select dia from proceso_riesgos.base_dias where ano="""+year+""" and mes="""+str(int(month)+1)+""" and dia_habil_no_dom_fest=1 limit 1 ) ) Consulta1
    where rn=1 """ 
    

    
    EjecuteSql(Query_Master_Customer)
    print(" ╔═════════════════════════════════════════════╗")
    print(" ║ CREACION EXITOSA PARA BASE MASTER           ║")
    print(" ╩═════════════════════════════════════════════╝") 

def Ifrs9_Colgap_Base(ingestion_year,ingestion_month,ingestion_day):
    Query_Variables_Base=""" 
     CREATE TABLE   proceso_riesgos.portafolio_ifrs9_202109 stored as parquet as 
    With 
    
  
    master_credit_risk as (
    
    SELECT 
           cast(num_doc as bigint) as num_doc
        ,cast(SUBSTRING(oblig, 3, 17) as bigint) as obl
        ,cod_clase as modalidad
        ,reestructurado
        ,producto_agrupado as producto 
        ,territorio
        ,causa_stage
        ,bucket
        ,CASE WHEN cod_prod in ('24','25') then 0 else sld_cap_tot END AS sld_cap_tot
        ,CASE WHEN cod_prod in ('24','25') then 0 else exposicion_tot_fa END AS exposicion_tot_fa
        ,CASE WHEN cod_prod in ('24','25') then 0 else cv END AS cv30
        ,CASE WHEN cod_prod in ('24','25') then 0 else c90 END AS cv90
        ,CASE WHEN cod_prod in ('24','25') then 0 else exp_tot_venc_fa END AS exp_tot_venc_fa
        ,CASE WHEN cod_prod in ('24','25') then 0 else exp_tot_venc90_fa END AS exp_tot_venc90_fa
        ,CASE WHEN cod_prod in ('24','25') then 0 else sld_ca END AS saldo_capital
        ,CASE WHEN cod_prod in ('24','25') then 0 else cc_mas END AS cc_mas
        ,BANCA as banca
        ,vic_ccial
        ,cod_subsegm
        ,(CASE WHEN desc_segm ='PYMES'THEN 'Segmento PYMES'
             WHEN desc_segm ='GOBIERNO DE RED' THEN 'Segmento Gob. de Red'
             WHEN desc_segm ='NEGOCIOS & INDEPEND' THEN 'Segmento N&I'
             WHEN  vic_ccial='Personas' and  producto_agrupado in ('CARTERA MICROCREDITO','CARTERA ORDINARIA', 'SOBREGIRO', 'LEASING' , 'ANTICIPOS' ,'TESORERIA','OTROS HIPOTECARIO') THEN 'C. Comercial y otros***'
             WHEN  vic_ccial='Personas' and  producto_agrupado in ('LEASING HABITACIONAL','HIPOTECARIO VIVIENDA') THEN 'Solución Inmobiliaria**'
             WHEN  vic_ccial='Personas' and  producto_agrupado in ('COTIDIANIDAD','MOVILIDAD','LIBRANZA','LIBRE INVERSION','ROTATIVOS','TARJETA DE CREDITO') THEN 'Consumo'
             else vic_ccial end ) as  vic_ccial_v
             ,(CASE  WHEN  vic_ccial='Personas' and  producto_agrupado in ('CARTERA MICROCREDITO','CARTERA ORDINARIA', 'SOBREGIRO', 'LEASING' , 'ANTICIPOS' ,'TESORERIA','OTROS HIPOTECARIO','LEASING HABITACIONAL','HIPOTECARIO VIVIENDA','COTIDIANIDAD','MOVILIDAD','LIBRANZA','LIBRE INVERSION','ROTATIVOS','TARJETA DE CREDITO') THEN 'Total Personas'
             WHEN desc_segm ='PYMES' THEN 'Total Banca Pyme'
             WHEN desc_segm ='GOBIERNO DE RED' THEN 'Total Banca Pyme'
             WHEN desc_segm ='NEGOCIOS & INDEPEND' THEN 'Total Banca Pyme'
            
             WHEN  vic_ccial='Personas' and  producto_agrupado in ('CARTERA MICROCREDITO','CARTERA ORDINARIA', 'SOBREGIRO', 'LEASING' , 'ANTICIPOS' ,'TESORERIA','OTROS HIPOTECARIO','LEASING HABITACIONAL','HIPOTECARIO VIVIENDA','COTIDIANIDAD','MOVILIDAD','LIBRANZA','LIBRE INVERSION','ROTATIVOS','TARJETA DE CREDITO') THEN 'Negocios Personas, Pymes y Empresas'
             WHEN desc_segm ='PYMES' THEN 'Negocios Personas, Pymes y Empresas'
             WHEN desc_segm ='GOBIERNO DE RED' THEN 'Negocios Personas, Pymes y Empresas'
             WHEN desc_segm ='NEGOCIOS & INDEPEND' THEN 'Negocios Personas, Pymes y Empresas' 
        
             WHEN  vic_ccial in ('Comercio, Manufactura, Agro y Bienes de Consumo','Infraestructura y Recursos Naturales', 'Grandes Corporativos','Inmobiliario y Constructor', 'Gobierno, Servicios Financieros, Salud y Educación','Otros Territorios') THEN 'Negocios Corporativos'
             WHEN vic_ccial ='Empresarial' THEN 'Total Empresarial'
         
             else 'Corresponsales y Otros' END) AS seg_banca
        ,cod_apli as aplicativo
        ,DIAS_MORA as dia_mora
        ,desc_segm as segmento
        --,Zona as zona
        ,calif_ext as calificacion_externa
        ,cast(calif_int_fa as string) as calificacion_interna
        ,Subsector as subsector
        ,sld_tot_provision  AS provision_total
        ,f_desemb as fecha_desemb
        ,cod_plan as plan
        ,region_of as region
        ,calif_ext
        ,pdi
        ,cast(cod_moneda as double) as cod_moneda 
        ,cast(tipo_identificacion_cli as string) as tipo_identificacion
        ,0 as castigos
        ,'NO' as marca_castigo
        ,CASE WHEN cod_prod in ('24','25') then 0  else cast_cap_tot END AS cast_cap_tot
        ,ajuste_interes_b3  AS ajuste_interes_b3
        ,ajuste_prov_int_b3  AS ajuste_prov_int_b3
        ,(gasto + coalesce(overlays,0))  AS gasto
        ,CASE WHEN cod_prod in ('24','25') then 0  else overlays END AS overlays
        ,gasto  AS gasto_puro
        ,CASE WHEN cod_prod in ('24','25') then 0  else exposicion_tot_fa END AS exposicion_total
        ,CASE WHEN cod_prod in ('24','25') then 0  else vlr_desemb_oblig END AS vlr_desemb_oblig
        ,CASE WHEN (YEAR(f_desemb)*100+month(f_desemb))=(YEAR(f_corte)*100+month(f_corte)) THEN vlr_desemb_oblig ELSE 0 END AS desembolso
        ,(ingestion_year*10000+ingestion_month*100+1) AS fcorte
        ,ingestion_year
        ,ingestion_month
        ,ingestion_day
    from proceso_riesgos.master_credit_risk
    where ingestion_year="""+ingestion_year+""" and ingestion_month="""+ingestion_month+"""  and libro=1
    ),
    
    Castigos as (
    select
   cast(num_doc as bigint) as num_doc
        ,cast(SUBSTRING(oblig, 3, 17) as bigint) as obl
        ,cod_clase as modalidad
        ,reestructurado
        ,producto_agrupado as producto
        ,territorio
        ,causa_stage
        ,bucket
        ,0 as sld_cap_tot
        ,0 as  exposicion_tot_fa
        ,0 as cv30
        ,0 as cv90
        ,0 as exp_tot_venc_fa
        ,0 as exp_tot_venc90_fa
        ,0 as saldo_capital
        ,0 as cc_mas
        ,BANCA as banca 
        ,vic_ccial
        ,cod_subsegm
        ,(CASE WHEN desc_segm ='PYMES'THEN 'Segmento PYMES'
             WHEN desc_segm ='GOBIERNO DE RED' THEN 'Segmento Gob. de Red'
             WHEN desc_segm ='NEGOCIOS & INDEPEND' THEN 'Segmento N&I'
             WHEN  vic_ccial='Personas' and  producto_agrupado in ('CARTERA MICROCREDITO','CARTERA ORDINARIA', 'SOBREGIRO', 'LEASING' , 'ANTICIPOS' ,'TESORERIA','OTROS HIPOTECARIO') THEN 'C. Comercial y otros***'
             WHEN  vic_ccial='Personas' and  producto_agrupado in ('LEASING HABITACIONAL','HIPOTECARIO VIVIENDA') THEN 'Solución Inmobiliaria**'
             WHEN  vic_ccial='Personas' and  producto_agrupado in ('COTIDIANIDAD','MOVILIDAD','LIBRANZA','LIBRE INVERSION','ROTATIVOS','TARJETA DE CREDITO') THEN 'Consumo'
             else vic_ccial end ) as  vic_ccial_v
             ,(CASE 
             WHEN  vic_ccial='Personas' and  producto_agrupado in ('CARTERA MICROCREDITO','CARTERA ORDINARIA', 'SOBREGIRO', 'LEASING' , 'ANTICIPOS' ,'TESORERIA','OTROS HIPOTECARIO','LEASING HABITACIONAL','HIPOTECARIO VIVIENDA','COTIDIANIDAD','MOVILIDAD','LIBRANZA','LIBRE INVERSION','ROTATIVOS','TARJETA DE CREDITO') THEN 'Total Personas'
             WHEN desc_segm ='PYMES' THEN 'Total Banca Pyme'
             WHEN desc_segm ='GOBIERNO DE RED' THEN 'Total Banca Pyme'
             WHEN desc_segm ='NEGOCIOS & INDEPEND' THEN 'Total Banca Pyme'
            
             WHEN  vic_ccial='Personas' and  producto_agrupado in ('CARTERA MICROCREDITO','CARTERA ORDINARIA', 'SOBREGIRO', 'LEASING' , 'ANTICIPOS' ,'TESORERIA','OTROS HIPOTECARIO','LEASING HABITACIONAL','HIPOTECARIO VIVIENDA','COTIDIANIDAD','MOVILIDAD','LIBRANZA','LIBRE INVERSION','ROTATIVOS','TARJETA DE CREDITO') THEN 'Negocios Personas, Pymes y Empresas'
             WHEN desc_segm ='PYMES' THEN 'Negocios Personas, Pymes y Empresas'
             WHEN desc_segm ='GOBIERNO DE RED' THEN 'Negocios Personas, Pymes y Empresas'
             WHEN desc_segm ='NEGOCIOS & INDEPEND' THEN 'Negocios Personas, Pymes y Empresas' 
        
             WHEN  vic_ccial in ('Comercio, Manufactura, Agro y Bienes de Consumo','Infraestructura y Recursos Naturales', 'Grandes Corporativos','Inmobiliario y Constructor', 'Gobierno, Servicios Financieros, Salud y Educación','Otros Territorios') THEN 'Negocios Corporativos'
             WHEN vic_ccial ='Empresarial' THEN 'Total Empresarial'
         
             else 'Corresponsales y Otros' END) AS seg_banca
        ,cod_apli as aplicativo
        ,DIAS_MORA as dia_mora
        ,desc_segm as segmento
        --,Zona as zona
        ,calif_ext as calificacion_externa
        ,cast(calif_int_fa as string) as calificacion_interna
        ,Subsector as subsector
        ,0 as provision_total
        ,f_desemb as fecha_desemb
        ,cod_plan as plan
        ,region_of as region
        ,calif_ext
        ,0 as pdi
        ,cast(cod_moneda as double) as cod_moneda 
        ,cast(tipo_identificacion_cli as string) as tipo_identificacion
        ,CASE WHEN cod_prod in ('24','25') then 0  else cast_cap_tot END AS Castigos
        ,'SI' as marca_castigo
        ,0 cast_cap_tot
        ,0 ajuste_interes_b3
        ,0 ajuste_prov_int_b3 
        ,0 as gasto
        ,0 as overlays
        ,0  as gasto_puro
        ,0 as exposicion_total
        ,0 as vlr_desemb_oblig
        ,0 as desembolso
        ,(ingestion_year*10000+ingestion_month*100+1) AS fcorte
        ,ingestion_year
        ,ingestion_month
        ,ingestion_day
        
    from proceso_riesgos.master_credit_risk
    where ingestion_year="""+ingestion_year+""" and ingestion_month="""+ingestion_month+"""  and libro=1 and marca_castigo='SI'
    ),
    
    Capa_final as (
      
   
    select * from master_credit_risk
    union all 
    select * from Castigos  ),
    
    Info_Master_customer as  (
    
        select 
         t1.*
        ,t2.cod_gte 
        ,t2.cod_ciiu
        ,t2.ZONA_GTE
        ,t2.REGION_GTE
        ,t2.genero_cli
        from Capa_final t1 
        left join proceso.master_customer t2 on t1.num_doc=t2.num_doc),
        
    riesgo_sectorial as (
    
    SELECT  
         cod_ciiu as ciiu,
         (CASE WHEN nivel_riesgo='alto' THEN 'ALTO'
               WHEN nivel_riesgo='bajo' THEN 'BAJO'
               WHEN nivel_riesgo='medio' THEN 'MEDIO'
               WHEN nivel_riesgo='Alto' THEN 'ALTO'
               WHEN nivel_riesgo='Bajo' THEN 'BAJO'
               WHEN nivel_riesgo='Medio' THEN 'MEDIO'
               WHEN nivel_riesgo is null then 'SIN INFORMACION' END) as riesgo_sectorial

        FROM  resultados_riesgos.master_riesgo_sectorial
        where  ingestion_year="""+ingestion_year+"""  and ingestion_month="""+ingestion_month+"""
    ),
    caracterizacion_base as (
    select  distinct
            num_doc
            ,(CASE
                  when upper(grupo_pad) = 'GRUPO 1' then '3. Riesgo Bajo'
                  when upper(grupo_pad) = 'GRUPO 2' then '2. Riesgo Medio'
                  when upper(grupo_pad) = 'GRUPO 3' then '1. Riesgo Alto'
                  when upper(grupo_pad) = 'GRUPO 4' then '4. Riesgo Desconocido'
                  else '4. Riesgo Desconocido'
              END) AS caracterizacion
           
              from resultados_riesgos.consolidado_caracterizacion t1
              where ingestion_year="""+ingestion_year+""" and ingestion_month="""+ingestion_month+""" and ingestion_day=(select max(ingestion_day) as ingestion_day from resultados_riesgos.consolidado_caracterizacion where ingestion_year="""+ingestion_year+""" and ingestion_month="""+ingestion_month+""" )
    ), Caracterizacion as (
    
    select num_doc, max(caracterizacion) as caracterizacion from caracterizacion_base
     group by 1),
            
            master_customer_subsegmento as (
            
    SELECT DISTINCT  subsegm, cod_subsegm FROM resultados_vspc_clientes.master_customer_data
    where year="""+ingestion_year+"""  and ingestion_month="""+str(int(ingestion_month)+1)+""" and ingestion_day=ingestion_day=(select dia from proceso_riesgos.base_dias where ano="""+ingestion_year+""" and mes="""+str(int(ingestion_month)+1)+""" and dia_habil_no_dom_fest=1 limit 1 )
            ),
            
    Arbol_ciiu as (
    
        Select distinct cod_ciiu
            ,sector
            ,Subsector_trabajado  
            from resultados_riesgos.master_riesgo_sectorial t1
            inner join (select ingestion_year,ingestion_month, ingestion_day  from  resultados_riesgos.master_riesgo_sectorial order by 1 desc, 2 desc, 3 desc limit 1  ) fecha_max
    on fecha_max.ingestion_year=t1.ingestion_year and    fecha_max.ingestion_month=t1.ingestion_month  and fecha_max.ingestion_day=t1.ingestion_day

       
        ), pre_base_colgalp_ifrs9 as (
    
    Select     t1.*
              ,t2.riesgo_sectorial
              ,t3.Caracterizacion
              ,t4.sector
              ,t4.Subsector_trabajado 
              ,t5.subsegm
              ,t6.ocupacion_nomina_homologado as ocupacion_nomina
              ,t7.LOCATIONS
              ,t7.departamento
    
    from Info_Master_customer t1
    
    left join  riesgo_sectorial t2 on  t1.cod_ciiu = t2.ciiu
    left join  Caracterizacion t3  on t1.num_doc=t3.num_doc
    left join  Arbol_ciiu t4 on t1.cod_ciiu=t4.cod_ciiu
    left join  master_customer_subsegmento t5 on cast(t1.cod_subsegm as int)  = cast(t5.cod_subsegm as int)
    left join  resultados_riesgos.ocupacion_nomina t6 ON cast(t1.num_doc AS bigint)=cast(t6.num_doc AS bigint) AND cast(t1.tipo_identificacion AS bigint)=cast(t6.cod_tipo_doc AS bigint) and t1.ingestion_year*100+t1.ingestion_month=t6.ingestion_year*100+t6.ingestion_month
    left join  proceso.base_GIS t7 on cast(t1.num_doc AS bigint)=cast(t7.num_doc AS bigint) and cast(t1.tipo_identificacion AS bigint)=cast(t7.tipo_doc AS bigint) 
    )
    
    
select 

    num_doc
    ,obl
    ,modalidad
    ,reestructurado
    ,producto
    ,territorio
    ,CASE WHEN Causa_stage IS NULL THEN 'SIN CAUSA' ELSE CAUSA_STAGE END AS CAUSA_STAGE
    ,CASE WHEN bucket IS NULL THEN 'SIN BUCKET' ELSE CAST(bucket AS STRING) end as bucket
    ,cv30
    ,cv90   
    ,exp_tot_venc_fa
    ,exp_tot_venc90_fa
    ,saldo_capital
    ,gasto
    ,overlays
    ,gasto_puro
    ,exposicion_total
    ,ajuste_interes_b3
    ,ajuste_prov_int_b3 
    ,vlr_desemb_oblig
    ,cc_mas
    ,desembolso
    ,banca
    ,vic_ccial
    ,vic_ccial_v
    ,seg_banca
    ,aplicativo
    ,dia_mora
    ,segmento
    ,calificacion_externa
    ,calificacion_interna
    ,subsector
    ,provision_total
    ,fecha_desemb
    ,plan
    ,region
    ,cod_moneda
    ,tipo_identificacion
    ,marca_castigo
    ,castigos
    ,cast_cap_tot
    ,fcorte
    ,ingestion_year
    ,ingestion_month
    ,ingestion_day    
    ,DATE_ADD(CAST(CONCAT(SUBSTRING(cast(fcorte as string),1,4),"-",SUBSTRING(cast(fcorte as string),5,2),"-",SUBSTRING(cast(fcorte as string),7,2)) AS TIMESTAMP),-0) AS fecha
    ,cod_gte
    ,cod_ciiu
    ,zona_gte
    ,region_gte
    ,riesgo_sectorial
    ,caracterizacion
    ,sector
    ,subsector_trabajado
    ,genero_cli

,(case
    when modalidad='1' then 'COMERCIAL'
    when modalidad='2' then 'CONSUMO'
    when modalidad='3' then 'HIPOTECARIO'
    when modalidad='4' then 'MICROCREDITO' else 'OTROS' end) as modalidad_v,
(case
    when producto in ('Cartera Microcredito','CARTERA MICROCRÉDITO') then 'CARTERA MICROCREDITO'
    when producto in ('CRÉDITO CONSTRUCTOR', 'Crédito Constructor') then 'CREDITO CONSTRUCTOR'
    when producto in ('LIBRE INVERSIÓN','Cartera Consumo', 'Vehículo','VEHíCULO','CARTERA CONSUMO') then 'LIBRE INVERSION'
    when producto in ('TARJETA DE CRÉDITO','Tarjeta de Crédito') then 'TARJETA DE CREDITO'
    when producto in ('LIBRE INVERSIÓN','LIBRE INVERSION') and plan in ('PS1','PS2','PS4') then 'NEQUI'  
    when producto='Anticipos' then 'ANTICIPOS'
    when upper(producto) in ('ARRENDAMIENTO OPERATIVO','CARTERA DE CRéDITOS','LEASING FINANCIERO','HATO LEASING') then 'LEASING'
    when producto='Cartera Comercial' then 'CARTERA ORDINARIA'
    when producto='CARTERA ORDINARIA' then 'CARTERA ORDINARIA'
    when producto in ('Ex-Empleado Libranza','Libranza') then 'LIBRANZA'
    when producto='Cartera Ordinaria' then 'CARTERA ORDINARIA'
    when producto='Factoring' then 'FACTORING'
    when producto='Hipotecario Vivienda' then 'HIPOTECARIO VIVIENDA'
    when producto='Otros Hipotecario' then 'OTROS HIPOTECARIO'
    when upper(producto)='EX-EMPLEADO LIBRANZA' then 'LIBRANZA'
    when upper(producto)='CCT SUFI' then 'COTIDIANIDAD'
    when upper(producto)='LIBRE INVERSIÓN SUFI' then 'LIBRE INVERSION SUFI'
    when upper(producto)='REESTRUC ROTATIVO' then 'REESTRUCTURADOS ROTATIVOS'
    when trim(aplicativo)= '7' and trim(producto) ='VEHICULOS SUFI' then 'MOVILIDAD'
    when trim(aplicativo)= '7' and trim(producto) <>'VEHICULOS SUFI' then 'COTIDIANIDAD'
    when trim(aplicativo)='1' then 'FACTORING'
    when trim(producto)='CREDIPAGO' then 'ROTATIVOS'
    else upper(replace(replace(replace(replace(replace(trim(lower(producto)), 'ó','O'), 'í','I'), 'á','A'), 'é','E'), 'ú','U')) end ) as producto_v
    
,(case 
	when producto in ('LIBRE INVERSIÓN','LIBRE INVERSION') and plan in ('PS1',) then 'NEQUI_PS1' 
    when producto in ('LIBRE INVERSIÓN','LIBRE INVERSION') and plan in ('PS2') then 'NEQUI_PS2' 
    when producto in ('LIBRE INVERSIÓN','LIBRE INVERSION') and plan in ('PS4') then 'NEQUI_PS4' 
	when producto in ('LIBRE INVERSIÓN','LIBRE INVERSION') and plan in ('P90','P91','P92') then 'BAJO MONTO'
	when producto in ('LIBRE INVERSIÓN','LIBRE INVERSION') and plan like 'F%'  then 'MODIFICADO O REESTRUCTURADO'
	when producto in ('LIBRE INVERSIÓN','LIBRE INVERSION') and plan like 'R%' then 'MODIFICADO O REESTRUCTURADO'
	when producto in ('LIBRE INVERSIÓN','LIBRE INVERSION') and plan in ('P59', 'P69','P85') then 'LIBRE INVERSION DIGITAL'
	when producto = 'LIBRANZA' and plan in ('P33', 'P34', 'P38', 'P42', 'P82', 'P89') then 'EX-EMPLEADO LIBRANZA'
	when producto = 'LIBRANZA' and plan in ('D79', 'D80', 'P71', 'P72') then 'LIBRANZA COLPENSIONES'
	when producto = 'LIBRANZA' and plan in ('D04', 'D07', 'D09', 'D13', 'D31', 'D81', 'P01', 'P03', 'P11', 'P25', 'P32', 'P40', 'P55', 'P65', 'P75', 'P88', 'P99', 'T03', 'T04', 'T36', 'T58') then 'LIBRANZA EMPLEADOS'
	when producto = 'LIBRANZA' and plan ='D14' then 'LIBRANZA FOPEP'
	when producto = 'LIBRANZA' and plan in ('D82','P67') then 'LIBRANZA PENSIONADOS'
	when producto = 'HIPOTECARIO VIVIENDA' and plan in ('AVM', 'CVM','RVM') then 'NO VIS'
	when producto = 'HIPOTECARIO VIVIENDA' and plan in ('AVV', 'CVV', 'P50', 'RVV') then 'VIS'
	Else upper(replace(replace(replace(replace(replace(trim(lower(producto)), 'ó','O'), 'í','I'), 'á','A'), 'é','E'), 'ú','U')) end ) as producto_detallado_v
    
,(case
    when segmento in ('SOCIAL','PERSONAL PLUS','PREFERENCIAL','PERSONAL','MICROFINANZAS','MICROPYME','PYMES','Empresarial1','EMPRESARIAL','GOBIERNO DE RED','PLUS','NEGOCIOS & INDEPEND') then 'NEPYP'
    when segmento in ('CORPORATIVO','GOBIERNO','INTERNACIONAL','CORPORATIVA','FINANCIERA','POTENCIAL','ESTANDAR','CONSTRUCTOR CORPORATIVO','CONSTRUCTOR EMPRESARIAL','CONSTRUCTOR PYME','CONSTRUCTOR C') then 'NCORP'
    end ) as banca_v
    
,( CASE 
	WHEN region IN ('Bogotá Centro','BOGOTÁ','BOGOTA','BOGOTÁ Y CENTRO','Bogotá','Bogotá Gobierno', 'GOBIERNO','INTERNACI','INTERNACIONAL','BOGOTA Y')  THEN "BOGOTÁ"
	WHEN region IN ('CENTRO', 'Centro') THEN "CENTRO"
	WHEN region IN ('Caribe', 'CARIBE') THEN "CARIBE"
	WHEN region IN ('SUR', 'Sur') THEN "SUR"
	WHEN region IN ('ANTIOQUIA', 'Antioquia') THEN "ANTIOQUIA"
	ELSE 'OTROS'  END ) as region_v
   
, 	(CASE 
    WHEN segmento IN ( 'CORPORATIVA' ,  'INTERNACIONAL','CORPORATIVO') THEN 'CORPORATIVA + INTERNACIONAL' 
    WHEN segmento= 'GOBIERNO' THEN 'GOBIERNO' 
    WHEN segmento= 'FINANCIERA' THEN 'FINANCIERA' 
    WHEN segmento= 'CONSTRUCTOR CORPORATIVO' THEN 'CONSTRUCTOR CORPORATIVO ' 
    WHEN segmento= 'CONSTRUCTOR EMPRESARIAL' THEN 'CONSTRUCTOR EMPRESARIAL' 
    WHEN segmento= 'CONSTRUCTOR PYME' THEN 'CONSTRUCTOR PYME' 
    WHEN segmento= 'EMPRESARIAL' THEN 'EMPRESARIAL'
    WHEN segmento= 'GOBIERNO DE RED' THEN 'GOBIERNO DE RED' 
    WHEN segmento= 'MICROPYME' THEN 'MI NEGOCIO' 
    WHEN segmento IN ('PERSONAL','SIN SEGMENTO') THEN 'PERSONAL' 
    WHEN segmento= 'PERSONAL PLUS' THEN 'PERSONAL PLUS' 
    WHEN segmento= 'PREFERENCIAL' THEN 'PREFERENCIAL'
    WHEN segmento= 'PYMES' THEN 'PYMES'
    WHEN segmento is null  THEN 'SIN INFORMACION'
    
    ELSE segmento end) as segmento_v  ,

    (CASE 
     WHEN SUBSEGM ='GRANDE' THEN  'GRANDE'
     WHEN SUBSEGM ='MEDIANO' THEN  'MEDIANA'
     WHEN SUBSEGM ='NULL' THEN  'OTROS'
     WHEN SUBSEGM ='PEQUE#O' THEN  'PEQUENA'
     WHEN SUBSEGM ='PEQUENA' THEN  'PEQUENA'
     WHEN SUBSEGM ='PEQUE#O' THEN  'PEQUENA'
     WHEN SUBSEGM ='PEQUENA' THEN  'PEQUENA'
     WHEN SUBSEGM ='ALTO' THEN  'GRANDE'
     else SUBSEGM end) as SUBSEGM,
    LOCATIONS,
    departamento,
    ocupacion_nomina,
    ingestion_year year,
    ingestion_month as month

    from pre_base_colgalp_ifrs9


    """
    EjecuteSql(Query_Variables_Base)

#tablas_borrado_fechas=['proceso.base_dias']
#Borrado_Tablas(tablas_borrado_fechas)
#Base_Festivos()

##  PARA EL AÑO 2020  ###

ingestion_year=['2020']
ingestion_month=['01','02','03','04','05','06','07','08','09','10','11']#,'12']
ingestion_day=  ['31','28','31','30','31','30','31','31','30','31','30']#,'31']

'''
ingestion_year=['2019']
ingestion_month=['01','02','03','04','05','06','07','08','09','10','11']#,'12']
ingestion_day=  ['31','28','31','30','31','30','31','31','30','31','30']#,'31']
'''
'''
ingestion_year=['2018']
ingestion_month=['01','02','03','04','05','06','07','08','09','10','11']#,'12']
ingestion_day=  ['31','28','31','30','31','30','31','31','30','31','30']#,'31']
'''

"""
ingestion_year=['2021']
ingestion_month=['01','02','03','04','05','06']#,'07','08','09','10','11']#,'12']
ingestion_day=  ['31','28','31','30','31','30']#,'31','31','30','31','30']#,'31']

"""

'''
for i in range(0,len(ingestion_month)):
    Borrado_Tablas(tablas_borrado)
    Master_Customer(ingestion_year[0],ingestion_month[i])
    Ifrs9_Colgap_Base(ingestion_year[0],ingestion_month[i],ingestion_day[i])
    #print(ingestion_year[0])
    #print(ingestion_month[i])
    #print(ingestion_day[i])
    print("   ╔═══════════════════════════════════════════════════════════════════════════════════════════╗")
    print("   ║ HACIENDO EL INSERT PARA EL MES  """+ingestion_month[i]+""" Y DIA """+ingestion_day[i]+""" ║""")
    print("   ╩═══════════════════════════════════════════════════════════════════════════════════════════╝") 
'''

Borrado_Tablas(tablas_borrado)
Query_Base_GIS()
Master_Customer('2021','08')
Ifrs9_Colgap_Base('2021','08','31')