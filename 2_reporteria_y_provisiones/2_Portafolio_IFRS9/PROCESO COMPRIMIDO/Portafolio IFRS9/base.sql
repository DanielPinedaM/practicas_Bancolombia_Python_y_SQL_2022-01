
Create table proceso_riesgos.base_ifrs9_colgap stored as parquet as 
With

Base_previa as ( 

SELECT 
     cast(id as string) as num_doc
    ,obl341	as obl
    ,clf	 as modalidad
    ,PCONS	as producto
    ,cv1	as cv30
    ,c90  as cv90
    ,sk as saldo_capital
    ,BANCA as banca 
    ,apl as aplicativo
    ,ALTMORA	as dia_mora
    ,SEGDESC as segmento
    --,Zona	 as zona
    ,calife	 as calificacion_externa
    ,cast (califi as string) as calificacion_interna
    ,Subsector as subsector
    ,pktotal as provision_total
    ,fdesem as fecha_desemb
    ,pl as plan
    ,'COLGAP' as marca
    ,ingestion_year
    ,ingestion_month
    ,ingestion_day
    ,(ingestion_year*10000+ingestion_month*100+ingestion_day) AS fcorte
from resultados_riesgos.CENIEGARC_LZ
where ingestion_year=2020 and ingestion_month=06 and ingestion_day=30
--limit 3

UNION ALL

SELECT 
     num_doc as num_doc
    ,oblig as obl
    ,cod_clase	 as modalidad
    ,producto_agrupado	as producto
    ,cv	as cv30
    ,c90  as cv90
    ,sld_cap_tot as saldo_capital
    ,BANCA as banca 
    ,cod_apli as aplicativo
    ,DIAS_MORA	as dia_mora
    ,desc_segm as segmento
    --,Zona	 as zona
    ,calif_ext	 as calificacion_externa
    ,cast(calif_int_fa as string) as calificacion_interna
    ,Subsector as subsector
    ,sld_tot_provision as provision_total 
    ,f_desemb as fecha_desemb
    ,cod_plan as plan
    ,'IFRS9' as marca 
    ,ingestion_year
    ,ingestion_month
    ,ingestion_day
    ,(ingestion_year*10000+ingestion_month*100+ingestion_day) AS f_corte
from resultados_riesgos.master_credit_risk
where ingestion_year=2020 and ingestion_month=06 and ingestion_day=30
--LIMIT 3
),
master_customer as (

select * from (
        select ROW_NUMBER() OVER (PARTITION BY  num_doc  ORDER BY f_ult_mantenimiento DESC) AS  rn
        ,num_doc
        ,cod_gte
        ,cod_ciiu
        ,REGION_GTE
        ,ZONA_GTE
        ,tipo_cli
        ,f_ult_mantenimiento
        
from resultados_vspc_clientes.master_customer_data
where  year=2020 and month=08  and ingestion_day=10 ) Consulta1
where rn=1
),

base_variables as (
select t1.num_doc
      ,t1. obl
    ,t1.modalidad
    ,t1.producto
    ,t1.cv30
    ,t1.cv90
    ,t1.saldo_capital
    ,t1.banca
    ,t1.aplicativo
    ,t1.dia_mora 
    ,t1.segmento
    ,t1.calificacion_externa
    ,t1.calificacion_interna
    ,t1.subsector
    ,t1.provision_total
    ,t1.fecha_desemb
    ,t1.plan
    ,t1.marca
    ,t2.tipo_cli
    ,t1.ingestion_year
    ,t1.ingestion_month
    ,t1.ingestion_day
    ,CAST(CONCAT(SUBSTRING(cast(t1.fcorte as string),1,4),"-",SUBSTRING(cast(t1.fcorte as string),5,2),"-",SUBSTRING(cast(t1.fcorte as string),7,2)) AS TIMESTAMP) AS fecha
      ,t2.cod_gte 
      ,t2.cod_ciiu
      ,t2.ZONA_GTE
      ,t2.REGION_GTE
from Base_previa t1 
left join master_customer t2 on t1.num_doc=t2.num_doc
    
),

riesgo_sectorial as (

 SELECT  
         ciiu
        ,riesgo_sectorial
        --,prioridad
        --,escenario
        --,profundizacion
        --,nombre_ciiu 
FROM proceso_riesgos.homologacion_ciiu
),

Caracterizacion as (

select 
     num_doc
    ,(CASE
            when upper(caracterizacion) = 'GRUPO 1' then '3. Riesgo Bajo'
            when upper(caracterizacion) = 'GRUPO 2' then '2. Riesgo Medio'
            when upper(caracterizacion) = 'GRUPO 3' then '1. Riesgo Alto'
            else '4. Riesgo Desconocido'
    END) AS caracterizacion
    from proceso_riesgos.consolidado_caracterizacion_v2
),

Arbol_ciiu as (
SELECT cod_ciiu
        ,sector
       ,Subsector_trabajado  
FROM proceso_riesgos.scc_arbol_ciiu


)

Select t1.*
      ,t2.riesgo_sectorial
      ,t3.Caracterizacion
      ,t4.sector
      ,t4.Subsector_trabajado
from base_variables t1 
left join  riesgo_sectorial t2 on  t1.cod_ciiu = t2.ciiu
left join  Caracterizacion t3  on t1.num_doc= cast(t3.num_doc as string)
left join  Arbol_ciiu t4 on t1.cod_ciiu=t4.cod_ciiu

