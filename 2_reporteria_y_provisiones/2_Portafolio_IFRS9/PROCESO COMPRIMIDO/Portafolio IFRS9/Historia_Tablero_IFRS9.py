
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
    where  year=2020 and month=01 and ingestion_day=(select dia from proceso.base_dias where ano=2020 and mes=01 and dia_habil_no_dom_fest=1 limit 1 ) ) Consulta1
    where rn=1 


;;
drop table proceso_riesgos.gran_base_ifrs9_colgalp purge
;;
create table  proceso_riesgos.gran_base_ifrs9_colgalp_copia stored as parquet as 
select * from  proceso_riesgos.gran_base_ifrs9_colgalp

;;
create table  proceso_riesgos.gran_base_ifrs9_colgalp stored as parquet as 
       With 
    
  
    master_credit_risk as (
    
    SELECT 
         cast(num_doc as bigint) as num_doc
        ,cast(SUBSTRING(oblig, 3, 17) as bigint) as obl
        ,cod_clase as modalidad
        ,producto_agrupado as producto
        ,cv as cv30
        ,c90 as cv90
        ,sld_ca as saldo_capital
        ,BANCA as banca 
        ,cod_apli as aplicativo
        ,DIAS_MORA as dia_mora
        ,desc_segm as segmento
        --,Zona as zona
        ,calif_ext as calificacion_externa
        ,cast(calif_int_fa as string) as calificacion_interna
        ,Subsector as subsector
        ,sld_tot_provision as provision_total 
        ,f_desemb as fecha_desemb
        ,cod_plan as plan
        ,region_of as region
        ,cast(cod_moneda as double) as cod_moneda 
        ,cast(tipo_identificacion_cli as string) as tipo_identificacion
        ,0 as castigos
        ,cast_cap_tot
        ,ajuste_interes_b3
        ,ajuste_prov_int_b3 
        ,(gasto + coalesce(overlays,0)) as gasto
        ,overlays as overlays
        ,gasto  as gasto_puro
        ,exposicion_tot_fa  as exposicion_total
        ,vlr_desemb_oblig
        ,CASE WHEN (YEAR(f_desemb)*100+month(f_desemb))=(YEAR(f_corte)*100+month(f_corte)) THEN vlr_desemb_oblig ELSE 0 END AS desembolso
        ,(ingestion_year*10000+ingestion_month*100+ingestion_day) AS fcorte
        ,ingestion_year
        ,ingestion_month
        ,ingestion_day

    
    
    from resultados_riesgos.master_credit_risk
    where ingestion_year=2019 and ingestion_month=12 and ingestion_day=31 and libro=1 
    ),
    
    Castigos as (
    select
         cast(num_doc as bigint) as num_doc
        ,cast(SUBSTRING(oblig, 3, 17) as bigint) as obl
        ,cod_clase as modalidad
        ,producto_agrupado as producto
        ,0 as cv30
        ,0 as cv90
        ,0 as saldo_capital
        ,BANCA as banca 
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
        ,cast(cod_moneda as double) as cod_moneda 
        ,cast(tipo_identificacion_cli as string) as tipo_identificacion
        ,cast(cast_cap_tot as bigint) as Castigos
        ,0 cast_cap_tot
        ,0 ajuste_interes_b3
        ,0 ajuste_prov_int_b3 
        ,0 as gasto
        ,0 as overlays
        ,0  as gasto_puro
        ,0 as exposicion_total
        ,0 as vlr_desemb_oblig
        ,0 as desembolso
        ,(ingestion_year*10000+ingestion_month*100+ingestion_day) AS fcorte
        ,ingestion_year
        ,ingestion_month
        ,ingestion_day



    
    from resultados_riesgos.master_credit_risk
    where ingestion_year=2019 and ingestion_month=12 and ingestion_day=31 and libro=1 and marca_castigo='SI'
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
         ciiu
        ,riesgo_sectorial
        FROM proceso_riesgos.homologacion_ciiu
    ),
    
    max_fecha as (
    
    select distinct ingestion_year, ingestion_month, ingestion_day from resultados_riesgos.consolidado_caracterizacion
    order by 1 desc, 2 desc, 3 desc 
    limit 1 
    ), caracterizacion_base as (
    
    select  distinct
            num_doc
            ,(CASE
                  when upper(caracterizacion) = 'GRUPO 1' then '3. Riesgo Bajo'
                  when upper(caracterizacion) = 'GRUPO 2' then '2. Riesgo Medio'
                  when upper(caracterizacion) = 'GRUPO 3' then '1. Riesgo Alto'
                  else '4. Riesgo Desconocido'
              END) AS caracterizacion
              from resultados_riesgos.consolidado_caracterizacion t1 
              inner join max_fecha t2 on t1.ingestion_year=t2.ingestion_year and t1.ingestion_month=t2.ingestion_month
              and t1.ingestion_day=t2.ingestion_day 
    ), Caracterizacion as (
    
    select num_doc, max(caracterizacion) as caracterizacion from caracterizacion_base
     group by 1),

        
    Arbol_ciiu as (
    
        Select distinct cod_ciiu
            ,sector
            ,Subsector_trabajado  
            from proceso_riesgos.scc_arbol_ciiu t1
            inner join (select ingestion_year,ingestion_month, ingestion_day  from proceso_riesgos.scc_arbol_ciiu  order by 1 desc, 2 desc, 3 desc limit 1  ) fecha_max
    on fecha_max.ingestion_year=t1.ingestion_year and    fecha_max.ingestion_month=t1.ingestion_month  and fecha_max.ingestion_day=t1.ingestion_day

       
        ),pre_base_colgalp_ifrs9 as (
    
    Select     t1.*
              ,t2.riesgo_sectorial
              ,t3.Caracterizacion
              ,t4.sector
              ,t4.Subsector_trabajado 
    from Info_Master_customer t1
    
    left join  riesgo_sectorial t2 on  t1.cod_ciiu = t2.ciiu
    left join  Caracterizacion t3  on t1.num_doc=t3.num_doc
    left join  Arbol_ciiu t4 on t1.cod_ciiu=t4.cod_ciiu
    )
    
    
select 

    num_doc
    ,obl
    ,modalidad
    ,producto
    ,cv30
    ,cv90   
    ,saldo_capital
    ,gasto
    ,overlays
    ,gasto_puro
    ,exposicion_total
    ,ajuste_interes_b3
    ,ajuste_prov_int_b3 
    ,vlr_desemb_oblig
    ,desembolso
    ,banca
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
	when producto in ('LIBRE INVERSIÓN','LIBRE INVERSION') and plan in ('PS1','PS2') then 'NEQUI' 
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
	WHEN region IN ('Bogotá Centro','BOGOTÁ','BOGOTÁ Y CENTRO','Bogotá','Bogotá Gobierno', 'GOBIERNO','INTERNACI','INTERNACIONAL','BOGOTA Y')  THEN "BOGOTÁ"
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
    
    ELSE segmento end) as segmento_v  

from pre_base_colgalp_ifrs9
    
    