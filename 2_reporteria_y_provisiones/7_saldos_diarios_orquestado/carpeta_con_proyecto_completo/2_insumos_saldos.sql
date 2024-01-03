/*
=============================================
    Autores:            Vanessa Osorio Urrea (vaosorio)
                        Duverney Londoño Sanchez (dulondon)
    Fecha de Modificación:  02/08/2021
    Descripción:        Consulta y crea los diferentes insumos que se usarán en el cálculo del reporte de Saldos Diarios.
    Dependencias:       
                        
=============================================
*/

/* ACTUALIZA ESTADÍSTICA DE TABLAS PARÁMETROS CARGADA POR SPARKY */
    compute stats {processZone2}.sd_parametros;
    compute stats {processZone}.saldos_diarios_rangos_apl;

/* Consulta Arbol Obligaciones */

    drop table if exists {processZone2}.arbolobl_saldos_diarios purge;
    create table {processZone2}.arbolobl_saldos_diarios stored as parquet as
    select distinct 
        cod_ciiu
        , vic_ccial
        from resultados_riesgos.erm_arbol_organizaciones;

    compute stats {processZone2}.arbolobl_saldos_diarios;

/* Parámetros LIBRANZA */    
    drop table if exists {processZone2}.sd_param_libranza purge;
    create table {processZone2}.sd_param_libranza stored as parquet as 
    with a as (
        select distinct 
            cod_clase
            , cod_apli
            , cod_plan
            , producto_detallado
        from resultados_riesgos.asignacion_productos 
        where producto_consolidado='LIBRANZA'
        --Se debe consultar la tb completa pq últ ingestión no tiene todos los planes
    )

    select
        cod_clase
            , cod_apli
            , cod_plan
            , if(producto_detallado='LIBRANZA PENSIONADOS','LIBRANZA PROTECCION', producto_detallado) as producto_detallado
    from a
    ;

    compute stats {processZone2}.sd_param_libranza;


/* INSUMO erm_vic_ccial */
    drop table if exists {processZone2}.insumo_erm_vic_ccial purge;
    create table {processZone2}.insumo_erm_vic_ccial stored as parquet as
    -- with fechas_ing as ( /* Se usa esta opción si no se quiere trabajar con la última fecha de ingestión de una tabla sino con otra fecha puntual*/
    --     select
    --     ingestion_year
    --     , ingestion_month	
    --     , ingestion_day
    --     from resultados_riesgos.erm_vic_ccial
    --     where ingestion_year = 2021 and ingestion_month <=  6 
    --     order by 1 desc, 2 desc, 3 desc
    --     limit 1
    -- )

    select 
      t1.tipo_identificacion_cli	
    , t1.num_doc	
    , t1.nombre
    , t1.tipo_cliente	
    , t1.cod_gte	
    , t1.cod_ciiu	
    , t1.cod_subciiu
    , t1.desc_seg
    , t1.desc_subseg
    , t1.sector_economico
    , t1.subsector_economico	
    , t1.grupo_de_riesgo	
    , t1.grupo_economico
    , t1.segmento_estrategico	
    , t1.vic_ccial	
    , t1.ingestion_day
    , t1.ingestion_year	
    , t1.ingestion_month	
    from resultados_riesgos.erm_vic_ccial t1
    -- inner join fechas_ing t2 
        -- on t1.ingestion_year = t2.ingestion_year and t1.ingestion_month = t2.ingestion_month and t1.ingestion_day = t2.ingestion_day
        where t1.ingestion_year = year(cast('{FECHA_VIC_CCIAL}' as timestamp)) and t1.ingestion_month = month(cast('{FECHA_VIC_CCIAL}' as timestamp)) and t1.ingestion_day = day(cast('{FECHA_VIC_CCIAL}' as timestamp))
    ;

    compute stats {processZone2}.insumo_erm_vic_ccial;

/* INSUMO erm_matriz_corporativa */

    drop table if exists {processZone2}.insumo_erm_matriz_corporativa purge;
    create table {processZone2}.insumo_erm_matriz_corporativa stored as parquet as
    select 
        t1.banca	
        , t1.cod_gte	
        , t1.vic_ccial	
        , t1.ingestion_day
        , t1.ingestion_year
        , t1.ingestion_month
    from resultados_riesgos.erm_matriz_corporativa t1
        where t1.ingestion_year = year(cast('{FECHA_MATRIZ}' as timestamp)) and t1.ingestion_month = month(cast('{FECHA_MATRIZ}' as timestamp)) and t1.ingestion_day = day(cast('{FECHA_MATRIZ}' as timestamp))
        
    ;

    compute stats {processZone2}.insumo_erm_matriz_corporativa;

/* INSUMO RECETA PCONS */
    drop table if exists {processZone2}.insumo_saldos_receta purge;
    create table {processZone2}.insumo_saldos_receta stored as parquet as
    with 
    distintos as (
    select distinct
        t1.llave
        , t1.cod_clase
        , t1.cod_apli	
        , t1.cod_plan	
        , t1.producto_consolidado
        , t1.ingestion_day
        , t1.ingestion_year
        , t1.ingestion_month
    from resultados_riesgos.asignacion_productos t1
        where t1.year = year(cast('{FECHA_RECETA}' as timestamp)) and t1.ingestion_month = month(cast('{FECHA_RECETA}' as timestamp)) and t1.ingestion_day = day(cast('{FECHA_RECETA}' as timestamp))
    ),

    duplicados as (
        select llave, count(*)
        from distintos
        group by 1
        having count(*) <> 1
    )

    select distinct
        t1.llave
        , t1.cod_clase
        , t1.cod_apli	
        , t1.cod_plan	
        , if(trim(t1.cod_apli) = '4', 'OTROS HIPOTECARIO', 'CARTERA ORDINARIA') as producto_consolidado
        , t1.ingestion_day
        , t1.ingestion_year
        , t1.ingestion_month
    from distintos t1
    inner join duplicados t2 on t1.llave = t2.llave

    union all
    select  
        t1.llave
        , t1.cod_clase
        , t1.cod_apli	
        , t1.cod_plan	
        , t1.producto_consolidado
        , t1.ingestion_day
        , t1.ingestion_year
        , t1.ingestion_month
    from distintos t1
    left anti join duplicados t2 on t1.llave = t2.llave

    ;
    compute stats {processZone2}.insumo_saldos_receta;

/* INSUMO BASE ALIVIOS */ -- Se usa para reporte prinicipales Recuperados
    drop table if exists {processZone2}.insumo_base_alivios purge;
    create table {processZone2}.insumo_base_alivios stored as parquet as
    select distinct
        t1.id
        , t1.tid    
    from resultados_riesgos.base_alivios_covid t1
    where t1.ingestion_year=year(cast('{FECHA_ALIVIOS}' as timestamp)) and t1.ingestion_month=month(cast('{FECHA_ALIVIOS}' as timestamp)) and t1.ingestion_day=day(cast('{FECHA_ALIVIOS}' as timestamp))
    ;

    compute stats {processZone2}.insumo_base_alivios;


/* Calcula Saldos Hoy sin duplicados */

    drop table if exists {processZone}.insumo_saldos_hoy purge;
    create table {processZone}.insumo_saldos_hoy stored as parquet as
    with sd as (
    select distinct
    'SALDOS_DIARIOS' as fuente 
    , concat(trim(t1.obl17), trim(if(trim(t1.apli) = 'Lea', '3', t1.apli)), trim(if(t1.moneda is null,'0',cast(t1.moneda as string))), if(cast(cast(t1.num_doc as bigint) as string) is null,'0',cast(cast(t1.num_doc as bigint) as string)), trim(if(t1.tipo_doc is null,'0',t1.tipo_doc))) as llave2
    , concat(trim(t1.obl17), trim(if(trim(t1.apli) = 'Lea', '3', t1.apli)), trim(if(t1.moneda is null,'0',cast(t1.moneda as string)))) as llave1
    , cast(t1.num_doc as bigint) as num_doc
    , t1.tipo_doc
    , t1.nombre
    , t1.segm
    , t1.desc_segm
    , t1.subsegm
    , t1.gte
    , t1.f_desemb
    , t1.f_pp
    , if(t1.apli = 'Lea', '3', trim(t1.apli)) as apli
    , t1.obl17 as obl_sd
    , t1.obl17 as obl_cn
    , if(t1.moneda is null, 0, t1.moneda) as moneda
    , t1.clasificacion
    , t1.sld_cap_final
    , if(t1.moneda=1, t1.sld_cap_final/t1.trm, 0) as sld_cap_final_dolares
    , t1.nueva_altura_mora
    , if(cast(t1.num_doc as bigint) = 811011779, 0, t1.cv) as cv
    , if(cast(t1.num_doc as bigint) = 811011779, 0, t1.c90) as c90
    , t1.banca
    , t1.modalidad
    , t1.plan
    , t1.pd
    , t1.pcons
    , t1.gerenciado
    , t1.of_prod
    , t1.nueva_region
    , t1.capital_ini
    , coalesce(t1.nutitula,0) as nutitula -- Query Bases >>> B.Base_saldos_diarios
    , t1.linea_negocio
    , t1.trm
    , t1.ingestion_year
    , t1.ingestion_month
    , t1.ingestion_day 
    from resultados_riesgos.saldo_diario t1
    where t1.year=year(cast('{FECHA_CORTE}' as timestamp)) and t1.month=month(cast('{FECHA_CORTE}' as timestamp)) and t1.ingestion_day = day(cast('{FECHA_CORTE}' as timestamp))
    ),

    sd_num as ( -- Enumera registros de acuerdo a llave, si están repetidos el número asignado será diferente de 1
     select  (row_number() OVER(partition by llave1 order by llave1 asc, num_doc desc )) as row_num
            , t1.fuente 
            , t1.llave2
            , t1.llave1
            , t1.num_doc
            , t1.tipo_doc
            , t1.nombre
            , t1.segm
            , t1.desc_segm
            , t1.subsegm
            , t1.gte
            , t1.f_desemb
            , t1.f_pp
            , t1.apli
            , t1.obl_sd
            , t1.obl_cn
            , t1.moneda
            , t1.clasificacion
            , t1.sld_cap_final
            , t1.sld_cap_final_dolares
            , t1.nueva_altura_mora
            , t1.cv
            , t1.c90
            , t1.banca
            , t1.modalidad
            , t1.plan
            , t1.pd
            , t1.pcons
            , t1.gerenciado
            , t1.of_prod
            , t1.nueva_region
            , t1.capital_ini
            , t1.nutitula
            , t1.linea_negocio
            , t1.trm
            , t1.ingestion_year
            , t1.ingestion_month
            , t1.ingestion_day
    from sd t1    
    ),

    dupl as (
        select distinct t1.llave1 
        from sd_num t1
        where t1.row_num <> 1
    ),

    no_dupl as (
    select 
         t1.llave1
        , sum(t1.sld_cap_final) as sld_cap_final
        , sum(t1.sld_cap_final_dolares) as sld_cap_final_dolares
        , max(t1.nueva_altura_mora) as nueva_altura_mora
        , sum(t1.cv) as cv
        , sum(t1.c90) as c90
    from sd t1
    inner join dupl t2 on t1.llave1 = t2.llave1
    group by 1

    union all 
    select  t1.llave1
        , t1.sld_cap_final
        , t1.sld_cap_final_dolares
        , t1.nueva_altura_mora
        , t1.cv
        , t1.c90
    from sd t1
    left anti join dupl t2 on t1.llave1 = t2.llave1
    )

    select 
        t1.fuente
        , t2.llave1
        , t1.llave2
        , t1.num_doc
        , t1.tipo_doc
        , t1.nombre
        , t1.segm
        , t1.desc_segm
        , t1.subsegm
        , t1.gte
        , t1.f_desemb
        , t1.f_pp
        , t1.apli
        , t1.obl_sd
        , t1.obl_cn
        , t1.moneda
        , t1.clasificacion
        , t2.sld_cap_final
        , t2.sld_cap_final_dolares
        , t2.nueva_altura_mora
        , t2.cv
        , if(t1.moneda=1, t2.cv/t1.trm, 0) as cv_dolares
        , t2.c90
        , t1.banca
        , t1.modalidad
        , t1.plan
        , t1.pd
        , t1.pcons
        , t1.gerenciado
        , t1.of_prod
        , t1.nueva_region
        , t1.capital_ini
        , t1.nutitula
        , t1.linea_negocio
        , '' as vic_ccial
        , t1.ingestion_year
        , t1.ingestion_month
        , t1.ingestion_day
    from sd_num t1 
    left join no_dupl t2 on t1.llave1 = t2.llave1
    where t1.row_num = 1
    ;

    compute stats   {processZone}.insumo_saldos_hoy;


/* Calcula Saldos Ayer sin duplicados */
    drop table if exists {processZone}.insumo_saldos_ayer purge;
    create table {processZone}.insumo_saldos_ayer stored as parquet as
    with sd as (
    select
    'SALDOS_DIARIOS' as fuente 
    , concat(trim(t1.obl17), trim(if(trim(t1.apli) = 'Lea', '3', t1.apli)), trim(if(t1.moneda is null,'0',cast(t1.moneda as string))), if(cast(cast(t1.num_doc as bigint) as string) is null,'0',cast(cast(t1.num_doc as bigint) as string)), trim(if(t1.tipo_doc is null,'0',t1.tipo_doc))) as llave2
    , concat(trim(t1.obl17), trim(if(trim(t1.apli) = 'Lea', '3', t1.apli)), trim(if(t1.moneda is null,'0',cast(t1.moneda as string)))) as llave1
    , cast(t1.num_doc as bigint) as num_doc
    , t1.tipo_doc
    , t1.nombre
    , t1.segm
    , t1.desc_segm
    , t1.subsegm
    , t1.gte
    , t1.f_desemb
    , t1.f_pp
    , if(t1.apli = 'Lea', '3', t1.apli) as apli
    , t1.obl17 as obl_sd
    , t1.obl17 as obl_cn
    , if(t1.moneda is null, 0, t1.moneda) as moneda
    , t1.clasificacion
    , t1.sld_cap_final
    , if(t1.moneda=1, t1.sld_cap_final/t1.trm, 0) as sld_cap_final_dolares
    , t1.nueva_altura_mora
    , if(cast(t1.num_doc as bigint) = 811011779, 0, t1.cv) as cv
    , if(cast(t1.num_doc as bigint) = 811011779, 0, t1.c90) as c90
    , t1.banca
    , t1.modalidad
    , t1.plan
    , t1.pd
    , t1.pcons
    , t1.gerenciado
    , t1.of_prod
    , t1.nueva_region
    , t1.capital_ini
    , coalesce(t1.nutitula,0) as nutitula -- Query Bases >>> B.Base_saldos_diarios
    , t1.linea_negocio
    , t1.trm
    , t1.ingestion_year
    , t1.ingestion_month
    , t1.ingestion_day 
    from resultados_riesgos.saldo_diario t1
    where t1.year=year(cast('{FECHA_CORTE_ANT}' as timestamp)) and t1.month=month(cast('{FECHA_CORTE_ANT}' as timestamp)) and t1.ingestion_day = day(cast('{FECHA_CORTE_ANT}' as timestamp))
    ),

    sd_num as ( -- Enumera registros de acuerdo a llave, si están repetidos el número asignado será diferente de 1
     select  (row_number() OVER(partition by llave1 order by llave1 asc, num_doc desc)) as row_num
            , t1.fuente 
            , t1.llave2
            , t1.llave1
            , t1.num_doc
            , t1.tipo_doc
            , t1.nombre
            , t1.segm
            , t1.desc_segm
            , t1.subsegm
            , t1.gte
            , t1.f_desemb
            , t1.f_pp
            , t1.apli
            , t1.obl_sd
            , t1.obl_cn
            , t1.moneda
            , t1.clasificacion
            , t1.sld_cap_final
            , t1.sld_cap_final_dolares
            , t1.nueva_altura_mora
            , t1.cv
            , t1.c90
            , t1.banca
            , t1.modalidad
            , t1.plan
            , t1.pd
            , t1.pcons
            , t1.gerenciado
            , t1.of_prod
            , t1.nueva_region
            , t1.capital_ini
            , t1.nutitula
            , t1.linea_negocio
            , t1.trm
            , t1.ingestion_year
            , t1.ingestion_month
            , t1.ingestion_day
    from sd t1    
    ),

    dupl as (
        select distinct t1.llave1 
        from sd_num t1
        where t1.row_num <> 1
    ),

    no_dupl as (
    select 
         t1.llave1
        , sum(t1.sld_cap_final) as sld_cap_final
        , sum(t1.sld_cap_final_dolares) as sld_cap_final_dolares
        , max(t1.nueva_altura_mora) as nueva_altura_mora
        , sum(t1.cv) as cv
        , sum(t1.c90) as c90
    from sd t1
    inner join dupl t2 on t1.llave1 = t2.llave1
    group by 1

    union all 
    select  t1.llave1
        , t1.sld_cap_final
        , t1.sld_cap_final_dolares
        , t1.nueva_altura_mora
        , t1.cv
        , t1.c90
    from sd t1
    left anti join dupl t2 on t1.llave1 = t2.llave1
    )

    select 
        t1.fuente
        , t2.llave1
        , t1.llave2
        , t1.num_doc
        , t1.tipo_doc
        , t1.nombre
        , t1.segm
        , t1.desc_segm
        , t1.subsegm
        , t1.gte
        , t1.f_desemb
        , t1.f_pp
        , t1.apli
        , t1.obl_sd
        , t1.obl_cn
        , t1.moneda
        , t1.clasificacion
        , t2.sld_cap_final
        , t2.sld_cap_final_dolares
        , t2.nueva_altura_mora
        , t2.cv
        , if(t1.moneda=1, t2.cv/t1.trm, 0) as cv_dolares
        , t2.c90
        , t1.banca
        , t1.modalidad
        , t1.plan
        , t1.pd
        , t1.pcons
        , t1.gerenciado
        , t1.of_prod
        , t1.nueva_region
        , t1.capital_ini
        , t1.nutitula
        , t1.linea_negocio
        , '' as vic_ccial
        , t1.ingestion_year
        , t1.ingestion_month
        , t1.ingestion_day
    from sd_num t1 
    left join no_dupl t2 on t1.llave1 = t2.llave1
    where t1.row_num = 1
    ;

    compute stats {processZone}.insumo_saldos_ayer;

/* Calcula CENIE */
    drop table if exists  {processZone2}.insumo_cenie purge;
    create table  {processZone2}.insumo_cenie
    stored as parquet as
    with fechas as (
        select
            anio_finalizacion as y
            , mes_finalizacion as m
            , dia_finalizacion as d
        from resultados.reporte_flujos_oozie
        where 
            anio_finalizacion = if(month(cast('{FECHA_CIERRE}' as timestamp))=12 , year(cast('{FECHA_CIERRE}' as timestamp)) + 1 , year(cast('{FECHA_CIERRE}' as timestamp)))
            and nombre_flujo = 'S_Productos_BVNC_Riesgos_CENIE'
            and mes_finalizacion = (if(month(cast('{FECHA_CIERRE}' as timestamp))=12, 1, month(cast('{FECHA_CIERRE}' as timestamp))+1)) 
            and dia_finalizacion < 15 -- Porque el CENIE tiene ingestiones preliminares los 28-30
        group by 1, 2, 3
        order by 1 desc, 2 desc, 3 desc
        limit 1
    )

    select distinct
         lpad(cast(t1.ceac21 as string),17,'00000000000000000') as obl_num
        , lpad(trim(t1.ces321),17,'00000000000000000') as obl_letras
        , trim(t1.ceap21) as apl
    from s_productos.bvnc_riesgos_cenie t1
    inner join fechas t2 on t1.year = t2.y and t1.ingestion_month = t2.m and t1.ingestion_day = t2.d
    ;

    compute stats {processZone2}.insumo_cenie;


/* Calcula CENIEGARC sin duplicados */    
    drop table if exists {processZone2}.insumo_ceniegarc purge;
    create table {processZone2}.insumo_ceniegarc stored as parquet as
    with 
        cn_ajusta_leas as (
            select t1.*
            , case 
                 when trim(t1.apl) = '3' and length(cast(cast(t1.obl341 as bigint) as string)) = 11 then lpad(substr(cast(cast(t1.obl341 as bigint) as string),6,6),17,'00000000000000000') -- Inicio con: '10000%', '20000%'
                 when trim(t1.apl) = '3' and length(cast(cast(t1.obl341 as bigint) as string)) = 12 then lpad(substr(cast(cast(t1.obl341 as bigint) as string),7,6),17,'00000000000000000') -- Inicio con: '400000%'
                 when trim(t1.apl) = '1' then t2.obl_letras
                 when trim(t1.apl) = 'C' and pcons IN ('ANTICIPOS','LEASING') AND length(cast(cast(t1.obl341 as bigint) as string)) >= 11 then lpad(strright(cast(cast(t1.obl341 as bigint) as string),6),17,'00000000000000000') -- Inicio con: '10000%', '20000%', '400000%'
                 when trim(t1.apl) = 'C' and pcons IN ('OTROS HIPOTECARIO') AND strleft(cast(cast(t1.obl341 as bigint) as string),4) not in ('1099','2099') and length(cast(cast(t1.obl341 as bigint) as string)) >= 11 then lpad(strright(cast(cast(t1.obl341 as bigint) as string),6),17,'00000000000000000') -- Inicio con: '10000%', '20000%', '400000%'
                 when trim(t1.apl) = 'C' and pl in ('A52') AND length(cast(cast(t1.obl341 as bigint) as string)) >= 11 then lpad(strright(cast(cast(t1.obl341 as bigint) as string),6),17,'00000000000000000') -- Inicio con: '10000%', '20000%', '400000%'
                 when trim(t1.apl) = 'C' and (cast(t1.obl341 as bigint)= 999585) then lpad('120001',17,'00000000000000000') --OBL PANAMA EN SD
                 when trim(t1.apl) = 'C' and (cast(t1.obl341 as bigint)= 999586) then lpad('120002',17,'00000000000000000') --OBL PANAMA EN SD
                 else CAST(t1.obl341 as string) end as obl17_aj
            , case 
                 when trim(t1.apl) = 'C' and pcons IN ('ANTICIPOS','LEASING') AND length(cast(cast(t1.obl341 as bigint) as string)) >= 11 then '3' 
                 when trim(t1.apl) = 'C' and pcons IN ('OTROS HIPOTECARIO') AND strleft(cast(cast(t1.obl341 as bigint) as string),4) not in ('1099','2099') and length(cast(cast(t1.obl341 as bigint) as string)) >= 11 then '3' 
                 --LISTAR TODOS LOS PLANES
                 when trim(t1.apl) = 'C' and pl in ('A52') AND length(cast(cast(t1.obl341 as bigint) as string)) >= 11 then '3'
                 when trim(t1.apl) = 'C' and (cast(t1.obl341 as bigint) IN (999585, 999586)) then '9' --PANAMA
                 else t1.apl end as apl_aj                

            from resultados_riesgos.ceniegarc_preliminar t1
            left join {processZone2}.insumo_cenie t2 on t1.obl341 = t2.obl_num and trim(t1.apl) = t2.apl
            where t1.year=year(cast('{FECHA_CIERRE}' as timestamp)) and t1.ingestion_month=month(cast('{FECHA_CIERRE}' as timestamp))
        ),

        ceniegarc as (
        select
        'CENIEGARC' as fuente
        , concat(trim(t1.obl17_aj), trim(t1.apl_aj), trim(cast(t1.md3411 as string)), if(cast(cast(t1.id as bigint) as string) is null,'0',cast(cast(t1.id as bigint) as string)), if(cast(t1.tid as string) is null, '0', cast(t1.tid as string))) as llave2
        , concat(trim(t1.obl17_aj),  trim(t1.apl_aj), trim(cast(t1.md3411 as string))) as llave1
        , cast(t1.id as bigint) as num_doc
        , cast(t1.tid as string) as tipo_doc
        , cast(t1.name as string) as nombre
        , cast(t1.sgto as string) as segm
        , cast(t1.segdesc as string) as desc_segm
        , cast(t1.subsegmento as string) as subsegm
        , cast(t1.gtea as double) as gte
        , trim(t1.apl_aj) as apli
        , ((year(t1.fdesem)*10000)+(month(t1.fdesem)*100)+day(t1.fdesem)) as f_desemb
        , cast(null as double) as f_pp
        , t1.obl17_aj as obl_sd
        , cast(t1.obl341 as string) as obl_cn
        , cast(t1.md3411 as double) as moneda
        , cast(t1.clf as string) as clasificacion
        , sum(t1.sk) as sld_cap_final
        , sum(if(t1.md3411=1, t1.sk/t2.trm_cierre, 0)) as sld_cap_final_dolares
        , max(cast(t1.altmora as int)) as nueva_altura_mora
        , sum(cast(t1.cv1 as double)) as cv
        , sum(if(t1.md3411=1, cast(t1.cv1 as double)/t2.trm_cierre, 0)) as cv_dolares
        , sum(t1.c90) AS c90
        , cast(t1.banca as string) as banca
        , cast(t1.modalidad as string) as modalidad
        , cast(t1.pl as string) as plan
        , cast(t1.pd as string) as pd
        , cast(t1.pcons as string) as pcons
        , cast(t1.gerenciado as string) as gerenciado
        , cast(t1.ofcenie as double) as of_prod
        , cast(t1.region as string) as nueva_region
        , cast(t1.vdesem as double) as capital_ini
        , cast(null as int) as nutitula
        , cast(null as string) as linea_negocio
        , cast(t1.vic_ccial as string) as vic_ccial
        , t1.ingestion_year
        , t1.ingestion_month
        , t1.ingestion_day 
        from cn_ajusta_leas t1
        join resultados_riesgos.trm_erm t2
        where t2.ingestion_year=year(cast('{FECHA_TRM}' as timestamp)) and t2.ingestion_month=month(cast('{FECHA_TRM}' as timestamp))
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38 
    ),

    cn_num as ( -- Enumera registros de acuerdo a llave, si están repetidos el número asignado será diferente de 1
     select  (row_number() OVER(partition by llave1 order by llave1 asc, num_doc desc)) as row_num
            , t1.fuente
            , t1.llave2
            , t1.llave1
            , t1.num_doc
            , t1.tipo_doc
            , t1.nombre
            , t1.segm
            , t1.desc_segm
            , t1.subsegm
            , t1.gte
            , t1.apli
            , t1.f_desemb
            , t1.f_pp
            , t1.obl_sd
            , t1.obl_cn
            , t1.moneda
            , t1.clasificacion
            , t1.sld_cap_final
            , t1.sld_cap_final_dolares
            , t1.nueva_altura_mora
            , t1.cv
            , t1.cv_dolares
            , t1.c90
            , t1.banca
            , t1.modalidad
            , t1.plan
            , t1.pd
            , t1.pcons
            , t1.gerenciado
            , t1.of_prod
            , t1.nueva_region
            , t1.capital_ini
            , t1.nutitula
            , t1.linea_negocio
            , t1.vic_ccial
            , t1.ingestion_year
            , t1.ingestion_month
            , t1.ingestion_day
    from ceniegarc t1    
    ),

    dupl as (
        select distinct t1.llave1 
        from cn_num t1
        where t1.row_num <> 1
    ),

    no_dupl as (
    select 
         t1.llave1
        , sum(t1.sld_cap_final) as sld_cap_final
        , sum(t1.sld_cap_final_dolares) as sld_cap_final_dolares
        , max(t1.nueva_altura_mora) as nueva_altura_mora
        , sum(t1.cv) as cv
        , sum(t1.cv_dolares) as cv_dolares
        , sum(t1.c90) as c90
    from ceniegarc t1
    inner join dupl t2 on t1.llave1 = t2.llave1
    group by 1

    union all 
    select  t1.llave1
        , t1.sld_cap_final
        , t1.sld_cap_final_dolares
        , t1.nueva_altura_mora
        , t1.cv
        , t1.cv_dolares
        , t1.c90
    from ceniegarc t1
    left anti join dupl t2 on t1.llave1 = t2.llave1
    )

    select 
        t1.fuente
        , t2.llave1
        , t1.llave2
        , t1.num_doc
        , t1.tipo_doc
        , t1.nombre
        , t1.segm
        , t1.desc_segm
        , t1.subsegm
        , t1.gte
        , t1.f_desemb
        , t1.f_pp
        , t1.apli
        , t1.obl_sd
        , t1.obl_cn
        , t1.moneda
        , t1.clasificacion
        , t2.sld_cap_final
        , t2.sld_cap_final_dolares
        , t2.nueva_altura_mora
        , t2.cv
        , t2.cv_dolares
        , t2.c90
        , t1.banca
        , t1.modalidad
        , t1.plan
        , t1.pd
        , t1.pcons
        , t1.gerenciado
        , t1.of_prod
        , t1.nueva_region
        , t1.capital_ini
        , t1.nutitula
        , t1.linea_negocio
        , t1.vic_ccial
        , t1.ingestion_year
        , t1.ingestion_month
        , t1.ingestion_day 
    from cn_num t1 
    left join no_dupl t2 on t1.llave1 = t2.llave1
    where t1.row_num = 1    
    ;

    compute stats {processZone2}.insumo_ceniegarc;

    
-- Crea insumo final de Saldos 

    drop table if exists {processZone2}.insumo_saldos purge;
    create table {processZone2}.insumo_saldos stored as parquet as
    
    select distinct
        t1.fuente
        , t1.llave1 -- con num id y tid
        , t1.llave2 
        , t1.num_doc
        , t1.tipo_doc
        , t1.nombre
        , t1.segm
        , t1.desc_segm
        , t1.subsegm
        , t1.gte
        , t1.f_desemb
        , t1.f_pp
        , t1.apli
        , t1.obl_sd
        , t1.obl_cn
        , t1.moneda
        , t1.clasificacion
        , t1.sld_cap_final
        , t1.sld_cap_final_dolares
        , t1.nueva_altura_mora
        , t1.cv
        , t1.cv_dolares
        , t1.c90
        , t1.banca
        , t1.modalidad
        , t1.plan
        , t1.pd
        , t1.pcons
        , t1.gerenciado
        , t1.of_prod
        , t1.nueva_region
        , t1.capital_ini
        , t1.nutitula
        , t1.linea_negocio
        , t1.vic_ccial
        , t1.ingestion_year
        , t1.ingestion_month
        , t1.ingestion_day
    from  {processZone}.insumo_saldos_hoy t1

    union all
    select distinct
        t1.fuente
        , t1.llave1 -- con num id y tid
        , t1.llave2 
        , t1.num_doc
        , t1.tipo_doc
        , t1.nombre
        , t1.segm
        , t1.desc_segm
        , t1.subsegm
        , t1.gte
        , t1.f_desemb
        , t1.f_pp
        , t1.apli
        , t1.obl_sd
        , t1.obl_cn
        , t1.moneda
        , t1.clasificacion
        , t1.sld_cap_final
        , t1.sld_cap_final_dolares
        , t1.nueva_altura_mora
        , t1.cv
        , t1.cv_dolares
        , t1.c90
        , t1.banca
        , t1.modalidad
        , t1.plan
        , t1.pd
        , t1.pcons
        , t1.gerenciado
        , t1.of_prod
        , t1.nueva_region
        , t1.capital_ini
        , t1.nutitula
        , t1.linea_negocio
        , t1.vic_ccial
        , t1.ingestion_year
        , t1.ingestion_month
        , t1.ingestion_day
    from  {processZone}.insumo_saldos_ayer  t1

    union all 
    select distinct
        t1.fuente
        , t1.llave1 -- con num id y tid
        , t1.llave2 
        , t1.num_doc
        , t1.tipo_doc
        , t1.nombre
        , t1.segm
        , t1.desc_segm
        , t1.subsegm
        , t1.gte
        , t1.f_desemb
        , t1.f_pp
        , t1.apli
        , t1.obl_sd
        , t1.obl_cn
        , t1.moneda
        , t1.clasificacion
        , t1.sld_cap_final
        , t1.sld_cap_final_dolares
        , t1.nueva_altura_mora
        , t1.cv
        , t1.cv_dolares
        , t1.c90
        , t1.banca
        , t1.modalidad
        , t1.plan
        , t1.pd
        , t1.pcons
        , t1.gerenciado
        , t1.of_prod
        , t1.nueva_region
        , t1.capital_ini
        , t1.nutitula
        , t1.linea_negocio
        , t1.vic_ccial
        , t1.ingestion_year
        , t1.ingestion_month
        , t1.ingestion_day 
    from {processZone2}.insumo_ceniegarc t1 

    ;

    compute stats {processZone2}.insumo_saldos;