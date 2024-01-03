/*
=============================================
    Autores:            Vanessa Osorio Urrea (vaosorio)
                        Duverney Londoño Sanchez (dulondon)
    Fecha de Modificación:  02/08/2021
    Descripción:        Crea agrupaciones para visualización de reporte en excel
    Dependencias:       
                        
=============================================
*/

/* AGRUPA REPORTE TODOS */ -- Tiempo Ejecución: 1m34s 
    drop table if exists {processZone2}.reporte_saldos_diarios_all_new purge;
    create table {processZone2}.reporte_saldos_diarios_all_new stored as parquet as
    select
      desc_segm_rpte -- igual para Nuevo
    , vicepresidencia
    , clasificacion -- igual para Nuevo
    , apli
    , pcons -- Reporte Saldos
    , pcons_sufi -- Reporte Saldos Sufi
    , producto_mp -- Reporte Saldos MP
    , producto_agr -- Reporte producto Agrupado
    , pdto_leasing -- Reporte Leasing Fila 186. igual para Nuevo
    , producto -- Reporte producto Detallado
    , pcons_reporte_banca -- Reporte Banca
    , nueva_banca -- Reporte Banca
    , clasif_micro -- Reporte Microcredito
    , prod_det_libranza -- Reporte Libranza
    , sum(sld_cap_act) as sum_sld_cap_act
    , sum(sld_cap_ant) as sum_sld_cap_ant
    , sum(sld_cap_cierre) as sum_sld_cap_cierre
    , sum(var_sld_cap) as sum_var_sld_cap
    , sum(var_sld_cap_cierre) as sum_var_sld_cap_cierre
    , sum(cv_act) as sum_cv_act
    , sum(cv_ant) as sum_cv_ant
    , sum(var_cv) as sum_var_cv
    , sum(cv_cierre) as sum_cv_cierre
    , sum(var_cv_cierre) as sum_var_cv_cierre
    , fecha_dia_actual
    , fecha_dia_anterior
    , fecha_cierre_mes
    , nutitula
    ,plan
    ,segm

    from {processZone2}.saldos_diarios_new_{FECHA_CORTE_NMB}
    group by  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 25, 26, 27, 28, 29,30
    ;

    compute stats {processZone2}.reporte_saldos_diarios_all_new;


/* REPORTE PRINCIPALES DESEMBOLSOS */
    drop table if exists {processZone}.reporte_sd_ppales_desem purge;
    create table {processZone}.reporte_sd_ppales_desem stored as parquet as 
    select
        num_doc
        , f_desemb  
        , trim(nombre) as nombre 
        , desc_segm 
        , sum(sld_cap_act)/1000000 as monto
    from {processZone2}.saldos_diarios_new_{FECHA_CORTE_NMB}
    where marca_can_cast = '' and f_desemb = {FECHA_CORTE_NMB} and apli <> 'C'
    group by 1, 2, 3, 4
    order by 5 desc
    limit 10;

    compute stats {processZone}.reporte_sd_ppales_desem;


/* REPORTE PRINCIPALES CANCELACIONES */
    drop table if exists {processZone}.reporte_sd_ppales_cancelaciones purge;
    create table {processZone}.reporte_sd_ppales_cancelaciones stored as parquet as 
    with   
    reporte as (
      select distinct llave1, num_doc, nombre, sld_cap_ant, sld_cap_act
      from {processZone2}.saldos_diarios_new_{FECHA_CORTE_NMB} t1
      where t1.marca_can_cast <> '' and t1.apli <> 'C'
    )

    select
        trim(cast(t1.nombre as string)) as nombre 
        , cast(t1.num_doc as bigint) as id
        , sum(t1.sld_cap_act)/1000000 as sld_cap_act
        , sum(t1.sld_cap_ant)/1000000 as sld_cap_final
    from reporte t1
    group by 1,2
    order by 4 desc
    limit 15
    ;

    compute stats {processZone}.reporte_sd_ppales_cancelaciones;

/* PPALES VENCIDOS */
    drop table if exists {processZone}.reporte_sd_planta_ccial purge; 
    create table {processZone}.reporte_sd_planta_ccial stored as parquet as
    select distinct
        t1.cod_asesor as cod_gerente
        , t1.nombre as nombre_gte
    from resultados_serv_para_los_clientes.planta_comercial_pgc t1
        where t1.ingestion_year = year(cast('{FECHA_PLANTA}' as timestamp)) and t1.ingestion_month = month(cast('{FECHA_PLANTA}' as timestamp)) and t1.ingestion_day = day(cast('{FECHA_PLANTA}' as timestamp))
        and t1.cod_estado = 1
    ; 

    compute stats {processZone}.reporte_sd_planta_ccial;


    drop table if exists {processZone}.reporte_sd_gerentes purge;
    create table {processZone}.reporte_sd_gerentes stored as parquet as
    with 
        gnral as (
            select distinct
            lpad(cast(t1.id as string),15,'0') as id, 
            t1.gtea,
            t1.gerente_leasing
            from resultados_riesgos.ceniegarc_lz t1
            where ingestion_year=year(cast('{FECHA_CIERRE}' as timestamp)) and ingestion_month=month(cast('{FECHA_CIERRE}' as timestamp))
            and apl not in ('2')
        ),

        gerentes_banco as(
        select distinct
        t1.id,
        t1.gtea
        from gnral t1) 

        select t1.id, 
        t2.nombre_gte as gerente
        from gerentes_banco t1
        left join {processZone}.reporte_sd_planta_ccial t2 on (t1.gtea = cast(t2.cod_gerente as bigint))
        where t2.nombre_gte <> '' and t1.gtea is not null

        union all
        select distinct
        t1.id, 
        t1.gerente_leasing as gerente
        from gnral t1
        where t1.gerente_leasing <> '';
    
    compute stats {processZone}.reporte_sd_gerentes;

    drop table if exists {processZone}.sd_ppales_vencidos purge;
    create table {processZone}.sd_ppales_vencidos stored as parquet as
    with cliente as (
        select t1.num_doc
               , t1.tipo_doc
               , max(t1.marca_aplc) as marca_aplc
               , max(trim(t1.nombre)) as nombre
               , max(t1.desc_segm) as descripcion_segmento
               , max(t1.nueva_region) as nueva_region
               , sum(t1.sld_cap_act) as saldo_capital_total
               , sum(t1.cv_act) as vencida_total
               , sum(t1.var_cv_cierre) as nueva_vencida
               , max(t1.nueva_altura_mora) as nueva_altura_mora

        from {processZone2}.saldos_diarios_new_{FECHA_CORTE_NMB} t1
        where t1.marca_can_cast in ('', 'Cierre') and t1.cod_clasificacion = '1' and t1.apli <> 'C'
        group by 1, 2
    ), 

        vencidos as (
        select 
          num_doc
        , tipo_doc  
        , marca_aplc
        , nombre
        , descripcion_segmento
        , nueva_region
        , saldo_capital_total
        , vencida_total
        , nueva_vencida
        , nueva_altura_mora
        from cliente     
        order by nueva_vencida desc
        limit 15 -- Cambio Spiwack
    )

    select 
        t1.num_doc
        , t1.nombre 
        , t1.descripcion_segmento
        , upper((max(t2.gerente))) as gerente
        , t1.nueva_region
        , t1.saldo_capital_total/1000000  as  saldo_capital_total
        , t1.vencida_total/1000000 as vencida_total
        , t1.nueva_vencida/1000000 as nueva_vencida
        , t1.nueva_altura_mora
        , if(t1.marca_aplc = 0, "Sin Obligaciones APL C", "Con Obligaciones APL C") as marca_aplc
        from vencidos t1
        left join {processZone}.reporte_sd_gerentes t2 on (t1.num_doc = cast(t2.id as bigint))
        group by 1, 2, 3, 5, 6, 7, 8, 9, 10
    ;

    compute stats  {processZone}.sd_ppales_vencidos;   
    

/* MÁXIMAS GESTIONES CLIENTES*/
    -- drop table if exists {processZone}.sd_ult_gestion purge;
    -- create table {processZone}.sd_ult_gestion stored as parquet as 
    -- with 
    -- max_gestion as (
    --     select id
    --             , max(fecha) as fecha
    --     from {processZone2}.comentarios_clientes
    --     group by 1
    -- ),

    -- une as (
    --     select t1.id
    --         , t1.gestion
    --         , t1.fecha
    --     from {processZone2}.comentarios_clientes t1
    --     inner join max_gestion t2 on t1.id = t2.id and t1.fecha = t2.fecha
    -- ),

    -- max_long_ges as (
    --     select t1.id
    --         , t1.fecha
    --         , max(length(t1.gestion)) as max_long_ges
    --         , max(t1.gestion) as max_gestion
    --     from une t1
    --     group by 1, 2
    -- )

    -- select distinct
    --     t1.id
    --     , t1.gestion
    --     , t1.fecha
    -- from une t1
    -- inner join max_long_ges t2 on t1.id = t2.id and t1.fecha = t2.fecha and length(t1.gestion) = t2.max_long_ges and t1.gestion = t2.max_gestion

    -- ;

    -- compute stats {processZone}.sd_ult_gestion;


    drop table if exists {processZone}.reporte_sd_ppales_vencidos purge;
    create table {processZone}.reporte_sd_ppales_vencidos stored as parquet as 
    select t1.num_doc
            , t1.nombre 
            , t1.descripcion_segmento
            , t1.gerente
            , t1.nueva_region
            , t1.saldo_capital_total
            , t1.vencida_total
            , t1.nueva_vencida
            , t1.nueva_altura_mora
  --          , if(t2.id is null, 'Pendiente de Información', t2.gestion) as gestion
            , 'Pendiente de Información' as gestion
            , t1.marca_aplc
    from {processZone}.sd_ppales_vencidos t1
    --left join {processZone}.sd_ult_gestion t2 on t1.num_doc = cast(t2.id as bigint)
    order by t1.nueva_vencida desc
    ;

    compute stats {processZone}.reporte_sd_ppales_vencidos;


/* PPALES RECUPERADOS */
    drop table if exists {processZone}.reporte_sd_ppales_recuperados purge;
    create table {processZone}.reporte_sd_ppales_recuperados stored as parquet as 
    with 
    cliente as (
        select t1.num_doc
               , trim(t1.nombre) as nombre
               , t1.desc_segm as descripcion_segmento
               , t1.nueva_region
               , sum(t1.cv_cierre) as vencida_cierre
               , sum(t1.cv_act) as vencida_total
               , sum(t1.var_cv_cierre) as nueva_vencida
               , max(t1.nueva_altura_mora) as nueva_altura_mora
               , max(t1.marca_aplc) as marca_aplc

        from {processZone2}.saldos_diarios_new_{FECHA_CORTE_NMB} t1
        where t1.marca_can_cast in ('', 'Cierre') and t1.cod_clasificacion = '1' and t1.apli <> 'C'
        group by 1, 2, 3, 4
    ), 

    ppales_rec as (
    select 
        t1.num_doc
        , t1.nombre 
        , t1.descripcion_segmento
        , (max(t2.gerente)) as gerente
        , t1.nueva_region
        , t1.vencida_cierre
        , t1.vencida_total
        , t1.nueva_vencida
        , t1.nueva_altura_mora
        , t1.marca_aplc
        from cliente t1
        left join {processZone}.reporte_sd_gerentes t2 on (t1.num_doc = cast(t2.id as bigint))
        -- left join {processZone2}.insumo_base_alivios t3 on t1.num_doc = t3.id
        -- where t3.id is null
        group by 1, 2, 3, 5, 6, 7, 8, 9, 10
        order by nueva_vencida asc
        limit 15
    )

    select t1.num_doc
            , t1.nombre 
            , t1.descripcion_segmento
            , upper(t1.gerente) as gerente
            , t1.nueva_region
            , t1.vencida_cierre/1000000 as vencida_cierre
            , t1.vencida_total/1000000 as vencida_total
            , abs(t1.nueva_vencida)/1000000 as nueva_vencida
            , t1.nueva_altura_mora
 --           , if(t2.id is null, 'Pendiente de Información', t2.gestion) as gestion
            , 'Pendiente de Información' as gestion
            , if(t1.marca_aplc = 0, "Sin Obligaciones APL C", "Con Obligaciones APL C") as marca_aplc 
    from ppales_rec t1
 --   left join {processZone}.sd_ult_gestion t2 on t1.num_doc = cast(t2.id as bigint)
    order by nueva_vencida desc    
    ;

    compute stats {processZone}.reporte_sd_ppales_recuperados;