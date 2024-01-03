/*
=============================================
    Autores:            Vanessa Osorio Urrea (vaosorio)
                        Duverney Londoño Sanchez (dulondon)
    Fecha de Modificación:  02/08/2021
    Descripción:        Calcula fechas de ingestión automáticas teniendo en cuenta la fecha actual de generación del reporte.
    Dependencias:       resultados_riesgos.inforec_tiempo 
                        
=============================================
*/

/* FECHAS SALDOS DIARIOS */

    drop table if exists proceso.fechas_saldos_act  purge ; 
    create table proceso.fechas_saldos_act 
    stored as parquet as 
    with actual as (
        select
        --to_timestamp( concat( substr(cast(now() as string),1,4), "-", 
        --                            substr(cast(now() as string),6,2), "-", 
        --                            substr(cast(now() as string),9,2) ) , 
        --                            'yyyy-MM-dd') as hoy
          cast('2022-01-12' as timestamp) as hoy
        --, to_timestamp( concat( substr(cast(adddate(now(),-1) as string),1,4), "-", 
        --                            substr(cast(adddate(now(),-1) as string),6,2), "-", 
        --                            substr(cast(adddate(now(),-1) as string),9,2) ) , 
        --                            'yyyy-MM-dd') as fecha_corte
         , adddate(cast('2022-01-12' as timestamp),-1) as fecha_corte
        ,'SALDOS_DIARIOS' as nomb_tabla
    ),

    calendario_pre as (
        select
            t1.nomb_tabla
            , t1.fecha_corte 
            , t2.* /*filtra Mes Actual*/
        from actual t1
        inner join resultados_riesgos.inforec_tiempo t2
        where t2.year = year(t1.fecha_corte) and t2.month = month(t1.fecha_corte)        
    )

    select
    t1.hoy
    , case 
       -- when t2.holiday_colombia = 1 then adddate(now(),-2)
         when t2.holiday_colombia = 1 then adddate(cast('2022-01-12' as timestamp),-2)
        else t1.fecha_corte end as fecha_corte 
    ,'SALDOS_DIARIOS' as nomb_tabla
    from actual t1
    inner join calendario_pre t2 on t1.fecha_corte = t2.dates
    ;

    compute stats proceso.fechas_saldos_act;


    drop table if exists proceso.fechas_saldos  purge ; 
    create table proceso.fechas_saldos 
    stored as parquet as 
    with 

    calendario as (
        select
            t1.nomb_tabla
            , t1.fecha_corte 
            , t2.* /*filtra Mes Anterior para tenerlo en cuenta si hay cambio de mes*/
        from proceso.fechas_saldos_act t1
        inner join resultados_riesgos.inforec_tiempo t2 on
             t2.year = if(month(t1.fecha_corte)=1, year(t1.fecha_corte)-1, year(t1.fecha_corte))
             and t2.month = if(month(t1.fecha_corte)=1, 12, month(t1.fecha_corte)-1)
        
        union all 
        select
            t1.nomb_tabla
            , t1.fecha_corte 
            , t2.* /*filtra Año Anterior para tenerlo en cuenta si hay cambio de año*/
        from proceso.fechas_saldos_act t1
        inner join resultados_riesgos.inforec_tiempo t2 on
             t2.year = year(t1.fecha_corte)-1 and t2.month = 12
        
        union all
        select
            t1.nomb_tabla
            , t1.fecha_corte 
            , t2.* /*filtra Mes Actual*/
        from proceso.fechas_saldos_act t1
        inner join resultados_riesgos.inforec_tiempo t2
        where t2.year = year(t1.fecha_corte) and t2.month = month(t1.fecha_corte)        
    ),

    max_semanas as (
        select 
            nomb_tabla
            , year
            , max(week) as max_semanas
        from calendario
        where year=year(fecha_corte)-1
        group by 1, 2
    ),

    calc_anterior as (
    select distinct
        t1.hoy
        , t1.fecha_corte
        , t3.max_semanas
        , t2.week as semana_actual
        , t2.day_name
        , case 
            when t2.day_name = 'Monday' and t2.holiday_colombia = 1 then if(t2.week=1 or t2.week=t3.max_semanas,t3.max_semanas,t2.week-1) /*Cambio semana Lunes Festivo*/
            when (t2.day_name in ('Monday')) then if(t2.week=1 or t2.week=t3.max_semanas,t3.max_semanas,t2.week-1) /*Cambio semana Lunes*/
            when (t2.day_name in ('Saturday', 'Sunday')) then if(t2.week=1 or t2.week=t3.max_semanas,t3.max_semanas,t2.week) /*Cambio semana Fin de Semana*/
            else if(t4.day_name = 'Monday' and t4.holiday_colombia = 1, t2.week-1, t2.week) end as semana_anterior /*Cambio de año, mes o de semana*/
        , t1.nomb_tabla
    from proceso.fechas_saldos_act t1
    inner join calendario t2 on t2.year = year(t1.fecha_corte) and t2.month = month(t1.fecha_corte) and t2.day = day(t1.fecha_corte)
    inner join max_semanas t3 on t1.nomb_tabla = t3.nomb_tabla
    left join calendario t4 on t4.dates = adddate(t1.fecha_corte, - 1) /*Para validar si el lunes fue festivo*/
    ),

    jueves as (
        select distinct t2.*
        from calc_anterior t1
        inner join calendario t2 
            on t2.year = if(t1.semana_actual=1 or t1.semana_actual=t1.max_semanas, year(t1.fecha_corte)-1, year(t1.fecha_corte)) 
                and t2.week = t1.semana_anterior and t2.day_name = 'Thursday'
    ),

    def as (    
    select distinct
        t1.hoy
        , t1.fecha_corte
        , case when t1.semana_actual = t1.semana_anterior and t1.day_name in ('Saturday', 'Sunday', 'Monday') then t2.dates
            when t1.semana_actual = t1.semana_anterior and t1.semana_actual <> t1.max_semanas then if(t3.holiday_colombia = 1, t2.dates, adddate(t1.fecha_corte, - 1)) /*cambio de año*/
            else adddate(t1.fecha_corte, - 1) end as fecha_corte_ant
    from calc_anterior t1 
    left join jueves t2 on t1.nomb_tabla = t2.nomb_tabla
    left join calendario t3 on t3.dates = adddate(t1.fecha_corte, - 1)
    )

    select 
        concat( substr(cast(hoy as string),1,4), "-", 
                                    substr(cast(hoy as string),6,2), "-", 
                                    substr(cast(hoy as string),9,2) ) as hoy
        , concat( substr(cast(fecha_corte as string),1,4), "-", 
                                    substr(cast(fecha_corte as string),6,2), "-", 
                                    substr(cast(fecha_corte as string),9,2) ) as fecha_corte
        , year(fecha_corte)*10000 + month(fecha_corte)*100 + day(fecha_corte) as fecha_corte_nmb
        , concat( substr(cast(fecha_corte_ant as string),1,4), "-", 
                                    substr(cast(fecha_corte_ant as string),6,2), "-", 
                                    substr(cast(fecha_corte_ant as string),9,2) ) as fecha_corte_ant
    from def

    ;

    compute stats proceso.fechas_saldos ;


/* FECHAS CIERRE MES */
    drop table if exists proceso.fechas_cierre  purge ; 
    create table proceso.fechas_cierre 
    stored as parquet as
    select 
    -- concat( substr(cast(now() as string),1,4), "-", 
    --                             substr(cast(now() as string),6,2), "-", 
    --                             substr(cast(now() as string),9,2) ) as hoy
    '2022-01-12' as hoy
    , concat( substr(cast(corte as string),1,4), "-", 
                                substr(cast(corte as string),5,2), "-", 
                                cast(ingestion_day as string))
                                    as fecha_corte
    , cast(null as string) as fecha_corte_ant
    from resultados_riesgos.ceniegarc_lz
    --where year = if(month(now())=1, year(now())-1, year(now())) 
    --and ingestion_month <= if(month(now())=1, 12,month(now())) 
    where year = if(month(cast('2022-01-12' as timestamp))=1, year(cast('2022-01-12' as timestamp))-1, year(cast('2022-01-12' as timestamp)))
    and ingestion_month < if(month(cast('2022-01-12' as timestamp))=1, 12,month(cast('2022-01-12' as timestamp))) 
    order by year desc, ingestion_month desc 
    limit 1  

    ;

    compute stats proceso.fechas_cierre;

/* FECHAS TRM */
    drop table if exists proceso.fechas_trm  purge ; 
    create table proceso.fechas_trm 
    stored as parquet as
    select
    concat( substr(cast(now() as string),1,4), "-", 
                               substr(cast(now() as string),6,2), "-", 
                               substr(cast(now() as string),9,2) ) as hoy
    --  '2022-01-12' as hoy
    , concat( substr(cast(fecha as string),1,4), "-", 
                                   substr(cast(fecha as string),6,2), "-", 
                                    substr(cast(fecha as string),9,2) )
                                    as fecha_corte
    , cast(null as string) as fecha_corte_ant
    from resultados_riesgos.trm_erm
    --where ingestion_year = if(month(now())=1,year(now())-1,year(now())) and ingestion_month <=  if(month(now())=1,12,month(now())-1) 
    where ingestion_year = if(month(cast('2022-01-12' as timestamp))=1,year(cast('2022-01-12' as timestamp))-1,year(cast('2022-01-12' as timestamp))) and ingestion_month <=  if(month(cast('2022-01-12' as timestamp))=1,12,month(cast('2022-01-12' as timestamp))-1)
    order by ingestion_year desc, ingestion_month desc 
    limit 1 
    ;

    compute stats proceso.fechas_trm;   