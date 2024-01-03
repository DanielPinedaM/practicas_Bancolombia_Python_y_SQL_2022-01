/*
=============================================
    Autores:            Vanessa Osorio Urrea (vaosorio)
                        Duverney Londoño Sanchez (dulondon)
    Fecha de Modificación:  02/08/2021
    Descripción:        Consulta Master Customer Data para tener el cod_ciiu y cod_tipo_cli y usar en los campos calculados del reporte de Saldos Diarios.
    Dependencias:       resultados_vspc_clientes.master_customer_data 
                        
=============================================
*/

/* Consulta Master toda la Historia y calcula máx fecha de ingestión 
NOTA: Se ejecuta sólo en el caso en que se elimine la tabla proceso_riesgos.master_saldos_diarios */

    /* Máxima fecha de ingestión */   
    -- drop table if exists {processZone}.master_saldos_diarios_maxfec purge;
    -- create table {processZone}.master_saldos_diarios_maxfec stored as parquet as
    -- with 
    --     maxfec as (
    --         select num_doc
    --             , cod_tipo_doc          
    --             , max(year * 10000 +  ingestion_month*100 + ingestion_day) as aaaammdd
    --         from resultados_vspc_clientes.master_customer_data
    --         where ((year*10000)+(month*100)+ingestion_day) <= {FECHA_CORTE_NMB}
    --         and cod_ciiu is not null
    --         group by 1, 2
    --         )

    --     select *
    --         , cast(substr(cast(aaaammdd as string),1,4) as int) as year
    --         , cast(substr(cast(aaaammdd as string),5,2) as int) as month
    --         , cast(substr(cast(aaaammdd as string),7,2) as int) as ingestion_day
    --     from maxfec;  
    -- ;

    -- compute stats {processZone}.master_saldos_diarios_maxfec;

    -- /* Cruza con tabla de máximas fechas para traer valores */
    -- drop table if exists {processZone2}.master_saldos_diarios purge;
    -- create table {processZone2}.master_saldos_diarios
    -- stored as parquet as
    -- with a as (
    -- select  t1.num_doc
    --         , t1.cod_tipo_doc
    --         , t1.cod_ciiu
    --         , t1.cod_tipo_cli
    --         , t1.year
    --         , t1.month
    --         , t1.ingestion_day
    --         , t2.aaaammdd
    -- from resultados_vspc_clientes.master_customer_data t1 
    -- inner join {processZone}.master_saldos_diarios_maxfec t2 on t1.num_doc = t2.num_doc
    --                                             and t1.cod_tipo_doc = t2.cod_tipo_doc
    --                                             and t1.year = t2.year 
    --                                             and t1.month = t2.month 
    --                                             and t1.ingestion_day = t2.ingestion_day
    -- where ((t1.year*10000)+(t1.month*100)+t1.ingestion_day) <= {FECHA_CORTE_NMB}
    -- )

    -- select distinct t1.num_doc
    --         , t1.cod_tipo_doc
    --         , t1.cod_ciiu
    --         , t1.cod_tipo_cli
    --         , t1.year
    --         , t1.month
    --         , t1.ingestion_day
    --         , t1.aaaammdd
    -- from a t1

    -- ;

    -- compute stats {processZone2}.master_saldos_diarios;

/* Calcula total clientes Master Customer con información más reciente (Actual) */
    drop table if exists {processZone}.master_saldos_diarios_act purge;
    create table {processZone}.master_saldos_diarios_act stored as parquet as
    with actual as (
    select num_doc
            , cod_tipo_doc   
            , cod_ciiu
            , cod_tipo_cli 
            , year
            , month
            , ingestion_day 
            , (year * 10000 +  ingestion_month*100 + ingestion_day) as aaaammdd
    from resultados_vspc_clientes.master_customer_data
    where year = year(cast('{FECHA_CORTE}' as timestamp))
                  and ingestion_month = month(cast('{FECHA_CORTE}' as timestamp))
                  and ingestion_day = day(cast('{FECHA_CORTE}' as timestamp))
                  and cod_ciiu is not null
    ),

    nuevos as (
        select t1.num_doc
            , t1.cod_tipo_doc   
            , t1.cod_ciiu 
            , t1.cod_tipo_cli
            , t1.year
            , t1.month
            , t1.ingestion_day 
            , t1.aaaammdd
        from actual t1
        left anti join {processZone2}.master_saldos_diarios t2 on t1.num_doc = t2.num_doc
                                                            and t1.cod_tipo_doc = t2.cod_tipo_doc
                                                            and t1.cod_ciiu = t2.cod_ciiu       
    ),

    actualiza_ant as (
        select t1.num_doc
            , t1.cod_tipo_doc   
            , t1.cod_ciiu 
            , t1.cod_tipo_cli
            , t1.year
            , t1.month
            , t1.ingestion_day 
            , t1.aaaammdd
        from {processZone2}.master_saldos_diarios t1
        left anti join nuevos t2 on t1.num_doc = t2.num_doc
                                  and t1.cod_tipo_doc = t2.cod_tipo_doc 
                                  /*No se cruza por cod_ciiu para excluir el cliente y actualizar con la info más reciente de cod_ciiu (nuevos)*/
    )

    select t1.num_doc
            , t1.cod_tipo_doc   
            , t1.cod_ciiu 
            , t1.cod_tipo_cli
            , t1.year
            , t1.month
            , t1.ingestion_day 
            , t1.aaaammdd
    from nuevos t1

    union all
    select t2.num_doc
            , t2.cod_tipo_doc   
            , t2.cod_ciiu 
            , t2.cod_tipo_cli
            , t2.year
            , t2.month
            , t2.ingestion_day 
            , t2.aaaammdd
    from actualiza_ant t2
    ;

    compute stats {processZone}.master_saldos_diarios_act;

/* Actualiza clientes Master Customer Total */
    drop table if exists {processZone2}.master_saldos_diarios purge;
    create table {processZone2}.master_saldos_diarios stored as parquet as
    select num_doc
            , cod_tipo_doc   
            , cod_ciiu 
            , cod_tipo_cli
            , year
            , month
            , ingestion_day 
            , aaaammdd
    from {processZone}.master_saldos_diarios_act
    ;

    compute stats {processZone2}.master_saldos_diarios;