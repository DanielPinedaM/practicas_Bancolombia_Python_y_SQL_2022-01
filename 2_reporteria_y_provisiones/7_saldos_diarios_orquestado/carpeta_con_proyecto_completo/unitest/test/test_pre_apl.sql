    drop table if exists proceso.valida_saldos_act  purge ; 
    create table proceso.valida_saldos_act stored as parquet as     
    with hoy as (
    select t1.*, concat(trim(t1.obl17), trim(if(trim(t1.apli) = 'Lea', '3', t1.apli)), trim(if(t1.moneda is null,'0',cast(t1.moneda as string)))) as llave1
    from resultados_riesgos.saldo_diario t1
    join proceso.fechas_saldos t2 on t1.year=year(cast(t2.fecha_corte as timestamp)) and t1.month=month(cast(t2.fecha_corte_ant as timestamp)) and t1.ingestion_day = day(cast(t2.fecha_corte_ant as timestamp))
    -- where t1.year=year(cast('{fecha_corte}' as timestamp)) and t1.month=month(cast('{fecha_corte}' as timestamp)) and t1.ingestion_day = day(cast('{fecha_corte}' as timestamp))
    ),

    ayer as (
    select t1.*, concat(trim(t1.obl17), trim(if(trim(t1.apli) = 'Lea', '3', t1.apli)), trim(if(t1.moneda is null,'0',cast(t1.moneda as string)))) as llave1
    from resultados_riesgos.saldo_diario t1
    join proceso.fechas_saldos t2 on t1.year=year(cast(t2.fecha_corte_ant as timestamp)) and t1.month=month(cast(t2.fecha_corte_ant as timestamp)) and t1.ingestion_day = day(cast(t2.fecha_corte_ant as timestamp))   
    -- where t1.year=year(cast('{fecha_corte_ant}' as timestamp)) and t1.month=month(cast('{fecha_corte_ant}' as timestamp)) and t1.ingestion_day = day(cast('{fecha_corte_ant}' as timestamp)) 
    ),

    compara as (
    select t1.llave1
        , t1.apli
        , if(t1.sld_cap_final = t2.sld_cap_final and t1.nueva_altura_mora = t2.nueva_altura_mora, 1, 0) as actualizados
        , t1.sld_cap_final as saldo_hoy
        , t2.sld_cap_final as saldo_ayer
    from hoy t1
    left join ayer t2 on t1.llave1 = t2.llave1
    )

    select apli
            , sum(actualizados) as actualizados
            , sum(saldo_hoy) as saldo_hoy
            , sum(saldo_ayer) as saldo_ayer
   from compara
   group by 1
   ;

   compute stats proceso.valida_saldos_act;

   drop table if exists proceso.valida_saldos_var  purge ; 
   create table proceso.valida_saldos_var stored as parquet as 
   with
   prepara as (
   select  
        apli
        , actualizados
        , saldo_hoy
        , saldo_ayer
        , saldo_hoy - saldo_ayer as var_saldo
        , (saldo_hoy - saldo_ayer)/saldo_hoy as porc_var_saldo
   from proceso.valida_saldos_act
   )

   select 
        t1.apli
        , t1.actualizados
        , t1.saldo_hoy
        , t1.saldo_ayer
        , t1.var_saldo
        , t1.porc_var_saldo
        , if((t1.porc_var_saldo>=t2.porc_min and t1.porc_var_saldo<=t2.porc_max),1,0) as en_rango_var
   from prepara t1
   left join proceso.saldos_diarios_rangos_apl t2 on t1.apli = t2.apli
   ;

  compute stats proceso.valida_saldos_var;