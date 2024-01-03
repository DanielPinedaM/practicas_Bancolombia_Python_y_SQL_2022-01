/*
=============================================
    Autores:            Vanessa Osorio Urrea (vaosorio)
                        Duverney Londoño Sanchez (dulondon)
    Fecha de Modificación:  02/08/2021
    Descripción:        Crea campos calculados, compara con día anterior y con cierre para generar detalle del reporte.
    Dependencias:       
                        
=============================================
*/

/* Marca Cancelaciones y/o Castigos */
    drop table if exists {processZone2}.saldos_diarios_cancast purge;
    create table {processZone2}.saldos_diarios_cancast stored as parquet as
    with saldos_hoy as (select 
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
    from {processZone2}.insumo_saldos t1
    where ingestion_year=year(cast('{FECHA_CORTE}' as timestamp)) and ingestion_month=month(cast('{FECHA_CORTE}' as timestamp)) and ingestion_day = day(cast('{FECHA_CORTE}' as timestamp))
    and apli not in ('2') -- Cambio Spiwack
    ),

    saldos_ayer as (select 
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
    from {processZone2}.insumo_saldos t1
    where ingestion_year=year(cast('{FECHA_CORTE_ANT}' as timestamp)) and ingestion_month=month(cast('{FECHA_CORTE_ANT}' as timestamp)) and ingestion_day = day(cast('{FECHA_CORTE_ANT}' as timestamp))
    and apli not in ('2') -- Cambio Spiwack
    ),

    ceniegarc as(select 
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
    from {processZone2}.insumo_saldos t1
    where ingestion_year=year(cast('{FECHA_CIERRE}' as timestamp)) and ingestion_month=month(cast('{FECHA_CIERRE}' as timestamp))
    and apli not in ('2') -- Cambio Spiwack
    ),

    preunic_cierre as (
        select
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
        from ceniegarc t1 
        left anti join saldos_hoy t2 on t1.llave1 = t2.llave1
    ),

    preunic_ayer as (
        select
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
        from saldos_ayer t1 
        left anti join saldos_hoy t2 on t1.llave1 = t2.llave1
    ),

    unic_com as (
        select
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
        from preunic_cierre t1
        inner join preunic_ayer t2 on t1.llave1 = t2.llave1
    )

    select 
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
        , 'Cierre' as marca_can_cast -- Únicos en Cierre (no hoy, ni Ayer)
    from preunic_cierre t1
    left anti join unic_com t2 on t1.llave1 = t2.llave1

    union all -- Únicos en Ayer (no hoy, ni en Cierre)
    select
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
        , 'Ayer' as marca_can_cast
    from preunic_ayer t1
    left anti join unic_com t2 on t1.llave1 = t2.llave1

    union all
    select
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
        , 'Cierre y Ayer' as marca_can_cast -- Únicos en Cierre y Ayer (no hoy). Para garantizar no incluir duplicados
    from unic_com t1
    ;

    compute stats {processZone2}.saldos_diarios_cancast;


/* Saldos diarios */
/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/

    drop table if exists {processZone2}.saldos_diarios_erm purge;
    create table {processZone2}.saldos_diarios_erm stored as parquet as
    with 
    obl_cn as (
        select t1.llave1, t1.obl_cn, t1.obl_sd, t1.apli, t1.num_doc, t1.tipo_doc
            , t1.sld_cap_final -- Cambio Spiwack
            , t1.sld_cap_final_dolares -- Cambio Spiwack
            , t1.nueva_altura_mora -- Cambio Spiwack
            , t1.cv -- Cambio Spiwack
            , t1.cv_dolares -- Cambio Spiwack
            , t1.c90 -- Cambio Spiwack
        from {processZone2}.insumo_saldos t1
        where t1.ingestion_year=year(cast('{FECHA_CIERRE}' as timestamp)) and t1.ingestion_month=month(cast('{FECHA_CIERRE}' as timestamp)) and t1.ingestion_day = day(cast('{FECHA_CIERRE}' as timestamp))
        and t1.apli not in ('2')
    ),
    filtra_cartera as (select t1.fuente
            , t1.llave1
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
            , if(t2.llave1 is null, t1.obl_cn, t2.obl_cn) as obl_cn
            , t1.moneda
            , t1.clasificacion
            , if(t3.apli = 'C', t2.sld_cap_final, t1.sld_cap_final) as sld_cap_final -- Cambio Spiwack
            , if(t3.apli = 'C', t2.sld_cap_final_dolares, t1.sld_cap_final_dolares) as sld_cap_final_dolares -- Cambio Spiwack
            , if(t3.apli = 'C', t2.nueva_altura_mora, t1.nueva_altura_mora) as nueva_altura_mora -- Cambio Spiwack
            , if(t3.apli = 'C', t2.cv, t1.cv) as cv -- Cambio Spiwack
            , if(t3.apli = 'C', t2.cv_dolares, t1.cv_dolares) as cv_dolares -- Cambio Spiwack
            , if(t3.apli = 'C', t2.c90, t1.c90) as c90 -- Cambio Spiwack
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
            , '' as marca_can_cast 
            , if(t3.apli = 'C', 1, 0) as ajuste_aplc -- Cambio Spiwack
    from {processZone2}.insumo_saldos t1
    left join obl_cn t2 on t1.llave1 = t2.llave1
    left join obl_cn t3 
        on t1.num_doc = t3.num_doc and t1.sld_cap_final = t3.sld_cap_final and t1.sld_cap_final_dolares = t3.sld_cap_final_dolares
    where t1.ingestion_year=year(cast('{FECHA_CORTE}' as timestamp)) and t1.ingestion_month=month(cast('{FECHA_CORTE}' as timestamp)) and t1.ingestion_day = day(cast('{FECHA_CORTE}' as timestamp))
    and t1.apli not in ('2') -- Cambio Spiwack

    union all
    select 
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
        , t1.marca_can_cast
        , cast(null as int) as ajuste_aplc -- Cambio Spiwack
    from {processZone2}.saldos_diarios_cancast t1
    ), 

    previo as (
    select distinct
      t1.fuente
    , t1.llave1
    , t1.num_doc /*vice*/
    , t1.tipo_doc
    , t3.cod_ciiu
    , t1.nombre
    , t1.segm /*vice*/
    , t1.desc_segm /*Vice act*/
    , t1.subsegm
    , t1.gte /*vice*/
    , t1.f_desemb
    , t1.f_pp
    , if(t1.apli = 'Lea', '3', t1.apli) as apli
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
    , t1.banca /*Vice act*/
    , t2.vic_ccial as erm_vic_ccial /*vice*/
    , t4.vic_ccial as vice_matriz /*vice*/
    , t5.vic_ccial as vice_arbol /*vice*/
    , case
        when t1.gte in (916,00905,30112,33914,33913,34764) and t1.segm IN ('1','3','7','8','A','B','C') then 'Otros Territorios'
        when t1.num_doc=112233445 and t1.segm IN ('7') then 'Grandes Corporativos'                                                
        when t1.num_doc=890903938 and t1.segm IN ('1','3','7','8','A','B','C') then 'Corresponsales y Otros' /*Bancolombia*/
        when t1.segm in('2') then 'Empresarial'
        when trim(t1.segm) in('4','6','M','S') then 'Personas'
        when trim(t1.segm) = '' and t3.cod_tipo_cli = 'I'  then 'Personas'
        when trim(t1.segm) = '' and t3.cod_tipo_cli = 'B' then 'Pymes'
        when t1.segm in('5','G') then 'Pymes'
        when t1.segm in('9') then 'Negocios & Independientes'
        when t1.segm in('A','B','C') then 'Inmobiliario y Constructor'
        when t1.num_doc in (4005500941, /*ATN 3 SA*/
                            4006600065, /*BANANA INTERNATIONAL CORPORATION*/
                            4005500747, /*MAPLE ETANOL S R L*/
                            4005500944, /*MAPLE BIOCOMBUSTIBLES S.R.L.*/
                            4005500339 /**DEEP BLACK DRILLING LLC*/) then 'Otros Territorios'
        when t1.gte IN (605,920,600,602,604,913,47746,902,910,906) then 'Otros Territorios'
        when t2.vic_ccial='Relacionamiento táctico' then 'Corresponsales y Otros' 
        when t2.vic_ccial is not null then t2.vic_ccial
        when t4.vic_ccial is not null then t4.vic_ccial
        when t4.vic_ccial is not null and t1.segm not in ('7','8') then 'sin vice por matriz corp'
        when t5.vic_ccial not in ('Sin Vice','Gobierno, Servicios Financieros, Salud y Educación') then t5.vic_ccial 
        when t1.banca in  ('PPyE', 'Sin Banca') then 'Personas'
        else 'Corresponsales y Otros'
        end as vic_ccial -- Campo reporte Nuevo
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
    , t1.marca_can_cast
    , t1.ajuste_aplc -- Cambio Spiwack
    , t1.ingestion_year
    , t1.ingestion_month
    , t1.ingestion_day

    from filtra_cartera t1
    left join {processZone2}.insumo_erm_vic_ccial t2 on t1.num_doc=cast(t2.num_doc as bigint)
    left join {processZone2}.master_saldos_diarios t3 on (t1.num_doc=cast(t3.num_doc as bigint)) and trim(t1.tipo_doc)=trim(t3.cod_tipo_doc)
    left join {processZone2}.insumo_erm_matriz_corporativa t4 on cast(t1.gte as int)=cast(t4.cod_gte as int)
    left join {processZone2}.arbolobl_saldos_diarios t5 on (cast(t3.cod_ciiu as string) = cast(t5.cod_ciiu as string))
    ),
    final as(
    select 
    t1.fuente
    , t1.llave1
    , t1.num_doc /*vice*/
    , t1.tipo_doc
    , t1.cod_ciiu
    , t1.nombre
    , t1.segm /*vice*/
    , t1.desc_segm /*Vice act*/
    , t1.subsegm
    , t1.gte /*vice*/
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
    , t1.banca /*Vice act*/
    , t1.erm_vic_ccial /*vice*/
    , t1.vice_matriz /*vice*/
    , t1.vice_arbol /*vice*/
    , t1.vic_ccial -- Campo reporte Nuevo
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
    , t1.marca_can_cast
    , t1.ajuste_aplc -- Cambio Spiwack
    , t1.ingestion_year
    , t1.ingestion_month
    , t1.ingestion_day
    from previo t1
    )
    SELECT * from final
    ;

    compute stats {processZone2}.saldos_diarios_erm;


/* Variaciones */

    drop table if exists {processZone2}.variaciones_saldos_diarios purge;
    create table {processZone2}.variaciones_saldos_diarios stored as parquet as
    WITH obl_cn AS
  (SELECT t1.llave1,
          t1.obl_cn,
          t1.obl_sd,
          t1.apli,
          t1.num_doc,
          t1.tipo_doc,
          t1.sld_cap_final,
          t1.sld_cap_final_dolares,
          t1.nueva_altura_mora ,
          t1.cv ,
          t1.cv_dolares,
          t1.c90,
          t1.ingestion_year,
          t1.ingestion_month,
          t1.ingestion_day
   FROM proceso_riesgos.insumo_saldos t1
   WHERE t1.ingestion_year=year(cast('{FECHA_CIERRE}' AS TIMESTAMP))
     AND t1.ingestion_month=month(cast('{FECHA_CIERRE}' AS TIMESTAMP))
     AND t1.ingestion_day = day(cast('{FECHA_CIERRE}' AS TIMESTAMP))
     AND t1.apli NOT IN ('2') ),
     base AS
  (SELECT llave1,
          apli,
          sld_cap_final,
          sld_cap_final_dolares,
          cv,
          cv_dolares,
          ingestion_year,
          ingestion_month,
          ingestion_day
   FROM {processZone2}.saldos_diarios_erm
   WHERE fuente = 'SALDOS_DIARIOS'
    AND ingestion_year=year(cast('{FECHA_CORTE}' AS TIMESTAMP))
    AND ingestion_month=month(cast('{FECHA_CORTE}' AS TIMESTAMP))
    AND ingestion_day = day(cast('{FECHA_CORTE}' AS TIMESTAMP))
   UNION ALL SELECT t1.llave1,
                    t1.apli,
                    t1.sld_cap_final,
                    t1.sld_cap_final_dolares,
                    t1.cv,
                    t1.cv_dolares,
                    t1.ingestion_year,
                    t1.ingestion_month,
                    t1.ingestion_day
   FROM {processZone2}.insumo_saldos t1
   LEFT JOIN obl_cn t3 ON t1.llave1 = t3.llave1
   AND t1.num_doc = t3.num_doc
   AND t1.sld_cap_final = t3.sld_cap_final
   AND t1.sld_cap_final_dolares = t3.sld_cap_final_dolares
   WHERE t1.ingestion_year=year(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP))
     AND t1.ingestion_month=month(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP))
     AND t1.ingestion_day = day(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP))
     AND t1.apli NOT IN ('2')-- Cambio Spiwack

   UNION ALL SELECT llave1,
                    apli,
                    sld_cap_final,
                    sld_cap_final_dolares,
                    cv,
                    cv_dolares,
                    ingestion_year,
                    ingestion_month,
                    ingestion_day
   FROM obl_cn
   WHERE apli NOT IN ('2')-- Cambio Spiwack
 )SELECT llave1,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('{FECHA_CORTE}' AS TIMESTAMP))
                                AND ingestion_month=month(cast('{FECHA_CORTE}' AS TIMESTAMP))
                                AND ingestion_day = day(cast('{FECHA_CORTE}' AS TIMESTAMP)) THEN sld_cap_final
                       END),0) AS sld_cap_Act,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP))
                                AND ingestion_month=month(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP))
                                AND ingestion_day = day(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP)) THEN sld_cap_final
                       END),0) AS sld_cap_Ant,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('{FECHA_CORTE}' AS TIMESTAMP))
                                AND ingestion_month=month(cast('{FECHA_CORTE}' AS TIMESTAMP))
                                AND ingestion_day = day(cast('{FECHA_CORTE}' AS TIMESTAMP)) THEN sld_cap_final_dolares
                       END),0) AS sld_dolares_Act,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP))
                                AND ingestion_month=month(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP))
                                AND ingestion_day = day(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP)) THEN sld_cap_final_dolares
                       END),0) AS sld_dolares_Ant,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('{FECHA_CIERRE}' AS TIMESTAMP))
                                AND ingestion_month=month(cast('{FECHA_CIERRE}' AS TIMESTAMP)) THEN sld_cap_final
                       END),0) AS sld_cap_cierre,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('{FECHA_CIERRE}' AS TIMESTAMP))
                                AND ingestion_month=month(cast('{FECHA_CIERRE}' AS TIMESTAMP)) THEN sld_cap_final_dolares
                       END),0) AS sld_cap_dolares_cierre,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('{FECHA_CORTE}' AS TIMESTAMP))
                                AND ingestion_month=month(cast('{FECHA_CORTE}' AS TIMESTAMP))
                                AND ingestion_day = day(cast('{FECHA_CORTE}' AS TIMESTAMP)) THEN cv
                       END),0) AS cv_Act,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP))
                                AND ingestion_month=month(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP))
                                AND ingestion_day = day(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP)) THEN cv
                       END),0) AS cv_Ant,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('{FECHA_CORTE}' AS TIMESTAMP))
                                AND ingestion_month=month(cast('{FECHA_CORTE}' AS TIMESTAMP))
                                AND ingestion_day = day(cast('{FECHA_CORTE}' AS TIMESTAMP)) THEN cv_dolares
                       END),0) AS cv_dolares_Act,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP))
                                AND ingestion_month=month(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP))
                                AND ingestion_day = day(cast('{FECHA_CORTE_ANT}' AS TIMESTAMP)) THEN cv_dolares
                       END),0) AS cv_dolares_Ant,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('{FECHA_CIERRE}' AS TIMESTAMP))
                                AND ingestion_month=month(cast('{FECHA_CIERRE}' AS TIMESTAMP)) THEN cv
                       END),0) AS cv_cierre
   FROM base
   WHERE apli NOT IN ('2')-- Cambio Spiwack

   GROUP BY llave1;

    compute stats {processZone2}.variaciones_saldos_diarios;



/* Saldos diario final */

    drop table if exists {processZone2}.saldos_diarios_erm_pre1 purge;
    create table {processZone2}.saldos_diarios_erm_pre1 stored as parquet as 

    with 
    ceniegarc as (select 
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
    from {processZone2}.insumo_saldos t1
    where ingestion_year=year(cast('{FECHA_CIERRE}' as timestamp)) and ingestion_month=month(cast('{FECHA_CIERRE}' as timestamp))
    and apli not in ('2')
    ),

    pcons_receta as (
        select 
            t1.fuente
            , t1.llave1
            , t1.num_doc
            , t1.tipo_doc
            , t1.cod_ciiu
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
            , t1.erm_vic_ccial
            , t1.vice_matriz
            , t1.vice_arbol
            , t1.vic_ccial -- Campo reporte Nuevo
            , t1.modalidad
            , t1.plan
            , t1.pd
            , if(t2.llave is null, t1.pcons, upper(t2.producto_consolidado)) as pcons
            , t1.gerenciado
            , t1.of_prod
            , t1.nueva_region
            , t1.capital_ini
            , t1.nutitula
            , t1.linea_negocio
            , t1.marca_can_cast
            , t1.ajuste_aplc -- Cambio Spiwack
            , t1.ingestion_year
            , t1.ingestion_month
            , t1.ingestion_day
        from {processZone2}.saldos_diarios_erm t1
        left join {processZone2}.insumo_saldos_receta t2 on concat(t1.clasificacion, t1.apli, t1.plan)=t2.llave
    )

    select 
      t1.llave1
    , t1.num_doc
    , t1.tipo_doc
    , t1.cod_ciiu
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
    , t1.clasificacion as cod_clasificacion
    , if(t5.categ_original is null, 'No Aplica', t5.categ_final) as clasificacion
    , t1.nueva_altura_mora
    , t1.cv
    , t1.cv_dolares
    , t1.c90
    , t1.banca
    , t1.vic_ccial -- Campo reporte Nuevo
    , t1.modalidad
    , t1.plan
    , t1.pd
    , t1.pcons as pcons_orig
    , case when t7.categ_original is not null and t1.apli = '7' then t7.categ_final
        when t6.categ_original is not null and t1.apli = '7' then t6.categ_final
        when t7.categ_original is null and t1.apli = '7' then t1.pcons
        else 'No Aplica' end as pcons_sufi_orig_sd
    , t2.pcons as pcons_cn
    , case when t7.categ_original is not null then t7.categ_final
        when t2.pcons /*cn*/ = 'OTROS HIPOTECARIO' then t2.pcons /*cn*/ 
        when ((t1.apli = 'L' and t1.pcons = 'LEASING') or t1.apli = '3') 
            and t1.pcons not in ('LEASING', 'LEASING HABITACIONAL', 'ANTICIPOS') then 'SIN PRODUCTO'  
        when (t2.pcons is null or trim(t2.pcons) = '') and t6.categ_original is not null then t6.categ_final
        when (t2.pcons is null or trim(t2.pcons) = '') and t6.categ_original is null then t1.pcons
        else t2.pcons /*cn*/ end as pcons
    , t1.gerenciado
    , t1.of_prod
    , t1.nueva_region
    , t1.capital_ini
    , t1.sld_cap_final
    , t1.nutitula
    , t1.linea_negocio
    , t3.sld_cap_act
    , t3.sld_cap_ant
    , t3.sld_cap_cierre
    , t3.sld_dolares_act
    , t3.sld_dolares_ant
    , t3.cv_act
    , t3.cv_ant
    , t3.cv_dolares_act
    , t3.cv_dolares_ant
    , t3.sld_cap_dolares_cierre
    , t3.cv_cierre
    , t1.marca_can_cast
    , t1.ajuste_aplc -- Cambio Spiwack
    , t1.ingestion_year
    , t1.ingestion_month
    , t1.ingestion_day

    from pcons_receta t1
    left join ceniegarc t2 on t1.llave1= t2.llave1
    left join {processZone2}.variaciones_saldos_diarios t3 on t1.llave1 = t3.llave1
    left join {processZone2}.sd_parametros t5 on t1.clasificacion = t5.categ_original and t5.parametro_id = 1
    left join {processZone2}.sd_parametros t6 on t1.pcons = t6.categ_original and t6.parametro_id = 2 -- Cruce con pcons sd
    left join {processZone2}.sd_parametros t7 on t2.pcons = t7.categ_original and t7.parametro_id = 2 -- Cruce con pcons cn
    ;

    compute stats {processZone2}.saldos_diarios_erm_pre1;


    drop table if exists {processZone2}.saldos_diarios_erm_pre2 purge;
    create table {processZone2}.saldos_diarios_erm_pre2 stored as parquet as 
    select 
    t1.llave1
    , t1.num_doc
    , t1.tipo_doc
    , t1.cod_ciiu
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
    , t1.cod_clasificacion
    , t1.clasificacion
    , t1.nueva_altura_mora
    , t1.cv
    , t1.cv_dolares
    , t1.c90
    , t1.banca
    , t1.vic_ccial -- Campo reporte Nuevo
    , t1.modalidad
    , t1.plan
    , t1.pd
    , t1.pcons_orig
    , t1.pcons_sufi_orig_sd
    , t1.pcons_cn
    , t1.pcons
    , t1.gerenciado
    , t1.of_prod
    , t1.nueva_region
    , t1.capital_ini
    , t1.sld_cap_final
    , t1.nutitula
    , t1.linea_negocio
    , t1.sld_cap_act
    , t1.sld_cap_ant
    , t1.sld_cap_cierre
    , t1.sld_dolares_act
    , t1.sld_dolares_ant
    , t1.cv_act
    , t1.cv_ant
    , t1.cv_dolares_act
    , t1.cv_dolares_ant
    , t1.sld_cap_dolares_cierre
    , t1.cv_cierre
    , t1.marca_can_cast
    , t1.ajuste_aplc -- Cambio Spiwack
    , t1.ingestion_year
    , t1.ingestion_month
    , t1.ingestion_day
    , if((t1.apli = 'L' and t1.pcons = 'LEASING') or t1.apli = '3',t1.pcons,'No Aplica') as pdto_leasing --Nuevo
    , case when t1.apli = '7' and t1.desc_segm = 'MICROPYME' then 'SUFI' 
            when t1.apli='1' and t1.desc_segm = 'MICROPYME' THEN 'FACTORING'
            when t2.categ_original is not null then t2.categ_final
            when t1.pcons is null and t1.desc_segm = 'MICROPYME' then 'SIN PRODUCTO'
            when t1.pcons is not null and t1.desc_segm = 'MICROPYME' then t1.pcons
            else 'No Aplica' end as producto_mp --pcons para reporte saldos con producto agrupado de NeI (MICROPYME) y usa pcons Nuevo
    , case when t1.apli='1' THEN 'FACTORING'
            when t3.categ_original is not null then t3.categ_final
            when (t1.pcons='' or t1.pcons = 'SIN CLASIFICAR' or t1.pcons is null) THEN 'Sin Producto'
            else t1.pcons
            end as producto    
    , case when t4.categ_original is not null then t4.categ_final
           else t1.desc_segm end as desc_segm_rpte --Reporte Saldos Segm Banca Personas. Con parámetros
    , case when t1.apli='1' then 'FACTORING'
        when t3.categ_original is not null then t3.categ_final
        when (t1.pcons in ('', 'No Aplica') or t1.pcons is null) then 'SIN PRODUCTO'
        else t1.pcons
        end as pcons_reporte_banca -- Reporte Banca
    from {processZone2}.saldos_diarios_erm_pre1 t1
    left join {processZone2}.sd_parametros t2 on t1.pcons = t2.categ_original and t1.desc_segm = 'MICROPYME' and t2.parametro_id = 3
    left join {processZone2}.sd_parametros t3 on t1.pcons = t3.categ_original and t3.parametro_id = 4
    left join {processZone2}.sd_parametros t4 on t1.desc_segm = t4.categ_original and t4.parametro_id = 5
    ;
    compute stats {processZone2}.saldos_diarios_erm_pre2;


    /* OBLIGACIONES APLICATIVO C - AJUSTES MANUALES */
    drop table if exists {processZone}.saldos_diarios_aplc purge; -- Cambio Spiwack
    create table {processZone}.saldos_diarios_aplc stored as parquet as
    with clientes as (
    select distinct t1.num_doc, t1.tipo_doc
        from {processZone2}.insumo_saldos t1
        where t1.ingestion_year=year(cast('{FECHA_CIERRE}' as timestamp)) and t1.ingestion_month=month(cast('{FECHA_CIERRE}' as timestamp)) and t1.ingestion_day = day(cast('{FECHA_CIERRE}' as timestamp))
        and t1.apli = 'C'
    )

    select t1.llave1, t1.obl_cn, t1.obl_sd, t1.apli, t1.num_doc, t1.tipo_doc
            , t1.sld_cap_final ---
            , t1.sld_cap_final_dolares ---
            , t1.nueva_altura_mora ---
            , t1.cv ---
            , t1.cv_dolares ---
            , t1.c90 ---
        from {processZone2}.insumo_saldos t1
        inner join clientes t2 on t1.num_doc = t2.num_doc and t1.tipo_doc = t2.tipo_doc
        where t1.ingestion_year=year(cast('{FECHA_CIERRE}' as timestamp)) and t1.ingestion_month=month(cast('{FECHA_CIERRE}' as timestamp)) and t1.ingestion_day = day(cast('{FECHA_CIERRE}' as timestamp))
    ;

    compute stats {processZone}.saldos_diarios_aplc;

    drop table if exists {processZone2}.saldos_diarios_new_{FECHA_CORTE_NMB} purge;
    create table {processZone2}.saldos_diarios_new_{FECHA_CORTE_NMB} stored as parquet as 
    with 
    cliente_aplc as (
        select distinct num_doc, tipo_doc
        from {processZone}.saldos_diarios_aplc
    )

    select 
      cast('{FECHA_CORTE}' as timestamp) as fecha_dia_actual
    , cast('{FECHA_CORTE_ANT}' as timestamp) as fecha_dia_anterior
    , cast('{FECHA_CIERRE}' as timestamp) as fecha_cierre_mes
    , t1.llave1
    , t1.num_doc
    , t1.tipo_doc
    , t1.nombre
    , t1.segm
    , t1.desc_segm
    , t1.desc_segm_rpte --Reporte Saldos
    , t1.subsegm
    , t1.gte
    , t1.f_desemb
    , t1.f_pp
    , t1.apli
    , t1.obl_sd
    , t1.obl_cn
    , t1.moneda
    , t1.cod_clasificacion
    , t1.clasificacion
    , t1.nueva_altura_mora
    , t1.banca
    , case when t5.categ_original is not null then t5.categ_final
        when t1.desc_segm_rpte ='EMPRESARIAL' then  'Empresas'
        when t1.desc_segm_rpte in ('PYMES', 'GOBIERNO DE RED') then 'Pyme'
        when t1.desc_segm_rpte = 'NeI' then t1.desc_segm_rpte
        else 'Personas'
        end as nueva_banca -- Reporte Banca - Con Param. Actual y Nuevo
    , t1.vic_ccial as vicepresidencia -- Campo reporte Nuevo
    , t1.modalidad
    , t1.plan
    , t1.pd
    , t1.pcons_orig as pcons_sd -- original SD
    , t1.pcons -- pcons calculado nueva versión
    , t1.pcons_sufi_orig_sd as pcons_sufi
    , case when t3.categ_original is not null then t3.categ_final
        when t1.desc_segm_rpte in ('PERSONAL', 'PERSONAL PLUS', 'PREFERENCIAL', 'SIN SEGMENTO') and t1.banca in ('PPyE', 'Sin Banca') then 'Comercial y otros'
        else 'No Aplica'
        end as producto_agr -- Reporte Producto
    , t1.pdto_leasing
    , t1.producto_mp -- Nuevo
    , t1.producto -- Nuevo
    , t1.pcons_reporte_banca -- Nuevo
    , if(t1.pcons = 'LIBRANZA', t2.producto_detallado, 'No Aplica') as prod_det_libranza -- Reporte Libranza - Nuevo Reporte
    , case
        when t4.categ_original is not null then t4.categ_final
        when t1.apli not in ( '1', '7') and t1.pcons = 'CARTERA MICROCREDITO' and (t1.linea_negocio = 'Bancolombia' or t1.linea_negocio is null) -- null para no excluir los del cierre
            then 'Cartera Microcrédito Reclasificada'
        else 'No Aplica' end as clasif_micro -- Reporte Microcrédito
    , t1.gerenciado
    , t1.of_prod
    , t1.nueva_region
    , t1.capital_ini
    , t1.nutitula
    , t1.linea_negocio
    , if(t1.nutitula = 0 or t1.nutitula is null, t1.sld_cap_act, 0) as sld_cap_act
    , if(t1.nutitula = 0 or t1.nutitula is null, t1.sld_cap_ant, 0) as sld_cap_ant
    , t1.sld_cap_cierre
    , (if(t1.nutitula = 0 or t1.nutitula is null, t1.sld_cap_act, 0)-if(t1.nutitula = 0 or t1.nutitula is null, t1.sld_cap_ant, 0)) as var_sld_cap
    , (if(t1.nutitula = 0 or t1.nutitula is null, t1.sld_cap_act, 0) - t1.sld_cap_cierre) as var_sld_cap_cierre
    , if(t1.nutitula = 0 or t1.nutitula is null, t1.sld_dolares_act, 0) as sld_dolares_act
    , if(t1.nutitula = 0 or t1.nutitula is null, t1.sld_dolares_ant, 0) as sld_dolares_ant
    , (if(t1.nutitula = 0 or t1.nutitula is null, t1.sld_dolares_act, 0) - if(t1.nutitula = 0 or t1.nutitula is null, t1.sld_dolares_ant, 0)) as var_sld_dolares
    , if(t1.nutitula = 0 or t1.nutitula is null, t1.cv_act, 0) as cv_act
    , if(t1.nutitula = 0 or t1.nutitula is null, t1.cv_ant, 0) as cv_ant
    , (if(t1.nutitula = 0 or t1.nutitula is null, t1.cv_act, 0) - if(t1.nutitula = 0 or t1.nutitula is null, t1.cv_ant, 0)) as var_cv
    , if(t1.nutitula = 0 or t1.nutitula is null, t1.cv_dolares_act, 0) as cv_dolares_act
    , if(t1.nutitula = 0 or t1.nutitula is null, t1.cv_dolares_ant, 0) as cv_dolares_ant
    , (if(t1.nutitula = 0 or t1.nutitula is null, t1.cv_dolares_act, 0) - if(t1.nutitula = 0 or t1.nutitula is null, t1.cv_dolares_ant, 0)) as var_cv_dolares
    , if(t1.nutitula = 0 or t1.nutitula is null, t1.sld_dolares_act, 0) - t1.sld_cap_dolares_cierre as var_sld_cap_dolares_cierre
    , t1.cv_cierre
    , (if(t1.nutitula = 0 or t1.nutitula is null, t1.cv_act, 0) - t1.cv_cierre) as var_cv_cierre
    , t1.marca_can_cast
    , if(t1.ajuste_aplc is null, 0, t1.ajuste_aplc) as ajuste_aplc -- Cambio Spiwack
    , if(t6.num_doc is null, 0, 1) as marca_aplc -- Cambio Spiwack
    , year(cast('{FECHA_CORTE}' as timestamp)) as ingestion_year
    , month(cast('{FECHA_CORTE}' as timestamp)) as ingestion_month
    , day(cast('{FECHA_CORTE}' as timestamp)) as ingestion_day
    from {processZone2}.saldos_diarios_erm_pre2 t1
    left join {processZone2}.sd_param_libranza t2 on (t1.cod_clasificacion=t2.cod_clase and t1.apli=t2.cod_apli and t1.plan=t2.cod_plan)
    left join {processZone2}.sd_parametros t3 on t1.pcons = t3.categ_original and t3.parametro_id = 7
                and t1.desc_segm_rpte in ('PERSONAL', 'PERSONAL PLUS', 'PREFERENCIAL', 'SIN SEGMENTO') and t1.banca in ('PPyE', 'Sin Banca')
    left join {processZone2}.sd_parametros t4 on t1.plan = t4.categ_original and t4.parametro_id = 8
                and t1.apli not in ('1', '7') and t1.pcons = 'CARTERA MICROCREDITO' and (t1.linea_negocio = 'Bancolombia' or t1.linea_negocio is null)
    left join {processZone2}.sd_parametros t5 on t1.banca = t5.categ_original and t5.parametro_id = 6
    left join cliente_aplc t6 on t1.num_doc = t6.num_doc -- Cambio Spiwack
    ;

    compute stats {processZone2}.saldos_diarios_new_{FECHA_CORTE_NMB};