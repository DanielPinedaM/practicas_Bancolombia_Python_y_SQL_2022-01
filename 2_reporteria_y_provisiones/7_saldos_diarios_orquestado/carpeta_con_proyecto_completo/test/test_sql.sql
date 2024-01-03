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
   WHERE t1.ingestion_year=year(cast('2021-08-30' AS TIMESTAMP))
     AND t1.ingestion_month=month(cast('2021-08-30' AS TIMESTAMP))
     AND t1.ingestion_day = day(cast('2021-08-30' AS TIMESTAMP))
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
   FROM proceso_riesgos.saldos_diarios_erm
   WHERE fuente = 'SALDOS_DIARIOS'
   UNION ALL SELECT t1.llave1,
                    t1.apli,
                    t1.sld_cap_final,
                    t1.sld_cap_final_dolares,
                    t1.cv,
                    t1.cv_dolares,
                    t1.ingestion_year,
                    t1.ingestion_month,
                    t1.ingestion_day
   FROM proceso_riesgos.insumo_saldos t1
   LEFT JOIN obl_cn t3 ON t1.llave1 = t3.llave1
   AND t1.num_doc = t3.num_doc
   AND t1.sld_cap_final = t3.sld_cap_final
   AND t1.sld_cap_final_dolares = t3.sld_cap_final_dolares
   WHERE t1.ingestion_year=year(cast('2021-10-14' AS TIMESTAMP))
     AND t1.ingestion_month=month(cast('2021-10-14' AS TIMESTAMP))
     AND t1.ingestion_day = day(cast('2021-10-14' AS TIMESTAMP))
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
 ),
 
 final as(SELECT llave1,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('2021-10-17' AS TIMESTAMP))
                                AND ingestion_month=month(cast('2021-10-17' AS TIMESTAMP))
                                AND ingestion_day = day(cast('2021-10-17' AS TIMESTAMP)) THEN sld_cap_final
                       END),0) AS sld_cap_Act,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('2021-10-14' AS TIMESTAMP))
                                AND ingestion_month=month(cast('2021-10-14' AS TIMESTAMP))
                                AND ingestion_day = day(cast('2021-10-14' AS TIMESTAMP)) THEN sld_cap_final
                       END),0) AS sld_cap_Ant,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('2021-10-17' AS TIMESTAMP))
                                AND ingestion_month=month(cast('2021-10-17' AS TIMESTAMP))
                                AND ingestion_day = day(cast('2021-10-17' AS TIMESTAMP)) THEN sld_cap_final_dolares
                       END),0) AS sld_dolares_Act,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('2021-10-14' AS TIMESTAMP))
                                AND ingestion_month=month(cast('2021-10-14' AS TIMESTAMP))
                                AND ingestion_day = day(cast('2021-10-14' AS TIMESTAMP)) THEN sld_cap_final_dolares
                       END),0) AS sld_dolares_Ant,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('2021-08-30' AS TIMESTAMP))
                                AND ingestion_month=month(cast('2021-08-30' AS TIMESTAMP)) THEN sld_cap_final
                       END),0) AS sld_cap_cierre,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('2021-08-30' AS TIMESTAMP))
                                AND ingestion_month=month(cast('2021-08-30' AS TIMESTAMP)) THEN sld_cap_final_dolares
                       END),0) AS sld_cap_dolares_cierre,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('2021-10-17' AS TIMESTAMP))
                                AND ingestion_month=month(cast('2021-10-17' AS TIMESTAMP))
                                AND ingestion_day = day(cast('2021-10-17' AS TIMESTAMP)) THEN cv
                       END),0) AS cv_Act,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('2021-10-14' AS TIMESTAMP))
                                AND ingestion_month=month(cast('2021-10-14' AS TIMESTAMP))
                                AND ingestion_day = day(cast('2021-10-14' AS TIMESTAMP)) THEN cv
                       END),0) AS cv_Ant,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('2021-10-17' AS TIMESTAMP))
                                AND ingestion_month=month(cast('2021-10-17' AS TIMESTAMP))
                                AND ingestion_day = day(cast('2021-10-17' AS TIMESTAMP)) THEN cv_dolares
                       END),0) AS cv_dolares_Act,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('2021-10-14' AS TIMESTAMP))
                                AND ingestion_month=month(cast('2021-10-14' AS TIMESTAMP))
                                AND ingestion_day = day(cast('2021-10-14' AS TIMESTAMP)) THEN cv_dolares
                       END),0) AS cv_dolares_Ant,
          coalesce(sum(CASE
                           WHEN ingestion_year=year(cast('2021-08-30' AS TIMESTAMP))
                                AND ingestion_month=month(cast('2021-08-30' AS TIMESTAMP)) THEN cv
                       END),0) AS cv_cierre
   FROM base
   WHERE apli NOT IN ('2')

   GROUP BY llave1)

   SELECT CAST(sum(sld_cap_act)/1000000 AS INT) as sk_hoy,
       CAST(sum(sld_cap_ant)/1000000 AS INT) as sk_ayer,
       CAST(sum(cv_act)/1000000 AS INT) as cv_hoy,
       CAST(sum(cv_ant)/1000000 AS INT) as cv_ayer FROM final
       ;





--------------------------------------------------------------------------------------------------------------------------------




           with 
    obl_cn as (
        select t1.llave1, t1.obl_cn, t1.obl_sd, t1.apli, t1.num_doc, t1.tipo_doc
            , t1.sld_cap_final -- Cambio Spiwack
            , t1.sld_cap_final_dolares -- Cambio Spiwack
            , t1.nueva_altura_mora -- Cambio Spiwack
            , t1.cv -- Cambio Spiwack
            , t1.cv_dolares -- Cambio Spiwack
            , t1.c90 -- Cambio Spiwack
        from proceso_riesgos.insumo_saldos t1
        where t1.ingestion_year=year(cast('2021-08-30' as timestamp)) and t1.ingestion_month=month(cast('2021-08-30' as timestamp)) and t1.ingestion_day = day(cast('2021-08-30' as timestamp))
        and t1.apli not in ('2')
    ),

    filtra_cartera as (
    select t1.fuente
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
    from proceso_riesgos.insumo_saldos t1
    left join obl_cn t2 on t1.llave1 = t2.llave1
    left join obl_cn t3 
        on t1.num_doc = t3.num_doc and t1.sld_cap_final = t3.sld_cap_final and t1.sld_cap_final_dolares = t3.sld_cap_final_dolares
    where t1.ingestion_year=year(cast('2021-10-17' as timestamp)) and t1.ingestion_month=month(cast('2021-10-17' as timestamp)) and t1.ingestion_day = day(cast('2021-10-17' as timestamp))
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
    from proceso_riesgos.saldos_diarios_cancast t1
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
    left join proceso_riesgos.insumo_erm_vic_ccial t2 on t1.num_doc=cast(t2.num_doc as bigint)
    left join proceso_riesgos.master_saldos_diarios t3 on (t1.num_doc=cast(t3.num_doc as bigint)) and trim(t1.tipo_doc)=trim(t3.cod_tipo_doc)
    left join proceso_riesgos.insumo_erm_matriz_corporativa t4 on cast(t1.gte as int)=cast(t4.cod_gte as int)
    left join proceso_riesgos.arbolobl_saldos_diarios t5 on (cast(t3.cod_ciiu as string) = cast(t5.cod_ciiu as string))
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

      SELECT ingestion_day,
      SUM(sld_cap_final)/1000000,
      SUM(cv)/1000000 FROM final GROUP BY 1

    ;