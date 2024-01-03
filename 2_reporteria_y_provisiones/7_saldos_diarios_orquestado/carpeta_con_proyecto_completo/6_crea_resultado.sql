/*
=============================================
    Autores:            Vanessa Osorio Urrea (vaosorio)
                        Duverney Londoño Sanchez (dulondon)
    Fecha de Modificación:  02/08/2021
    Descripción:        Crea estructura final del reporte para publicar en Resultados Riesgos. Contiene la comparación del día actual con la información 
                        del cierre y del día anterior. Adicional las agrupaciones necesarias para las diferentes vistas en los reportes de seguimiento.
    Dependencias:       
                        
=============================================
*/

    drop table if exists {processZone2}.reporte_saldos_diarios_{FECHA_CORTE_NMB} purge;
    create table {processZone2}.reporte_saldos_diarios_{FECHA_CORTE_NMB} stored as parquet as 
    -- with fechas_existentes as (
    --     select distinct
    --         fecha_dia_actual
    --     from resultados_riesgos.reporte_saldos_diarios
    --     where year = year(cast('{FECHA_CORTE}' as timestamp)) and month = month(cast('{FECHA_CORTE}' as timestamp)) and day = day(cast('{FECHA_CORTE}' as timestamp))
    -- )
    select 
        cast( fecha_dia_actual as timestamp) fecha_dia_actual
        ,cast( fecha_dia_anterior as timestamp) fecha_dia_anterior
        ,cast( fecha_cierre_mes as timestamp) fecha_cierre_mes
        ,cast( llave1 as string) llave1
        ,cast( num_doc as bigint) num_doc
        ,cast( tipo_doc as string) tipo_doc
        ,cast( nombre as string) nombre
        ,cast( segm as string) segm
        ,cast( desc_segm as string) desc_segm
        ,cast( desc_segm_rpte as string) desc_segm_rpte
        ,cast( subsegm as string) subsegm
        ,cast( gte as double) gte
        ,cast( f_desemb as double) f_desemb
        ,cast( f_pp as double) f_pp
        ,cast( apli as string) apli
        ,cast( obl_sd as string) obl_sd
        ,cast( obl_cn as string) obl_cn
        ,cast( moneda as double) moneda
        ,cast( cod_clasificacion as string) cod_clasificacion
        ,cast( clasificacion as string) clasificacion
        ,cast( nueva_altura_mora as int) nueva_altura_mora
        ,cast( banca as string) banca
        ,cast( nueva_banca as string) nueva_banca
        ,cast( vicepresidencia as string) vicepresidencia
        ,cast( modalidad as string) modalidad
        ,cast( plan as string) plan
        ,cast( pd as string) pd
        ,cast( pcons_sd as string) pcons_sd
        ,cast( pcons as string) pcons
        ,cast( pcons_sufi as string) pcons_sufi
        ,cast( producto_agr as string) producto_agr
        ,cast( pdto_leasing as string) pdto_leasing
        ,cast( producto_mp as string) producto_mp
        ,cast( producto as string) producto
        ,cast( pcons_reporte_banca as string) pcons_reporte_banca
        ,cast( prod_det_libranza as string) prod_det_libranza
        ,cast( clasif_micro as string) clasif_micro
        ,cast( gerenciado as string) gerenciado
        ,cast( of_prod as double) of_prod
        ,cast( nueva_region as string) nueva_region
        ,cast( capital_ini as double) capital_ini
        ,cast( nutitula as double) nutitula
        ,cast( linea_negocio as string) linea_negocio
        ,cast( sld_cap_act as double) sld_cap_act
        ,cast( sld_cap_ant as double) sld_cap_ant
        ,cast( sld_cap_cierre as double) sld_cap_cierre
        ,cast( var_sld_cap as double) var_sld_cap
        ,cast( var_sld_cap_cierre as double) var_sld_cap_cierre
        ,cast( sld_dolares_act as double) sld_dolares_act
        ,cast( sld_dolares_ant as double) sld_dolares_ant
        ,cast( var_sld_dolares as double) var_sld_dolares
        ,cast( cv_act as double) cv_act
        ,cast( cv_ant as double) cv_ant
        ,cast( var_cv as double) var_cv
        ,cast( cv_dolares_act as double) cv_dolares_act
        ,cast( cv_dolares_ant as double) cv_dolares_ant
        ,cast( var_cv_dolares as double) var_cv_dolares
        ,cast( var_sld_cap_dolares_cierre as double) var_sld_cap_dolares_cierre
        ,cast( cv_cierre as double) cv_cierre
        ,cast( var_cv_cierre as double) var_cv_cierre
        ,cast( marca_can_cast as string) marca_can_cast
        ,cast( ingestion_year as int) ingestion_year
        ,cast( ingestion_month as int) ingestion_month
        ,cast( ingestion_day as int) ingestion_day
        ,cast( ajuste_aplc as int ) ajuste_aplc -- Cambio Spiwack
        ,cast( marca_aplc as int ) marca_aplc -- Cambio Spiwack

    from {processZone2}.saldos_diarios_new_{FECHA_CORTE_NMB} t1
    -- Asegura de no generar una ejecución ya existente
    -- left anti join fechas_existentes t2
    --             on t1.fecha_dia_actual = t2.fecha_dia_actual
    ;

    compute stats {processZone2}.reporte_saldos_diarios_{FECHA_CORTE_NMB};