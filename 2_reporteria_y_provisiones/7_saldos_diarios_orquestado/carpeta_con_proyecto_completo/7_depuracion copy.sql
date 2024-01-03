/*
=============================================
    Autores:            Vanessa Osorio Urrea (vaosorio)
                        Duverney Londoño Sanchez (dulondon)
    Fecha de Modificación:  02/08/2021
    Descripción:        Depura tablas temporales
    Dependencias:       
                        
=============================================
*/

/* Parámetros */
drop table if exists proceso_riesgos.sd_parametros purge;
drop table if exists proceso.saldos_diarios_rangos_apl purge;

/* 1_Ingestiones */
drop table if exists {processZone}.fechas_saldos_act purge;
drop table if exists {processZone}.fechas_cierre purge;
drop table if exists {processZone}.insumo_saldos purge;
drop table if exists proceso.fechas_trm  purge ;

/* 2_Insumos Saldos */
drop table if exists {processZone2}.arbolobl_saldos_diarios purge;
drop table if exists {processZone2}.sd_param_libranza purge;
drop table if exists {processZone2}.insumo_erm_vic_ccial purge;
drop table if exists {processZone2}.insumo_erm_matriz_corporativa purge;
drop table if exists {processZone2}.insumo_saldos_receta purge;
drop table if exists {processZone2}.insumo_base_alivios purge;
drop table if exists {processZone}.insumo_saldos_hoy purge;
drop table if exists {processZone}.insumo_saldos_ayer purge;
drop table if exists  {processZone2}.insumo_cenie purge;
drop table if exists {processZone2}.insumo_ceniegarc purge;
drop table if exists {processZone2}.insumo_saldos purge;

/* 3_consulta_master */
drop table if exists {processZone}.master_saldos_diarios_act purge;


/* 4_query_saldos */
drop table if exists {processZone2}.saldos_diarios_cancast purge;
drop table if exists {processZone2}.saldos_diarios_erm purge;
drop table if exists {processZone2}.variaciones_saldos_diarios purge;
drop table if exists {processZone2}.saldos_diarios_erm_pre1 purge;
drop table if exists {processZone2}.saldos_diarios_erm_pre2 purge;
-- drop table if exists {processZone2}.saldos_diarios_new_{FECHA_CORTE_NMB} purge; -- Revisar

/* 5_reportes  -- Revisar
drop table if exists {processZone2}.reporte_saldos_diarios_all_new purge;
drop table if exists {processZone}.reporte_sd_ppales_desem purge;
drop table if exists {processZone}.reporte_sd_ppales_cancelaciones purge;
drop table if exists {processZone}.reporte_sd_planta_ccial purge; 
drop table if exists {processZone}.reporte_sd_gerentes purge;
drop table if exists {processZone}.sd_ppales_vencidos purge;
drop table if exists {processZone}.sd_ult_gestion purge;
drop table if exists {processZone}.reporte_sd_ppales_vencidos purge;
drop table if exists {processZone}.reporte_sd_ppales_recuperados purge;*/