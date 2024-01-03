-----------------------------------------------------------------------------------------------

/* 
Al PRINCIPIO las elimino por si ya han sido creadas anteriormente
Eliminar las tablas q tienen cada uno de los pasos por separado (cuando las tablas no se han unido) 
*/

DROP TABLE IF EXISTS proceso_riesgos.tabla_1_cafe purge;
DROP TABLE IF EXISTS proceso_riesgos.tabla_2_cafe purge;
DROP TABLE IF EXISTS proceso_riesgos.tabla_3_cafe purge;
DROP TABLE IF EXISTS proceso_riesgos.tabla_4_y_5_cafe purge;
-- El df_resultado_paso_6 ya esta en la LZ - HUE (base de datos de Bancolombia) - resultados_canales.tdc_trm_hist
DROP TABLE IF EXISTS proceso_riesgos.tabla_7_cafe purge;
DROP TABLE IF EXISTS proceso_riesgos.tabla_8_cafe purge;
DROP TABLE IF EXISTS proceso_riesgos.tabla_9_cafe purge;

-----------------------------------------------------------------------------------------------

/*  Eliminar tabla de la ETL */
DROP TABLE IF EXISTS proceso_riesgos.tbl_ficha_sector_cafe purge
;

-----------------------------------------------------------------------------------------------
/* 
La ETL se sube a la zona (base de datos) proceso_riesgos
y la tabla de la ETL se llama tbl_ficha_sector_cafe

Crear tabla de la ETL - esta es la tabla final que une todas las tablas */

CREATE TABLE proceso_riesgos.tbl_ficha_sector_cafe stored as parquet as

WITH unir_tabla AS 
(
               /*tabla 1*/
        SELECT fecha,  
               departamento, 
               tecnificacion, 
               cantidad_anual_ha, 
               unidad_ha, 
               /*tabla 2*/
               0 as produccion_mensual_sacos,
               0 as exportacion_mensual_sacos, 
               '' as unidad_produccion_y_exportacion_mensual_sacos, 
               /*tabla 3*/ 
               '' as pais_destino,
               '' as exportador, 
               0 as cantidad_sacos_exportacion, 
               '' as unidad_cantidad_sacos_exportacion, 
               /*tablas 4 y 5*/
               0 as precio_oic,
               0 as precio_suaves_col, 
               0 as precio_internacional, 
               0 as prima_precio_colombia, 
               0 as precio_otros_suaves,
               0 as precio_naturales_brasil, 
               0 as precio_robustas, 
               '' as unidad_precio_suaves_naturales_robustas_prima, 
               /*tabla 7*/
               0 as precio_interno, 
               '' as unidad_precio_interno, 
               /*tabla 8*/ 
               0 as ipp_cafe, 
               '' as unidad_ipp_cafe, 
               /*tabla 9*/
               0 as ipc_cafe, 
               '' as unidad_ipc_cafe
FROM proceso_riesgos.tabla_1_cafe

UNION ALL

SELECT /*tabla 1*/ 
       fecha,  
       '' as departamento, 
       '' as tecnificacion, 
       0 as cantidad_anual_ha, 
       '' as unidad_ha,
       /*tabla 2*/ 
       produccion_mensual_sacos, 
       exportacion_mensual_sacos, 
       unidad_produccion_y_exportacion_mensual_sacos,
       /*tabla 3*/ 
       '' as pais_destino, 
       '' as exportador, 
       0 as cantidad_sacos_exportacion, 
       '' as unidad_cantidad_sacos_exportacion,
       /*tablas 4 y 5*/
       0 as precio_oic,
       0 as precio_suaves_col, 
       0 as precio_internacional, 
       0 as prima_precio_colombia, 
       0 as precio_otros_suaves,
       0 as precio_naturales_brasil, 
       0 as precio_robustas, 
       '' as unidad_precio_suaves_naturales_robustas_prima,
       /*tabla 7*/
       0 as precio_interno, 
       '' as unidad_precio_interno,
       /*tabla 8*/ 
       0 as ipp_cafe, 
       '' as unidad_ipp_cafe,
       /*tabla 9*/
       0 as ipc_cafe, '' as unidad_ipc_cafe
FROM proceso_riesgos.tabla_2_cafe

UNION all

SELECT /*tabla 1*/ 
       fecha, 
       '' as departamento, 
       '' as tecnificacion, 
       0 as cantidad_anual_ha, 
       '' as unidad_ha,
       /*tabla 2*/ 
       0 as produccion_mensual_sacos, 
       0 as exportacion_mensual_sacos, 
       '' as  unidad_produccion_y_exportacion_mensual_sacos,
       /*tabla 3*/ 
       pais_destino, 
       exportador,
       cantidad_sacos_exportacion,
       unidad_cantidad_sacos_exportacion,
       /*tablas 4 y 5*/ 
       0 as precio_oic,
       0 as precio_suaves_col, 
       0 as precio_internacional, 
       0 as prima_precio_colombia, 
       0 as precio_otros_suaves,
       0 as precio_naturales_brasil, 
       0 as precio_robustas, 
       '' as unidad_precio_suaves_naturales_robustas_prima,
       /*tabla 7*/
       0 as precio_interno, 
       '' as unidad_precio_interno,
       /*tabla 8*/ 
       0 as ipp_cafe, 
       '' as unidad_ipp_cafe,
       /*tabla 9*/
       0 as ipc_cafe, 
       '' as unidad_ipc_cafe
FROM proceso_riesgos.tabla_3_cafe

UNION ALL

SELECT /*tabla 1*/ 
      fecha,  
      '' as departamento, 
      '' as tecnificacion, 
      0 as cantidad_anual_ha, 
      '' as unidad_ha,
      /*tabla 2*/ 
      0 as produccion_mensual_sacos, 
      0 as exportacion_mensual_sacos, 
      '' as  unidad_produccion_y_exportacion_mensual_sacos,
      /*tabla 3*/ 
      '' as pais_destino, 
      '' as exportador, 
      0 as cantidad_sacos_exportacion, 
      '' as unidad_cantidad_sacos_exportacion,
      /*tablas 4 y 5*/ 
      precio_oic,
      precio_suaves_col, 
      precio_internacional, 
      prima_precio_colombia, 
      precio_otros_suaves,
      precio_naturales_brasil, precio_robustas,  unidad_precio_suaves_naturales_robustas_prima,
      /*tabla 7*/
      0 as precio_interno, 
      '' as unidad_precio_interno,
      /*tabla 8*/ 
      0 as ipp_cafe, 
      '' as unidad_ipp_cafe,
      /*tabla 9*/
      0 as ipc_cafe, 
      '' as unidad_ipc_cafe
FROM proceso_riesgos.tabla_4_y_5_cafe

UNION ALL

SELECT /*tabla 1*/ 
       fecha, 
       '' as departamento, 
       '' as tecnificacion, 
       0 as cantidad_anual_ha, 
       '' as unidad_ha, 
       /*tabla 2*/ 
       0 as produccion_mensual_sacos, 
       0 as exportacion_mensual_sacos, 
       '' as  unidad_produccion_y_exportacion_mensual_sacos,
       /*tabla 3*/ 
       '' as pais_destino, 
       '' as exportador, 
       0 as cantidad_sacos_exportacion, 
       '' as unidad_cantidad_sacos_exportacion,
       /*tablas 4 y 5*/ 
       0 as precio_oic,
       0 as precio_suaves_col, 
       0 as precio_internacional,
       0 as prima_precio_colombia, 
       0 as precio_otros_suaves,
       0  as precio_naturales_brasil, 
       0 as precio_robustas, 
       '' as unidad_precio_suaves_naturales_robustas_prima,
       /*tabla 7*/
       precio_interno, unidad_precio_interno,
      /*tabla 8*/ 
      0 as ipp_cafe, 
      '' as unidad_ipp_cafe,
      /*tabla 9*/
      0 as ipc_cafe, '' as unidad_ipc_cafe
      from proceso_riesgos.tabla_7_cafe

UNION ALL

SELECT /*tabla 1*/ 
       fecha, 
       '' as departamento, 
       '' as tecnificacion, 
       0 as cantidad_anual_ha, 
       '' as unidad_ha, 
       /*tabla 2*/ 
       0 as produccion_mensual_sacos, 
       0 as exportacion_mensual_sacos, 
       '' as  unidad_produccion_y_exportacion_mensual_sacos,
       /*tabla 3*/ '' 
       as pais_destino, 
       '' as exportador, 
       0 as cantidad_sacos_exportacion, 
       '' as unidad_cantidad_sacos_exportacion,
       /*tablas 4 y 5*/ 
       0 as precio_oic,
       0 as precio_suaves_col, 
       0 as precio_internacional, 
       0 as prima_precio_colombia, 
       0 as precio_otros_suaves,
       0 as precio_naturales_brasil, 
       0 as precio_robustas, 
       '' as unidad_precio_suaves_naturales_robustas_prima,
       /*tabla 7*/
       0 as precio_interno, 
       '' as unidad_precio_interno,
       /*tabla 8*/ 
       ipp_cafe, 
       unidad_ipp_cafe,
       /*tabla 9*/
       0 as ipc_cafe, '' as unidad_ipc_cafe
FROM proceso_riesgos.tabla_8_cafe

UNION ALL

SELECT /*tabla 1*/
       fecha,  
       '' as departamento, 
       '' as tecnificacion, 
       0 as cantidad_anual_ha, 
       '' as unidad_ha,
       /*tabla 2*/ 
       0 as produccion_mensual_sacos, 
       0 as exportacion_mensual_sacos, 
       '' as unidad_produccion_y_exportacion_mensual_sacos,
       /*tabla 3*/ 
       '' as pais_destino, 
       '' as exportador, 
       0 as cantidad_sacos_exportacion, 
       '' as unidad_cantidad_sacos_exportacion,
       /*tabla_4_y_5*/ 
       0 as precio_oic,
       0 as precio_suaves_col, 
       0 as precio_internacional, 
       0 as prima_precio_colombia, 
       0 as precio_otros_suaves,
       0 as precio_naturales_brasil, 
       0 as precio_robustas, 
       '' as unidad_precio_suaves_naturales_robustas_prima,
       /*tabla 7*/
       0 as precio_interno, 
       '' as unidad_precio_interno,
       /*tabla 8*/ 
       0 as ipp_cafe, 
       '' as unidad_ipp_cafe,
       /*tabla 9*/
       ipc_cafe, 
       unidad_ipc_cafe
FROM proceso_riesgos.tabla_9_cafe
)

SELECT /*fecha de ingestion*/
      year(now()) as ingestion_year,
      month(now()) as ingestion_month,
      day(now()) as ingestion_day,
      t1.*,
      year(now()) as year,
      month(now()) as month
from unir_tabla t1;
compute stats proceso_riesgos.tbl_ficha_sector_cafe
;

-----------------------------------------------------------------------------------------------
/* 
Al FINAL las elimino para q no queden guardas tablas temporales en la base de datos del banco
Eliminar las tablas q tienen cada uno de los pasos por separado (cuando las tablas no se han unido) 
*/

DROP TABLE IF EXISTS proceso_riesgos.tabla_1_cafe purge;
DROP TABLE IF EXISTS proceso_riesgos.tabla_2_cafe purge;
DROP TABLE IF EXISTS proceso_riesgos.tabla_3_cafe purge;
DROP TABLE IF EXISTS proceso_riesgos.tabla_4_y_5_cafe purge;
-- El df_resultado_paso_6 ya esta en la LZ - HUE (base de datos de Bancolombia) - resultados_canales.tdc_trm_hist
DROP TABLE IF EXISTS proceso_riesgos.tabla_7_cafe purge;
DROP TABLE IF EXISTS proceso_riesgos.tabla_8_cafe purge;
DROP TABLE IF EXISTS proceso_riesgos.tabla_9_cafe purge;

-----------------------------------------------------------------------------------------------
