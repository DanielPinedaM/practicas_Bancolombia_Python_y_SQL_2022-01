/* Codigo q me paso Gian Paul

1) Crear tabla proceso.recuperaciones_capa 

Cruzar el archivo base_consolidada_mes-año.xlsx por obligación contra la base de master_credit_risk */
DROP TABLE IF EXISTS proceso_generadores.RECUPERACIONES_CAPA PURGE
;

CREATE TABLE proceso_generadores.RECUPERACIONES_CAPA STORED AS PARQUET AS 

WITH /* crear una tabla temporal llamada CAPA */

CAPA AS (
SELECT * FROM  (SELECT 
          ROW_NUMBER() OVER(PARTITION BY oblig,num_doc
                         ORDER BY (ingestion_year*100+ingestion_month) DESC) AS numero,
          num_doc,
          SUBSTR(oblig, 3, LENGTH(oblig)-1)AS oblig,
          producto_agrupado AS PRODUCTO_AGRUPADO,
          banca AS banca,
          COD_CLASE AS MODalidad,
          desc_segm AS desc_segm,
          vic_ccial AS vic_ccial

   FROM resultados_riesgos.master_credit_risk)
  BASE
WHERE numero=1)

SELECT X.BASE,
       2022 AS ANHO, 
       01 AS MES, 
       X.ID,
       X.OBL17,
       V.PRODUCTO_AGRUPADO AS PRODUCTO_AJUSTADO,
       V.BANCA AS BANCA_AJUSTADA,
       v.vic_ccial,
       V.MODALIDAD,
       V.DESC_SEGM AS SGTO_AJUSTADO,
       X.MONTO,
       X.aplicativo,
       X.amcdty,
       X.cncdbi
FROM proceso_generadores.base_consolidada_anio_mes X
LEFT JOIN CAPA V ON CAST(X.OBL17 AS DOUBLE)=CAST(V.oblig AS DOUBLE)
AND CAST(X.ID AS BIGINT)=CAST(V.num_doc AS BIGINT)
;


-----------------------------------------------------------------------------------------------

/* 2) Crear tabla proceso.recuperaciones_cenie 

Cruzar base_consolidada_mes-año.xlsx contra el ceniegarc_lz */
DROP TABLE IF EXISTS proceso_generadores.RECUPERACIONES_CENIE PURGE
;

CREATE TABLE proceso_generadores.RECUPERACIONES_CENIE STORED AS PARQUET AS 
WITH 

CENIE AS (
 SELECT * FROM  (SELECT ROW_NUMBER() OVER(PARTITION BY obl341,id
                         ORDER BY (ingestion_year*100+ingestion_month) DESC) AS numero,
       id,
       obl341,
       pcons AS pcons,
       banca AS banca,
       vic_ccial AS vic_ccial,
       clf AS modalidad,
       segdesc AS segdesc
   FROM resultados_riesgos.ceniegarc_lz) BASE WHERE numero=1)
SELECT x.base,
       2022 AS anho, 
       01 AS mes, 
       x.id,
       x.obl17,
       v.pcons AS producto_ajustado,
       v.banca AS banca_ajustada,
       v.vic_ccial AS vic_ccial,
       v.modalidad,
       v.segdesc AS sgto_ajustado,
       x.monto,
       X.aplicativo,
       X.amcdty,
       X.cncdbi
FROM proceso_generadores.RECUPERACIONES_CAPA X
LEFT JOIN CENIE V ON CAST(X.OBL17 AS DOUBLE)=CAST(V.OBL341 AS DOUBLE)
AND CAST(X.ID AS DOUBLE)=CAST(V.ID AS DOUBLE)
WHERE X.PRODUCTO_AJUSTADO IS NULL
;

-----------------------------------------------------------------------------------------------

/* 3) Crear tabla proceso.informe_recu_2020102_v2
Consulta que parametriza los campos */
DROP TABLE IF EXISTS proceso_generadores.informe_recu_2020102_v2 PURGE
;

CREATE TABLE proceso_generadores.informe_recu_2020102_v2 STORED AS PARQUET AS


WITH 

BASE AS
  (SELECT *
   FROM proceso_generadores.RECUPERACIONES_CAPA
   WHERE PRODUCTO_AJUSTADO IS NOT NULL
   
   UNION ALL 
   SELECT *
   FROM proceso_generadores.RECUPERACIONES_CENIE
   )
   
SELECT base,
       anho,
       mes,
       id,
       lpad(cast(obl17 AS string),
            17,
            '0') AS obl17,
       CASE
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '37781%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '40998%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '4491%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '45130%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '53037%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '5491%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '53069%' THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IS NULL
                AND CAST(obl17 AS STRING) LIKE '10000%' THEN 'LEASING'
           WHEN producto_ajustado IN ('Cartera Consumo',
                                      'Vehículo',
                                      'LIBRE INVERSION')
                AND modalidad = '2' THEN 'LIBRE INVERSION'
           WHEN producto_ajustado IN ('Cartera Comercial',
                                      'Vehículo',
                                      'CARTERA ORDINARIA')
                AND modalidad = '1' THEN 'CARTERA ORDINARIA'
           WHEN producto_ajustado IN ('Credipago virtual') THEN 'CREDIPAGO'
           WHEN producto_ajustado IN ('Ex-Empleado Libranza',
                                      'Libranza',
                                      'LIBRANZA')
                AND modalidad = '2' THEN 'LIBRANZA'
           WHEN producto_ajustado IN ('Tarjeta de Crédito',
                                      'TARJETA DE CREDITO') THEN 'TARJETA DE CREDITO'
           WHEN producto_ajustado IN ('CCT SUFI',
                                      'CPE CORTO PLAZO SUFI',
                                      'CPE LARGO PLAZO SUFI',
                                      'MOTOS GAMA BAJA SUFI',
                                      'COTIDIANIDAD') THEN 'COTIDIANIDAD'
           WHEN producto_ajustado IN ('MOVILIDAD',
                                      'VEHÍCULOS SUFI') THEN 'MOVILIDAD'
           ELSE upper(producto_ajustado)
       END AS producto_ajustado,
       CASE
           WHEN banca_ajustada = 'Personas y Pyme' THEN 'NEPYP'
           WHEN banca_ajustada = 'PPyE' THEN 'NEPYP'
           WHEN banca_ajustada = 'Empresarial' THEN 'NEPYP'
           WHEN banca_ajustada IN ('BEG',
                                   'VEG')
                AND sgto_ajustado = 'EMPRESARIAL' THEN 'NEPYP'
           WHEN banca_ajustada IN ('BEG',
                                   'VEG')
                AND sgto_ajustado != 'EMPRESARIAL' THEN 'NCORP'
           ELSE banca_ajustada
       END AS banca_ajustada,
       CASE
           WHEN vic_ccial IS NULL
                AND banca_ajustada = 'Personas y Pyme'
                AND sgto_ajustado NOT IN ('GOBIERNO DE RED',
                                          'PYMES',
                                          'NEGOCIOS E INDEPEND',
                                          'MICROFINANZAS',
                                          'MI NEGOCIO',
                                          'MICROPYME') THEN 'PERSONAS'
           WHEN vic_ccial IS NULL
                AND banca_ajustada = 'Personas y Pyme'
                AND sgto_ajustado NOT IN ('GOBIERNO DE RED',
                                          'PYMES',
                                          'NEGOCIOS E INDEPEND',
                                          'MICROFINANZAS',
                                          'MI NEGOCIO',
                                          'MICROPYMES') THEN 'PYMES'
           ELSE upper(vic_ccial)
       END AS vic_ccial,
       modalidad,
       CASE
           WHEN sgto_ajustado IN ('NEGOCIOS E INDEPEND',
                                  'MICROFINANZAS',
                                  'MI NEGOCIO',
                                  'MICROPYME') THEN 'NEGOCIOS & INDEPEND'
           WHEN sgto_ajustado IN ('PLUS',
                                  'PERSONAL PLUS') THEN 'PLUS'
           ELSE UPPER(sgto_Ajustado)
       END AS sgto_ajustado,
       monto,
       aplicativo,
       amcdty,
       cncdbi
FROM base 
;

-----------------------------------------------------------------------------------------------
