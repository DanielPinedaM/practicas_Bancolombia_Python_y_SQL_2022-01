
DROP TABLE IF EXISTS proceso.LEFT_JOIN_punto_1 PURGE
;

-----------------------------------------------------------------------------------------------

CREATE TABLE proceso.LEFT_JOIN_punto_1 STORED AS PARQUET AS  

WITH hipo AS 
(
SELECT *,
       ROW_NUMBER() OVER(PARTITION BY numero_de_identificacion_del_cliente
                         ORDER BY numero_de_identificacion_del_cliente ASC
                         ) AS numero
FROM proceso.hipotecario_base_inicial
),

/* seleccionar SOLAMENTE la primera vez q aparece cada uno de los registros */
hipo2 AS
(SELECT *
 FROM hipo
 WHERE numero=1
)

/* seleccionar las columnas q tendra la tabla resultante del LEFT JOIN */
SELECT /* t1.*, */ /* necesito todas las columnas de la tabla 1 proceso.informe_recu_2020102_v2 */
       t1.id AS id_cliente, /* como proceso.informe_recu_2020102_v2 es la tabla con mas filas entonces de aqui saco los ID de la tabla resultante (LEFT JOIN) */
       t2.clasificacion_de_cartera,
       t1.modalidad,
       t1.producto_ajustado,
       t1.sgto_ajustado,
       t1.vic_ccial,
       t1.banca_ajustada,
       t1.base,
       t1.anho,
       t1.mes,
       t1.obl17,
       t1.monto,
       t1.aplicativo,
       t1.amcdty,
       t1.cncdbi
FROM proceso.informe_recu_2020102_v2 AS t1
LEFT JOIN hipo2 AS t2 
ON t1.id = t2.numero_de_identificacion_del_cliente /* Cruzar por ID */
ORDER BY 1 ASC /* Hacer q el ID se ordene asi: Mostrar primero los numeros y despues (de ultimo) los id NULL / orden ascendente (de menor a mayor) */
;

-----------------------------------------------------------------------------------------------
COMPUTE STATS proceso.LEFT_JOIN_punto_1
;
-----------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS proceso.rellenar_base_hipotecario PURGE
;

-----------------------------------------------------------------------------------------------

/* crear la tabla final q contiene la solucion del punto 1 */
CREATE TABLE proceso.rellenar_base_hipotecario STORED AS PARQUET AS  

WITH rellenar_base_hipotecario AS
(
   SELECT id_cliente, /* como proceso.informe_recu_2020102_v2 es la tabla con mas filas entonces de aqui saco los ID de la tabla resultante (LEFT JOIN) */
          clasificacion_de_cartera,
          --modalidad AS modalidad_anterior,
          --producto_ajustado AS producto_ajustado_anterior,
          --sgto_ajustado AS sgto_ajustado_anterior,
          --vic_ccial AS vic_ccial_anterior,
          --banca_ajustada AS banca_ajustada_anterior,
          base,

          anho,
          mes,
          obl17,
          monto,
          aplicativo,
          amcdty,
          cncdbi,

          /* ------- PUNTO 1 ------- */
          /* modalidad */
          CASE
              WHEN (clasificacion_de_cartera = 'COMER'
                    AND clasificacion_de_cartera IS NOT NULL
                    AND base = 'HIPOTECARIO'
                   )
                   AND (TRIM(modalidad) IN("",NULL))
                   THEN '1' /* modalidad = '1' */
              WHEN (clasificacion_de_cartera <> 'COMER'
                    AND clasificacion_de_cartera IS NOT NULL
                    AND base = 'HIPOTECARIO' 
                   )
                   AND (TRIM(modalidad) IN("",NULL))
                   THEN '3' /* modalidad = '3' */
              ELSE modalidad
          END AS modalidad,

          /* producto_ajustado */
          CASE
              WHEN (modalidad = '1'
                    AND modalidad IS NOT NULL 
                    AND base = 'HIPOTECARIO'
                   )
                   AND (TRIM(producto_ajustado) IN("",NULL)) 
                   THEN 'OTROS HIPOTECARIO' /* producto_ajustado = 'OTROS HIPOTECARIO' */
              WHEN (modalidad <> '1'
                    AND modalidad IS NOT NULL 
                    AND base = 'HIPOTECARIO'
                   )
                   AND (TRIM(producto_ajustado) IN("",NULL))
                   THEN 'HIPOTECARIO VIVIENDA' /* producto_ajustado = 'HIPOTECARIO VIVIENDA' */
              ELSE producto_ajustado
          END AS producto_ajustado,
          
          /* sgto_ajustado */
          CASE
              WHEN (modalidad = '1'
                    AND modalidad IS NOT NULL
                    AND base = 'HIPOTECARIO' 
                   )
                   AND (TRIM(sgto_ajustado) IN("",NULL))
                   THEN 'NEGOCIOS & INDEPEND' /* sgto_ajustado = 'NEGOCIOS & INDEPEND' */
              WHEN (modalidad <> '1'
                    AND modalidad IS NOT NULL
                    AND base = 'HIPOTECARIO'
                   )
                   AND (TRIM(sgto_ajustado) IN ("",NULL))
                   THEN 'PERSONAL' /* sgto_ajustado = 'PERSONAL' */
              ELSE sgto_ajustado
          END AS sgto_ajustado,
          
          /* vic_ccial */
          CASE
              WHEN (modalidad = '1'
                    AND modalidad IS NOT NULL
                    AND base = 'HIPOTECARIO' 
                   )
                   AND (TRIM(vic_ccial) IN("",NULL))
                   THEN 'PYMES' /* vic_ccial = 'PYMES' */
              WHEN (modalidad <> '1'
                   AND modalidad IS NOT NULL
                   AND base = 'HIPOTECARIO'
                   )
                   AND (TRIM(vic_ccial) IN ("",NULL))
                   THEN 'PERSONAS' /* vic_ccial = 'PERSONAS' */
              ELSE vic_ccial
          END AS vic_ccial,
          
          /* banca_ajustada */
          CASE
              WHEN base = 'HIPOTECARIO'
                   AND (TRIM(banca_ajustada) IN ("",NULL)) 
                   THEN 'NEPYP' /* banca_ajustada = 'NEPYP' */
              ELSE banca_ajustada
          END AS banca_ajustada


   FROM proceso.LEFT_JOIN_punto_1
)
SELECT id_cliente,
       clasificacion_de_cartera,
       modalidad,
       producto_ajustado,
       sgto_ajustado,
       vic_ccial,
       banca_ajustada,
       base,

       anho,
       mes,
       obl17,
       monto,
       aplicativo,
       amcdty,
       cncdbi 

FROM rellenar_base_hipotecario
;


COMPUTE STATS proceso.rellenar_base_hipotecario

;