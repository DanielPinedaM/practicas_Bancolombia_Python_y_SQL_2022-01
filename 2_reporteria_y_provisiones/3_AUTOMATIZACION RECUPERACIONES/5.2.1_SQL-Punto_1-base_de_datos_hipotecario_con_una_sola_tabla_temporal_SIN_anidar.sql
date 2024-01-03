/* PUNTO 1
Convertir el Python “base_rellenar.py” a SQL
Tarea practicante - Reemplazar las casillas NULL de la tabla si se cumplen ciertas condiciones
--en proceso_generadores.hipotecarios-- 


eliminar la tabla si ya existe */
DROP TABLE IF EXISTS proceso_consumidores.LEFT_JOIN_punto_1 PURGE
;

-----------------------------------------------------------------------------------------------

/* Ver los nombres y tipos de datos de las columnas 
de la tabla proceso_generadores.hipotecario_202205

NOMBRE COLUMNA                          TIPO DE DATO
numero_de_credito	                      double	
numero_de_identificacion_del_cliente	  double	
llave_productocontabilidad	            double	
puc_cuif_operativo	                    double	
fecha_de_corte_de_la_informacion	      double	
sucursal_de_transaccion	                string	
pagos_realizados	                      double	
clasificacion_de_cartera	              string	
modalidad_del_credito	                  string	
codigo_de_producto	                    string
*/

DESCRIBE proceso_generadores.hipotecario_202205
;

-----------------------------------------------------------------------------------------------

/* Ver los nombres y tipos de datos de las columnas 
de la tabla proceso_generadores.informe_recu_2020102_v2 

NOMBRE COLUMNA                          TIPO DE DATO
base	                                  string	
anho	                                  string	
mes	                                  string	
id	                                  bigint	
obl17	                                  string	
producto_ajustado	                      string	
banca_ajustada	                      string	
vic_ccial	                            string	
modalidad	                            string	
sgto_ajustado	                      string	
monto	                                  double	
aplicativo	                            string	
amcdty	                            double	
cncdbi	                            string
*/

DESCRIBE proceso_generadores.informe_recu_2020102_v2
;

-----------------------------------------------------------------------------------------------

/* CREAR TABLA RESULTANTE
crear una nueva tabla llamada proceso_consumidores.LEFT_JOIN_punto_1

SELECT column_name(s)
FROM table1
LEFT JOIN table2
ON table1.column_name = table2.column_name;

CREATE TABLE
https://www.techonthenet.com/sql/tables/create_table2.php

La nueva tabla se crea a partir de un
LEFT JOIN (cruce) por ID...
t1 = proceso_generadores.informe_recu_2020102_v2   → id
= 
t2 = proceso_generadores.hipotecario_202205        → numero_de_indetificacion_del_cliente
*/

CREATE TABLE IF NOT EXISTS proceso_consumidores.LEFT_JOIN_punto_1 STORED AS PARQUET AS  

WITH hipo AS 
(
    SELECT numero_de_credito,	                   
           numero_de_identificacion_del_cliente,	
           llave_productocontabilidad,           
           puc_cuif_operativo,                  	
           fecha_de_corte_de_la_informacion,  
           sucursal_de_transaccion,        
           pagos_realizados,
           /* TRIM()              eliminar espacios en blanco al principio y al final    https://www.w3schools.com/sql/func_sqlserver_trim.asp
              UPPER()             convertir texto (string) a MAYUSCULA                   https://www.w3schools.com/sql/func_sqlserver_upper.asp
              CAST(... AS ...)    convertir a cualquier tipo de dato                     https://www.w3schools.com/sql/func_sqlserver_cast.asp */
           TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,          	
           modalidad_del_credito,                	
           codigo_de_producto,                  

       ROW_NUMBER() OVER(PARTITION BY numero_de_identificacion_del_cliente
                         ORDER BY numero_de_identificacion_del_cliente ASC
                         ) AS numero
    FROM proceso_generadores.hipotecario_202205
),

/* seleccionar SOLAMENTE la primera vez q aparece cada uno de los registros */
hipo2 AS
( 
    SELECT *
    FROM hipo
    WHERE numero=1
)

/* seleccionar las columnas q tendra la tabla resultante del LEFT JOIN */
SELECT /* t1.*, */ /* necesito todas las columnas de la tabla 1 proceso_generadores.informe_recu_2020102_v2 */
       t1.id AS id_cliente, /* como proceso_generadores.informe_recu_2020102_v2 es la tabla con mas filas entonces de aqui saco los ID de la tabla resultante (LEFT JOIN) */
       TRIM(UPPER(CAST(t2.clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,
       t1.modalidad,
       t1.producto_ajustado,
       TRIM(UPPER(CAST(t1.sgto_ajustado AS STRING))) AS sgto_ajustado,
       t1.vic_ccial,
       TRIM(UPPER(CAST(t1.banca_ajustada AS STRING))) AS banca_ajustada,
       TRIM(UPPER(CAST(t1.base AS STRING))) AS base,
       t1.anho,
       t1.mes,
       t1.obl17,
       t1.monto,
       t1.aplicativo,
       t1.amcdty,
       t1.cncdbi
FROM proceso_generadores.informe_recu_2020102_v2 AS t1
LEFT JOIN hipo2 AS t2 
ON t1.id = t2.numero_de_identificacion_del_cliente /* Cruzar por ID */
ORDER BY 1 ASC /* Hacer q el ID se ordene asi: 
                  Mostrar primero los numeros 
                  y despues (de ultimo) los ID q sean NULL 
                  / orden ascendente (de menor a mayor) */
;

-----------------------------------------------------------------------------------------------
/* PARTE 1 - Verificar q el LEFT JOIN este bueno, 
para ello la suma de la columna monto 
en las tablas proceso_consumidores.LEFT_JOIN_punto_1 y proceso_generadores.informe_recu_2020102_v2
tiene q ser la misma

en este caso el numero tiene q ser -27 499 490 658.069996

combinar el resultado de 2 consultas SQL en 2 columnas diferentes:
https://stackoverflow.com/questions/16364187/combining-the-results-of-two-sql-queries-as-separate-columns
*/

WITH tabla_temporal_1 AS 
(
  SELECT SUM(monto) AS suma_proceso_consumidores_LEFT_JOIN_punto_1
  FROM proceso_consumidores.LEFT_JOIN_punto_1
),
tabla_temporal_2 AS 
(
  SELECT SUM(monto) AS suma_proceso_generadores_informe_recu_2020102_v2
  FROM proceso_generadores.informe_recu_2020102_v2
)
SELECT suma_proceso_consumidores_LEFT_JOIN_punto_1,
       suma_proceso_generadores_informe_recu_2020102_v2
FROM tabla_temporal_1
JOIN tabla_temporal_2 
ON (tabla_temporal_1.suma_proceso_consumidores_LEFT_JOIN_punto_1) = (tabla_temporal_2.suma_proceso_generadores_informe_recu_2020102_v2)
;

-----------------------------------------------------------------------------------------------
/* PARTE 2 - Verificar q el LEFT JOIN este bueno, 
para ello el numero de filas de proceso_consumidores.LEFT_JOIN_punto_1
tiene q ser 75 362
q es el numero de filas de la tabla q mas filas tiene q es proceso_generadores.informe_recu_2020102_v2 */

WITH tabla_temporal_1 AS 
(
  SELECT COUNT(*) AS numero_de_filas_proceso_consumidores_LEFT_JOIN_punto_1
  FROM proceso_consumidores.LEFT_JOIN_punto_1
),
tabla_temporal_2 AS
(
  SELECT COUNT(*) AS numero_de_filas_proceso_generadores_informe_recu_2020102_v2
  FROM proceso_generadores.informe_recu_2020102_v2
)
SELECT numero_de_filas_proceso_consumidores_LEFT_JOIN_punto_1,
       numero_de_filas_proceso_generadores_informe_recu_2020102_v2
FROM tabla_temporal_1
JOIN tabla_temporal_2 
ON (tabla_temporal_1.numero_de_filas_proceso_consumidores_LEFT_JOIN_punto_1) = (tabla_temporal_2.numero_de_filas_proceso_generadores_informe_recu_2020102_v2)
;

-----------------------------------------------------------------------------------------------

/* REEMPLAZAR CAMPOS (CASILLAS) NULL Ó VACIOS SI SE CUMPLEN CIERTAS CONDICIONES
En el punto 1 las condiciones con las q se reemplazan los valores de las casillas son las siguientes:

modalidad
SI ( ( (clasificacion_de_cartera='COMER')
        AND (clasificación_de_cartera IS NOT NULL)
        AND (clasificación_de_cartera<>'')
     )
    AND ((modalidad IS NULL)
         OR (modalidad='')
        )
    AND ((base='HIPOTECARIO')
         AND (base IS NOT NULL)
         AND (base<>'')
        )
   )
    modalidad = '1'

SI NO ((clasificación_de_cartera<>'COMER')
        AND (clasificación_de_cartera IS NOT NULL) -- INCOMPLETO - NO ESTOY SEGURO DE ESTA CONDICION - CREO Q NO HAY Q ESCRIBIRLA, BORRELA (CREO)
        AND ((modalidad IS NULL)
              OR (modalidad='')
            )
        AND ((base='HIPOTECARIO')
              AND (base IS NOT NULL)
              AND (base<>'')
            )
      )
       modalidad = '3'



producto_ajustado
SI ( ( (modalidad='1')
        AND (modalidad IS NOT NULL)
        AND (modalidad<>'')
     )
     AND ((producto_ajustado IS NULL)
           OR (producto_ajustado='')
         )
     AND ((base='HIPOTECARIO')
          AND (base IS NOT NULL)
          AND (base<>'')
         )
   )
    producto_ajustado='OTROS HIPOTECARIO'

SI NO ((modalidad<>'1')
       AND (modalidad IS NOT NULL) -- INCOMPLETO - NO ESTOY SEGURO DE ESTA CONDICION - CREO Q NO HAY Q ESCRIBIRLA, BORRELA (CREO)
       AND ((producto_ajustado IS NULL)
             OR (producto_ajustado='')
           )
       AND ((base='HIPOTECARIO')
             AND (base IS NOT NULL)
             AND (base<>'')
           )
      )
       producto_ajustado='HIPOTECARIO VIVIENDA'



sgto_ajustado
SI ( ( (modalidad='1')
        AND (modalidad IS NOT NULL)
        AND (modalidad<>'')
     )
    AND ((sgto_ajustado IS NULL)
          OR (sgto_ajustado='')
        )
    AND ((base='HIPOTECARIO')
          AND (base IS NOT NULL)
          AND (base<>'')
        )
   )
    sgto_ajustado='NEGOCIOS & INDEPEND'

SI NO ((modalidad<>'1')
        AND (modalidad IS NOT NULL) -- INCOMPLETO - NO ESTOY SEGURO DE ESTA CONDICION - CREO Q NO HAY Q ESCRIBIRLA, BORRELA (CREO)
        AND ((sgto_ajustado IS NULL)
              OR (sgto_ajustado='')
            )
        AND ((base='HIPOTECARIO')
              AND (base IS NOT NULL)
              AND (base<>'')
            )
      )
       sgto_ajustado='PERSONAL'



vic_ccial
SI ( ( (modalidad='1')
        AND (modalidad IS NOT NULL)
        AND (modalidad<>'')
     )
     AND ((vic_ccial IS NULL)
           OR (vic_ccial='')
         )
     AND ((base='HIPOTECARIO')
           AND (base IS NOT NULL)
           AND (base<>'')
         )
   )
    vic_ccial='PYMES'

SI NO ((modalidad<>'1')
        AND (modalidad IS NOT NULL) -- INCOMPLETO - NO ESTOY SEGURO DE ESTA CONDICION - CREO Q NO HAY Q ESCRIBIRLA, BORRELA (CREO)
        AND ((vic_ccial IS NULL)
              OR (vic_ccial='')
            )
        AND ((base='HIPOTECARIO')
              AND (base IS NOT NULL)
              AND (base<>'')
            )
      )
       vic_ccial='PERSONAS'



banca_ajustada
SI ( ( (banca_ajustada IS NULL)
        OR (banca_ajustada='')
     )
     AND ((base='HIPOTECARIO')
           AND (base IS NOT NULL)
           AND (base<>'')
         )
   )
   banca_ajustada='NEPYP'
*/

-----------------------------------------------------------------------------------------------

/* eliminar la tabla si ya existe */
DROP TABLE IF EXISTS proceso_consumidores.punto_1_terminado PURGE
;

-----------------------------------------------------------------------------------------------
/* 
Crear la tabla final q contiene la solucion del punto 1

la siguiente consulta SQL es la q tiene q se tiene q ejecutar desde el Python
*/
CREATE TABLE IF NOT EXISTS proceso_consumidores.punto_1_terminado STORED AS PARQUET AS  

WITH punto_1_terminado AS
(
   SELECT id_cliente, /* como proceso_generadores.informe_recu_2020102_v2 
                         es la tabla con mas filas 
                         entonces de aqui saco los ID de la tabla resultante (LEFT JOIN) */
          TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,
          /*modalidad AS modalidad_anterior,*/
          /*TRIM(UPPER(CAST(producto_ajustado AS STRING))) AS producto_ajustado_anterior,*/
          /*TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado_anterior,*/
          /*vic_ccial AS vic_ccial_anterior,*/
          TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada_anterior,
          TRIM(UPPER(CAST(base AS STRING))) AS base,

          anho,
          mes,
          obl17,
          monto,
          TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,
          amcdty,
          cncdbi,


          /* ------- PUNTO 1 ------- */
          /* modalidad */
          (CASE
              WHEN ( ( (TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING)))='COMER')
                        AND (clasificacion_de_cartera IS NOT NULL)
                        AND (TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING)))<>'')
                     )
                     AND ((modalidad IS NULL)
                           OR (TRIM(CAST(modalidad AS STRING))='NULL')
                           OR (TRIM(CAST(modalidad AS STRING))='')
                         )
                     AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                            AND (base IS NOT NULL)
                            AND (TRIM(CAST(base AS STRING))<>'')
                         )
                   )
                   THEN '1' /* modalidad = '1' */
              WHEN ( (TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING)))<>'COMER')
                      /*AND (clasificación_de_cartera IS NOT NULL)*/
                     AND ((modalidad IS NULL)
                           OR (TRIM(CAST(modalidad AS STRING))='NULL')
                           OR (TRIM(CAST(modalidad AS STRING))='')
                         )
                     AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                            AND (base IS NOT NULL)
                            AND (TRIM(CAST(base AS STRING))<>'')
                         )
                   )
                   THEN '3' /* modalidad = '3' */
              ELSE modalidad
          END) AS modalidad,


          /* producto_ajustado */
          (CASE 
               WHEN ( ( (TRIM(CAST(modalidad AS STRING))='1')
                         AND (modalidad IS NOT NULL)
                         AND (TRIM(CAST(modalidad AS STRING))<>'')
                      )
                      AND ((producto_ajustado IS NULL)
                            OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                            OR (TRIM(CAST(producto_ajustado AS STRING))='')
                          )
                      AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                             AND (base IS NOT NULL)
                             AND (TRIM(CAST(base AS STRING))<>'')
                          )
                    )
                    THEN 'OTROS HIPOTECARIO' /* producto_ajustado = 'OTROS HIPOTECARIO' */
               WHEN ((TRIM(CAST(modalidad AS STRING))<>'1')
                      /*AND (modalidad IS NOT NULL)*/
                      AND ((producto_ajustado IS NULL)
                            OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                            OR (TRIM(CAST(producto_ajustado AS STRING))='')
                          )
                      AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                             AND (base IS NOT NULL)
                             AND (TRIM(CAST(base AS STRING))<>'')
                          )
                    )
                    THEN 'HIPOTECARIO VIVIENDA' /* producto_ajustado = 'HIPOTECARIO VIVIENDA' */
               ELSE producto_ajustado
          END) AS producto_ajustado,


          /* sgto_ajustado */
          (CASE
              WHEN ( ( (TRIM(CAST(modalidad AS STRING))='1')
                        AND (modalidad IS NOT NULL)
                        AND (TRIM(CAST(modalidad AS STRING))<>'')
                     )
                     AND ((sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                         )
                      AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                             AND (base IS NOT NULL)
                             AND (TRIM(CAST(base AS STRING))<>'')
                          )
                   )
                   THEN 'NEGOCIOS & INDEPEND' /* sgto_ajustado = 'NEGOCIOS & INDEPEND' */
              WHEN ( (TRIM(CAST(modalidad AS STRING))<>'1')
                      /*AND (modalidad IS NOT NULL)*/
                      AND ((sgto_ajustado IS NULL)
                            OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                            OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                          )
                      AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                             AND (base IS NOT NULL)
                             AND (TRIM(CAST(base AS STRING))<>'')
                          )
                   )
                   THEN 'PERSONAL' /* sgto_ajustado = 'PERSONAL' */
              ELSE sgto_ajustado
          END) AS sgto_ajustado,


          /* vic_ccial */
          (CASE
              WHEN ( ( (TRIM(CAST(modalidad AS STRING))='1')
                        AND (modalidad IS NOT NULL)
                        AND (TRIM(CAST(modalidad AS STRING))<>'')
                     )
                     AND ((vic_ccial IS NULL)
                           OR (TRIM(CAST(vic_ccial AS STRING))='NULL')
                           OR (TRIM(CAST(vic_ccial AS STRING))='')
                         )
                     AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                            AND (base IS NOT NULL)
                            AND (TRIM(CAST(base AS STRING))<>'')
                         )
                   )
                   THEN 'PYMES' /* vic_ccial = 'PYMES' */
              WHEN ( (TRIM(CAST(modalidad AS STRING))<>'1') 
                      /*AND (modalidad IS NOT NULL)*/
                      AND ((vic_ccial IS NULL)
                            OR (TRIM(CAST(vic_ccial AS STRING))='NULL')
                            OR (TRIM(CAST(vic_ccial AS STRING))='') 
                          )
                      AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                             AND (base IS NOT NULL)
                             AND (TRIM(CAST(base AS STRING))<>'')
                          )
                   )
                   THEN 'PERSONAS' /* vic_ccial = 'PERSONAS' */
              ELSE vic_ccial
          END) AS vic_ccial,


          /* banca_ajustada */
          (CASE
              WHEN ( ( (banca_ajustada IS NULL)
                        OR (TRIM(CAST(banca_ajustada AS STRING))='NULL')
                        OR (TRIM(CAST(banca_ajustada AS STRING))='') 
                     )
                     AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                            AND (base IS NOT NULL)
                            AND (TRIM(CAST(base AS STRING))<>'')
                         )
                   )
                   THEN 'NEPYP' /* banca_ajustada = 'NEPYP' */
              ELSE banca_ajustada
          END) AS banca_ajustada

   FROM proceso_consumidores.LEFT_JOIN_punto_1
)
SELECT id_cliente,
       TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,
       modalidad,
       TRIM(UPPER(CAST(producto_ajustado AS STRING))) AS producto_ajustado,
       TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado,
       vic_ccial,
       TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada,
       TRIM(UPPER(CAST(base AS STRING))) AS base,

       anho,
       mes,
       obl17,
       monto,
       TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,
       amcdty,
       cncdbi 
/* la tabla "punto_1_terminado" es la tabla FINAL despues de llenar los campos NULL ó vacíos,
es la respuesta (resultado) del punto 1 */
FROM punto_1_terminado
;

-----------------------------------------------------------------------------------------------
/* 
La consulta de a continuacion es exactamente igual a la anterior,
lo unico q cambia es q 
se puede ver como estaban las columnas antes y despues de ser reemplazadas segun los condicionales 

La siguiente consulta NO se tiene q ejecutar desde el Python

CASE WHEN en SQL 
https://www.w3schools.com/sql/sql_case.asp
*/

WITH punto_1_terminado AS
(
   SELECT id_cliente, /* como proceso_generadores.informe_recu_2020102_v2 
                         es la tabla con mas filas 
                         entonces de aqui saco los ID de la tabla resultante (LEFT JOIN) */
          TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,
          modalidad AS modalidad_anterior,
          TRIM(UPPER(CAST(producto_ajustado AS STRING))) AS producto_ajustado_anterior,
          TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado_anterior,
          vic_ccial AS vic_ccial_anterior,
          TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada_anterior,
          TRIM(UPPER(CAST(base AS STRING))) AS base,

          anho,
          mes,
          obl17,
          monto,
          TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,
          amcdty,
          cncdbi,


          /* ------- PUNTO 1 ------- */
          /* modalidad */
          (CASE
              WHEN ( ( (TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING)))='COMER')
                        AND (clasificacion_de_cartera IS NOT NULL)
                        AND (TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING)))<>'')
                     )
                     AND ((modalidad IS NULL)
                           OR (TRIM(CAST(modalidad AS STRING))='NULL')
                           OR (TRIM(CAST(modalidad AS STRING))='')
                         )
                     AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                            AND (base IS NOT NULL)
                            AND (TRIM(CAST(base AS STRING))<>'')
                         )
                   )
                   THEN '1' /* modalidad = '1' */
              WHEN ( (TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING)))<>'COMER')
                      /*AND (clasificación_de_cartera IS NOT NULL)*/
                     AND ((modalidad IS NULL)
                           OR (TRIM(CAST(modalidad AS STRING))='NULL')
                           OR (TRIM(CAST(modalidad AS STRING))='')
                         )
                     AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                            AND (base IS NOT NULL)
                            AND (TRIM(CAST(base AS STRING))<>'')
                         )
                   )
                   THEN '3' /* modalidad = '3' */
              ELSE modalidad
          END) AS modalidad_nuevo,


          /* producto_ajustado */
          (CASE 
               WHEN ( ( (TRIM(CAST(modalidad AS STRING))='1')
                         AND (modalidad IS NOT NULL)
                         AND (TRIM(CAST(modalidad AS STRING))<>'')
                      )
                      AND ((producto_ajustado IS NULL)
                            OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                            OR (TRIM(CAST(producto_ajustado AS STRING))='')
                          )
                      AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                             AND (base IS NOT NULL)
                             AND (TRIM(CAST(base AS STRING))<>'')
                          )
                    )
                    THEN 'OTROS HIPOTECARIO' /* producto_ajustado = 'OTROS HIPOTECARIO' */
               WHEN ((TRIM(CAST(modalidad AS STRING))<>'1')
                      /*AND (modalidad IS NOT NULL)*/
                      AND ((producto_ajustado IS NULL)
                            OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                            OR (TRIM(CAST(producto_ajustado AS STRING))='')
                          )
                      AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                             AND (base IS NOT NULL)
                             AND (TRIM(CAST(base AS STRING))<>'')
                          )
                    )
                    THEN 'HIPOTECARIO VIVIENDA' /* producto_ajustado = 'HIPOTECARIO VIVIENDA' */
               ELSE producto_ajustado
          END) AS producto_ajustado_nuevo,


          /* sgto_ajustado */
          (CASE
              WHEN ( ( (TRIM(CAST(modalidad AS STRING))='1')
                        AND (modalidad IS NOT NULL)
                        AND (TRIM(CAST(modalidad AS STRING))<>'')
                     )
                     AND ((sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                         )
                      AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                             AND (base IS NOT NULL)
                             AND (TRIM(CAST(base AS STRING))<>'')
                          )
                   )
                   THEN 'NEGOCIOS & INDEPEND' /* sgto_ajustado = 'NEGOCIOS & INDEPEND' */
              WHEN ( (TRIM(CAST(modalidad AS STRING))<>'1')
                      /*AND (modalidad IS NOT NULL)*/
                      AND ((sgto_ajustado IS NULL)
                            OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                            OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                          )
                      AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                             AND (base IS NOT NULL)
                             AND (TRIM(CAST(base AS STRING))<>'')
                          )
                   )
                   THEN 'PERSONAL' /* sgto_ajustado = 'PERSONAL' */
              ELSE sgto_ajustado
          END) AS sgto_ajustado_nuevo,


          /* vic_ccial */
          (CASE
              WHEN ( ( (TRIM(CAST(modalidad AS STRING))='1')
                        AND (modalidad IS NOT NULL)
                        AND (TRIM(CAST(modalidad AS STRING))<>'')
                     )
                     AND ((vic_ccial IS NULL)
                           OR (TRIM(CAST(vic_ccial AS STRING))='NULL')
                           OR (TRIM(CAST(vic_ccial AS STRING))='')
                         )
                     AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                            AND (base IS NOT NULL)
                            AND (TRIM(CAST(base AS STRING))<>'')
                         )
                   )
                   THEN 'PYMES' /* vic_ccial = 'PYMES' */
              WHEN ( (TRIM(CAST(modalidad AS STRING))<>'1') 
                      /*AND (modalidad IS NOT NULL)*/
                      AND ((vic_ccial IS NULL)
                            OR (TRIM(CAST(vic_ccial AS STRING))='NULL')
                            OR (TRIM(CAST(vic_ccial AS STRING))='') 
                          )
                      AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                             AND (base IS NOT NULL)
                             AND (TRIM(CAST(base AS STRING))<>'')
                          )
                   )
                   THEN 'PERSONAS' /* vic_ccial = 'PERSONAS' */
              ELSE vic_ccial
          END) AS vic_ccial_nuevo,


          /* banca_ajustada */
          (CASE
              WHEN ( ( (banca_ajustada IS NULL)
                        OR (TRIM(CAST(banca_ajustada AS STRING))='NULL')
                        OR (TRIM(CAST(banca_ajustada AS STRING))='') 
                     )
                     AND ( (TRIM(UPPER(CAST(base AS STRING)))='HIPOTECARIO')
                            AND (base IS NOT NULL)
                            AND (TRIM(CAST(base AS STRING))<>'')
                         )
                   )
                   THEN 'NEPYP' /* banca_ajustada = 'NEPYP' */
              ELSE banca_ajustada
          END) AS banca_ajustada_nuevo

   FROM proceso_consumidores.LEFT_JOIN_punto_1
)
SELECT *
/* la tabla "punto_1_terminado" es la tabla FINAL despues de llenar los campos NULL ó vacíos,
es la respuesta (resultado) del punto 1 */
FROM punto_1_terminado
ORDER BY id_cliente ASC /* ordenar los id_cliente en orden ascendente
                           (de menor a mayor) */
;

-----------------------------------------------------------------------------------------------

/* PARTE 3 - Verificar q la tabla punto_1_terminado este buena, 
para ello la suma de la columna monto  
en las tablas proceso_consumidores.punto_1_terminado y proceso_generadores.informe_recu_2020102_v2
tiene q ser la misma

en este caso el numero tiene q ser -27 499 490 658.069996

combinar el resultado de 2 consultas SQL en 2 columnas diferentes:
https://stackoverflow.com/questions/16364187/combining-the-results-of-two-sql-queries-as-separate-columns
*/

WITH tabla_temporal_1 AS 
(
  SELECT SUM(monto) AS suma_proceso_consumidores_punto_1_terminado
  FROM proceso_consumidores.punto_1_terminado
),
tabla_temporal_2 AS 
(
  SELECT SUM(monto) AS suma_proceso_generadores_informe_recu_2020102_v2
  FROM proceso_generadores.informe_recu_2020102_v2
)
SELECT suma_proceso_consumidores_punto_1_terminado,
       suma_proceso_generadores_informe_recu_2020102_v2
FROM tabla_temporal_1
JOIN tabla_temporal_2 
ON (tabla_temporal_1.suma_proceso_consumidores_punto_1_terminado) = (tabla_temporal_2.suma_proceso_generadores_informe_recu_2020102_v2)
;

-----------------------------------------------------------------------------------------------

/* PARTE 2 - Verificar q la tabla punto_1_terminado este buena, 
para ello el numero de filas de proceso_consumidores.punto_1_terminado
tiene q ser 75 362
q es el numero de filas de la tabla q mas filas tiene q es proceso_generadores.informe_recu_2020102_v2 */

WITH tabla_temporal_1 AS 
(
  SELECT COUNT(*) AS numero_de_filas_proceso_consumidores_punto_1_terminado
  FROM proceso_consumidores.punto_1_terminado
),
tabla_temporal_2 AS
(
  SELECT COUNT(*) AS numero_de_filas_proceso_generadores_informe_recu_2020102_v2
  FROM proceso_generadores.informe_recu_2020102_v2
)
SELECT numero_de_filas_proceso_consumidores_punto_1_terminado,
       numero_de_filas_proceso_generadores_informe_recu_2020102_v2
FROM tabla_temporal_1
JOIN tabla_temporal_2 
ON (tabla_temporal_1.numero_de_filas_proceso_consumidores_punto_1_terminado) = (tabla_temporal_2.numero_de_filas_proceso_generadores_informe_recu_2020102_v2)
;

-----------------------------------------------------------------------------------------------
