/* PUNTO 2
Convertir el Python “base_rellenar.py” a SQL
Tarea practicante - Reemplazar campos vacios (NULL) si se cumplen ciertas condiciones
--en las bases de datos de BANCO y VENTA-- 

Trabajar con la misma tabla del punto 1

en TODOS los condicionales TIENE q estar esto:
        (
        CASE -- columna_llenar_null 
            WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                    OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                  ) --- (TRIM(base) IN('BANCO', 'VENTA'))
                  AND ((TRIM(columna_llenar_null)='')
                        OR columna_llenar_null IS NULL
                      ) -- AND (TRIM(columna_llenar_null) IN('',NULL))

            ELSE columna_llenar_null
        END) AS columna_llenar_null

En el punto 1 las condiciones con las q se reemplazan los valores de las casillas son las siguientes: 

modalidad
//invertir los números de la modalidad
SI (TRIM(base) IN('BANCO', 'VENTA'))
    AND (TRIM(modalidad) IN('',NULL))
    AND (TRIM(aplicativo) IN('L','E'))

    SI (amcdty = 1) 
        modalidad = '2' 
    SINO (amcdty = 2)
          modalidad = '1'
    SINO (amcdty = 4)
         modalidad = '4'

// Para el resto de aplicativos, la modalidad es igual al amcdty
SINO (TRIM(base) IN('BANCO','VENTA'))
     AND (TRIM(modalidad) IN('',NULL))
     AND (TRIM(aplicativo) NOT IN('L','E'))

      SI (amcdty = 1)
          modalidad = '1' 
      SINO (amcdty = 2)
            modalidad = '2'
      SINO (amcdty = 4)
            modalidad = '4'



producto_ajustado
SI (TRIM(base) IN('BANCO','VENTA'))
   AND (TRIM(producto_ajustado) IN('',NULL))
   AND (TRIM(aplicativo) IN('L','E'))

       SI (TRIM(modalidad) = '2')
          producto_ajustado = 'LIBRE INVERSION'
       SINO (TRIM(modalidad) = '1')
            producto_ajustado = 'CARTERA ORDINARIA'
       SINO (TRIM(modalidad) = '4')
            producto_ajustado = 'CARTERA MICROCREDITO' 

// SINO (NO hacer NADA)



SI (TRIM(base) IN('BANCO','VENTA'))
   AND (TRIM(producto_ajustado) IN('',NULL))

       SI (TRIM(aplicativo) IN('K','M','V'))
          producto_ajustado = 'TARJETA DE CREDITO'
       SINO (TRIM(aplicativo)='D') 
            producto_ajustado = 'SOBREGIROS'

// SINO (NO hacer NADA)



cncdbi    sgto_ajustado
5	    PYMES
9	    NEGOCIOS E INDEPEN
G	    GOBIERNO DE RED
4	    PERSONAL
M	    PLUS
6	    PREFERENCIAL
3	    CORPORATIVO
2	    EMPRESARIAL
1	    GOBIERNO
7	    INTERNACIONAL
8	    INSTITUCIONES FINANCIERAS
C	    CONSTRUCTOR CORPORATIVO
A	    CONSTRUCTOR EMPRESARIAL
B	    CONSTRUCTOR PYME
S	    SOCIAL

sgto_ajustado
SI (TRIM(base) IN('BANCO','VENTA'))
    AND (TRIM(sgto_ajustado) IN('',NULL))

   SI (TRIM(cncdbi)='5')
   sgto_ajustado = 'PYMES'

   SINO (TRIM(cncdbi)='9')
   sgto_ajustado = 'NEGOCIOS E INDEPEN'

   SINO (TRIM(cncdbi)='G')	
   sgto_ajustado = 'GOBIERNO DE RED'

   SINO (TRIM(cncdbi)='4')
   sgto_ajustado = 'PERSONAL'

   SINO (TRIM(cncdbi)='M')
   sgto_ajustado = 'PLUS'

   SINO (TRIM(cncdbi)='6')
   sgto_ajustado = 'PREFERENCIAL'

   SINO (TRIM(cncdbi)='3')
   sgto_ajustado = 'CORPORATIVO'

   SINO (TRIM(cncdbi)='2')
   sgto_ajustado = 'EMPRESARIAL'

   SINO (TRIM(cncdbi)='1')
   sgto_ajustado = 'GOBIERNO'

   SINO (TRIM(cncdbi)='7')
   sgto_ajustado = 'INTERNACIONAL'

   SINO (TRIM(cncdbi)='8')
   sgto_ajustado = 'INSTITUCIONES FINANCIERAS'

   SINO (TRIM(cncdbi)='C')
   sgto_ajustado = 'CONSTRUCTOR CORPORATIVO'

   SINO (TRIM(cncdbi)='A')
   sgto_ajustado = 'CONSTRUCTOR EMPRESARIAL'

   SINO (TRIM(cncdbi)='B')
   sgto_ajustado = 'CONSTRUCTOR PYME'

   SINO (TRIM(cncdbi)='S')
   sgto_ajustado = 'SOCIAL'

// SINO (NO hacer NADA)
*/

-----------------------------------------------------------------------------------------------
/* 
Ver los nombres y tipos de datos de las columnas 
de la tabla proceso_generadores.hipotecario_202205

NOMBRE COLUMNA                          TIPO DE DATO
id_cliente	                            bigint	
clasificacion_de_cartera	             string	
modalidad	                            string	
producto_ajustado	                      string	
sgto_ajustado	                         string	
vic_ccial	                            string	
banca_ajustada	                         string	
base	                                  string	
anho	                                  string	
mes	                                  string	
obl17	                                  string	
monto	                                  double	
aplicativo	                            string	
amcdty	                               double	
cncdbi	                               string
*/

DESCRIBE proceso_consumidores.punto_1_terminado

-----------------------------------------------------------------------------------------------

/* eliminar la tabla si ya existe */
DROP TABLE IF EXISTS proceso_generadores.punto_2_terminado PURGE
;

-----------------------------------------------------------------------------------------------

/* crear la tabla final q contiene la solucion del punto 2 */
--CREATE TABLE IF NOT EXISTS proceso_generadores.punto_2_terminado STORED AS PARQUET AS  
--
--WITH tabla_1 AS
--(
--   SELECT id_cliente,
--          /* TRIM()              eliminar espacios en blanco al principio y al final    https://www.w3schools.com/sql/func_sqlserver_trim.asp
--             UPPER()             convertir texto (string) a MAYUSCULA                   https://www.w3schools.com/sql/func_sqlserver_upper.asp
--             CAST(... AS ...)    convertir a cualquier tipo de dato                     https://www.w3schools.com/sql/func_sqlserver_cast.asp */
--          TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,
--          /*TRIM(CAST(modalidad AS STRING)) AS modalidad,*/
--          TRIM(UPPER(CAST(producto_ajustado AS STRING))) AS producto_ajustado,
--          TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado,
--          TRIM(UPPER(CAST(vic_ccial AS STRING))) AS vic_ccial,
--          TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada,
--          TRIM(UPPER(CAST(base AS STRING))) AS base,
--
--          anho,
--          mes,
--          obl17,
--          monto,
--          TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,
--          amcdty,
--          cncdbi,
--
--          /* ------- PUNTO 2 
--          Es muy extraño q esto suceda porq no deberia suceder, pero aqui sucede:
--          cuando uso IN() (abreviacion de OR) NO funciona los CASE, 
--          en cambio cuando uso muchos OR (SIN abreviar) entonces SI funciona
--          ------- */
--
--          (CASE /* modalidad
--                   invertir los números de la modalidad */
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (modalidad IS NULL)
--                           OR (TRIM(CAST(modalidad AS STRING))='NULL')
--                           OR (TRIM(CAST(modalidad AS STRING))='')
--                        )
--                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
--                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
--                        )
--                    AND (CAST(amcdty AS DOUBLE)=1)
--                    THEN '2' /* modalidad='2' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (modalidad IS NULL)
--                           OR (TRIM(CAST(modalidad AS STRING))='NULL')
--                           OR (TRIM(CAST(modalidad AS STRING))='')
--                        )
--                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
--                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
--                        )
--                    AND (CAST(amcdty AS DOUBLE)=2)
--                    THEN '1' /* modalidad='1' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (modalidad IS NULL)
--                           OR (TRIM(CAST(modalidad AS STRING))='NULL')
--                           OR (TRIM(CAST(modalidad AS STRING))='')
--                        )
--                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
--                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
--                        )
--                    AND (CAST(amcdty AS DOUBLE)=4)
--                    THEN '4' /* modalidad = '4' */
--
--               /* Para el resto de aplicativos, la modalidad es igual al amcdty */
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ((modalidad IS NULL)
--                        OR (TRIM(CAST(modalidad AS STRING))='NULL')
--                        OR (TRIM(CAST(modalidad AS STRING))='')
--                        )
--                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'L')
--                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'E')
--                        )
--                    AND (CAST(amcdty AS DOUBLE)=1)
--                    THEN '1' /* modalidad = '1' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                  AND ( (modalidad IS NULL)
--                         OR (TRIM(CAST(modalidad AS STRING))='NULL')
--                         OR (TRIM(CAST(modalidad AS STRING))='')
--                      )
--                 AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'L')
--                        OR (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'E')
--                     )
--                  AND (CAST(amcdty AS DOUBLE)=2)
--                 THEN '2' /* modalidad = '2' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                  AND ( (modalidad IS NULL)
--                         OR (TRIM(CAST(modalidad AS STRING))='NULL')
--                         OR (TRIM(CAST(modalidad AS STRING))='')
--                      )
--                 AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'L')
--                        OR (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'E')
--                     )
--                 AND (CAST(amcdty AS DOUBLE)=4)
--                 THEN '4' /* modalidad = '4' */
--              ELSE modalidad
--          END) AS modalidad
--
--   FROM proceso_consumidores.punto_1_terminado
--),
--
--tabla_2 AS (
--    SELECT id_cliente,
--           TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,
--           TRIM(CAST(modalidad AS STRING)) AS modalidad,
--           /*TRIM(UPPER(CAST(producto_ajustado AS STRING))) AS producto_ajustado_anterior, */
--           /*TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado_anterior,*/
--           TRIM(UPPER(CAST(vic_ccial AS STRING))) AS vic_ccial,
--           TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada,
--           TRIM(UPPER(CAST(base AS STRING))) AS base,
--
--           anho,
--           mes,
--           obl17,
--           monto,
--           TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,
--           amcdty,
--           cncdbi,
--
--          (CASE /* producto_ajustado */
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (producto_ajustado IS NULL)
--                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
--                        )                      
--                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
--                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
--                        )
--                    AND (TRIM(CAST(modalidad AS STRING))='2')
--                    THEN 'LIBRE INVERSION' /* producto_ajustado = 'LIBRE INVERSION' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (producto_ajustado IS NULL)
--                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
--                        )
--                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
--                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
--                        )
--                  AND (TRIM(CAST(modalidad AS STRING))='1')
--                  THEN 'CARTERA ORDINARIA' /* producto_ajustado = 'CARTERA ORDINARIA' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (producto_ajustado IS NULL)
--                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
--                        )
--                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
--                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
--                        )
--                    AND (TRIM(CAST(modalidad AS STRING))='4')
--                    THEN 'CARTERA MICROCREDITO' /* producto_ajustado = 'CARTERA MICROCREDITO' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (producto_ajustado IS NULL)
--                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
--                        )
--                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='K')
--                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='M')
--                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='V')
--                        )
--                THEN 'TARJETA DE CREDITO' /* producto_ajustado = 'TARJETA DE CREDITO' */
--
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (producto_ajustado IS NULL)
--                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
--                        )
--                    AND (TRIM(CAST(aplicativo AS STRING))='D') 
--                THEN 'SOBREGIROS' /* producto_ajustado = 'SOBREGIROS' */
--
--            ELSE producto_ajustado
--        END) AS producto_ajustado,
--
--          (CASE /* sgto_ajustado */ 
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        )
--                    AND (TRIM(CAST(cncdbi AS STRING))='5')
--                    THEN 'PYMES' /* sgto_ajustado = 'PYMES' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        )
--                    AND (TRIM(CAST(cncdbi AS STRING))='9')
--                    THEN 'NEGOCIOS E INDEPEN' /* sgto_ajustado = 'NEGOCIOS E INDEPEN' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        )
--                    AND (TRIM(CAST(cncdbi AS STRING))='G')
--                    THEN 'GOBIERNO DE RED' /* sgto_ajustado = 'GOBIERNO DE RED' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    ) 
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        )
--                  AND (TRIM(CAST(cncdbi AS STRING))='4')
--                  THEN 'PERSONAL' /* sgto_ajustado = 'PERSONAL' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        )
--                  AND (TRIM(CAST(cncdbi AS STRING))='M')
--                  THEN 'PLUS' /* sgto_ajustado = 'PLUS' */
--            
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        )
--                  AND (TRIM(CAST(cncdbi AS STRING))='6')
--                  THEN 'PREFERENCIAL' /* sgto_ajustado = 'PREFERENCIAL' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        )
--                  AND (TRIM(CAST(cncdbi AS STRING))='3')
--                  THEN 'CORPORATIVO' /* sgto_ajustado = 'CORPORATIVO' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        )
--                    AND (TRIM(CAST(cncdbi AS STRING))='2')
--                    THEN 'EMPRESARIAL' /* sgto_ajustado = 'EMPRESARIAL' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        )
--                    AND (TRIM(CAST(cncdbi AS STRING))='1')
--                    THEN 'GOBIERNO' /* sgto_ajustado = 'GOBIERNO' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        ) 
--                  AND (TRIM(CAST(cncdbi AS STRING))='7')
--                  THEN 'INTERNACIONAL' /* sgto_ajustado = 'INTERNACIONAL' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        )
--                  AND (TRIM(CAST(cncdbi AS STRING))='8')
--                  THEN 'INSTITUCIONES FINANCIERAS' /* sgto_ajustado = 'INSTITUCIONES FINANCIERAS' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        ) 
--                  AND (TRIM(CAST(cncdbi AS STRING))='C')
--                  THEN 'CONSTRUCTOR CORPORATIVO' /* sgto_ajustado = 'CONSTRUCTOR CORPORATIVO' */
--            
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        )
--                  AND (TRIM(CAST(cncdbi AS STRING))='A')
--                  THEN 'CONSTRUCTOR EMPRESARIAL' /* sgto_ajustado = 'CONSTRUCTOR EMPRESARIAL' */
--
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        )
--                  AND (TRIM(CAST(cncdbi AS STRING))='B')
--                  THEN 'CONSTRUCTOR PYME' /* sgto_ajustado = 'CONSTRUCTOR PYME' */
--            
--               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
--                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
--                    )
--                    AND ( (sgto_ajustado IS NULL)
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
--                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
--                        )
--                  AND (TRIM(CAST(cncdbi AS STRING))='S')
--                  THEN 'SOCIAL' /* sgto_ajustado = 'SOCIAL' */
--            ELSE sgto_ajustado
--        END) AS sgto_ajustado
--
--
--    FROM tabla_1
--)
--SELECT id_cliente,
--       TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,
--       TRIM(CAST(modalidad AS STRING)) AS modalidad,
--       TRIM(UPPER(CAST(producto_ajustado AS STRING))) AS producto_ajustado,
--       TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado,
--       TRIM(UPPER(CAST(vic_ccial AS STRING))) AS vic_ccial,
--       TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada,
--       TRIM(UPPER(CAST(base AS STRING))) AS base,
--
--       anho,
--       mes,
--       obl17,
--       monto,
--       TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,
--       amcdty,
--       cncdbi
--FROM tabla_2
--;

/* la "tabla_2" es la tabla FINAL despues de llenar los campos NULL ó vacíos,
es la respuesta (resultado) del punto 2 

crear varias tablas temporales 
(tablas temporales anidadas):
https://stackoverflow.com/questions/49990666/trying-to-create-multiple-temporary-tables-in-a-single-query */

----------------

CREATE TABLE IF NOT EXISTS proceso_generadores.punto_2_terminado STORED AS PARQUET AS  

WITH tabla_temporal_1_proceso_consumidores_punto_1_terminado AS
(
   SELECT id_cliente,
          /* TRIM()              eliminar espacios en blanco al principio y al final    https://www.w3schools.com/sql/func_sqlserver_trim.asp
             UPPER()             convertir texto (string) a MAYUSCULA                   https://www.w3schools.com/sql/func_sqlserver_upper.asp
             CAST(... AS ...)    convertir a cualquier tipo de dato                     https://www.w3schools.com/sql/func_sqlserver_cast.asp */
          TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,
          TRIM(CAST(modalidad AS STRING)) AS modalidad,
          TRIM(UPPER(CAST(producto_ajustado AS STRING))) AS producto_ajustado,
          TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado,
          TRIM(UPPER(CAST(vic_ccial AS STRING))) AS vic_ccial,
          TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada,
          TRIM(UPPER(CAST(base AS STRING))) AS base,

          anho,
          mes,
          obl17,
          monto,
          TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,
          amcdty,
          cncdbi

      FROM proceso_consumidores.punto_1_terminado
      ORDER BY id_cliente ASC /* ordenar los id_cliente en orden ascendente
                                 (de menor a mayor) */
),

tabla_temporal_2_modalidad AS
(
      SELECT id_cliente,                           	
             TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,	
             TRIM(CAST(modalidad AS STRING)) AS modalidad_anterior,        	            	
             TRIM(UPPER(CAST(producto_ajustado AS STRING))) AS producto_ajustado,
             TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado,
             TRIM(UPPER(CAST(vic_ccial AS STRING))) AS vic_ccial,                     	
             TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada,                      	
             TRIM(UPPER(CAST(base AS STRING))) AS base,
        	
             anho,                                  	
             mes,                                  	
             obl17,	                            	
             monto,	                            	
             TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,	                            	
             amcdty,                            	
             cncdbi,

          /* ------- PUNTO 2 
          Es muy extraño q esto suceda porq no deberia suceder, pero aqui sucede:
          cuando uso IN() (abreviacion de OR) NO funciona los CASE, 
          en cambio cuando uso muchos OR (SIN abreviar) entonces SI funciona
          ------- */

          (CASE /* modalidad
                   invertir los números de la modalidad */
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (modalidad IS NULL)
                           OR (TRIM(CAST(modalidad AS STRING))='NULL')
                           OR (TRIM(CAST(modalidad AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
                        )
                    AND (CAST(amcdty AS DOUBLE)=1)
                    THEN '2' /* modalidad='2' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (modalidad IS NULL)
                           OR (TRIM(CAST(modalidad AS STRING))='NULL')
                           OR (TRIM(CAST(modalidad AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
                        )
                    AND (CAST(amcdty AS DOUBLE)=2)
                    THEN '1' /* modalidad='1' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (modalidad IS NULL)
                           OR (TRIM(CAST(modalidad AS STRING))='NULL')
                           OR (TRIM(CAST(modalidad AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
                        )
                    AND (CAST(amcdty AS DOUBLE)=4)
                    THEN '4' /* modalidad = '4' */

               /* Para el resto de aplicativos, la modalidad es igual al amcdty */
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ((modalidad IS NULL)
                        OR (TRIM(CAST(modalidad AS STRING))='NULL')
                        OR (TRIM(CAST(modalidad AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'E')
                        )
                    AND (CAST(amcdty AS DOUBLE)=1)
                    THEN '1' /* modalidad = '1' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                  AND ( (modalidad IS NULL)
                         OR (TRIM(CAST(modalidad AS STRING))='NULL')
                         OR (TRIM(CAST(modalidad AS STRING))='')
                      )
                 AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'L')
                        OR (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'E')
                     )
                  AND (CAST(amcdty AS DOUBLE)=2)
                 THEN '2' /* modalidad = '2' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                  AND ( (modalidad IS NULL)
                         OR (TRIM(CAST(modalidad AS STRING))='NULL')
                         OR (TRIM(CAST(modalidad AS STRING))='')
                      )
                 AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'L')
                        OR (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'E')
                     )
                 AND (CAST(amcdty AS DOUBLE)=4)
                 THEN '4' /* modalidad = '4' */

              ELSE modalidad
          END) AS modalidad_nuevo

      FROM tabla_temporal_1_proceso_consumidores_punto_1_terminado
      ORDER BY id_cliente ASC
),

tabla_temporal_3_producto_ajustado AS
(
      SELECT id_cliente,                           	
             TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,	
             modalidad_anterior,
             modalidad_nuevo,
             TRIM(UPPER(CAST(producto_ajustado AS STRING))) AS producto_ajustado_anterior,
             TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado,
             TRIM(UPPER(CAST(vic_ccial AS STRING))) AS vic_ccial,                        	
             TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada,                     	
             TRIM(UPPER(CAST(base AS STRING))) AS base,
        	
             anho,                                  	
             mes,                                  	
             obl17,	                            	
             monto,	                            	
             TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,	                            	
             amcdty,                            	
             cncdbi,

          (CASE /* producto_ajustado */
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (producto_ajustado IS NULL)
                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
                        )                      
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
                        )
                    AND (TRIM(CAST(modalidad_nuevo AS STRING))='2')
                    THEN 'LIBRE INVERSION' /* producto_ajustado = 'LIBRE INVERSION' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (producto_ajustado IS NULL)
                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
                        )
                  AND (TRIM(CAST(modalidad_nuevo AS STRING))='1')
                  THEN 'CARTERA ORDINARIA' /* producto_ajustado = 'CARTERA ORDINARIA' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (producto_ajustado IS NULL)
                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
                        )
                    AND (TRIM(CAST(modalidad_nuevo AS STRING))='4')
                    THEN 'CARTERA MICROCREDITO' /* producto_ajustado = 'CARTERA MICROCREDITO' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (producto_ajustado IS NULL)
                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='K')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='M')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='V')
                        )
                THEN 'TARJETA DE CREDITO' /* producto_ajustado = 'TARJETA DE CREDITO' */


               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (producto_ajustado IS NULL)
                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
                        )
                    AND (TRIM(CAST(aplicativo AS STRING))='D') 
                THEN 'SOBREGIROS' /* producto_ajustado = 'SOBREGIROS' */

            ELSE producto_ajustado
        END) AS producto_ajustado_nuevo

      FROM tabla_temporal_2_modalidad
      ORDER BY id_cliente ASC
),

tabla_temporal_4_sgto_ajustado AS
(
      SELECT id_cliente,                           	
             TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,	          	
             modalidad_anterior,
             modalidad_nuevo, 
             producto_ajustado_anterior,
             producto_ajustado_nuevo,
             TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado_anterior,
             TRIM(UPPER(CAST(vic_ccial AS STRING))) AS vic_ccial,                        	
             TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada,                    	
             TRIM(UPPER(CAST(base AS STRING))) AS base,
        	
             anho,                                  	
             mes,                                  	
             obl17,	                            	
             monto,	                            	
             TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,	                            	
             amcdty,                            	
             cncdbi,

          (CASE /* sgto_ajustado */ 
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                    AND (TRIM(CAST(cncdbi AS STRING))='5')
                    THEN 'PYMES' /* sgto_ajustado = 'PYMES' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                    AND (TRIM(CAST(cncdbi AS STRING))='9')
                    THEN 'NEGOCIOS E INDEPEN' /* sgto_ajustado = 'NEGOCIOS E INDEPEN' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                    AND (TRIM(CAST(cncdbi AS STRING))='G')
                    THEN 'GOBIERNO DE RED' /* sgto_ajustado = 'GOBIERNO DE RED' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    ) 
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='4')
                  THEN 'PERSONAL' /* sgto_ajustado = 'PERSONAL' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='M')
                  THEN 'PLUS' /* sgto_ajustado = 'PLUS' */
            
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='6')
                  THEN 'PREFERENCIAL' /* sgto_ajustado = 'PREFERENCIAL' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='3')
                  THEN 'CORPORATIVO' /* sgto_ajustado = 'CORPORATIVO' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                    AND (TRIM(CAST(cncdbi AS STRING))='2')
                    THEN 'EMPRESARIAL' /* sgto_ajustado = 'EMPRESARIAL' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                    AND (TRIM(CAST(cncdbi AS STRING))='1')
                    THEN 'GOBIERNO' /* sgto_ajustado = 'GOBIERNO' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        ) 
                  AND (TRIM(CAST(cncdbi AS STRING))='7')
                  THEN 'INTERNACIONAL' /* sgto_ajustado = 'INTERNACIONAL' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='8')
                  THEN 'INSTITUCIONES FINANCIERAS' /* sgto_ajustado = 'INSTITUCIONES FINANCIERAS' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        ) 
                  AND (TRIM(CAST(cncdbi AS STRING))='C')
                  THEN 'CONSTRUCTOR CORPORATIVO' /* sgto_ajustado = 'CONSTRUCTOR CORPORATIVO' */
            
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='A')
                  THEN 'CONSTRUCTOR EMPRESARIAL' /* sgto_ajustado = 'CONSTRUCTOR EMPRESARIAL' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='B')
                  THEN 'CONSTRUCTOR PYME' /* sgto_ajustado = 'CONSTRUCTOR PYME' */
            
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='S')
                  THEN 'SOCIAL' /* sgto_ajustado = 'SOCIAL' */

            ELSE sgto_ajustado
        END) AS sgto_ajustado_nuevo

     FROM tabla_temporal_3_producto_ajustado
     ORDER BY id_cliente ASC
)
SELECT id_cliente,                           	
       TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,	          	
       TRIM(CAST(modalidad_nuevo AS STRING)) AS modalidad,
       TRIM(UPPER(CAST(producto_ajustado_nuevo AS STRING))) AS producto_ajustado,
       TRIM(UPPER(CAST(sgto_ajustado_nuevo AS STRING))) AS sgto_ajustado,
       TRIM(UPPER(CAST(vic_ccial AS STRING))) AS vic_ccial,                        	
       TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada,                    	
       TRIM(UPPER(CAST(base AS STRING))) AS base,
        	
       anho,                                  	
       mes,                                  	
       obl17,	                            	
       monto,	                            	
       TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,	                            	
       amcdty,                            	
       cncdbi
FROM tabla_temporal_4_sgto_ajustado
ORDER BY id_cliente ASC
;

-----------------------------------------------------------------------------------------------
/* 
La siguiente consulta es exactamente igual a la anterior,
lo unico q cambia es q 
se puede ver como estaban las columnas antes y despues de ser reemplazadas segun los condicionales 

La siguiente consulta NO se tiene q ejecutar desde el Python

CASE WHEN en SQL 
https://www.w3schools.com/sql/sql_case.asp
*/

WITH tabla_temporal_1_proceso_consumidores_punto_1_terminado AS
(
   SELECT id_cliente,
          /* TRIM()              eliminar espacios en blanco al principio y al final    https://www.w3schools.com/sql/func_sqlserver_trim.asp
             UPPER()             convertir texto (string) a MAYUSCULA                   https://www.w3schools.com/sql/func_sqlserver_upper.asp
             CAST(... AS ...)    convertir a cualquier tipo de dato                     https://www.w3schools.com/sql/func_sqlserver_cast.asp */
          TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,
          TRIM(CAST(modalidad AS STRING)) AS modalidad,
          TRIM(UPPER(CAST(producto_ajustado AS STRING))) AS producto_ajustado,
          TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado,
          TRIM(UPPER(CAST(vic_ccial AS STRING))) AS vic_ccial,
          TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada,
          TRIM(UPPER(CAST(base AS STRING))) AS base,

          anho,
          mes,
          obl17,
          monto,
          TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,
          amcdty,
          cncdbi

      FROM proceso_consumidores.punto_1_terminado
      ORDER BY id_cliente ASC /* ordenar los id_cliente en orden ascendente
                                 (de menor a mayor) */
),

tabla_temporal_2_modalidad AS
(
      SELECT id_cliente,                           	
             TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,	
             TRIM(CAST(modalidad AS STRING)) AS modalidad_anterior,        	            	
             TRIM(UPPER(CAST(producto_ajustado AS STRING))) AS producto_ajustado,
             TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado,
             TRIM(UPPER(CAST(vic_ccial AS STRING))) AS vic_ccial,                     	
             TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada,                      	
             TRIM(UPPER(CAST(base AS STRING))) AS base,
        	
             anho,                                  	
             mes,                                  	
             obl17,	                            	
             monto,	                            	
             TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,	                            	
             amcdty,                            	
             cncdbi,

          /* ------- PUNTO 2 
          Es muy extraño q esto suceda porq no deberia suceder, pero aqui sucede:
          cuando uso IN() (abreviacion de OR) NO funciona los CASE, 
          en cambio cuando uso muchos OR (SIN abreviar) entonces SI funciona
          ------- */

          (CASE /* modalidad
                   invertir los números de la modalidad */
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (modalidad IS NULL)
                           OR (TRIM(CAST(modalidad AS STRING))='NULL')
                           OR (TRIM(CAST(modalidad AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
                        )
                    AND (CAST(amcdty AS DOUBLE)=1)
                    THEN '2' /* modalidad='2' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (modalidad IS NULL)
                           OR (TRIM(CAST(modalidad AS STRING))='NULL')
                           OR (TRIM(CAST(modalidad AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
                        )
                    AND (CAST(amcdty AS DOUBLE)=2)
                    THEN '1' /* modalidad='1' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (modalidad IS NULL)
                           OR (TRIM(CAST(modalidad AS STRING))='NULL')
                           OR (TRIM(CAST(modalidad AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
                        )
                    AND (CAST(amcdty AS DOUBLE)=4)
                    THEN '4' /* modalidad = '4' */

               /* Para el resto de aplicativos, la modalidad es igual al amcdty */
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ((modalidad IS NULL)
                        OR (TRIM(CAST(modalidad AS STRING))='NULL')
                        OR (TRIM(CAST(modalidad AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'E')
                        )
                    AND (CAST(amcdty AS DOUBLE)=1)
                    THEN '1' /* modalidad = '1' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                  AND ( (modalidad IS NULL)
                         OR (TRIM(CAST(modalidad AS STRING))='NULL')
                         OR (TRIM(CAST(modalidad AS STRING))='')
                      )
                 AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'L')
                        OR (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'E')
                     )
                  AND (CAST(amcdty AS DOUBLE)=2)
                 THEN '2' /* modalidad = '2' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                  AND ( (modalidad IS NULL)
                         OR (TRIM(CAST(modalidad AS STRING))='NULL')
                         OR (TRIM(CAST(modalidad AS STRING))='')
                      )
                 AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'L')
                        OR (TRIM(UPPER(CAST(aplicativo AS STRING)))<>'E')
                     )
                 AND (CAST(amcdty AS DOUBLE)=4)
                 THEN '4' /* modalidad = '4' */

              ELSE modalidad
          END) AS modalidad_nuevo

      FROM tabla_temporal_1_proceso_consumidores_punto_1_terminado
      ORDER BY id_cliente ASC
),

tabla_temporal_3_producto_ajustado AS
(
      SELECT id_cliente,                           	
             TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,	
             modalidad_anterior,
             modalidad_nuevo,
             TRIM(UPPER(CAST(producto_ajustado AS STRING))) AS producto_ajustado_anterior,
             TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado,
             TRIM(UPPER(CAST(vic_ccial AS STRING))) AS vic_ccial,                        	
             TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada,                     	
             TRIM(UPPER(CAST(base AS STRING))) AS base,
        	
             anho,                                  	
             mes,                                  	
             obl17,	                            	
             monto,	                            	
             TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,	                            	
             amcdty,                            	
             cncdbi,

          (CASE /* producto_ajustado */
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (producto_ajustado IS NULL)
                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
                        )                      
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
                        )
                    AND (TRIM(CAST(modalidad_nuevo AS STRING))='2')
                    THEN 'LIBRE INVERSION' /* producto_ajustado = 'LIBRE INVERSION' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (producto_ajustado IS NULL)
                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
                        )
                  AND (TRIM(CAST(modalidad_nuevo AS STRING))='1')
                  THEN 'CARTERA ORDINARIA' /* producto_ajustado = 'CARTERA ORDINARIA' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (producto_ajustado IS NULL)
                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='L')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='E')
                        )
                    AND (TRIM(CAST(modalidad_nuevo AS STRING))='4')
                    THEN 'CARTERA MICROCREDITO' /* producto_ajustado = 'CARTERA MICROCREDITO' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (producto_ajustado IS NULL)
                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
                        )
                    AND ( (TRIM(UPPER(CAST(aplicativo AS STRING)))='K')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='M')
                           OR (TRIM(UPPER(CAST(aplicativo AS STRING)))='V')
                        )
                THEN 'TARJETA DE CREDITO' /* producto_ajustado = 'TARJETA DE CREDITO' */


               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (producto_ajustado IS NULL)
                           OR (TRIM(CAST(producto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(producto_ajustado AS STRING))='')
                        )
                    AND (TRIM(CAST(aplicativo AS STRING))='D') 
                THEN 'SOBREGIROS' /* producto_ajustado = 'SOBREGIROS' */

            ELSE producto_ajustado
        END) AS producto_ajustado_nuevo

      FROM tabla_temporal_2_modalidad
      ORDER BY id_cliente ASC
),

tabla_temporal_4_sgto_ajustado AS
(
      SELECT id_cliente,                           	
             TRIM(UPPER(CAST(clasificacion_de_cartera AS STRING))) AS clasificacion_de_cartera,	          	
             modalidad_anterior,
             modalidad_nuevo, 
             producto_ajustado_anterior,
             producto_ajustado_nuevo,
             TRIM(UPPER(CAST(sgto_ajustado AS STRING))) AS sgto_ajustado_anterior,
             TRIM(UPPER(CAST(vic_ccial AS STRING))) AS vic_ccial,                        	
             TRIM(UPPER(CAST(banca_ajustada AS STRING))) AS banca_ajustada,                    	
             TRIM(UPPER(CAST(base AS STRING))) AS base,
        	
             anho,                                  	
             mes,                                  	
             obl17,	                            	
             monto,	                            	
             TRIM(UPPER(CAST(aplicativo AS STRING))) AS aplicativo,	                            	
             amcdty,                            	
             cncdbi,

          (CASE /* sgto_ajustado */ 
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                    AND (TRIM(CAST(cncdbi AS STRING))='5')
                    THEN 'PYMES' /* sgto_ajustado = 'PYMES' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                    AND (TRIM(CAST(cncdbi AS STRING))='9')
                    THEN 'NEGOCIOS E INDEPEN' /* sgto_ajustado = 'NEGOCIOS E INDEPEN' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                    AND (TRIM(CAST(cncdbi AS STRING))='G')
                    THEN 'GOBIERNO DE RED' /* sgto_ajustado = 'GOBIERNO DE RED' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    ) 
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='4')
                  THEN 'PERSONAL' /* sgto_ajustado = 'PERSONAL' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='M')
                  THEN 'PLUS' /* sgto_ajustado = 'PLUS' */
            
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='6')
                  THEN 'PREFERENCIAL' /* sgto_ajustado = 'PREFERENCIAL' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='3')
                  THEN 'CORPORATIVO' /* sgto_ajustado = 'CORPORATIVO' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                    AND (TRIM(CAST(cncdbi AS STRING))='2')
                    THEN 'EMPRESARIAL' /* sgto_ajustado = 'EMPRESARIAL' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                    AND (TRIM(CAST(cncdbi AS STRING))='1')
                    THEN 'GOBIERNO' /* sgto_ajustado = 'GOBIERNO' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        ) 
                  AND (TRIM(CAST(cncdbi AS STRING))='7')
                  THEN 'INTERNACIONAL' /* sgto_ajustado = 'INTERNACIONAL' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='8')
                  THEN 'INSTITUCIONES FINANCIERAS' /* sgto_ajustado = 'INSTITUCIONES FINANCIERAS' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        ) 
                  AND (TRIM(CAST(cncdbi AS STRING))='C')
                  THEN 'CONSTRUCTOR CORPORATIVO' /* sgto_ajustado = 'CONSTRUCTOR CORPORATIVO' */
            
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='A')
                  THEN 'CONSTRUCTOR EMPRESARIAL' /* sgto_ajustado = 'CONSTRUCTOR EMPRESARIAL' */

               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='B')
                  THEN 'CONSTRUCTOR PYME' /* sgto_ajustado = 'CONSTRUCTOR PYME' */
            
               WHEN ( (TRIM(UPPER(CAST(base AS STRING)))='BANCO')
                       OR (TRIM(UPPER(CAST(base AS STRING)))='VENTA')
                    )
                    AND ( (sgto_ajustado IS NULL)
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='NULL')
                           OR (TRIM(CAST(sgto_ajustado AS STRING))='')
                        )
                  AND (TRIM(CAST(cncdbi AS STRING))='S')
                  THEN 'SOCIAL' /* sgto_ajustado = 'SOCIAL' */

            ELSE sgto_ajustado
        END) AS sgto_ajustado_nuevo

     FROM tabla_temporal_3_producto_ajustado
     ORDER BY id_cliente ASC
)
SELECT * 
FROM tabla_temporal_4_sgto_ajustado
ORDER BY id_cliente ASC
;

-----------------------------------------------------------------------------------------------
