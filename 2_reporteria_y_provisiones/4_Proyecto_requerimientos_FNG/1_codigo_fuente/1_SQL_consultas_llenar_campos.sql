/*
los numero_obligacion son UNICOS
y los numero_identificacion se pueden REPETIR
esto significa q un solo cliente puede tener varias obligaciones (deudas)

En esta parte de las consultas
WHERE ... AND CAST(... AS BIGINT) IN (...)
cuando dice id entonces es numero_identificacion
y cuando dice obl entonces es numero_obligacion

TABLA PRE-LIMINAR
proceso_riesgos.ceniegarc_preliminar
resultados_riesgos.ceniegarc_preliminar

TABLA DEFINITIVA
resultados_riesgos.ceniegarc_lz

ERROR (LA TABLA NO EXISTE)
proceso_riesgos.ceniegarc_lz




Ejemplo de consulta_1

Dentro del IN (...) va el
numero_identificacion

El nombre de la nueva columna en el Excel de respuesta es
max_altmora_a_corte_de_febrero_20_del_cliente_interna */

SELECT CAST(id AS BIGINT) AS numero_identificacion, 
       MAX(altmora) AS max_altmora_a_corte_de_febrero_20_del_cliente_interna
FROM resultados_riesgos.ceniegarc_lz
WHERE (CAST(year AS BIGINT)=2020)
      AND (CAST(corte AS BIGINT)=202002) /*año mes*/
      AND (CAST(id AS BIGINT))
      IN (
          /*aqui va el numero_identificacion*/
         )
GROUP BY 1
;



-----------------------------------------------------------------------------------------------
/* Ejemplo de consulta_2

El nombre de la nueva columna en el Excel de respuesta es
calificacion_al_momento_del_desembolso

Dependiendo de la tabla las consultas son diferentes

Dentro de IN (...) va el
numero_obligacion

consulta para (TABLA DEFINITIVA):    resultados_riesgos.ceniegarc_lz

Hay 2 formas de hacer la consulta a la tabla definitiva



forma 1: esta es la mejor forma de hacer la consulta a la tabla definitiva 

Crear una tabla_temporal 
a partir de FROM resultados_riesgos.ceniegarc_lz */

WITH tabla_temporal_1 AS
(
           /* nombres de las columnas de la tabla_temporal_1 (FROM resultados_riesgos.ceniegarc_lz) */
    SELECT CAST(id AS BIGINT) AS numero_identificacion,
           CAST(obl341 AS BIGINT) AS numero_obligacion,
           TRIM(CAST(calife AS STRING)) AS calificacion_al_momento_del_desembolso,
           ((year*100) + ingestion_month) AS fecha_de_desembolso
    FROM resultados_riesgos.ceniegarc_lz
    WHERE CAST(obl341 AS BIGINT) 
          IN (
               /*aqui va el numero_obligacion*/
             )
),

tabla_temporal_2 AS 
(
    SELECT numero_identificacion,
           numero_obligacion,
           calificacion_al_momento_del_desembolso,
           fecha_de_desembolso,

           /*esto ROW_NUMBER() OVER(PARTITION BY ...) 
           va a seleccionar la PRIMERA vez q aparece cada uno de los registros */
           ROW_NUMBER() OVER(PARTITION BY numero_obligacion 
                             ORDER BY fecha_de_desembolso ASC /* orden ascendente (de menor a mayor) */
                            ) AS numero /* El nombre de la columna con el resultado de ROW_NUMBER() OVER(PARTITION BY ...) es numero */
    FROM tabla_temporal_1 /* las columnas de la tabla_temporal_1 se toman de la tabla llamada resultados_riesgos.ceniegarc_lz */
)

       /* nombres de las columnas q estan en el AS ... de la tabla_temporal */
SELECT numero_identificacion,
       numero_obligacion,
       calificacion_al_momento_del_desembolso
FROM tabla_temporal_2 /* seleccionar la tabla_temporal_2 */
WHERE numero=1 /* seleccionar la PRIMERA vez q aparece cada uno de los registros */
;


-------


/*
consulta para (TABLA DEFINITIVA):    resultados_riesgos.ceniegarc_lz

forma 2: Esta consulta de aqui abajo es la unica a la q le cambia la fecha
         Lo q cambia de la fecha es: year
                                     ingestion_mont
              
         year y ingestion_mont cambian de acuerdo a la columna fecha_desembolso 
         para cada una de las filas de los Excel q estan en \2_excel\1_insumo

         Esta consulta NO esta en el codigo de Python  */
SELECT CAST(id AS BIGINT) AS numero_identificacion,
       CAST(obl341 AS BIGINT) AS numero_obligacion,
       TRIM(CAST(calife AS STRING)) AS calificacion_al_momento_del_desembolso
FROM resultados_riesgos.ceniegarc_lz
WHERE (CAST(year AS BIGINT)=2021)
      AND (CAST(ingestion_month AS BIGINT)=11)
      AND (FROM_TIMESTAMP(fdesem,'yyyyMM') = CAST(corte /*202111*/ AS STRING))
      AND (CAST(obl341 AS BIGINT))
      IN (
          /*aqui va el numero_obligacion*/    
         )
;

-------

/* consulta para (TABLA PRE-LIMINAR):    proceso_riesgos.ceniegarc_preliminar */
SELECT CAST(id AS BIGINT) AS numero_identificacion,
       CAST(obl341 AS BIGINT) AS numero_obligacion,
       TRIM(CAST(calife AS STRING)) AS calificacion_al_momento_del_desembolso
FROM proceso_riesgos.ceniegarc_preliminar 
WHERE CAST(obl341 AS BIGINT)
      IN ( 
          /*aqui va el numero_obligacion*/
         )
;

-------

/* consulta para (TABLA PRE-LIMINAR):    resultados_riesgos.ceniegarc_preliminar */
SELECT DISTINCT CAST(id AS BIGINT) AS numero_identificacion,
                CAST(obl341 AS BIGINT) AS numero_obligacion,
                TRIM(CAST(calife AS STRING)) AS calificacion_al_momento_del_desembolso
FROM resultados_riesgos.ceniegarc_preliminar
WHERE CAST(obl341 AS BIGINT)
      IN ( 
          /*aqui va el numero_obligacion*/
         )
;



-----------------------------------------------------------------------------------------------
/* Ejemplo de consulta_3

El nombre de la nueva columna en el Excel de respuesta es
max_calificacion_del_cliente_a_corte_de_junio_20

Dentro del IN (...) va el
numero_identificacion

Las tablas en las q se hace esta consulta son (FROM ...)
TABLA PRE-LIMINAR
proceso_riesgos.ceniegarc_preliminar
resultados_riesgos.ceniegarc_preliminar

TABLA DEFINITIVA
resultados_riesgos.ceniegarc_lz */

SELECT CAST(id AS BIGINT) AS numero_identificacion,
       MAX(calife) AS max_calificacion_del_cliente_a_corte_de_junio_20
FROM resultados_riesgos.ceniegarc_lz
WHERE (CAST(year AS BIGINT)=2020) 
      AND (CAST(corte AS BIGINT)=202006) 
      AND CAST(id AS BIGINT)
      IN (
          /*aqui va el numero_identificacion*/  
         )
GROUP BY 1
;



-----------------------------------------------------------------------------------------------
/* Ejemplo de consulta_4

El nombre de la nueva columna en el Excel de respuesta es
max_altamora_a_corte_de_febrero_20_del_cliente_externa

Dentro del IN (...) va el
numero_identificacion

TODAS las formas son iguales,
lo UNICO q cambia es:
- FROM NombreBaseDeDatos.NombreTabla
- El mes de ingestion, a lo q es igual el ingestion_month
  AND (CAST(ingestion_month AS BIGINT)=...) 

Los datos se buscan en las tablas segun el tipo de ID (persona):
NOMBRE TABLA EN LA Q SE HACE LA CONSULTA (FROM ...)              TIPO DE ID (PERSONA)
resultados_preaprobados.buro_pj_detallado_principal              persona juridica
resultados_preaprobados.buro_pn_detallado_principal              persona natural
resultados_preaprobados.buro_indep_sineeff_detallado_principal   persona independiente



Saber TODOS los datos q contiene la columna mora_0
esto es lo q esta en el Excel 7_catalogo_moras_experian_(consulta_4).xlsx */
WITH tabla_temporal AS 
(
         /* Forma 1 */
         SELECT DISTINCT CAST(mora_0 AS BIGINT) AS todos_los_datos_de_columna_mora_0
         FROM resultados_preaprobados.buro_pj_detallado_principal /* persona juridica */

         UNION 

         /* Forma 2 */
         SELECT DISTINCT CAST(mora_0 AS BIGINT) AS todos_los_datos_de_columna_mora_0
         FROM resultados_preaprobados.buro_pn_detallado_principal /* persona natural */

         UNION

         /* Forma 3 */
         SELECT DISTINCT CAST(mora_0 AS BIGINT) AS todos_los_datos_de_columna_mora_0
         FROM resultados_preaprobados.buro_indep_sineeff_detallado_principal /* persona independiente */
)
SELECT todos_los_datos_de_columna_mora_0
FROM tabla_temporal
ORDER BY 1 ASC /* orden ascendente: de menor a mayor */
;

-----------------

/*  Forma 1: 
- tabla resultados_preaprobados.buro_pj_detallado_principal
- ingestion_month = 2 */
SELECT CAST(num_doc AS BIGINT) AS numero_identificacion,
       MAX(mora_0) AS max_altamora_a_corte_de_febrero_20_del_cliente_externa
FROM resultados_preaprobados.buro_pj_detallado_principal /* persona juridica */
WHERE (CAST(ingestion_year AS BIGINT)=2020)
      AND (CAST(ingestion_month AS BIGINT)=2) 
      AND CAST(num_doc AS BIGINT)
      IN (
          /*aqui va el numero_identificacion*/
         )
GROUP BY 1
;

-----------------

/* 
Forma 2: 
- tabla resultados_preaprobados.buro_pn_detallado_principal
- ingestion_month = 8 */
SELECT CAST(num_doc AS BIGINT) AS numero_identificacion, 
       MAX(mora_0) AS max_altamora_a_corte_de_febrero_20_del_cliente_externa
FROM resultados_preaprobados.buro_pn_detallado_principal /* persona natural */
WHERE (CAST(ingestion_year AS BIGINT)=2020)
      AND (CAST(ingestion_month AS BIGINT)=8)
      AND CAST(num_doc AS BIGINT)
      IN (
          /*aqui va el numero_identificacion*/
         )
GROUP BY 1
;

-----------------

/* 
Forma 3:
- tabla resultados_preaprobados.buro_indep_sineeff_detallado_principal
- ingestion_month = 4 */
SELECT CAST(num_doc AS BIGINT) AS numero_identificacion, 
       MAX(mora_0) AS max_altamora_a_corte_de_febrero_20_del_cliente_externa
FROM resultados_preaprobados.buro_indep_sineeff_detallado_principal /* persona independiente */
WHERE (CAST(ingestion_year AS BIGINT)=2020)
      AND (CAST(ingestion_month AS BIGINT)=4)
      AND CAST(num_doc AS BIGINT)
      IN (
          /*aqui va el numero_identificacion*/
         )
GROUP BY 1
;

-----------------

/* consulta 4 - uniendo todas las formas 1, 2 y 3 en una sola consulta 

La forma correcta de hacer esta consulta es creando una tabla_temporal,
por eso use WITH tabla_temporal AS (...) SELECT * FROM tabla_temporal
porque cuando NO haces esto y ejecutas la consulta en Python con el ODBC 
en esta linea de codigo de Python:
df_ejecutar_consulta_4 = pd.read_sql_query(consulta_4, cn)

te da este error:
columns = [col_desc[0] for col_desc in cursor.description]
TypeError: 'NoneType' object is not iterable

Intente solucionarlo asi pero no funciono:
https://stackoverflow.com/questions/57866905/typeerror-nonetype-object-is-not-iterable-from-pandas-read-sql

Pero SI me funciono con la WITH tabla_temporal...*/
WITH tabla_temporal AS 
(        /*Forma 1*/
         SELECT CAST(num_doc AS BIGINT) AS numero_identificacion,
                /*TRIM (...) elimina espacios en blanco al principio y al final
                CAST(... AS ...) convertir a cualquier tipo de dato
                MAX(...) selecciona el dato maximo */
                TRIM(CAST(MAX(mora_0) AS STRING)) AS max_altamora_a_corte_de_febrero_20_del_cliente_externa
         FROM resultados_preaprobados.buro_pj_detallado_principal /* persona juridica */
         WHERE (CAST(ingestion_year AS BIGINT)=2020)
               AND (CAST(ingestion_month AS BIGINT)=2) 
               AND CAST(num_doc AS BIGINT)
               IN (
                   /*aqui va el numero_identificacion
                   para q esto funcione en todos los SELECT tiene q estar los mismos numeros de ID*/
                   --94502368
                  )
         GROUP BY 1
         
         /* Concatenar (unir) consultas SQL
         UNION elimina filas (registros) duplicados,
         solamente selecciona las filas (registros) que son diferentes
         https://stackoverflow.com/questions/49925/what-is-the-difference-between-union-and-union-all */
         UNION
         
         /* Forma 2 */
         SELECT CAST(num_doc AS BIGINT) AS numero_identificacion, 
                TRIM(CAST(MAX(mora_0) AS STRING)) AS max_altamora_a_corte_de_febrero_20_del_cliente_externa
         FROM resultados_preaprobados.buro_pn_detallado_principal /* persona natural */
         WHERE (CAST(ingestion_year AS BIGINT)=2020)
               AND (CAST(ingestion_month AS BIGINT)=8)
               AND CAST(num_doc AS BIGINT)
               IN (
                   /*aqui va el numero_identificacion*/
                   --94502368
                  )
         GROUP BY 1
         
         UNION
         
         /* Forma 3 */
         SELECT CAST(num_doc AS BIGINT) AS numero_identificacion, 
                TRIM(CAST(MAX(mora_0) AS STRING)) AS max_altamora_a_corte_de_febrero_20_del_cliente_externa
         FROM resultados_preaprobados.buro_indep_sineeff_detallado_principal /* persona independiente */
         WHERE (CAST(ingestion_year AS BIGINT)=2020)
               AND (CAST(ingestion_month AS BIGINT)=4)
               AND CAST(num_doc AS BIGINT)
               IN (
                   /*aqui va el numero_identificacion*/
                   --94502368
                  )
         GROUP BY 1
)
SELECT numero_identificacion,
       max_altamora_a_corte_de_febrero_20_del_cliente_externa
FROM tabla_temporal
;

/* numero_identificacion 94502368 */

-----------------

/* 
consulta 4 - uniendo todas las formas 1, 2 y 3 en una sola consulta 
             y reemplazando los valores q estan en 7_catalogo_moras_experian_(consulta_4).xlsx
             Para reemplazar los valores usamos CASE WHEN

consulta_4 - calificacion_externa	    Descripcion Mora
0	                                EL MES NO EXISTE
1	                                MORA 30 DIAS
2	                                MORA 60 DIAS
3	                                MORA 90 DIAS
4	                                MORA 120 DIAS
5	                                AL DIA
6	                                N/A
7	                                CADUCIDAD
8	                                N/A
9	                                SIN INFORMACION
10	                                MORA 150 DIAS
11	                                MORA 180 DIAS
12	                                MORA 210+ DIAS
13	                                DUDOSO RECAUDO
14	                                CARTERA CASTIGADA
15	                                N/A

ESTA CONSULTA 4 DE AQUI ABAJO ES LA Q FINALMENTE ESTA EN EL PYTHON */

WITH tabla_temporal AS 
( 
         /* Forma 1 */
         SELECT CAST(num_doc AS BIGINT) AS numero_identificacion,
                TRIM(CAST(MAX(mora_0) AS STRING)) AS max_altamora_a_corte_de_febrero_20_del_cliente_externa
         FROM resultados_preaprobados.buro_pj_detallado_principal /* persona juridica */
         WHERE (CAST(ingestion_year AS BIGINT)=2020)
               AND (CAST(ingestion_month AS BIGINT)=2) 
               AND CAST(num_doc AS BIGINT)
               IN (
                   /*aqui va el numero_identificacion
                   para q esto funcione en todos los SELECT tiene q estar los mismos numeros de ID*/
                   --94502368
                  )
         GROUP BY 1

         UNION
         
         /* Forma 2 */
         SELECT CAST(num_doc AS BIGINT) AS numero_identificacion, 
                TRIM(CAST(MAX(mora_0) AS STRING)) AS max_altamora_a_corte_de_febrero_20_del_cliente_externa
         FROM resultados_preaprobados.buro_pn_detallado_principal /* persona natural */
         WHERE (CAST(ingestion_year AS BIGINT)=2020)
               AND (CAST(ingestion_month AS BIGINT)=8)
               AND CAST(num_doc AS BIGINT)
               IN (
                   /*aqui va el numero_identificacion*/
                   --94502368
                  )
         GROUP BY 1
         
         UNION
         
         /* Forma 3 */
         SELECT CAST(num_doc AS BIGINT) AS numero_identificacion, 
                TRIM(CAST(MAX(mora_0) AS STRING)) AS max_altamora_a_corte_de_febrero_20_del_cliente_externa
         FROM resultados_preaprobados.buro_indep_sineeff_detallado_principal /* persona independiente */
         WHERE (CAST(ingestion_year AS BIGINT)=2020)
               AND (CAST(ingestion_month AS BIGINT)=4)
               AND CAST(num_doc AS BIGINT)
               IN (
                   /*aqui va el numero_identificacion*/
                   --94502368
                  )
         GROUP BY 1
)
SELECT numero_identificacion,
       /*max_altamora_a_corte_de_febrero_20_del_cliente_externa,*/
        (
         /* Reemplazar en la consulta_4 los valores q estan en el Excel 7_catalogo_moras_experian_(consulta_4).xlsx
            usando CASE WHEN */
         CASE
             /* Convertir la columna max_altamora_a_corte_de_febrero_20_del_cliente_externa 
                a tipo string para solucionar este error:
                https://stackoverflow.com/questions/48892701/impala-query-error-analysisexception-operands-of-type-int-and-string-are-not */
             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '0')
                  THEN 'EL MES NO EXISTE' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'EL MES NO EXISTE' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '1')
                  THEN 'MORA 30 DIAS' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 30 DIAS' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '2')
                  THEN 'MORA 60 DIAS' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 60 DIAS' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '3')
                  THEN 'MORA 90 DIAS' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 90 DIAS' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '4')
                  THEN 'MORA 120 DIAS' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 120 DIAS' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '5')
                  THEN 'AL DIA' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'AL DIA' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '6')
                  THEN 'N/A' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'N/A' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '7')
                  THEN 'CADUCIDAD' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'CADUCIDAD' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '8')
                  THEN 'N/A' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'N/A' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '9')
                  THEN 'SIN INFORMACION' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'SIN INFORMACION' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '10')
                  THEN 'MORA 150 DIAS' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 150 DIAS' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '11')
                  THEN 'MORA 180 DIAS' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 180 DIAS' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '12')
                  THEN 'MORA 210+ DIAS' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'MORA 210+ DIAS' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '13')
                  THEN 'DUDOSO RECAUDO' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'DUDOSO RECAUDO' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '14')
                  THEN 'CARTERA CASTIGADA' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'CARTERA CASTIGADA' */

             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '15')
                  THEN 'N/A' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'N/A' */

            /* Saber en SQL si una casilla esta vacia o NULL: 
               https://stackoverflow.com/questions/15663207/how-to-use-null-or-empty-string-in-sql */
             WHEN (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = '')
                   OR (CAST(max_altamora_a_corte_de_febrero_20_del_cliente_externa AS STRING) = 'NULL')
                   OR (max_altamora_a_corte_de_febrero_20_del_cliente_externa IS NULL)
                  THEN 'SIN INFORMACION' /* max_altamora_a_corte_de_febrero_20_del_cliente_externa = 'SIN INFORMACION' */

             ELSE max_altamora_a_corte_de_febrero_20_del_cliente_externa
         END) AS max_altamora_a_corte_de_febrero_20_del_cliente_externa

FROM tabla_temporal
;

/* numero_identificacion 94502368 */

-----------------------------------------------------------------------------------------------
