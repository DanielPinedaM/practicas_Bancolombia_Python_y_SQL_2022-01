/* 
Crear consultas SQL para insertar (subir) los datos q estan en los .txt

Nombres de las tablas:
TEMPORAL
proceso_consumidores.base_historicas_lotus_txt_tmp

DEFINITIVO 1
proceso_consumidores.base_historicas_lotus_txt_definitivo

DEFINITIVO 2
proceso_consumidores.base_historicas_lotus_txt_definitivo_2

DEFINITIVO 3 - esta es la tabla final buena (corregida)
proceso_consumidores.base_historicas_lotus_txt_definitivo_2



eliminar la tabla si ya existe */

DROP TABLE IF EXISTS proceso_consumidores.base_historicas_lotus_txt PURGE
;

-----------------------------------------------------------------------------------------------
/* 
crear una tabla vacia con sus nombres de columnas (campos)
https://www.tutorialspoint.com/impala/impala_create_table_statement.htm */

CREATE TABLE IF NOT EXISTS proceso_consumidores.base_historicas_lotus_txt (
    radicado STRING,	
    identificacion_del_cliente STRING,
    tipo_de_identificacion STRING,
    nombre_del_cliente STRING,
    region STRING,
    zona STRING,
    segmento STRING,
    sector STRING,
    actividad_economica STRING,
    codigo_CIIU STRING,
    gerente STRING,
    codigo_del_gerente STRING,
    centro_de_costos STRING,
    grupo_de_riesgo STRING,
    codigo_de_riesgo STRING,
    calificacion_superbancaria STRING,
    calificacion_interna_actual STRING,
    fecha_de_creacion STRING,
    autor STRING,
    finalidad_del_credito STRING,
    fecha_de_decision STRING,
    nombre_de_quien_aprueba STRING,
    codigo_de_quien_aprueba STRING,
    numero_acta_de_comite_de_credito STRING,
    LME_solicitado STRING,
    LME_PIC STRING,
    LME_aprobado STRING,
    flujo_actual STRING,
    estado_actual STRING,
    responsable_actual STRING,
    flujo STRING,
    estado STRING,
    responsable STRING,
    fecha_de_entrada STRING,
    hora_de_entrada STRING,
    fecha_de_salida STRING,
    hora_de_salida STRING,
    tiempo STRING,
    vigencia_LME STRING
)
;

-----------------------------------------------------------------------------------------------
/* 
Verificar q se haya creado la tabla */

SELECT * 
FROM proceso_consumidores.base_historicas_lotus_txt LIMIT 1
;

-----------------------------------------------------------------------------------------------
/*
Insertar datos en una tabla
escribiendo cuales son los datos (uno por uno)
https://www.w3schools.com/sql/sql_insert_into_select.asp */

INSERT INTO proceso_consumidores.base_historicas_lotus_txt (
            radicado,	
            identificacion_del_cliente,
            tipo_de_identificacion,
            nombre_del_cliente,
            region,
            zona,
            segmento,
            sector,
            actividad_economica,
            codigo_CIIU,
            gerente,
            codigo_del_gerente,
            centro_de_costos,
            grupo_de_riesgo,
            codigo_de_riesgo,
            calificacion_superbancaria,
            calificacion_interna_actual,
            fecha_de_creacion,
            autor,
            finalidad_del_credito,
            fecha_de_decision,
            nombre_de_quien_aprueba,
            codigo_de_quien_aprueba,
            numero_acta_de_comite_de_credito,
            LME_solicitado,
            LME_PIC,
            LME_aprobado,
            flujo_actual,
            estado_actual,
            responsable_actual,
            flujo,
            estado,
            responsable,
            fecha_de_entrada,
            hora_de_entrada,
            fecha_de_salida,
            hora_de_salida,
            tiempo,
            vigencia_LME
)
VALUES ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39')
;

-----------------------------------------------------------------------------------------------
/*
Insertar datos haciendo un SELECT (INSERT + SELECT)
esto se hace para copiar los datos de la tabla temporal a la definitiva */

INSERT INTO proceso_consumidores.base_historicas_lotus_txt_definitivo (
            radicado,	
            identificacion_del_cliente,
            tipo_de_identificacion,
            nombre_del_cliente,
            region,
            zona,
            segmento,
            sector,
            actividad_economica,
            codigo_CIIU,
            gerente,
            codigo_del_gerente,
            centro_de_costos,
            grupo_de_riesgo,
            codigo_de_riesgo,
            calificacion_superbancaria,
            calificacion_interna_actual,
            fecha_de_creacion,
            autor,
            finalidad_del_credito,
            fecha_de_decision,
            nombre_de_quien_aprueba,
            codigo_de_quien_aprueba,
            numero_acta_de_comite_de_credito,
            LME_solicitado,
            LME_PIC,
            LME_aprobado,
            flujo_actual,
            estado_actual,
            responsable_actual,
            flujo,
            estado,
            responsable,
            fecha_de_entrada,
            hora_de_entrada,
            fecha_de_salida,
            hora_de_salida,
            tiempo,
            vigencia_LME
)
/* todos los datos son tipo string 
para no tener problemas con el tipo de dato */
SELECT TRIM(CAST(radicado AS STRING)) AS radicado,	
       TRIM(CAST(identificacion_del_cliente AS STRING)) AS identificacion_del_cliente,
       TRIM(CAST(tipo_de_identificacion AS STRING)) AS tipo_de_identificacion,
       TRIM(CAST(nombre_del_cliente AS STRING)) AS nombre_del_cliente,
       TRIM(CAST(region AS STRING)) AS region,
       TRIM(CAST(zona AS STRING)) AS zona,
       TRIM(CAST(segmento AS STRING)) AS segmento,
       TRIM(CAST(sector AS STRING)) AS sector,
       TRIM(CAST(actividad_economica AS STRING)) AS actividad_economica,
       TRIM(CAST(codigo_CIIU AS STRING)) AS codigo_CIIU,
       TRIM(CAST(gerente AS STRING)) AS gerente,
       TRIM(CAST(codigo_del_gerente AS STRING)) AS codigo_del_gerente,
       TRIM(CAST(centro_de_costos AS STRING)) AS centro_de_costos,
       TRIM(CAST(grupo_de_riesgo AS STRING)) AS grupo_de_riesgo,
       TRIM(CAST(codigo_de_riesgo AS STRING)) AS codigo_de_riesgo ,
       TRIM(CAST(calificacion_superbancaria AS STRING)) AS calificacion_superbancaria,
       TRIM(CAST(calificacion_interna_actual AS STRING)) AS calificacion_interna_actual,
       TRIM(CAST(fecha_de_creacion AS STRING)) AS fecha_de_creacion,
       TRIM(CAST(autor AS STRING)) AS autor,
       TRIM(CAST(finalidad_del_credito AS STRING)) AS finalidad_del_credito,
       TRIM(CAST(fecha_de_decision AS STRING)) AS fecha_de_decision,
       TRIM(CAST(nombre_de_quien_aprueba AS STRING)) AS nombre_de_quien_aprueba,
       TRIM(CAST(codigo_de_quien_aprueba AS STRING)) AS codigo_de_quien_aprueba,
       TRIM(CAST(numero_acta_de_comite_de_credito AS STRING)) AS numero_acta_de_comite_de_credito,
       TRIM(CAST(LME_solicitado AS STRING)) AS LME_solicitado,
       TRIM(CAST(LME_PIC AS STRING)) AS LME_PIC,
       TRIM(CAST(LME_aprobado AS STRING)) AS LME_aprobado,
       TRIM(CAST(flujo_actual AS STRING)) AS flujo_actual ,
       TRIM(CAST(estado_actual AS STRING)) AS estado_actual,
       TRIM(CAST(responsable_actual AS STRING)) AS responsable_actual,
       TRIM(CAST(flujo AS STRING)) AS flujo,
       TRIM(CAST(estado AS STRING)) AS estado,
       TRIM(CAST(responsable AS STRING)) AS responsable,
       TRIM(CAST(fecha_de_entrada AS STRING)) AS fecha_de_entrada,
       TRIM(CAST(hora_de_entrada AS STRING)) AS hora_de_entrada,
       TRIM(CAST(fecha_de_salida AS STRING)) AS fecha_de_salida,
       TRIM(CAST(hora_de_salida AS STRING)) AS hora_de_salida,
       TRIM(CAST(tiempo AS STRING)) AS tiempo,
       TRIM(CAST(vigencia_LME AS STRING)) AS vigencia_LME
FROM proceso_consumidores.base_historicas_lotus_txt_tmp
;

-----------------------------------------------------------------------------------------------
/* 
Contar el numero total de filas (registros) q se subieron a LZ */

SELECT COUNT(*) AS numero_total_de_filas
FROM proceso_consumidores.base_historicas_lotus_txt
;

/* 338817 + 381734 = 720551 */

-----------------------------------------------------------------------------------------------
/* 
seleccionar toda la tabla completa 


FORMA 1 */
SELECT * 
FROM proceso_consumidores.base_historicas_lotus_txt
;

---------

/* Forma 2 */
SELECT TRIM(CAST(radicado AS STRING)) AS radicado,	
       TRIM(CAST(identificacion_del_cliente AS STRING)) AS identificacion_del_cliente,
       TRIM(CAST(tipo_de_identificacion AS STRING)) AS tipo_de_identificacion,
       TRIM(CAST(nombre_del_cliente AS STRING)) AS nombre_del_cliente,
       TRIM(CAST(region AS STRING)) AS region,
       TRIM(CAST(zona AS STRING)) AS zona,
       TRIM(CAST(segmento AS STRING)) AS segmento,
       TRIM(CAST(sector AS STRING)) AS sector,
       TRIM(CAST(actividad_economica AS STRING)) AS actividad_economica,
       TRIM(CAST(codigo_CIIU AS STRING)) AS codigo_CIIU,
       TRIM(CAST(gerente AS STRING)) AS gerente,
       TRIM(CAST(codigo_del_gerente AS STRING)) AS codigo_del_gerente,
       TRIM(CAST(centro_de_costos AS STRING)) AS centro_de_costos,
       TRIM(CAST(grupo_de_riesgo AS STRING)) AS grupo_de_riesgo,
       TRIM(CAST(codigo_de_riesgo AS STRING)) AS codigo_de_riesgo ,
       TRIM(CAST(calificacion_superbancaria AS STRING)) AS calificacion_superbancaria,
       TRIM(CAST(calificacion_interna_actual AS STRING)) AS calificacion_interna_actual,
       TRIM(CAST(fecha_de_creacion AS STRING)) AS fecha_de_creacion,
       TRIM(CAST(autor AS STRING)) AS autor,
       TRIM(CAST(finalidad_del_credito AS STRING)) AS finalidad_del_credito,
       TRIM(CAST(fecha_de_decision AS STRING)) AS fecha_de_decision,
       TRIM(CAST(nombre_de_quien_aprueba AS STRING)) AS nombre_de_quien_aprueba,
       TRIM(CAST(codigo_de_quien_aprueba AS STRING)) AS codigo_de_quien_aprueba,
       TRIM(CAST(numero_acta_de_comite_de_credito AS STRING)) AS numero_acta_de_comite_de_credito,
       TRIM(CAST(LME_solicitado AS STRING)) AS LME_solicitado,
       TRIM(CAST(LME_PIC AS STRING)) AS LME_PIC,
       TRIM(CAST(LME_aprobado AS STRING)) AS LME_aprobado,
       TRIM(CAST(flujo_actual AS STRING)) AS flujo_actual ,
       TRIM(CAST(estado_actual AS STRING)) AS estado_actual,
       TRIM(CAST(responsable_actual AS STRING)) AS responsable_actual,
       TRIM(CAST(flujo AS STRING)) AS flujo,
       TRIM(CAST(estado AS STRING)) AS estado,
       TRIM(CAST(responsable AS STRING)) AS responsable,
       TRIM(CAST(fecha_de_entrada AS STRING)) AS fecha_de_entrada,
       TRIM(CAST(hora_de_entrada AS STRING)) AS hora_de_entrada,
       TRIM(CAST(fecha_de_salida AS STRING)) AS fecha_de_salida,
       TRIM(CAST(hora_de_salida AS STRING)) AS hora_de_salida,
       TRIM(CAST(tiempo AS STRING)) AS tiempo,
       TRIM(CAST(vigencia_LME AS STRING)) AS vigencia_LME
FROM proceso_consumidores.base_historicas_lotus_txt
;

-----------------------------------------------------------------------------------------------
/*
Imprimir lo siguiente:
- Nombres de las columnas
- Numero de columnas
- Tipo de dato de las columnas*/
DESCRIBE proceso_consumidores.base_historicas_lotus_txt_definitivo
;

-----------------------------------------------------------------------------------------------
/* 
ESTO SE TIENE Q CUMPLIR PARA CREAR UNA TABLA CON LOS REGISTROS CORREGIDOS:
(numero de filas de la tabla definitiva) = 20 212 623

(definitiva_2) + (temporal) = 20 212 623 tabla definitiva



Despues de subir los .txt a LZ 
se descubrio un error q se puede ver con las siguientes consultas SQL,
el error es q los datos de las columnas estan mezcladas.
Ejemplo: La columna 1 tiene los datos de la columna 1 y 2 juntos
Esto es ocasionado por q en el .txt llamado
16.txt ó 1B110EDA Base Actual.txt 
los datos no estan separados por ^ */

SELECT *
FROM proceso_consumidores.base_historicas_lotus_txt_definitivo
WHERE LENGTH(TRIM(CAST(radicado AS STRING))) > 15 /* LENGTH() es el numero de caracteres (letras, numeros, espacios, etc) de una cadena de texto (tipo string) https://www.w3schools.com/sql/func_sqlserver_len.asp
                                                     TRIM() elimina los espacios en blanco al principio y al final https://www.w3schools.com/sql/func_sqlserver_trim.asp
                                                     CAST() es para convertir a cualquier tipo de dato https://www.w3schools.com/sql/func_sqlserver_cast.asp */
ORDER BY 1
;

---------

SELECT radicado, 
       estado, 
       fecha_de_entrada, 
       hora_de_entrada, 
       COUNT(*) AS count_sql
FROM proceso_consumidores.base_historicas_lotus_txt_definitivo
GROUP BY 1, 2, 3, 4 
ORDER BY 5 DESC
LIMIT 50
;

---------

/* contar el numero de filas q estan mal */
SELECT COUNT(*) AS numero_de_filas_malas
FROM proceso_consumidores.base_historicas_lotus_txt_definitivo
WHERE LENGTH(TRIM(CAST(radicado AS STRING))) > 15
;

/* 1 711 97 */

---------

/* crear tabla con los datos buenos */
CREATE TABLE proceso_consumidores.base_historicas_lotus_txt_definitivo_2 STORED AS PARQUET AS

SELECT * 
FROM proceso_consumidores.base_historicas_lotus_txt_definitivo 
WHERE LENGTH(TRIM(CAST(radicado AS STRING))) <= 15
      OR radicado IS NULL
      --OR radicado=''  
;

---------

CREATE TABLE proceso_consumidores.base_historicas_lotus_txt_definitivo_3 STORED AS PARQUET AS

SELECT * 
FROM proceso_consumidores.base_historicas_lotus_txt_definitivo 
WHERE LENGTH(TRIM(CAST(radicado AS STRING))) <= 15
      OR radicado IS NULL
      --OR radicado=''
;

-----------------------------------------------------------------------------------------------

SELECT COUNT(*) AS count_temporal
FROM proceso_consumidores.base_historicas_lotus_txt_tmp
;

/* 1 711 97 */

---------

SELECT COUNT(*) AS count_definitivo
FROM proceso_consumidores.base_historicas_lotus_txt_definitivo
;

/* 20 212 623 */

---------

SELECT COUNT(*) AS count_definitivo_2
FROM proceso_consumidores.base_historicas_lotus_txt_definitivo_2
;

/* 20 040 709 */

-----------------------------------------------------------------------------------------------

WITH tmp AS (
       SELECT * 
       FROM proceso_consumidores.base_historicas_lotus_txt_definitivo
       WHERE LENGTH(TRIM(CAST(radicado AS STRING))) > 15
       OR radicado IS NULL
       --OR radicado=''  
),
tmp2 AS (
       SELECT t1.* 
       FROM proceso_consumidores.base_historicas_lotus_txt_definitivo t1
       LEFT ANTI JOIN tmp t2
       ON t1.radicado = t2.radicado 
       AND t1.identificacion_del_cliente = t2.identificacion_del_cliente
       )
SELECT COUNT(*) 
FROM tmp2
--AND LENGTH(t1.radicado) > 15
;

/* 20 189 526 + 1 711 97 = 20 360 723 */

-----------------------------------------------------------------------------------------------
/* 
Seleccionar las filas (registros) repetidos
https://stackoverflow.com/questions/18390574/how-to-delete-duplicate-rows-in-sql-server */

WITH tabla_temporal AS (
     SELECT radicado,	
            identificacion_del_cliente,
            tipo_de_identificacion,
            nombre_del_cliente,
            region,
            zona,
            segmento,
            sector,
            actividad_economica,
            codigo_CIIU,
            gerente,
            codigo_del_gerente,
            centro_de_costos,
            grupo_de_riesgo,
            codigo_de_riesgo,
            calificacion_superbancaria,
            calificacion_interna_actual,
            fecha_de_creacion,
            autor,
            finalidad_del_credito,
            fecha_de_decision,
            nombre_de_quien_aprueba,
            codigo_de_quien_aprueba,
            numero_acta_de_comite_de_credito,
            LME_solicitado,
            LME_PIC,
            LME_aprobado,
            flujo_actual,
            estado_actual,
            responsable_actual,
            flujo,
            estado,
            responsable,
            fecha_de_entrada,
            hora_de_entrada,
            fecha_de_salida,
            hora_de_salida,
            tiempo,
            vigencia_LME,
       (ROW_NUMBER()OVER(PARTITION BY             
                         radicado,	
                         identificacion_del_cliente,
                         tipo_de_identificacion,
                         nombre_del_cliente,
                         region,
                         zona,
                         segmento,
                         sector,
                         actividad_economica,
                         codigo_CIIU,
                         gerente,
                         codigo_del_gerente,
                         centro_de_costos,
                         grupo_de_riesgo,
                         codigo_de_riesgo,
                         calificacion_superbancaria,
                         calificacion_interna_actual,
                         fecha_de_creacion,
                         autor,
                         finalidad_del_credito,
                         fecha_de_decision,
                         nombre_de_quien_aprueba,
                         codigo_de_quien_aprueba,
                         numero_acta_de_comite_de_credito,
                         LME_solicitado,
                         LME_PIC,
                         LME_aprobado,
                         flujo_actual,
                         estado_actual,
                         responsable_actual,
                         flujo,
                         estado,
                         responsable,
                         fecha_de_entrada,
                         hora_de_entrada,
                         fecha_de_salida,
                         hora_de_salida,
                         tiempo,
                         vigencia_LME
                         ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39
                        )
       ) AS repetido
   FROM proceso_consumidores.base_historicas_lotus_txt_definitivo_3
)
SELECT radicado,	
       identificacion_del_cliente,
       tipo_de_identificacion,
       nombre_del_cliente,
       region,
       zona,
       segmento,
       sector,
       actividad_economica,
       codigo_CIIU,
       gerente,
       codigo_del_gerente,
       centro_de_costos,
       grupo_de_riesgo,
       codigo_de_riesgo,
       calificacion_superbancaria,
       calificacion_interna_actual,
       fecha_de_creacion,
       autor,
       finalidad_del_credito,
       fecha_de_decision,
       nombre_de_quien_aprueba,
       codigo_de_quien_aprueba,
       numero_acta_de_comite_de_credito,
       LME_solicitado,
       LME_PIC,
       LME_aprobado,
       flujo_actual,
       estado_actual,
       responsable_actual,
       flujo,
       estado,
       responsable,
       fecha_de_entrada,
       hora_de_entrada,
       fecha_de_salida,
       hora_de_salida,
       tiempo,
       vigencia_LME
FROM tabla_temporal 
WHERE repetido > 1
--LIMIT 1
;

-----------------------------------------------------------------------------------------------
