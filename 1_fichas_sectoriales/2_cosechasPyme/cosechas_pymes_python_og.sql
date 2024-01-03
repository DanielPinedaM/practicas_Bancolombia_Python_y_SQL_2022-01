SET SYNC_DDL=1

;

DROP TABLE IF EXISTS proceso.garc_marca_novaciones PURGE

;

CREATE TABLE proceso.garc_marca_novaciones STORED AS PARQUET AS 

WITH lphis AS
  (SELECT lphcpd, /*Marca de producto*/ CASE
                                            WHEN lphcpd IN ('SEB',
                                                            'OLF',
                                                            'INS',
                                                            'CTE',
                                                            'CHQ',
                                                            'AHO',
                                                            'AGR') THEN 'NUEVO'
                                            WHEN lphcpd IN ('DOC',
                                                            'TMP',
                                                            'TGB',
                                                            'ABO') THEN 'REESTRUCTURACIÓN Y NOVACIÓN'
                                            ELSE 'REESTRUCTURACIÓN Y NOVACIÓN'
                                        END AS marca_producto,
                                        lphapd, /*Monto*/ lphnct AS producto_destino, /*obligacion a la que llega el desembolso*/ lphcap AS tipo_producto, /*Aplicativo del producto destino*/ lphden, /*yyyymmdd*/ lphten, /*hhmmss*/ lphuse,
                                                                                                                                                                                                                                       lphnac, /*Obligacion desembolsada de cartera*/ (lag(lphnac, 1) over(partition BY lphnac
                                                                                                                                                                                                                                                                                                           ORDER BY YEAR*100+ingestion_month ASC)) AS obl_lag,
                                                                                                                                                                                                                                                                                      YEAR*10000+ingestion_month*100+ingestion_day AS f_ingestion
   FROM s_productos.cps_visionr_lphis
   WHERE YEAR >= 2017
     AND lphnpt=0 /*Cartera activa*/
     AND lphcpd NOT IN ('SEB',
                        'OLF',
                        'INS',
                        'CTE',
                        'CHQ',
                        'AHO',
                        'AGR') 
     ) ,
        
     nov AS
  (SELECT lphnac AS obl_nova,
          min(lphden) AS fecha,
          min(lphten) AS hora,
          max(f_ingestion) AS f_ingestion
   FROM lphis
   WHERE obl_lag = lphnac
   GROUP BY 1

)

SELECT obl_nova,
       max(tipo_producto) AS cod_apli
FROM lphis
INNER JOIN nov ON obl_nova = lphnac
GROUP BY 1

;

COMPUTE STATS proceso.garc_marca_novaciones

;

DROP TABLE IF EXISTS  proceso.cd_cecsif1_pymes PURGE

;

CREATE TABLE proceso.cd_cecsif1_pymes  STORED AS PARQUET AS

WITH

BASE AS (
SELECT  ROW_NUMBER() OVER ( PARTITION BY apl, obl341, md3411, fdesem, vdesem ORDER BY corte ASC) AS rn
, ingestion_year*100+ingestion_month AS fdesem_ajustada
, tid
, id
, obl341
, gerenciado
, apl
, regcons
, CASE WHEN TRIM(subsegmento) = '' OR TRIM(subsegmento) IN ('05','00',NULL) THEN 'NO APLICA'
       WHEN TRIM(subsegmento) IN ('01','07','09','23') THEN 'PEQUENA'
       WHEN TRIM(subsegmento) IN ('02','08','10','20') THEN 'MEDIANA'
       WHEN TRIM(subsegmento) IN ('03','06','11','21') THEN 'GRANDE'
       WHEN TRIM(subsegmento) IN ('25') THEN 'PLUS'
  END AS subsegmento
, sector
, subsector
, md3411
, clf
, fdesem
, pl
, int_months_between(fvto,fdesem) as plazo
, vdesem 
FROM resultados_riesgos.ceniegarc_lz
WHERE year in (2017,2018,2019,2020,2021) AND 
      from_timestamp(fdesem, 'yyyyMM') >= '201701' AND 
      vdesem > 0 AND 
      vdesem IS NOT NULL AND
      trim(sgto)='5' AND 
      apl NOT IN ('C','D') AND 
      pl NOT IN ('A43','A13','A53','A50','A42','A41','A11',	'A15','A14','A12','A52',	'A47','A16','A45','A21',	'A51','A56','A55','A17','A10','A20','A40') AND
      CAST(from_timestamp(fdesem, 'yyyyMM') AS BIGINT)=CAST(CORTE AS BIGINT)
),

PREVIA AS (
SELECT tid
, id
, obl341
, gerenciado
, apl
, regcons
, subsegmento
, sector
, subsector
, md3411
, clf
, fdesem
, fdesem_ajustada
, pl
, vdesem
, CASE WHEN plazo > 300 then null 
       WHEN plazo <=6 then 'a. <= 6meses'
       WHEN plazo BETWEEN 7  AND 12 THEN 'b. 7 a 12 meses'
       WHEN plazo BETWEEN 13 AND 18 THEN 'c. 13 a 18 meses'
       WHEN plazo BETWEEN 19 AND 24 THEN 'd. 1.6 a 2 años'
       WHEN plazo BETWEEN 25 AND 36 THEN 'e. 2.1 a 3 años'
       WHEN plazo BETWEEN 37 AND 48 THEN 'f. 3.1 a 4 años'
       WHEN plazo BETWEEN 49 AND 60 THEN 'g. 4.1 a 5 años'
       WHEN plazo BETWEEN 61 AND 72 THEN 'h. 5.1 a 6 años'
       WHEN plazo >72 then 'i. > 6 años' END AS rango_plazo_m_ajust
FROM base
WHERE rn = 1
),

FIN AS (
SELECT *,  ROW_NUMBER() OVER ( PARTITION BY apl, obl341, md3411, vdesem ORDER BY fdesem_ajustada ASC) AS rn
FROM previa
)

SELECT *
FROM fin
WHERE rn=1

;

COMPUTE STATS proceso.cd_cecsif1_pymes

;

DROP TABLE IF EXISTS  proceso.cd_cecsif2_pymes  PURGE

;


CREATE TABLE proceso.cd_cecsif2_pymes  STORED AS PARQUET AS

WITH

CECSIF1 AS (
SELECT *
FROM  proceso.cd_cecsif1_pymes 
),

COMPORTAMIENTO AS (
SELECT corte AS f_proceso, fdesem, vdesem, tid, id, obl341, apl, pl, md3411, altmora, sk, cv1
FROM resultados_riesgos.ceniegarc_lz 
WHERE year in (2017,2018,2019,2020,2021) AND 
      from_timestamp(fdesem, 'yyyyMM') >= '201701' AND 
      corte >= 201702 AND 
      vdesem > 0 AND 
      vdesem IS NOT NULL AND
      apl NOT IN ('C','D') AND 
      pl NOT IN ('A43','A13','A53','A50','A42','A41','A11',	'A15','A14','A12','A52',	'A47','A16','A45','A21',	'A51','A56','A55','A17','A10','A20','A40') 

),

PREVIA AS (
SELECT x.*, v.f_proceso, v.altmora, v.sk, v.cv1
FROM cecsif1 x
LEFT JOIN comportamiento v ON CAST(x.obl341 AS BIGINT)=CAST(v.obl341 AS BIGINT) AND 
                              CAST(x.MD3411 AS BIGINT)=CAST(v.MD3411 AS BIGINT) AND 
                              x.apl=v.apl AND 
                              x.vdesem=v.vdesem 
)

 
SELECT *
FROM previa

;

COMPUTE STATS proceso.cd_cecsif2_pymes

;

DROP TABLE IF EXISTS   proceso.cd_cecsif_pymes PURGE

;

CREATE TABLE proceso.cd_cecsif_pymes  STORED AS PARQUET AS

WITH

CECSIF1 AS (
SELECT *
FROM proceso.cd_cecsif1_pymes 
),

CECSIF2 AS (
SELECT *, ID AS ID2, OBL341 AS OBL2
FROM proceso.cd_cecsif2_pymes 
),

PREVIA AS (
SELECT t1.tid, t1.id, t1.obl341, t1.gerenciado, t1.apl, t1.regcons, t1.subsegmento, t1.sector, t1.subsector, t1.md3411, t1.clf, t1.fdesem, t1.fdesem_ajustada, t1.rango_plazo_m_ajust,  t1.pl,  t1.vdesem, t2.f_proceso, t2.altmora, t2.sk, t2.cv1
FROM CECSIF1 t1
LEFT JOIN CECSIF2 t2 ON CAST(t1.OBL341 as BIGINT)=CAST(t2.OBL341 as BIGINT) AND
                        CAST(t1.md3411 as BIGINT)=CAST(t2.md3411 as BIGINT) AND 
                        TRIM(t1.apl)=TRIM(t2.apl) AND 
                        t1.rango_plazo_m_ajust=t2.rango_plazo_m_ajust AND
                        t1.vdesem=t2.vdesem AND
                        CAST(t1.fdesem_ajustada AS BIGINT)<CAST(t2.f_proceso AS BIGINT)
)

SELECT *, CASE WHEN (sk-vdesem)>0 THEN 'SOBRECUPO' ELSE 'NORMAL' END AS marca_sobrecupo
FROM previa
;


COMPUTE STATS proceso.cd_cecsif_pymes

;

DROP TABLE IF EXISTS proceso.cd_cecaif1_pymes PURGE

;

CREATE TABLE proceso.cd_cecaif1_pymes STORED AS PARQUET AS

WITH

BASE  AS (
SELECT DISTINCT
id, obl341, apl, moneda_2, f_inic_cast AS f_proceso, cap_cast_inic AS castigo
FROM resultados_riesgos.gerc_ambal
WHERE year IN (2017,2018,2019,2020,2021) AND ingestion_year>=2017 AND ingestion_month>=01 AND 
     f_entrada>=20170101 AND f_entrada<=20211231 AND 
     segdesc_cod = '5' AND 
     cap_cast_inic > 0 AND 
     pl NOT IN ('A43','A13','A53','A50','A42','A41','A11',	'A15','A14','A12','A52',	'A47','A16','A45','A21',	'A51','A56','A55','A17','A10','A20','A40') 

)

SELECT *
FROM BASE

;


COMPUTE STATS proceso.cd_cecaif1_pymes

;

DROP TABLE IF EXISTS proceso.cd_cecaif2_pymes PURGE

;

CREATE TABLE proceso.cd_cecaif2_pymes STORED AS PARQUET AS

WITH

CECAIF1  AS (
SELECT *
FROM proceso.cd_cecaif1_pymes
),

CECAIF2 AS (
SELECT id, obl341, apl, moneda_2, ingestion_year*10000+ingestion_month*100+ingestion_day AS f_proceso, cap_cast_inic AS castigo
FROM resultados_riesgos.gerc_ambal
WHERE year IN (2017,2018,2019,2020,2021) AND ingestion_year>=2017 AND ingestion_month>=01 AND 
     (ingestion_year*100+ingestion_month)<=202112  AND 
     f_entrada>=20170101 AND f_entrada<20211231 AND 
     segdesc_cod = '5' 
)

SELECT DISTINCT t2.*
FROM CECAIF1 t1
INNER JOIN CECAIF2 t2 ON t1.id=t2.id AND t1.obl341=t2.obl341 AND t1.apl=t2.apl AND t1.moneda_2=t2.moneda_2 AND t2.f_proceso>t1.f_proceso

;

COMPUTE STATS proceso.cd_cecaif2_pymes

;

DROP TABLE IF EXISTS  proceso.cd_cecaif_pymes PURGE

;

CREATE TABLE proceso.cd_cecaif_pymes STORED AS PARQUET AS

WITH 

CECSIF AS (

    SELECT DISTINCT
            id,
            tid,
            obl341,
            gerenciado,
            apl,
            regcons,
            subsegmento,
            sector,
            subsector,
            md3411,
            clf,
            fdesem,
            fdesem_ajustada,
            rango_plazo_m_ajust,
            pl,
            vdesem

    FROM proceso.cd_cecsif_pymes
),


CECAIF AS (

SELECT *
FROM proceso.cd_cecaif1_pymes

UNION ALL
SELECT *
FROM proceso.cd_cecaif2_pymes
)

SELECT DISTINCT t1.*, 'NORMAL' AS marca_sobrecupo, t2.castigo, t2.f_proceso, -999 AS altmora
FROM CECSIF t1
INNER JOIN CECAIF t2 ON CAST(t1.obl341 AS BIGINT)=CAST(t2.obl341 AS BIGINT) AND TRIM(t1.apl)=TRIM(t2.apl) AND CAST(t1.md3411 AS BIGINT)=CAST(t2.moneda_2 AS BIGINT)

;

COMPUTE STATS proceso.cd_cecaif_pymes

;


DROP TABLE IF EXISTS proceso.cd_pymes  PURGE

;

CREATE TABLE proceso.cd_pymes STORED AS PARQUET AS

WITH 

BASE AS (
    SELECT DISTINCT
            id,
            tid,
            obl341,
            gerenciado,
            apl,
            regcons,
            subsegmento,
            sector, 
            subsector,
            md3411,
            clf,
            fdesem,
            fdesem_ajustada,
            rango_plazo_m_ajust,
            cast(concat_ws('-',cast(substr(cast(f_proceso as string),1,4) as string),
            lpad(cast(substr(cast(f_proceso as string),5,2) as string),2,'0'),
            '01')
            as timestamp) as fecha_proceso,
            pl,
            marca_sobrecupo,
            vdesem,
            sk,
            cv1,
            0 AS castigo

    FROM proceso.cd_cecsif_pymes

UNION ALL
    SELECT  id,
            tid,
            obl341,
            gerenciado,
            apl,
            regcons,
            subsegmento,
            sector, 
            subsector,
            md3411,
            clf,
            fdesem,
            fdesem_ajustada,
            rango_plazo_m_ajust,
            cast(concat_ws('-',cast(substr(cast(f_proceso as string),1,4) as string),
            lpad(cast(substr(cast(f_proceso as string),5,2) as string),2,'0'),
            '01')
            as timestamp) as fecha_proceso,
            pl,
            marca_sobrecupo,
            vdesem,
            0 AS sk,
            0 AS cv1,
            castigo
    FROM proceso.cd_cecaif_pymes
),


BASE_FIN as (
SELECT 
            t1.id,
            t1.tid,
            t1.obl341,
            t1.gerenciado,
            CASE WHEN t1.apl = '3' THEN lpad(substr(t1.obl341,12,6),17,'0') ELSE t1.obl341 END AS obl_cruce,
            t1.apl,
            t1.regcons,
            t1.subsegmento,
            t1.sector, 
            t1.subsector,
            t1.md3411,
            t1.clf,
            t1.fdesem,
            t1.fdesem_ajustada,
            t1.rango_plazo_m_ajust,
            t1.fecha_proceso,
            t1.pl,
            CASE WHEN t2.obl_nova IS NOT NULL THEN 'NOVACION' 
                 WHEN t1.pl IN ('F10','F12','F13','F14','F02','F03','F09','F22','F23','F29','F31','F32','F36','F39','F40','F41','F42','F49','F50','F60','F61','F62','F63','F64','F65',
                               'F66','F68','F69','F70','F74','F76','F77','F79','F80','F81','F82','F83','F84','F85','F86','F87','F88','F89','F78') THEN 'MODIFICADO'
                 WHEN t1.pl in ('R01','R02','R03','R04','R07','R08','R09','R10','R11','R13','R14','R15','R16','R17','R18','R19','R20','R24','R25','R26','R27','R30',
                               'R33','R34','R35','R36','R37','R38','R39','R40','R43','R46','R47','R48','R50','R55','R56','R57','R60','R61','R62','R63','R64','R65','R66','R67','R68','R69','R70','R71','R72','R73','R76','R79','R80','R81','R87','R88','R89','R90','R91','R92','R93','R94','R95','R96','R97','R98','R99') THEN 'REESTRUCTURADO'
                 ELSE 'NORMAL' 
            END AS tipo_Cartera,
            t1.marca_sobrecupo,
            SUM(t1.vdesem) AS vdesem,
            SUM(t1.sk) AS sk,
            SUM(t1.cv1) AS cv1,
            SUM(t1.castigo) AS castigo
FROM BASE t1
LEFT JOIN proceso.garc_marca_novaciones t2 ON CAST(t1.obl341 AS BIGINT)=CAST(t2.obl_nova AS BIGINT) AND TRIM(t1.apl)=TRIM(t2.cod_apli)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
),


ALIVIO AS (

SELECT DISTINCT CAST(num_obligacion AS BIGINT) AS obl17, 
                aplicativo AS apl,
                CASE WHEN moneda ='PESOS' THEN 0 ELSE 1 END AS cod_md,
                tid,
                CASE WHEN aplicativo = '3' AND tid <> '1' THEN CAST(substring(CAST(CAST(id AS BIGINT) AS STRING),1,9) AS BIGINT) ELSE CAST(id AS BIGINT) END AS id,
                'ALIVIO' AS marca_alivio
FROM resultados_vspc_finanzas.fco_alivios
where ingestion_year*100+ingestion_month=202111 -- MES DE ANTICIPACION

),

PREVIA AS (
SELECT t1.*, CASE WHEN t2.marca_alivio IS NULL THEN 'SIN ALIVIO' ELSE t2.marca_alivio END AS  marca_alivio
FROM base_fin t1 
LEFT JOIN alivio t2 ON (CAST(t1.obl_cruce AS BIGINT)=CAST(obl17 as BIGINT) 
                       AND CAST(t1.apl AS STRING)=cast(t2.apl AS STRING) 
                       AND CAST(t1.id AS BIGINT)=cast(t2.id AS BIGINT)
                       AND CAST(t1.md3411 AS string)=CAST(t2.cod_md AS string))
)

SELECT *
FROM previa

;

COMPUTE STATS proceso.cd_pymes


;

DROP TABLE IF EXISTS proceso.cd_pymes_variables PURGE

;

CREATE TABLE proceso.cd_pymes_variables STORED AS PARQUET AS 



WITH 
BASE_PRE AS (
SELECT 
f_estudio,
f_ano_mes,
dia_apr,
radicado,
llave_nombre, 
tipo_doc, 
num_doc, 
segm,
CASE WHEN descr_segm IS NULL OR descr_segm = '' THEN 'NO DISPONIBLE' ELSE descr_segm END AS descr_segm,
subsegm, 
descr_subsegm, 
zona, 
descr_zona, 
descr_sector,
descr_subsector, 
descr_region,
CASE WHEN campana!='' THEN campana
     WHEN (campana='' AND fecha_Campana BETWEEN 20150101 AND 20150330) THEN 'Campana I-2015'
     WHEN (campana='' AND fecha_Campana BETWEEN 20150401 AND 20150630) THEN 'Campana II-2015'
     WHEN (campana='' AND fecha_Campana BETWEEN 20150701 AND 20150930) THEN 'Campana III-2015'
     WHEN (campana='' AND fecha_Campana BETWEEN 20151001 AND 20151231) THEN 'Campana IV-2015'
     WHEN (campana='' AND fecha_Campana BETWEEN 20160101 AND 20160330) THEN 'Campana I-2016'
     WHEN (campana='' AND fecha_Campana BETWEEN 20160401 AND 20160630) THEN 'Campana II-2016'
     WHEN (campana='' AND fecha_Campana BETWEEN 20160701 AND 20160930) THEN 'Campana III-2016'
     WHEN (campana='' AND fecha_Campana BETWEEN 20161001 AND 20161231) THEN 'Campana IV-2016'
     WHEN (campana='' AND fecha_Campana BETWEEN 20170101 AND 20170330) THEN 'Campana I-2017'
     WHEN (campana='' AND fecha_Campana BETWEEN 20170401 AND 20170630) THEN 'Campana II-2017'
     WHEN (campana='' AND fecha_Campana BETWEEN 20170701 AND 20170930) THEN 'Campana III-2017'
     WHEN (campana='' AND fecha_Campana BETWEEN 20171001 AND 20171231) THEN 'Campana IV-2017'
     WHEN (campana='' AND fecha_Campana BETWEEN 20180101 AND 20180330) THEN 'Campana I-2018'
     WHEN (campana='' AND fecha_Campana BETWEEN 20180401 AND 20180630) THEN 'Campana II-2018'
     WHEN (campana='' AND fecha_Campana BETWEEN 20180701 AND 20180930) THEN 'Campana III-2018'
     WHEN (campana='' AND fecha_Campana BETWEEN 20181001 AND 20181231) THEN 'Campana IV-2018'
     WHEN (campana='' AND fecha_Campana BETWEEN 20190101 AND 20190330) THEN 'Campana I-2019'
     WHEN (campana='' AND fecha_Campana BETWEEN 20190401 AND 20190630) THEN 'Campana II-2019'
     WHEN (campana='' AND fecha_Campana BETWEEN 20190701 AND 20190930) THEN 'Campana III-2019'
     WHEN (campana='' AND fecha_Campana BETWEEN 20191001 AND 20191231) THEN 'Campana IV-2019'
     WHEN (campana='' AND fecha_Campana BETWEEN 20200101 AND 20200330) THEN 'Campana I-2020'
     WHEN (campana='' AND fecha_Campana BETWEEN 20200401 AND 20200630) THEN 'Campana II-2020'
     WHEN (campana='' AND fecha_Campana BETWEEN 20200701 AND 20200930) THEN 'Campana III-2020'
     WHEN (campana='' AND fecha_Campana BETWEEN 20201001 AND 20201231) THEN 'Campana IV-2020'
     WHEN (campana='' AND fecha_Campana BETWEEN 20210101 AND 20210331) THEN 'Campana I-2021'
     WHEN (campana='' AND fecha_Campana BETWEEN 20210401 AND 20210630) THEN 'Campana II-2021'
    
     ELSE 'SIN INFORMACION' END AS campana,
flujo, 
flujo_actual, 
riesgo_sectorial,
estrategia,
cargo_apr as cargo_aprobador,
modelo_optimo,
ROW_NUMBER() OVER (PARTITION BY f_ano_mes, tipo_doc, num_doc ORDER BY f_estudio DESC, f_ano_mes DESC, dia_apr DESC) AS RN 
FROM resultados_riesgos.gbpyme_aprobaciones_sd
WHERE year=2021 AND ingestion_month=12
),

BASE AS (
SELECT MAX(f_estudio) as f_estudio,
MAX(dia_apr) as dia_apr,
MAX(f_ano_mes) as f_ano_mes,
MAX(radicado) as radicado,
MAX(llave_nombre) AS llave_nombre, 
tipo_doc, 
num_doc, 
segm,
MAX(descr_segm) descr_segm,
subsegm, 
descr_subsegm, 
zona, 
descr_zona, 
descr_sector,
descr_subsector, 
descr_region,
campana,
estrategia,
MAX(flujo) flujo,
MAX(flujo_actual) flujo_actual, 
cargo_aprobador,
modelo_optimo,
riesgo_sectorial

FROM BASE_PRE 
WHERE rn=1

GROUP BY 
tipo_doc, 
num_doc, 
segm,
descr_subsegm,
subsegm,  
zona, 
descr_zona, 
descr_sector,
descr_subsector, 
descr_region,
campana,
riesgo_sectorial,
estrategia,
cargo_aprobador,
modelo_optimo
),


GBPYME_UTI AS (
SELECT 
f_estudio, 
f_ano_mes, 
dia_apr,
radicado, 
tipo_doc, 
num_doc, 
segm, 
obl341, 
apl, 
pl, 
vdesem, 
from_timestamp(fdesem, 'yyyyMM') AS fdesem_ajustada,
MAX(CASE WHEN flujo = 'Preaprobado MT' then 'Preaprobados MT' else flujo END) AS flujo,
MAX(CASE WHEN flujo_actual = 'Preaprobado MT' then 'Preaprobados MT' else flujo_actual END) AS flujo_Actual,
rango_plazo_m_ajust, 
califi_trad, 
cr_final_opt  as califi_c,
riesgo_Sectorial,
subsegm, 
case
    when segm = '2' AND subsegm IN ('PEQUENA','PEQUEÃ‘O') then 'PEQUENA'
    when segm = '2' AND subsegm IN ('MEDIANA','MEDIANO') then 'MEDIANA'
    when segm = '2' AND subsegm IN ('ALTO','INST EDUCATIVAS','PLUS','OTROS') then 'POR DEFINIR'
    when segm = '2' AND subsegm IN ('GRANDE') then 'GRANDE'
    when segm = '5' AND subsegm IN ('BASICO','PEQUENA','PREFERENCIAL COLOMBIA','INSTITUCIONES SALUD','INST EDUCATIVAS','OTROS','PEQUEÃ‘O','PREFERENCIAL PLUS') then 'PEQUEÑA'
    when segm = '5' AND subsegm IN ('MEDIANA','MEDIO','MEDIANO') then 'MEDIANA'
    when segm = '5' AND subsegm IN ('ALTO','GRANDE') then 'GRANDE'
    when segm = '5' AND subsegm IN ('PLUS') then 'PLUS'
    when trim(segm) = '2' AND trim(subsegm) IN ('23','09','07','') AND trim(descr_subsegm) IN ('VALOR BAJO','PEQUEÑA','PEQUENA','PEQUEÑO','PEQUE#O','') then 'PEQUEÑA'
    when trim(segm) = '2' AND trim(subsegm) IN ('20','','10') AND trim(descr_subsegm) IN ('VALOR MEDIO','MEDIANA','MEDIANO','') then 'MEDIANA'
    when trim(segm) = '2' AND trim(subsegm) IN ('25','','05','00','12','17') AND descr_subsegm IN ('ALTO','INST EDUCATIVAS','PLUS','OTROS','','NEGOCIO FORMAL','VIP','ENT TERRITORIALES','NEGOCIO EN DLLO') then 'POR DEFINIR'
    when trim(segm) = '2' AND trim(subsegm) IN ('21','','11','22','03','06') AND trim(descr_subsegm) IN ('VIP','NEGOCIO FORMAL','VALOR ALTO','GRANDE','') then 'GRANDE'
    when trim(segm) = '5' AND trim(subsegm) IN ('00','01','02','03','04','05','06','07','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','25','27','30','29','28','31','') AND descr_subsegm IN ('','BASICO','PEQUENA','PEQUEÑA','PREFERENCIAL COLOMBIA','INSTITUCIONES SALUD','PEQUE#O','INST EDUCATIVAS','OTROS','PEQUEÑO','PEQuE#O','PEQUE#0','NEGOCIO FORMAL','NEGOCIO EN DLLO','INDEPENDIENTE EN DLLO','PREFERENCIAL PLUS') then 'PEQUEÑA'
    when trim(segm) = '5' AND trim(subsegm) IN ('10','','08') AND descr_subsegm IN ('','MEDIANA','MEDIO','MEDIANO','INDEPENDIENTE FORMAL') then 'MEDIANA'
    when trim(segm) = '5' AND descr_subsegm IN ('ALTO','GRANDE') then 'GRANDE'
    when trim(segm) = '5' AND descr_subsegm IN ('PLUS') then 'PLUS'
    when trim(segm) = '5' AND descr_subsegm IN ('PEQUENA','PEQUEÑA') then 'PEQUEÑA'
    when descr_subsegm IN ('PEQUEÑA','PEQUENA','PEQUEÑO','PEQUE#O') then 'PEQUEÑA'
    when descr_subsegm IN ('MEDIANO','MEDIANO') then 'MEDIANA'
    else descr_subsegm
end as descr_subsegm,
zona, 
descr_zona, 
descr_sector,
descr_subsector, 
descr_region,
CASE WHEN periodo_gracia='Y' THEN 'CON PERIODO DE GRACIA' WHEN periodo_gracia IS NULL THEN 'SIN INFORMACION' ELSE 'SIN PERIODO DE GRACIA' END AS periodo_gracia
--estrategia -- INCLUIR CUANDO SE TENGA

FROM 
resultados_riesgos.gbpyme_utilizaciones
 WHERE year=2021 AND 
       ingestion_month=12 AND 
       apl NOT IN ('D') AND 
       pl NOT IN ('A43','A13','A53','A50','A42','A41','A11','A15','A14','A12','A52',	'A47','A16','A45','A21',	'A51','A56','A55','A17','A10','A20','A40') 
 GROUP BY 
       1, 
       2,
       3,  
       4, 
       5, 
       6,
       7, 
       8,
       9,
       10, 
       11, 
       12,
       15,
       16, 
       17,
       18, 
       19, 
       20, 
       21,
       22,
       23,
       24,
       25,
       26
),

PIC AS (
SELECT 
num_doc,
radicado,
MAX(servicio_1) as tipo_estudio 
FROM resultados_riesgos.gbpyme_estudios_pic -- solo va a aparecer obl de lotus (banco) y delfil, gran % es pq para los apl no se tiene la obl del estudio 
WHERE year=2021 AND ingestion_month=12
GROUP BY 1,2
),

PREVIA AS (

SELECT 
v.tipo_doc, 
v.num_doc,
v.segm,
x.estrategia,
MAX(CASE WHEN x.descr_segm IS NULL THEN 'NO DISPONIBLE' ELSE x.descr_segm END) AS descr_segm_aprobacion,
v.subsegm,
v.descr_subsegm,


case 
     when v.descr_region in ('DIRECCIÃ“N GENERAL','DIRECIÓN GENERAL','OFICINAS DEL EXTERIOR','BANCO DE COLOMBIA','ANTIOQUIA','BANCO') then 'ANTIOQUIA'
     when v.descr_region in ('CONSOLIDADORA','BOGOTA Y CUNDINAMARCA','BOGOTÃ�') then 'BOGOTA'
     else v.descr_region end as region,

v.zona,
v.descr_zona,
CASE WHEN v.descr_sector IS NULL OR v.descr_sector = '' THEN 'SIN INFORMACION' ELSE v.descr_sector END AS descr_sector_aprobacion,
CASE WHEN v.descr_subsector IS NULL OR v.descr_subsector = '' THEN 'SIN INFORMACION' ELSE v.descr_subsector END AS descr_subsector,
x.campana,
x.cargo_aprobador,
x.modelo_optimo,
MAX(v.flujo) AS flujo,
MAX(v.flujo_actual) AS flujo_actual,
v.riesgo_sectorial,
V.periodo_gracia,
v.obl341, 
v.apl,
v.pl,
v.vdesem,
v.rango_plazo_m_ajust,
v.califi_trad,
v.califi_c,  --- esta cambiaria con el nuevo  
CASE WHEN w.tipo_estudio IS NULL OR w.tipo_estudio = '' THEN 'SIN INFORMACION' ELSE w.tipo_estudio END AS tipo_estudio,
fdesem_ajustada

FROM gbpyme_uti v
LEFT JOIN base x ON CAST(x.num_doc AS BIGINT)=CAST(v.num_doc AS BIGINT)  AND CAST(x.f_ano_mes AS BIGINT)=CAST(v.f_ano_mes AS BIGINT) 
AND TRIM(x.flujo)=TRIM(v.flujo)  AND CAST(x.dia_apr AS BIGINT)=CAST(v.dia_apr AS BIGINT) -- flujo es por donde se aprobo , revisar bien para comentar esta linea 
LEFT JOIN pic w ON  w.radicado=v.radicado 
 GROUP BY 
1, 
2,
3, 
4,  
6,
7, 
8,
9,
10, 
11, 
12,
13,
14,
15, 
18, 
19,
20, 
21,
22,
23,
24,
25,
26,
27, 
28
)

SELECT DISTINCT *
FROM PREVIA
;

 COMPUTE STATS proceso.cd_pymes_variables

;

 DROP TABLE IF EXISTS  proceso.cd_pymes_sd PURGE

;

CREATE TABLE  proceso.cd_pymes_sd STORED AS PARQUET AS
WITH

BASE AS (

SELECT *
FROM proceso.cd_pymes
),

ESTRATEGIA AS (

     WITH

     BASE AS (
          SELECT num_doc, cod_tipo_doc, estrategia, ingestion_year*100+ingestion_month AS corte, ROW_NUMBER() OVER (PARTITION BY num_doc, cod_tipo_doc, ingestion_month ORDER BY ingestion_day DESC) AS RN
          FROM resultados_riesgos.estrategia_orig_pyme
          WHERE ingestion_year>=2020 
     )

     SELECT *
     FROM base
     WHERE rn=1


),

VARIABLES AS (
SELECT *
FROM proceso.cd_pymes_variables
),

PREVIA AS (
SELECT x.*, 
v.region,
v.segm,
v.descr_segm_aprobacion,
v.subsegm,
v.descr_subsegm,
v.zona,
v.descr_zona,
v.descr_sector_aprobacion,
v.descr_subsector,
v.cargo_aprobador,
v.modelo_optimo,
v.campana,
v.flujo,
v.flujo_actual,
v.riesgo_sectorial,

y.estrategia as estrategia_og, --SMS
v.estrategia as estrategia_gbpj,--SMS
CASE WHEN y.estrategia IS NULL THEN v.estrategia ELSE y.estrategia END AS estrategia,
v.rango_plazo_m_ajust AS rango_plazo_m_ajust_gbp,
v.tipo_estudio,
v.califi_trad,
v.califi_c,
v.periodo_gracia

FROM base x
LEFT JOIN variables v ON CAST(x.id AS BIGINT)=CAST(v.num_doc AS BIGINT) AND CAST(x.tid AS BIGINT)=CAST(v.tipo_doc AS BIGINT)  AND  CAST(x.obl341 AS BIGINT)=CAST(v.obl341 AS BIGINT) AND  TRIM(CAST(x.apl AS STRING))=TRIM(CAST(v.apl AS STRING)) AND CAST(x.vdesem AS BIGINT)=CAST(v.vdesem AS BIGINT) AND CAST(x.fdesem_ajustada AS BIGINT)=CAST(v.fdesem_ajustada AS BIGINT)
LEFT JOIN estrategia y ON CAST(x.id AS BIGINT)=CAST(y.num_doc AS BIGINT) AND CAST(x.tid AS BIGINT)=CAST(y.cod_tipo_doc AS BIGINT) AND x.fdesem_ajustada=y.corte
),

FIN AS (
SELECT 
id,
obl341,
gerenciado,
md3411,
apl,
clf,
fdesem,
fdesem_ajustada,
fecha_proceso,
pl,
CASE WHEN apl IN ('1','3') THEN regcons
     WHEN fdesem_ajustada>201807 THEN region 
     ELSE regcons 
END AS regcons,
tipo_Cartera,
marca_alivio,
marca_sobrecupo,
vdesem,
sk,
cv1,
castigo,
MAX(segm) AS segm,
MAX(CASE WHEN descr_segm_aprobacion IS NULL OR descr_segm_aprobacion = '' THEN 'NO DISPONIBLE' ELSE descr_segm_aprobacion END) AS descr_segm,
subsegm,
subsegmento AS descr_subsegm,
zona,
descr_zona,
CASE WHEN apl IN ('1','3') THEN sector
     WHEN fdesem_ajustada>201807 THEN descr_sector_aprobacion
     ELSE sector 
END AS descr_sector_aprobacion,
CASE WHEN apl IN ('1','3') THEN subsector
     WHEN fdesem_ajustada>201807 THEN descr_subsector 
     ELSE subsector 
END AS descr_subsector,
cargo_aprobador,
modelo_optimo,
campana,
MAX(tipo_estudio) AS tipo_estudio,
MAX(flujo) AS flujo,
MAX(flujo_actual) AS flujo_actual,
riesgo_sectorial,
estrategia,
estrategia_og, --SMS
estrategia_gbpj,--SMS
CASE WHEN apl IN ('1','3') THEN rango_plazo_m_ajust ELSE rango_plazo_m_ajust_gbp END AS rango_plazo_m_ajust,
califi_trad,
califi_c,
periodo_gracia
FROM previa
GROUP BY 
id,
obl341,
gerenciado,
md3411,
apl,
clf,
fdesem,
fdesem_ajustada,
fecha_proceso,
pl,
regcons,
tipo_Cartera,
marca_alivio,
marca_sobrecupo,
vdesem,
sk,
cv1,
castigo,
subsegm,
cargo_aprobador,
modelo_optimo,
descr_subsegm,
zona,
descr_zona,
descr_sector_aprobacion,
descr_subsector,
campana,
riesgo_sectorial,
estrategia,
estrategia_og, --SMS
estrategia_gbpj,--SMS
rango_plazo_m_ajust,
califi_trad, -- Calificacion vieja, con R que empezo a convertirse en C 
califi_c,  -- Toda la calificacion R, reprocesada a C
periodo_gracia

)

SELECT
id,
obl341,
CASE WHEN gerenciado IS NULL THEN 'NO G' ELSE gerenciado END AS gerenciado,
apl AS aplicativo,
Case 
When apl = '1' then 'FACTORING'
When apl = '3' then 'LEASING'
When apl = '7' then 'SUFI'
When apl = '4' then 'HIPOTECARIO'
When apl  in ('M','V','K')   then 'TARJETA DE CREDITO'
When pl in ('P04','EB8') Then 'CREDIPAGO'
When apl = 'L' and pl not in ('P04','EB8') Then 'CARTERA'
ELSE 'OTROS' End as Apl,
md3411,
clf,
fdesem,
fdesem_ajustada,
fecha_proceso,
pl,
tipo_Cartera,
CASE WHEN regcons IN ('BOGOTÁ','BOGOTÁ CENTRO','BOGOTÁ GOBIERNO', 'GOBIERNO','BOGOTÁ Y CENTRO','BOGOTA') THEN 'BOGOTÁ'
     WHEN regcons IN ('OTRAS CIUDADES','OTROS','') THEN 'OTROS'
     WHEN regcons IN ('DIRECCIÓN GENERAL', 'ANTIOQUIA') THEN 'ANTIOQUIA'
     WHEN regcons IS NULL THEN 'OTROS'
     ELSE regcons
END AS regcons,
marca_alivio,
marca_sobrecupo,
vdesem,
sk,
cv1,
castigo,

descr_subsegm AS subsegmento,
CASE WHEN descr_zona IS NULL THEN 'NO DISPONIBLE' ELSE descr_zona END AS descr_zona_aprobacion,
CASE WHEN descr_sector_aprobacion IN ('AGROINDUSTRIA') THEN 'AGRO'
     WHEN descr_sector_aprobacion IN ('MANUFACTURA INSUMOS') THEN 'MANUFACTURA'
     WHEN descr_sector_aprobacion IS NULL OR descr_sector_aprobacion = '' THEN 'NO DISPONIBLE'
     ELSE descr_sector_aprobacion
END AS descr_sector_aprobacion,
CASE WHEN descr_subsector IN ('COMERCIO DE VARIEDADES Y VESTUARIO' ) THEN 'COMERCIO DE VARIEDADES Y VESTUARIO'
     WHEN descr_subsector IN ('DISTRIBUIDORES PRODUCTOS CONSUMO MASIVO' ) THEN 'DISTRIBUIDORES PRODUCTOS CONSUMO MASIVO'
     WHEN descr_subsector IN ('SERVICIOS A EMPRESAS' ) THEN 'SERVICIOS A EMPRESAS'
     WHEN descr_subsector IN ('OBRAS DE INFRAESTRUCTURA' ) THEN 'OBRAS DE INFRAESTRUCTURA'
     WHEN descr_subsector LIKE '%FERRETE%' THEN 'FERRETERIA, MATERIALES DE CONSTRUCCION Y MAQUINARIA'
     WHEN descr_subsector LIKE '%PORCICULTURA%' THEN 'GANADERIA Y PORCICULTURA'
     WHEN descr_subsector IN ('EDIFICACIONES') THEN 'EDIFICACIONES'
     WHEN descr_subsector IN ('SUPERMERCADOS') THEN 'SUPERMERCADOS'
     WHEN descr_subsector IN ('IPS') THEN 'IPS'
     WHEN descr_subsector IN ('SERVICIOS A PERSONAS') THEN 'SERVICIOS A PERSONAS'
     WHEN descr_subsector IN ('SIN INFORMACION') THEN 'SIN INFORMACION'
     ELSE 'OTROS'
END AS descr_subsector_aprobacion,  
CASE WHEN campana IS NULL THEN 'SIN INFORMACION' ELSE campana END AS campana,
CASE WHEN tipo_estudio IS NULL THEN 'SIN INFORMACION' ELSE tipo_estudio END AS tipo_estudio,
CASE WHEN riesgo_sectorial IS NULL THEN 'SIN INFORMACION'
     ELSE riesgo_sectorial
END AS riesgo_Sectorial,
CASE WHEN estrategia IS NULL OR estrategia = '' THEN 'SIN INFORMACION'
     ELSE estrategia
END AS estrategia,
estrategia_og, --SMS
estrategia_gbpj,--SMS
CASE WHEN rango_plazo_m_ajust IS NULL THEN 'j. Sin Rango'
     ELSE rango_plazo_m_ajust
END AS rango_plazo_m_ajust,
CASE WHEN califi_trad IS NULL THEN 'SIN INFORMACION'
     WHEN trim(califi_trad) = '' THEN 'SIN INFORMACION'
     ELSE califi_trad
END AS califi,
CASE WHEN califi_c IS NULL THEN 'SIN INFORMACION'
     WHEN trim(califi_c) = '' THEN 'SIN INFORMACION'
     ELSE califi_c
END AS califi_c,
CASE WHEN periodo_gracia IS NULL THEN 'SIN INFORMACION'
     ELSE periodo_gracia
  END AS periodo_gracia,
CASE  WHEN cargo_aprobador IS NULL THEN 'SIN INFORMACION'
     ELSE cargo_aprobador
  END AS cargo_aprobador,

CASE  WHEN modelo_optimo IS NULL THEN 'SIN INFORMACION'
     ELSE modelo_optimo
  END AS modelo_optimo,

MAX(CASE WHEN descr_segm IS NULL OR descr_segm = '' THEN 'NO DISPONIBLE'  ELSE descr_segm END) AS segmento_aprobacion,
MAX(CASE WHEN flujo IS NULL THEN 'SIN INFORMACION'
     ELSE flujo
END) AS flujo,
MAX(CASE WHEN flujo_actual IS NULL OR flujo_actual = '' THEN 'SIN INFORMACION'
     ELSE flujo_actual
END) AS flujo_actual

FROM FIN
GROUP BY 
          1, 
          2,
          3, 
          4, 
          5, 
          6,
          7, 
          8,
          9,
          10, 
          11, 
          12,
          13,
          14,
          15,
          16,
          17, 
          18, 
          19,
          20, 
          21,
          22,
          23,
          24,
          25,
          26,
          27,
          28,
          29,
          30,
          31,
          32,
          33,
          34,
          35

;

 DROP TABLE IF EXISTS  proceso_riesgos.cd_pymes_insumo_sd  PURGE

;

CREATE TABLE  proceso_riesgos.cd_pymes_insumo_sd STORED AS PARQUET AS 

WITH 
BASE AS (
SELECT DISTINCT t1.*, case when ((year(t1.fecha_proceso)-year(t1.fdesem))*12)+(month(t1.fecha_proceso)-month(t1.fdesem)) = 0 then 1 
                  else ((year(t1.fecha_proceso)-year(t1.fdesem))*12)+(month(t1.fecha_proceso)-month(t1.fdesem)) end as altura
FROM proceso.cd_pymes_sd t1
ORDER BY t1.obl341, t1.fecha_proceso 
)
SELECT *
FROM base 
;

COMPUTE STATS proceso_riesgos.cd_pymes_insumo_sd

;

DROP TABLE IF EXISTS  proceso.cd_pymes_desembolso_sd PURGE

;

CREATE TABLE  proceso.cd_pymes_desembolso_sd STORED AS PARQUET AS 

WITH

MARCAJE AS (

SELECT t1.*,
LAG(t1.obl341) OVER (PARTITION BY t1.obl341, t1.aplicativo, t1.pl, t1.md3411 ORDER BY t1.obl341, t1.md3411, t1.fecha_proceso asc) AS lag_obl
from proceso_riesgos.cd_pymes_insumo_sd t1
),


MARCA_VENCIDOS AS (

SELECT 
id,
obl341,
gerenciado,
aplicativo,
apl,
md3411,
clf,
fdesem,
fdesem_ajustada,
fecha_proceso,
pl,
tipo_Cartera,
regcons,
marca_alivio,
marca_sobrecupo,
vdesem,
CASE WHEN segmento_aprobacion IS NULL THEN 'SIN INFORMACION' WHEN segmento_aprobacion = '' THEN 'SIN INFORMACION' ELSE segmento_Aprobacion END AS segmento_aprobacion,
CASE WHEN subsegmento IS NULL THEN 'SIN INFORMACION' WHEN subsegmento = '' THEN 'SIN INFORMACION' ELSE subsegmento END AS subsegmento,
CASE WHEN descr_zona_aprobacion IS NULL THEN 'SIN INFORMACION' WHEN descr_zona_aprobacion = '' THEN 'SIN INFORMACION' ELSE descr_zona_aprobacion END AS  descr_zona_aprobacion,
CASE WHEN descr_sector_aprobacion  IS NULL THEN 'SIN INFORMACION'  WHEN descr_sector_aprobacion  IN ('','SIN INFORMACION') THEN 'SIN INFORMACION' ELSE descr_sector_aprobacion  END AS descr_sector_aprobacion,
CASE WHEN descr_subsector_aprobacion  IS NULL THEN 'SIN INFORMACION'  WHEN descr_subsector_aprobacion = '' THEN 'SIN INFORMACION' ELSE descr_subsector_aprobacion  END AS descr_subsector_aprobacion,
campana,
cargo_aprobador,
modelo_optimo,
tipo_estudio,
flujo,
CASE WHEN flujo_actual = '' THEN 'SIN INFORMACION' ELSE flujo_actual END AS flujo_actual,
CASE WHEN riesgo_Sectorial = '' THEN 'SIN INFORMACION' ELSE riesgo_Sectorial END AS riesgo_Sectorial,
CASE WHEN TRIM(estrategia) = '' THEN 'SIN INFORMACION'
     ELSE estrategia
END AS estrategia,
rango_plazo_m_ajust,
califi,
califi_c,
periodo_gracia,
CASE WHEN obl341 <> coalesce(lag_obl,'0') THEN 1
     ELSE 0
END AS marca_desem,
CASE WHEN obl341 <> coalesce(lag_obl,'0') THEN vdesem
     ELSE 0
END AS saldo_desem,
CASE WHEN altura IS NULL THEN 0 ELSE altura END AS altura,
CASE WHEN sk IS NULL THEN 0 ELSE sk END AS sk,
CASE WHEN CV1 IS NULL THEN 0 ELSE cv1 END AS saldo_vencido,
CASE WHEN cv1>0 THEN 1 
     ELSE 0 
END AS casos_vencidos,
CASE WHEN castigo IS NULL THEN 0 ELSE castigo END AS castigo,
CASE WHEN castigo>0 THEN 1 
     ELSE 0 
END AS casos_castigados,
CASE WHEN cv1>0 or castigo>0 THEN vdesem
     ELSE 0 
END AS cupo_vencido
from marcaje
),

TABLA_1 AS (

SELECT 
t1.fdesem_ajustada,
t1.aplicativo,
t1.gerenciado,
t1.md3411,
t1.apl,
t1.pl,
t1.tipo_Cartera,
t1.clf,
t1.regcons,
t1.marca_alivio,
t1.marca_sobrecupo,
t1.segmento_aprobacion,
t1.subsegmento,
t1.descr_zona_aprobacion,
t1.descr_sector_aprobacion,
t1.descr_subsector_aprobacion,
t1.campana,
t1.cargo_aprobador,
t1.modelo_optimo,
t1.tipo_estudio,
t1.flujo,
t1.flujo_actual,
t1.riesgo_sectorial,
t1.estrategia,
t1.rango_plazo_m_ajust,
t1.califi,
t1.califi_c,
t1.periodo_gracia,
sum(t1.marca_desem) AS casos_total_desem,
sum(t1.saldo_desem) AS valor_total_desem
FROM marca_vencidos t1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
),

TABLA_2 AS (
SELECT 
t1.fdesem_ajustada,
CASE WHEN t1.altura IS NULL THEN 0 ELSE altura END AS altura,
count(*) AS casos
FROM marca_vencidos t1
GROUP BY 1,2
)

SELECT 
t1.*,
t2.altura
FROM tabla_1 t1
LEFT JOIN tabla_2 t2 ON t1.fdesem_ajustada=t2.fdesem_ajustada 
;

COMPUTE STATS proceso.cd_pymes_desembolso_sd

;

DROP TABLE IF EXISTS  proceso.cd_pymes_cosecha_sd PURGE

;
CREATE TABLE  proceso.cd_pymes_cosecha_sd STORED AS PARQUET AS 

WITH

MARCAJE AS (

SELECT t1.*,
LAG(t1.obl341) OVER (PARTITION BY  t1.obl341, t1.aplicativo, t1.pl, t1.md3411  ORDER BY t1.obl341, t1.md3411, t1.fecha_proceso asc) AS lag_obl
from proceso_riesgos.cd_pymes_insumo_sd t1
),


MARCA_VENCIDOS AS (

SELECT 
id,
obl341,
gerenciado,
aplicativo,
apl,
md3411,
clf,
fdesem,
fdesem_ajustada,
fecha_proceso,
pl,
regcons,
tipo_Cartera,
marca_alivio,
marca_sobrecupo,
vdesem,
CASE WHEN segmento_aprobacion IS NULL THEN 'SIN INFORMACION' WHEN segmento_aprobacion = '' THEN 'SIN INFORMACION' ELSE segmento_Aprobacion END AS segmento_aprobacion,
CASE WHEN subsegmento IS NULL THEN 'SIN INFORMACION' WHEN subsegmento = '' THEN 'SIN INFORMACION' ELSE subsegmento END AS subsegmento,
CASE WHEN descr_zona_aprobacion IS NULL THEN 'SIN INFORMACION' WHEN descr_zona_aprobacion = '' THEN 'SIN INFORMACION' ELSE descr_zona_aprobacion END AS  descr_zona_aprobacion,
CASE WHEN descr_sector_aprobacion  IS NULL THEN 'SIN INFORMACION'  WHEN descr_sector_aprobacion = '' THEN 'SIN INFORMACION' ELSE descr_sector_aprobacion  END AS descr_sector_aprobacion,
CASE WHEN descr_subsector_aprobacion  IS NULL THEN 'SIN INFORMACION'  WHEN descr_subsector_aprobacion = '' THEN 'SIN INFORMACION' ELSE descr_subsector_aprobacion  END AS descr_subsector_aprobacion,
campana,
cargo_aprobador,
modelo_optimo,
tipo_estudio,
flujo,
CASE WHEN flujo_actual = '' THEN 'SIN INFORMACION' ELSE flujo_actual END AS flujo_actual,
CASE WHEN riesgo_Sectorial = '' THEN 'SIN INFORMACION' ELSE riesgo_Sectorial END AS riesgo_Sectorial,
CASE WHEN TRIM(estrategia) = '' THEN 'SIN INFORMACION'
     ELSE estrategia
END AS estrategia,
rango_plazo_m_ajust,
califi,
califi_c,
periodo_gracia,
CASE WHEN obl341 <> coalesce(lag_obl,'0') THEN 1
     ELSE 0
END AS marca_desem,
CASE WHEN obl341 <> coalesce(lag_obl,'0') THEN vdesem
     ELSE 0
END AS saldo_desem,
CASE WHEN altura IS NULL THEN 0 ELSE altura END AS altura,
CASE WHEN sk IS NULL THEN 0 ELSE sk END AS sk,
CASE WHEN CV1 IS NULL THEN 0 ELSE cv1 END AS saldo_vencido,
CASE WHEN cv1>0 THEN 1 
     ELSE 0 
END AS casos_vencidos,
CASE WHEN castigo IS NULL THEN 0 ELSE castigo END AS castigo,
CASE WHEN castigo>0 THEN 1 
     ELSE 0 
END AS casos_castigados,
CASE WHEN cv1>0 or castigo>0 THEN vdesem
     ELSE 0 
END AS cupo_vencido
from marcaje
)

SELECT 
t1.fdesem_ajustada,
CASE WHEN t1.altura IS NULL THEN 0 ELSE altura END AS altura,
t1.aplicativo,
t1.md3411,
t1.gerenciado,
t1.apl,
t1.pl,
t1.clf,
t1.regcons,
t1.tipo_Cartera,
t1.marca_alivio,
t1.marca_Sobrecupo,
t1.segmento_aprobacion,
t1.subsegmento,
t1.descr_zona_aprobacion,
t1.descr_sector_aprobacion,
t1.descr_subsector_aprobacion,
t1.campana,
t1.cargo_aprobador,
t1.modelo_optimo,
t1.tipo_estudio,
t1.flujo,
t1.flujo_actual,
t1.riesgo_sectorial,
t1.estrategia,
t1.rango_plazo_m_ajust,
t1.califi,
t1.califi_c,
t1.periodo_gracia,
sum(t1.marca_desem) AS casos_total_desem,
sum(t1.saldo_desem) AS valor_total_desem,
sum(t1.sk) AS saldo_capital,
sum(t1.saldo_vencido) AS saldo_vencido,
sum(t1.casos_vencidos) AS casos_vencidos,
sum(t1.castigo) as castigos,
sum(t1.casos_castigados) AS casos_castigados,
sum(t1.cupo_vencido) AS cupo_vencido


FROM marca_vencidos t1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29

;

COMPUTE STATS proceso.cd_pymes_cosecha_sd

;

DROP TABLE IF EXISTS  proceso.base_cosechas_pymes_sd PURGE

;
CREATE TABLE proceso.base_cosechas_pymes_sd STORED AS PARQUET AS

WITH

BASE AS (

SELECT t1.*,
       t2.saldo_capital,
       t2.saldo_vencido,
       t2.casos_vencidos,
       t2.castigos,
       t2.casos_castigados,
       t2.cupo_vencido
FROM proceso.cd_pymes_desembolso_sd t1
LEFT JOIN  proceso.cd_pymes_cosecha_sd t2 ON
      t1.fdesem_ajustada=t2.fdesem_ajustada AND
      t1.apl=t2.apl AND
      t1.aplicativo=t2.aplicativo AND
      t1.md3411=t2.md3411 AND
      t1.gerenciado=t2.gerenciado AND
      t1.clf=t2.clf AND
      t1.pl=t2.pl AND
      t1.regcons=t2.regcons AND
      t1.tipo_Cartera=t2.tipo_Cartera AND
      t1.marca_alivio = t2.marca_alivio AND
      t1.marca_sobrecupo = t2.marca_sobrecupo AND
      t1.altura=t2.altura AND
      t1.segmento_aprobacion=t2.segmento_aprobacion AND
      t1.subsegmento=t2.subsegmento AND
      t1.descr_zona_aprobacion=t2.descr_zona_aprobacion AND
      t1.descr_sector_aprobacion=t2.descr_sector_aprobacion AND
      t1.descr_subsector_aprobacion=t2.descr_subsector_aprobacion AND
      t1.campana=t2.campana AND
      t1.cargo_aprobador=t2.cargo_aprobador AND
      t1.modelo_optimo=t2.modelo_optimo AND
      t1.tipo_estudio=t2.tipo_estudio AND
      t1.flujo=t2.flujo AND
      t1.flujo_actual=t2.flujo_actual AND
      t1.riesgo_sectorial=t2.riesgo_sectorial AND
      t1.estrategia=t2.estrategia AND
      t1.rango_plazo_m_ajust=t2.rango_plazo_m_ajust AND
      t1.califi=t2.califi AND
      t1.califi_c=t2.califi_C AND
      t1.periodo_gracia=t2.periodo_gracia
      
      
      
      
      )
      
SELECT *
FROM BASE
;

COMPUTE STATS proceso.base_cosechas_pymes_sd

;

DROP TABLE IF EXISTS proceso_riesgos.cosechas_pymes PURGE

;
CREATE TABLE proceso_riesgos.cosechas_pymes STORED AS PARQUET AS

WITH

BASE AS (

SELECT *, concat(aplicativo, pl, clf) as llave
FROM proceso.base_cosechas_pymes_sd
),

RECETA AS (

SELECT llave, MAX(producto_consolidado) AS producto_consolidado
FROM resultados_riesgos.asignacion_productos
WHERE year=2021 AND ingestion_month=12
GROUP BY 1
),

PREVIA AS (

SELECT 
X.fdesem_ajustada,
x.gerenciado,
X.apl,
X.pl,
X.clf,
X.regcons,
x.tipo_Cartera,
X.marca_alivio,
X.marca_sobrecupo,
X.segmento_aprobacion,
X.subsegmento,
X.descr_zona_aprobacion,
X.descr_sector_aprobacion,
X.descr_subsector_aprobacion,
x.campana,
x.cargo_aprobador,
x.modelo_optimo,
x.tipo_estudio,
UPPER(Case
When x.flujo_Actual in ( 'Flujo Instancia Directa', 'Flujo Acta Sucursal') then 'Flujos Comerciales'
When x.flujo_actual in ('Fast Tack Cartera', 'Fast Track OPE cta cte', 'Fast Track TDC', 'Fast Tack Mesa de Dinero') then 'Fast track'
When TRIM(x.flujo_actual) IN ('Preaprobados','Preaprobado MT') then 'Preaprobados Anterior'
When TRIM(x.flujo)='Preaprobados Proyecto' and x.flujo_Actual in ('SIN INFORMACION','NULL', 'Renovados', 'Preaprobados') then 'Preaprobados'
When x.flujo_actual='' then 'Aprobaciones Automaticas Anterior'
Else x.flujo_actual End) as flujo_aprobacion,

--X.flujo,
X.riesgo_sectorial,
X.estrategia,
X.rango_plazo_m_ajust,
CASE WHEN X.califi IN ('R1','R2','R3') THEN 'R1-R3'
     WHEN X.califi IN ('R4','R5','R6') THEN 'R4-R6'
     WHEN X.califi IN ('R7') THEN 'R7'
     WHEN X.califi IN ('R8','R9','R10','R11') THEN 'R8-R11'
     ELSE x.califi END AS califi,
CASE WHEN X.califi_c IN ('C1','C2','C3','C4','C5','C6') THEN 'C1-C6'
     WHEN X.califi_c IN ('C7','C8', 'C9','C10','C11','C12','C13') THEN 'C7-C13'
     WHEN X.califi_c IN ('C14') THEN 'C14'
     WHEN X.califi_c IN ('C15','C16') THEN 'C15-C16'
     WHEN X.califi_c IN ('C17','C18','C19') THEN 'C17-C19'
     WHEN x.califi_c is null then 'SIN CALIFICACION'
     ELSE x.califi_c
END AS califi_c,
X.periodo_gracia,
X.casos_total_desem,
X.valor_total_desem,
X.altura,
X.saldo_capital,
X.saldo_vencido,
X.casos_vencidos,
X.castigos,
X.casos_castigados,
X.cupo_vencido,
v.producto_consolidado

FROM base x
LEFT JOIN receta v ON x.llave=v.llave

)

SELECT 
fdesem_ajustada,
campana,
cargo_aprobador,
modelo_optimo,
gerenciado,
apl,
clf,
regcons,
marca_alivio,
segmento_aprobacion,
subsegmento,
descr_zona_aprobacion,
descr_sector_aprobacion,
descr_subsector_aprobacion,
flujo_aprobacion,
riesgo_sectorial,
estrategia,
rango_plazo_m_ajust,
califi,
califi_c,
tipo_estudio, 
periodo_gracia,
casos_total_desem,
valor_total_desem,
altura,
saldo_capital,
saldo_vencido,
casos_vencidos,
castigos,
casos_castigados,
cupo_vencido,
tipo_cartera,
CASE WHEN producto_consolidado is NULL then 'SIN ASIGNAR' ELSE producto_consolidado END AS producto_consolidado,
year(now()) AS ingestion_year,
month(now())-1 AS ingestion_month,
1 AS ingestion_day
FROM PREVIA

;

COMPUTE STATS proceso_riesgos.cosechas_pymes

;
