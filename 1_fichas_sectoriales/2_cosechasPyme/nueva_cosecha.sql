-------------------------- MARCA NOVACIONES -------------------------- 
SET SYNC_DDL=1
;
DROP TABLE IF EXISTS proceso_generadores.garc_marca_novaciones_sms PURGE
;
CREATE TABLE proceso_generadores.garc_marca_novaciones_sms STORED AS PARQUET AS 
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
COMPUTE STATS proceso_generadores.garc_marca_novaciones_sms
;

-------------------------- OBLIGACIONES CAPA -------------------------- 

DROP TABLE IF EXISTS  proceso_generadores.cd_cecsif1_pymes_pre PURGE

;

CREATE TABLE proceso_generadores.cd_cecsif1_pymes_pre  STORED AS PARQUET AS

WITH

BASE AS (
SELECT  ROW_NUMBER() OVER ( PARTITION BY cod_apli, oblig, cod_moneda, f_desemb, vlr_desemb_oblig ORDER BY ingestion_year*100+ingestion_month ASC) AS rn
, ingestion_year*100+ingestion_month AS fdesem_ajustada
, tipo_identificacion_cli as tid
, num_doc as  id
, oblig as obl341
, marca_gerenciado as gerenciado
, cod_apli as apl
, region_of as regcons
, CASE WHEN TRIM(cod_subsegm) = '' OR TRIM(cod_subsegm) IN ('05','00',NULL) THEN 'NO APLICA'
       WHEN TRIM(cod_subsegm) IN ('01','07','09','23') THEN 'PEQUENA'
       WHEN TRIM(cod_subsegm) IN ('02','08','10','20') THEN 'MEDIANA'
       WHEN TRIM(cod_subsegm) IN ('03','06','11','21') THEN 'GRANDE'
       WHEN TRIM(cod_subsegm) IN ('25') THEN 'PLUS'
  END AS subsegmento
, sector_corporativo as sector
, subsector
, cod_moneda as md3411
, cod_clase as clf
, f_desemb as  fdesem
, cod_plan as  pl
, int_months_between(f_venc,f_desemb) as plazo
, case
  when cod_apli in ('K','M','V')
  then cupo_tot_tdc_revolventes_y_sob else vlr_desemb_oblig end as vdesem

,if( vlr_desemb_oblig=0 and upper(trim(cod_apli)) not in ("K","M","V"),1,0) as excluir --En la master el  vlr_desemb_oblig viene en 0 para las TDC
, marca_castigo
, cod_segm
FROM resultados_riesgos.master_credit_risk
WHERE (ingestion_year BETWEEN 2018 and year(now()))  AND 
      from_timestamp(f_desemb, 'yyyyMM') >= '201701' AND 
      --vlr_desemb_oblig > 0 AND 
      vlr_desemb_oblig IS NOT NULL AND
      --trim(cod_segm)='5' AND 
      trim(cod_apli) NOT IN ('C','D','S') AND 
      trim(cod_plan) NOT IN ('A43','A13','A53','A50','A42','A41','A11',	'A15','A14','A12','A52',	'A47','A16','A45','A21',	'A51','A56','A55','A17','A10','A20','A40') AND
      CAST(from_timestamp(f_desemb, 'yyyyMM') AS BIGINT)=ingestion_year*100+ingestion_month AND
      libro=1
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
, marca_castigo
, cod_segm AS segm_master
FROM base
WHERE rn = 1 and excluir=0
),

FIN AS (
SELECT *,  ROW_NUMBER() OVER ( PARTITION BY apl, obl341, md3411, vdesem ORDER BY fdesem_ajustada ASC) AS rn
FROM previa
)

SELECT *
FROM fin
WHERE rn=1

;

-------------------------- BASE DESEMBOLSOS ----------------------------- 


-------------------------- CRUCE SEGMENTO ---------------------------- 

DROP TABLE IF EXISTS  proceso_generadores.cruce_master_ifrs9 PURGE;

CREATE TABLE proceso_generadores.cruce_master_ifrs9 STORED AS PARQUET AS

WITH cru AS (
       SELECT
       t1.tid
       , t1.id
       , t1.obl341
       , t1.gerenciado
       , t1.apl
       , t1.regcons
       , t1.subsegmento
       , t1.sector
       , t1.subsector
       , t1.md3411
       , t1.clf
       , t1.fdesem
       , t1.fdesem_ajustada
       , t1.pl
       , t1.vdesem
       , t1.rango_plazo_m_ajust
       , t1.marca_castigo
       --, t1.segm_master
       , t2.cod_segm
       --, t2.fecha_ajustada
       
       FROM proceso_generadores.cd_cecsif1_pymes_pre AS t1
       LEFT JOIN proceso_generadores.ifrs9_origen AS t2
       ON cast(t1.obl341 AS BIGINT) = cast(t2.oblig AS BIGINT)
       AND trim(t1.md3411)=trim(t2.cod_moneda)
       AND lower(trim(t1.apl))= lower(trim(t2.cod_apli))
       AND cast(t1.fdesem_ajustada AS BIGINT) = cast(t2.fecha_ajustada AS BIGINT)
)

SELECT 
       tid
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
       , rango_plazo_m_ajust
       , marca_castigo
FROM cru
WHERE trim(cod_segm)  = "5" 
;

-------------------------- OBLIGACIONES CENIEGARG -------------------------- 

DROP TABLE IF EXISTS  proceso_generadores.cd_cecsif1_pymes_garc PURGE

;

CREATE TABLE proceso_generadores.cd_cecsif1_pymes_garc  STORED AS PARQUET AS

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
WHERE year in (2017) AND 
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

COMPUTE STATS proceso_generadores.cd_cecsif1_pymes_garc

;

DROP TABLE IF EXISTS  proceso_generadores.cd_cecsif1_pymes PURGE;

CREATE TABLE proceso_generadores.cd_cecsif1_pymes STORED AS PARQUET AS

SELECT 
cast(tid AS double) AS tid
, cast(id AS double) AS id
, obl341
, gerenciado
, apl
, regcons
, subsegmento
, sector
, subsector
, cast(md3411 AS double) AS md3411 
, clf
, fdesem
, fdesem_ajustada
, pl
, vdesem
, rango_plazo_m_ajust
FROM proceso_generadores.cd_cecsif1_pymes_pre
UNION 
SELECT
tid
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
, rango_plazo_m_ajust
FROM proceso_generadores.cd_cecsif1_pymes_garc

;

-- -------------------------- COMPORTAMIENTO OBLIGACIONES MASTER -------------------------- 

DROP TABLE IF EXISTS proceso_generadores.cd_cecsif2_pymes_capa  PURGE

;

CREATE TABLE proceso_generadores.cd_cecsif2_pymes_capa  STORED AS PARQUET AS

WITH

CECSIF1 AS (
SELECT *
FROM  proceso_generadores.cd_cecsif1_pymes 
),

COMPORTAMIENTO AS (
SELECT 
CAST(from_timestamp(f_desemb, 'yyyyMM') AS BIGINT) AS f_proceso
,f_desemb as fdesem 
,vlr_desemb_oblig as vdesem
,tipo_identificacion_cli as tid 
,num_doc as  id 
,oblig as obl341 
,cod_apli as apl 
,cod_plan as  pl
,cod_moneda as md3411  
,dias_mora as altmora 
,sld_cap_tot as sk 
,cv as cv1
FROM resultados_riesgos.master_credit_risk
WHERE (ingestion_year BETWEEN 2018 and year(now())) AND 
     from_timestamp(f_desemb, 'yyyyMM') >= '201701' AND 
     CAST(from_timestamp(f_desemb, 'yyyyMM') AS BIGINT) >= 201702 AND 
     --vlr_desemb_oblig  > 0 AND 
     vlr_desemb_oblig  IS NOT NULL AND
     trim(cod_apli) NOT IN ('C','D') AND 
     trim(cod_plan) NOT IN ('A43','A13','A53','A50','A42','A41','A11',	'A15','A14','A12','A52',	'A47','A16','A45','A21',	'A51','A56','A55','A17','A10','A20','A40') 
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

COMPUTE STATS proceso_generadores.cd_cecsif2_pymes_capa
;

-------------------------- COMPORTAMIENTO OBLIGACIONES CENIEGARC -------------------------- 

DROP TABLE IF EXISTS  proceso_generadores.cd_cecsif2_pymes_garc  PURGE;
CREATE TABLE proceso_generadores.cd_cecsif2_pymes_garc  STORED AS PARQUET AS

WITH

CECSIF1 AS (
SELECT *
FROM  proceso_generadores.cd_cecsif1_pymes 
),

COMPORTAMIENTO AS (
SELECT corte AS f_proceso, fdesem, vdesem, tid, id, obl341, apl, pl, md3411, altmora, sk, cv1
FROM resultados_riesgos.ceniegarc_lz 
WHERE year in (2017) AND 
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

COMPUTE STATS proceso_generadores.cd_cecsif2_pymes_garc

;

DROP TABLE IF EXISTS proceso_generadores.cd_cecsif2_pymes PURGE;

CREATE TABLE proceso_generadores.cd_cecsif2_pymes STORED AS PARQUET AS

SELECT 
tid
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
, rango_plazo_m_ajust
, f_proceso
, altmora
, sk
, cv1
FROM proceso_generadores.cd_cecsif2_pymes_garc
UNION 
SELECT 
tid
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
, rango_plazo_m_ajust
, f_proceso
, altmora
, sk
, cv1
FROM proceso_generadores.cd_cecsif2_pymes_capa

;
