SELECT * FROM proceso_generadores.cd_cecsif1_pymes_sms 
WHERE fdesem_ajustada=202101 AND trim(apl) ="L"
LIMIT 100

;

--0004910000002734611
--201801
--3028304

-- 0000000000020096609
-- 202101
-- 200000000

-- #############################################

SELECT month, cod_segm, oblig, f_origen, sld_cap_vigente, sld_cap_tot 
FROM resultados_ifrs9.bco_resultado_final 
WHERE year IN(2021, 2020) AND CAST(oblig AS BIGINT) = 0000000000020096609
ORDER BY year DESC, 
      month DESC
      ;

-- #############################################

SELECT year(f_desemb) 
FROM resultados_ifrs9.bco_resultado_final
GROUP BY 1
ORDER BY 1 ASC
       ;

-- #############################################

SELECT year, month
FROM resultados_ifrs9.bco_resultado_final
GROUP BY 1, 2 
ORDER BY 1, 2
       ;

-- #############################################

DROP TABLE IF EXISTS proceso_generadores.ifrs9_origen PURGE
CREATE TABLE proceso_generadores.ifrs9_origen stored AS parquet as
SELECT 
oblig, 
cod_segm,
num_doc,
f_desemb,
year(f_desemb) * 1000 + month(f_desemb) AS fecha_ajustada,
cod_apli,
year, 
month
FROM resultados_ifrs9.bco_resultado_final
WHERE ( year BETWEEN 2018 AND year(now()) )
AND trim(cod_segm) = "5"
AND year(f_desemb) * 1000 + month(f_desemb) = year * 1000 + month
;

-- #############################################

SELECT oblig, cod_apli, num_doc,
COUNT (*)
FROM proceso_generadores.ifrs9_origen 
GROUP BY 1, 2, 3
ORDER BY 4 DESC
;

-- #############################################

SELECT oblig, cod_apli, num_doc, f_desemb, cod_moneda,
COUNT (*)
FROM proceso_generadores.ifrs9_origen 
GROUP BY 1, 2, 3, 4, 5
ORDER BY 6 DESC
;

-- #############################################

SELECT oblig, cod_apli, f_desemb,
COUNT (*)
FROM proceso_generadores.ifrs9_origen 
GROUP BY 1, 2, 3
ORDER BY 4 DESC
;

-- #############################################

with grupo AS (
SELECT oblig, cod_apli, cod_moneda, num_doc,
COUNT (*)
FROM proceso_generadores.ifrs9_origen 
GROUP BY 1, 2, 3, 4 having count(*) > 1 
ORDER BY 5 DESC
)
SELECT cod_apli, COUNT(*)
FROM grupo
GROUP BY 1
;

-- #############################################

with grupo AS (
SELECT obl341, apl, md3411, id,
COUNT (*)
FROM proceso_generadores.cd_cecsif1_pymes_sms
GROUP BY 1, 2, 3, 4 having count(*) > 1 
ORDER BY 5 DESC
)
SELECT apl, COUNT(*)
FROM grupo
GROUP BY 1
;

-- #############################################

SELECT obl341, apl, id, md3411,
COUNT (*)
FROM proceso_generadores.cecsif1_pymes_garc
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC
;

-- #############################################

CREATE TABLE proceso_generadores.cecsif1_pymes_garc  STORED AS PARQUET AS

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

-- #############################################

with grupo AS (
SELECT obl341, apl, md3411, id, fdesem_ajustada,
COUNT (*)
FROM proceso_generadores.cecsif1_pymes_garc
GROUP BY 1, 2, 3, 4, 5 having count(*) > 1 
ORDER BY 6 DESC
)
SELECT apl, COUNT(*)
FROM grupo
GROUP BY 1
;

-- #############################################

SELECT apl,
COUNT (*)
FROM proceso_generadores.cecsif1_pymes_garc
GROUP BY 1
;

-- #############################################

with grupo AS (
SELECT obl341, apl, md3411, id, fdesem_ajustada, 
COUNT (*)
FROM proceso_generadores.cd_cecsif1_pymes_sms
GROUP BY 1, 2, 3, 4, 5 having count(*) > 1 
ORDER BY 6 DESC
)
SELECT apl, COUNT(*)
FROM grupo
GROUP BY 1
;

-- #############################################

SELECT COUNT (*)
FROM proceso_generadores.cd_cecsif1_pymes_sms

--19461608

-- #############################################

with grupo AS (
SELECT oblig, cod_apli, cod_moneda, num_doc, fecha_ajustada,
COUNT (*)
FROM proceso_generadores.ifrs9_origen 
GROUP BY 1, 2, 3, 4, 5 having count(*) > 1 
ORDER BY 6 DESC
)
SELECT cod_apli, COUNT(*)
FROM grupo
GROUP BY 1
;

-- #############################################

SELECT segm_master, cod_segm,
COUNT(*)
FROM proceso_generadores.cruce_master_ifrs9 
GROUP BY 1, 2
ORDER BY 1, 2 

-- #############################################

SELECT
count(*)
FROM proceso_generadores.cruce_master_ifrs9;
-- 19461608

-- #############################################

SELECT
count(*)
FROM proceso_generadores.cd_cecsif1_pymes_sms;
-- 19461608

-- #############################################

DESCRIBE proceso_generadores.cd_cecsif1_pymes_garc

-- #############################################

DESCRIBE proceso_generadores.cd_cecsif1_pymes_pre

-- #############################################

with grupo AS (
SELECT obl341, apl, md3411, id, fdesem_ajustada, 
COUNT (*)
FROM proceso_generadores.cd_cecsif1_pymes
GROUP BY 1, 2, 3, 4, 5 having count(*) > 1 
ORDER BY 6 DESC
)
SELECT apl, COUNT(*)
FROM grupo
GROUP BY 1
;

-- #############################################

DESCRIBE proceso_generadores.cd_cecsif2_pymes_garc;

-- #############################################

DESCRIBE proceso_generadores.cd_cecsif2_pymes_capa;

-- #############################################

SELECT * FROM resultados_riesgos.detalle_desembolsos LIMIT 50

-- #############################################


-- #############################################

DESCRIBE proceso_generadores.cd_cecsif1_pymes_pre

-- #############################################

DESCRIBE resultados_riesgos.detalle_desembolsos

-- #############################################

SELECT  
year(fdesem) * 100 + month(fdesem)
FROM resultados_riesgos.detalle_desembolsos 
GROUP BY 1
ORDER BY 1 ASC
;

-- #############################################

DROP TABLE IF EXISTS proceso_generadores.cd_cecsif1_pymes_ofi PURGE;

CREATE TABLE proceso_generadores.cd_cecsif1_pymes_ofi STORED AS PARQUET AS
SELECT
t1.tipo_doc AS tid
, t1.num_doc AS id 
, t1.obligacion AS obl341
, t2.gerenciado AS gerenciado
, t1.cod_apli AS apl
, t2.regcons AS regcons
, t2.subsegmento AS subsegmento
, t2.sector AS sector
, t2.subsector AS subsector
, t2.md3411 AS md3411
, t2.clf AS clf
, t1.fdesem AS fdesem
, year(t1.fdesem) * 100 + month(t1.fdesem) AS fdesem_ajustada
, t2.pl AS pl
, t1.cupo_desembolso AS vdesem
, t2.rango_plazo_m_ajust AS rango_plazo_m_ajust

FROM resultados_riesgos.detalle_desembolsos AS t1
LEFT JOIN  proceso_generadores.cd_cecsif1_pymes_pre AS t2


 ON cast(t2.obl341 AS BIGINT) = cast(t1.obligacion AS BIGINT)
       AND trim(t2.md3411)=trim(t1.moneda)
       AND lower(trim(t2.apl))= lower(trim(t1.cod_apli))
       AND cast(t2.fdesem_ajustada AS BIGINT) = year(t1.fdesem) * 100 + month(t1.fdesem)
;

-- #############################################


