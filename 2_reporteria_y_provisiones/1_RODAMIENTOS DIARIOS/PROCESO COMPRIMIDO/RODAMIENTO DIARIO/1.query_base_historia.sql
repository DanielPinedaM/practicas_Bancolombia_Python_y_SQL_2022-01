CREATE TABLE proceso_consumidores.RODAMIENTOS_DIARIOS_${fcorte} /*NombreBaseDeDatos.NombreTabla*/
STORED AS PARQUET AS
WITH SALDOS_ANTERIOR AS (
Select
sum(sld_cap_act) as sk_inicial,
obl_cn as obl341_ant,
DESC_SEGM as segdesc_ant,
  (CASE
                WHEN MAX(nueva_altura_mora) = 0 THEN '00. 0' 
                WHEN MAX(nueva_altura_mora) >= 1 AND MAX(nueva_altura_mora) <=15 THEN '01. 1 - 15'   
                WHEN MAX(nueva_altura_mora) >= 16 AND MAX(nueva_altura_mora) <=30 THEN '02. 16 - 30' 
                WHEN MAX(nueva_altura_mora) >= 31 AND MAX(nueva_altura_mora) <=60 THEN '03. 31 - 60'    
                WHEN MAX(nueva_altura_mora) >= 61 AND MAX(nueva_altura_mora) <=90 THEN '04. 61 - 90'    
                WHEN MAX(nueva_altura_mora) >= 91 AND MAX(nueva_altura_mora) <=120 THEN '05. 91 - 120'  
                WHEN MAX(nueva_altura_mora) >= 121 AND MAX(nueva_altura_mora) <=150 THEN '06. 121 - 150' 
                WHEN MAX(nueva_altura_mora) >= 151 AND MAX(nueva_altura_mora) <=180 THEN '07. 151 - 180' 
                WHEN MAX(nueva_altura_mora) >= 181 THEN '08. Mayor 181' END) as Rango_Mora_Inicial, 
(case
when trim(apli)= '7' and trim(pcons) ='VEHICULOS SUFI' then 'MOVILIDAD'
when trim(apli)= '7' and trim(pcons) <>'VEHICULOS SUFI' then 'COTIDIANIDAD'
when trim(apli)='1' then 'FACTORING'
when trim(pcons)='CREDIPAGO' then 'ROTATIVOS'
else pcons end) as pcons_ant,
moneda  as moneda_ant,
apli as apl_ant,
nueva_altura_mora as nueva_altura_mora_ant,
num_doc as num_doc_ant,
tipo_doc as tipo_identificacion_ant,
banca as banca_ant,
nueva_banca as nueva_banca_ant,
vicepresidencia as vicepresidencia_ant,
producto_agr as producto_agrupado_ant,
clasificacion as clf_ant,
( CASE 
        	WHEN nueva_region IN ('Bogotá Centro','BOGOTÁ','BOGOTÁ Y CENTRO','BOGOTA','Bogotá','Bogotá Gobierno', 'GOBIERNO','INTERNACI','INTERNACIONAL','BOGOTA Y')  THEN "BOGOTÁ"
        	WHEN nueva_region IN ('CENTRO', 'Centro') THEN "CENTRO"
        	WHEN nueva_region IN ('Caribe', 'CARIBE') THEN "CARIBE"
        	WHEN nueva_region IN ('SUR', 'Sur') THEN "SUR"
        	WHEN nueva_region IN ('ANTIOQUIA', 'Antioquia') THEN "ANTIOQUIA"
        	ELSE 'OTROS'  END ) AS regcons_ant 
from resultados_riesgos.reporte_saldos_diarios 
where ingestion_year=${year_ayer} and ingestion_month=${month_ayer} and ingestion_day=${day_ayer} and  (nutitula=0 or nutitula is null)
group by 2,3,5,6,7,8,9,10,11,12,13,14,15,16
)
,SALDOS_ACTUAL AS (
Select
sum(sld_cap_act) as sk_final,
obl_cn as obl341,
DESC_SEGM as segdesc,
  (CASE
                WHEN MAX(nueva_altura_mora) = 0 THEN '00. 0' 
                WHEN MAX(nueva_altura_mora) >= 1 AND MAX(nueva_altura_mora) <=15 THEN '01. 1 - 15'   
                WHEN MAX(nueva_altura_mora) >= 16 AND MAX(nueva_altura_mora) <=30 THEN '02. 16 - 30' 
                WHEN MAX(nueva_altura_mora) >= 31 AND MAX(nueva_altura_mora) <=60 THEN '03. 31 - 60'    
                WHEN MAX(nueva_altura_mora) >= 61 AND MAX(nueva_altura_mora) <=90 THEN '04. 61 - 90'    
                WHEN MAX(nueva_altura_mora) >= 91 AND MAX(nueva_altura_mora) <=120 THEN '05. 91 - 120'  
                WHEN MAX(nueva_altura_mora) >= 121 AND MAX(nueva_altura_mora) <=150 THEN '06. 121 - 150' 
                WHEN MAX(nueva_altura_mora) >= 151 AND MAX(nueva_altura_mora) <=180 THEN '07. 151 - 180' 
                WHEN MAX(nueva_altura_mora) >= 181 THEN '08. Mayor 181' END) as Rango_Mora_final,
(case
when trim(apli)= '7' and trim(pcons) ='VEHICULOS SUFI' then 'MOVILIDAD'
when trim(apli)= '7' and trim(pcons) <>'VEHICULOS SUFI' then 'COTIDIANIDAD'
when trim(apli)='1' then 'FACTORING'
when trim(pcons)='CREDIPAGO' then 'ROTATIVOS'
else pcons end) as pcons,
moneda  as moneda,
apli as apl,
nueva_altura_mora as nueva_altura_mora,
num_doc as num_doc,
tipo_doc as tipo_identificacion,
banca as banca,
nueva_banca as nueva_banca,
vicepresidencia as vicepresidencia,
producto_agr as producto_agrupado,
clasificacion as clf,
( CASE 
        	WHEN nueva_region IN ('Bogotá Centro','BOGOTÁ','BOGOTÁ Y CENTRO','BOGOTA','Bogotá','Bogotá Gobierno', 'GOBIERNO','INTERNACI','INTERNACIONAL','BOGOTA Y')  THEN "BOGOTÁ"
        	WHEN nueva_region IN ('CENTRO', 'Centro') THEN "CENTRO"
        	WHEN nueva_region IN ('Caribe', 'CARIBE') THEN "CARIBE"
        	WHEN nueva_region IN ('SUR', 'Sur') THEN "SUR"
        	WHEN nueva_region IN ('ANTIOQUIA', 'Antioquia') THEN "ANTIOQUIA"
        	ELSE 'OTROS'  END ) AS regcons
from resultados_riesgos.reporte_saldos_diarios 
where ingestion_year=${year_hoy} and ingestion_month=${month_hoy} and ingestion_day=${day_hoy} and  (nutitula=0 or nutitula is null)
group by 2,3,5,6,7,8,9,10,11,12,13,14,15,16
)
,FINAL AS (
SELECT
t5.*,
t6.*
from SALDOS_ANTERIOR  t5 full join SALDOS_ACTUAL t6
on (t5.obl341_ant=t6.obl341 and t5.apl_ant=t6.apl and t5.moneda_ant=t6.moneda and t5.num_doc_ant=t6.num_doc)
)
SELECT 
sk_inicial as sk_inicial,
sk_final as sk_final,
rango_mora_final,
rango_mora_inicial,
(case
when num_doc is null then num_doc_ant
else num_doc
end) AS id,
(case
when tipo_identificacion is null then tipo_identificacion_ant
else tipo_identificacion
end) AS tipo_identificacion,
(case
when segdesc is null then segdesc_ant
else segdesc
end) AS segdesc,
(case
when apl is null then apl_ant
else apl
end) AS apl,
(case
when OBL341 is null then OBL341_ant
else OBL341
end) AS obl341,
(case
when moneda is null then moneda_ant
else moneda
end) AS moneda,
(case
when PCONS is null then PCONS_ant
else PCONS end) as pcons,
(case
when Rango_Mora_Final ='09. Castigo' then "CASTIGO"
when Rango_Mora_Inicial is null and Rango_Mora_Final is not null then "NUEVO DESEMBOLSO"
when Rango_Mora_Final<Rango_Mora_Inicial then "MEJORA"
when Rango_Mora_Final=Rango_Mora_Inicial then "MANTIENE"
when Rango_Mora_Final>Rango_Mora_Inicial then "EMPEORA"
when Rango_Mora_Inicial is not null and Rango_Mora_Final is null then "CANCELADO"
else 'ERROR'
end) AS Estado_Transicion,
(case
when vicepresidencia  is null then vicepresidencia_ant
else vicepresidencia
end) AS vic_ccial,
(case
when banca  is null then banca_ant
else banca
end) AS banca,
(case
when clf  is null then clf_ant
else clf
end) AS clf,
(case
when regcons  is null then regcons_ant
else regcons
end) AS regcons,
${fcorte} as fcorte,
${year_hoy} as ingestion_year,
${month_hoy} as ingestion_month,
${day_hoy} as ingestion_day
from final