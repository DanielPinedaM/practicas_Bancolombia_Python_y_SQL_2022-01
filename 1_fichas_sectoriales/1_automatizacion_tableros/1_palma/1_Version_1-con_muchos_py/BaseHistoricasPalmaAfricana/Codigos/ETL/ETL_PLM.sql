---------------------------Vector fechas 1---------------------------------------------------------------
DROP table if exists proceso.vector1_fechas_plm purge;
CREATE table proceso.vector1_fechas_plm stored as parquet as 
SELECT DISTINCT year(fecha)*10000 + month(fecha)*100 +day(fecha) as fecha_cru from proceso_riesgos.IPC_plm UNION 
SELECT DISTINCT year(fecha)*10000 + month(fecha)*100 +day(fecha) as fecha_cru from proceso_riesgos.Precio_DelPetroleo_plm UNION 
SELECT DISTINCT year(fecha)*10000 + month(fecha)*100 +day(fecha) as fecha_cru from proceso_riesgos.Precio_AceiteDeSoya_plm UNION 
SELECT DISTINCT year(fecha)*10000 + month(fecha)*100 +day(fecha) as fecha_cru from proceso_riesgos.Precio_AceiteDePalmaCruda_plm UNION 
SELECT DISTINCT year(fecha)*10000 + month(fecha)*100 +day(fecha) as fecha_cru from proceso_riesgos.PrecioFrutoyAceiteDePalma_plm UNION 
SELECT DISTINCT year(fecha)*10000 + month(fecha)*100 +day(fecha) as fecha_cru from proceso_riesgos.BioCombustibles_sec_plm; 


--------------Join1------------------------------------------------------------------------------------
DROP table if exists proceso_riesgos.Tabla_palma_precios purge;
CREATE table proceso_riesgos.Tabla_palma_precios stored as parquet as 
SELECT
t1.fecha_cru
,CASE
WHEN t2.fecha is not null then t2.fecha
WHEN t3.fecha is not null then t3.fecha
WHEN t4.fecha is not null then t4.fecha
WHEN t5.fecha is not null then t5.fecha
WHEN t6.fecha is not null then t6.fecha
else t7.fecha end as fecha
,t2.aceites_comestibls as IPC_aceites_combustibles
,t2.margarinas_y_grasas__animales_y_vegetales_ as IPC_margarinas_y_grasas
,t3.precio_petroleo as precio_inter_petro
,t3.cambio as var_precios_petroleo
,t4.precio_aceite_soya as precio_inter_soya
,t4.cambio as var_precios_soya
,t5.precio as precio_inter_aceite_crudo
,t5.cambio as var_precios_aceite_palma
,t6.precio_nacional_del_aceite_crudo_de_palma as precio_nacional_del_aceite_crudo
,t7.precio_biodiesel as precio_biodiesel
,t7.precio_diesel_mezcla as precio_diesel
FROM proceso.vector1_fechas_plm as t1
LEFT JOIN proceso_riesgos.IPC_plm as t2
on t1.fecha_cru=year(t2.fecha)*10000 + month(t2.fecha)*100 +day(t2.fecha)
LEFT JOIN proceso_riesgos.Precio_DelPetroleo_plm  as t3
on t1.fecha_cru=year(t3.fecha)*10000 + month(t3.fecha)*100 +day(t3.fecha)
LEFT JOIN proceso_riesgos.Precio_AceiteDeSoya_plm  as t4
on t1.fecha_cru=year(t4.fecha)*10000 + month(t4.fecha)*100 +day(t4.fecha)
LEFT JOIN proceso_riesgos.Precio_AceiteDePalmaCruda_plm as t5
on t1.fecha_cru=year(t5.fecha)*10000 + month(t5.fecha)*100 +day(t5.fecha)
LEFT JOIN proceso_riesgos.PrecioFrutoyAceiteDePalma_plm  as t6
on t1.fecha_cru=year(t6.fecha)*10000 + month(t6.fecha)*100 +day(t6.fecha)
LEFT JOIN proceso_riesgos.BioCombustibles_sec_plm as t7
on t1.fecha_cru=year(t7.fecha)*10000 + month(t7.fecha)*100 +day(t7.fecha)
;

---------------------------Vector fechas 2---------------------------------------------------------------
DROP table if exists proceso.vector2_fechas_plm purge;
CREATE table proceso.vector2_fechas_plm stored as parquet as 
SELECT DISTINCT cast(fecha_cru/100 as BIGINT) as fecha from proceso.vector1_fechas_plm UNION 
SELECT fecha*100+mes as fecha from proceso_riesgos.IndiceProduccionEMMET_plm UNION 
SELECT DISTINCT
fecha*100+
(CASE
WHEN lower(trim(meses))="ene" THEN 1
WHEN lower(trim(meses))="feb" THEN 2
WHEN lower(trim(meses))="mar" THEN 3
WHEN lower(trim(meses))="abr" THEN 4
WHEN lower(trim(meses))="may" THEN 5
WHEN lower(trim(meses))="jun" THEN 6 
WHEN lower(trim(meses))="jul" THEN 7 
WHEN lower(trim(meses))="ago" THEN 8
WHEN lower(trim(meses))="sep" THEN 9
WHEN lower(trim(meses))="oct" THEN 10 
WHEN lower(trim(meses))="nov" THEN 11 
WHEN lower(trim(meses))="dic" THEN 12
end) as fecha
from proceso_riesgos.VentasAceite_Industriasec_plm UNION 
SELECT DISTINCT
100*(2000+ cast(substr(fecha,5,2) as int))+
(case 
when lower(trim(substr(fecha,1,3)))="ene" then 1
when lower(trim(substr(fecha,1,3)))="feb" then 2
when lower(trim(substr(fecha,1,3)))="mar" then 3
when lower(trim(substr(fecha,1,3)))="abr" then 4
when lower(trim(substr(fecha,1,3)))="may" then 5
when lower(trim(substr(fecha,1,3)))="jun" then 6
when lower(trim(substr(fecha,1,3)))="jul" then 7
when lower(trim(substr(fecha,1,3)))="ago" then 8
when lower(trim(substr(fecha,1,3)))="sep" then 9
when lower(trim(substr(fecha,1,3)))="oct" then 10
when lower(trim(substr(fecha,1,3)))="nov" then 11
when lower(trim(substr(fecha,1,3)))="dic" then 12
end) as fecha
from proceso_riesgos.ipp_nacional_plm UNION 

select DISTINCT
100*(2000+ cast(substr(fecha,5,2) as int))+
(case 
when lower(trim(substr(fecha,1,3)))="ene" then 1
when lower(trim(substr(fecha,1,3)))="feb" then 2
when lower(trim(substr(fecha,1,3)))="mar" then 3
when lower(trim(substr(fecha,1,3)))="abr" then 4
when lower(trim(substr(fecha,1,3)))="may" then 5
when lower(trim(substr(fecha,1,3)))="jun" then 6
when lower(trim(substr(fecha,1,3)))="jul" then 7
when lower(trim(substr(fecha,1,3)))="ago" then 8
when lower(trim(substr(fecha,1,3)))="sep" then 9
when lower(trim(substr(fecha,1,3)))="oct" then 10
when lower(trim(substr(fecha,1,3)))="nov" then 11
when lower(trim(substr(fecha,1,3)))="dic" then 12
end) as fecha
from proceso_riesgos.ipp_internacional_plm UNION

select distinct year(fecha)*100+month(fecha) as fecha
from  proceso_riesgos.DeMandaEnergia_plm
;


----------------Join2------------------------------------------------------------------------------------
DROP table if exists proceso_riesgos.Tabla_palma_produccion purge;
CREATE table proceso_riesgos.Tabla_palma_produccion stored as parquet as 
with indus as (
    SELECT
    fecha*100+
    (CASE
    WHEN lower(trim(meses))="ene" THEN 1
    WHEN lower(trim(meses))="feb" THEN 2
    WHEN lower(trim(meses))="mar" THEN 3
    WHEN lower(trim(meses))="abr" THEN 4
    WHEN lower(trim(meses))="may" THEN 5
    WHEN lower(trim(meses))="jun" THEN 6 
    WHEN lower(trim(meses))="jul" THEN 7 
    WHEN lower(trim(meses))="ago" THEN 8
    WHEN lower(trim(meses))="sep" THEN 9
    WHEN lower(trim(meses))="oct" THEN 10 
    WHEN lower(trim(meses))="nov" THEN 11 
    WHEN lower(trim(meses))="dic" THEN 12
    end) as fecha_ok,
    t1.*
    from proceso_riesgos.VentasAceite_Industriasec_plm as t1

),

naci as (

SELECT 
100*(2000+ cast(substr(fecha,5,2) as int))+
(case 
when lower(trim(substr(fecha,1,3)))="ene" then 1
when lower(trim(substr(fecha,1,3)))="feb" then 2
when lower(trim(substr(fecha,1,3)))="mar" then 3
when lower(trim(substr(fecha,1,3)))="abr" then 4
when lower(trim(substr(fecha,1,3)))="may" then 5
when lower(trim(substr(fecha,1,3)))="jun" then 6
when lower(trim(substr(fecha,1,3)))="jul" then 7
when lower(trim(substr(fecha,1,3)))="ago" then 8
when lower(trim(substr(fecha,1,3)))="sep" then 9
when lower(trim(substr(fecha,1,3)))="oct" then 10
when lower(trim(substr(fecha,1,3)))="nov" then 11
when lower(trim(substr(fecha,1,3)))="dic" then 12
end) as fecha,
aceite_de_palma_crudo,
aceites_vegetales_refinados as aceites_vegetales_refinados_nac,
margarina_y_preparaciones_similares as margarina_y_preparaciones_similares_nac
from proceso_riesgos.ipp_nacional_plm
),

inter as (
select
100*(2000+ cast(substr(fecha,5,2) as int))+
(case 
when lower(trim(substr(fecha,1,3)))="ene" then 1
when lower(trim(substr(fecha,1,3)))="feb" then 2
when lower(trim(substr(fecha,1,3)))="mar" then 3
when lower(trim(substr(fecha,1,3)))="abr" then 4
when lower(trim(substr(fecha,1,3)))="may" then 5
when lower(trim(substr(fecha,1,3)))="jun" then 6
when lower(trim(substr(fecha,1,3)))="jul" then 7
when lower(trim(substr(fecha,1,3)))="ago" then 8
when lower(trim(substr(fecha,1,3)))="sep" then 9
when lower(trim(substr(fecha,1,3)))="oct" then 10
when lower(trim(substr(fecha,1,3)))="nov" then 11
when lower(trim(substr(fecha,1,3)))="dic" then 12
end) as fecha,
aceites_vegetales_refinados as aceites_vegetales_refinados_inter,
margarina_y_preparaciones_similares as margarina_y_preparaciones_similares_inter
from proceso_riesgos.ipp_internacional_plm
),

energia as (

SELECT year(fecha)*100+month(fecha) as f_anio_mes, avg(decomkwh) as demanda_energia 
from proceso_riesgos.DeMandaEnergia_plm
GROUP BY 1

)


SELECT
t1.fecha as anio_mes
,t2.fecha_cru
,t2.fecha
,t2.ipc_aceites_combustibles
,t2.ipc_margarinas_y_grasas
,t2.precio_inter_petro
,t2.var_precios_petroleo
,t2.precio_inter_soya
,t2.var_precios_soya
,t2.precio_inter_aceite_crudo
,t2.var_precios_aceite_palma
,t2.precio_nacional_del_aceite_crudo
,t2.precio_biodiesel
,t2.precio_diesel
,t3.biodiesel
,t3.empresas_tradicionales
,t3.industriales_alimentos_concentrados
,t3.industriales_jaboneros
,t3.otros_industriales
,t3.industriales_de_aceites_y_grasas
,t4.clases_industriales
,t4.produccion_nominal
,t4.produccion_real
,t4.ventas_nominales
,t5.aceite_de_palma_crudo
,t5.aceites_vegetales_refinados_nac
,t5.margarina_y_preparaciones_similares_nac
,t6.aceites_vegetales_refinados_inter
,t6.margarina_y_preparaciones_similares_inter
,t7.demanda_energia 
FROM proceso.vector2_fechas_plm as t1
LEFT JOIN proceso_riesgos.Tabla_palma_precios as t2
ON t1.fecha=cast(t2.fecha_cru/100 as BIGINT)
LEFT JOIN indus as t3
ON t1.fecha=t3.fecha_ok
LEFT JOIN proceso_riesgos.IndiceProduccionEMMET_plm as t4
ON t1.fecha=t4.fecha*100+t4.mes

LEFT JOIN naci as t5
ON t1.fecha=t5.fecha
LEFT JOIN inter as t6
ON t1.fecha=t6.fecha 
LEFT JOIN energia as t7
ON t1.fecha=t7.f_anio_mes;




----------------union3------------------------------------------------------------------------------------

drop table if exists proceso_riesgos.Areas_Product_rendi_plm purge;
create table proceso_riesgos.Areas_Product_rendi_plm stored as parquet as 
with vector_cruce as (
    select distinct fecha, zonas as zona 
    from proceso_riesgos.RendimientoFrutosec_plm union
    SELECT distinct fecha, zona_nombre as zona 
    from proceso_riesgos.AreaDesarrolloProduccion_plm
),

cruce_1 as (
    select 
    t1.fecha,
    t1.zona,
    t2.rendimiento,
    t3.areaproduccion,
    t3.areadesarrollo
    from vector_cruce as t1
    left join proceso_riesgos.RendimientoFrutosec_plm as t2
    on lower(trim(t1.zona))=lower(trim(t2.zonas))
    and t1.fecha=t2.fecha
    left join proceso_riesgos.AreaDesarrolloProduccion_plm as t3
    on lower(trim(t1.zona))=lower(trim(t3.zona_nombre))
    and t1.fecha=t3.fecha
),

product as(
    SELECT 
    fecha*100+
    (CASE
    WHEN lower(trim(substr(mes,1,3)))="ene" THEN 1
    WHEN lower(trim(substr(mes,1,3)))="feb" THEN 2
    WHEN lower(trim(substr(mes,1,3)))="mar" THEN 3
    WHEN lower(trim(substr(mes,1,3)))="abr" THEN 4
    WHEN lower(trim(substr(mes,1,3)))="may" THEN 5
    WHEN lower(trim(substr(mes,1,3)))="jun" THEN 6 
    WHEN lower(trim(substr(mes,1,3)))="jul" THEN 7 
    WHEN lower(trim(substr(mes,1,3)))="ago" THEN 8
    WHEN lower(trim(substr(mes,1,3)))="sep" THEN 9
    WHEN lower(trim(substr(mes,1,3)))="oct" THEN 10 
    WHEN lower(trim(substr(mes,1,3)))="nov" THEN 11 
    WHEN lower(trim(substr(mes,1,3)))="dic" THEN 12
    end) as anio_mes,
    zonas,
    produccion
from proceso_riesgos.ProduccionAceiteZonasec_plm

)


SELECT
fecha as anio
,null as anio_mes
,zona_nombre as zona
,aceitepalma
,palmiste
,totfrutoprocesado
,ciudad_nombre
,depart_nombre
,null as  rendimiento
,null as  areaproduccion
,null as  areadesarrollo
,null as produccion
from proceso_riesgos.ProduccionFrutosec_plm UNION

select
fecha as anio
,null as anio_mes
,zona
,null as aceitepalma
,null as palmiste
,null as totfrutoprocesado
,null as ciudad_nombre
,null as depart_nombre
,rendimiento
,areaproduccion
,areadesarrollo
,null as produccion
from cruce_1 UNION

select 
null as anio
,anio_mes
,zonas as zona
,null as aceitepalma
,null as palmiste
,null as totfrutoprocesado
,null as ciudad_nombre
,null as depart_nombre
,null as rendimiento
,null as areaproduccion
,null as areadesarrollo
,produccion
from product
;


------------Trasnpone Proyeccion4----------------------------------

drop table if exists proceso.proyeccion_precios_plm purge;
create table proceso.proyeccion_precios_plm stored as parquet as 
with anio1 as (
SELECT 
productos_internacionales
,unidades
,2019 as anio
,`2019` as precio_proyectado_producto
from proceso_riesgos.ProyeccionPreciosInternacional_plm
),
anio2 as (
SELECT 
productos_internacionales
,unidades
,2020 as anio
,`2020` as precio_proyectado_producto
from proceso_riesgos.ProyeccionPreciosInternacional_plm
),
anio3 as (
SELECT 
productos_internacionales
,unidades
,2021 as anio
,`2021` as precio_proyectado_producto
from proceso_riesgos.ProyeccionPreciosInternacional_plm
),
anio4 as (
SELECT 
productos_internacionales
,unidades
,2022 as anio
,`2022` as precio_proyectado_producto
from proceso_riesgos.ProyeccionPreciosInternacional_plm
),
anio5 as (
SELECT 
productos_internacionales
,unidades
,2023 as anio
,`2023` as precio_proyectado_producto
from proceso_riesgos.ProyeccionPreciosInternacional_plm
),
anio6 as (
SELECT 
productos_internacionales
,unidades
,2024 as anio
,`2024` as precio_proyectado_producto
from proceso_riesgos.ProyeccionPreciosInternacional_plm
)

select * from anio1 union
select * from anio2 union
select * from anio3 union
select * from anio4 union
select * from anio5 union
select * from anio6;

----------------union5------------------------------------------------------------------------------------
drop table if exists proceso_riesgos.Areas_Product_rendi_plm2 purge;
create table proceso_riesgos.Areas_Product_rendi_plm2 stored as parquet as 

select 
anio
,anio_mes
,zona
,aceitepalma
,palmiste
,totfrutoprocesado
,ciudad_nombre
,depart_nombre
,rendimiento
,areaproduccion
,areadesarrollo
,produccion
,null as productos_internacionales
,null as unidades
,null as precio_proyectado_producto
from proceso_riesgos.Areas_Product_rendi_plm UNION

select
anio
,null as anio_mes
,null as zona
,null as aceitepalma
,null as palmiste
,null as totfrutoprocesado
,null as ciudad_nombre
,null as depart_nombre
,null as rendimiento
,null as areaproduccion
,null as areadesarrollo
,null as produccion
,productos_internacionales
,unidades
,precio_proyectado_producto
from proceso.proyeccion_precios_plm;

----------------union final 6------------------------------------------------------------------------------------

drop table if exists proceso_riesgos.tabla_palma_cera purge;
create table proceso_riesgos.tabla_palma_cera stored as parquet as 

SELECT 
year(now()) as ingestion_year
,month(now()) as ingestion_month
,day(now()) as ingestion_day
,cast(anio_mes/100 as bigint) as anio
,anio_mes
,fecha
,ipc_aceites_combustibles
,ipc_margarinas_y_grasas
,precio_inter_petro
,var_precios_petroleo
,precio_inter_soya
,var_precios_soya
,precio_inter_aceite_crudo
,var_precios_aceite_palma
,precio_nacional_del_aceite_crudo
,precio_biodiesel
,precio_diesel
,biodiesel as venta_biodiesel
,empresas_tradicionales as venta_empresas_tradicionales
,industriales_alimentos_concentrados as venta_alimentos_conce
,industriales_jaboneros as venta_jaboneros
,otros_industriales as venta_otras_industrias
,industriales_de_aceites_y_grasas as venta_aceites_grasas
,clases_industriales as clase_industrial
,produccion_nominal
,produccion_real
,ventas_nominales
,null as productos_internacionales
,null as unidad
,null as precio_proyectado_producto
,null as zona
,null as produccion
,aceite_de_palma_crudo
,aceites_vegetales_refinados_nac
,margarina_y_preparaciones_similares_nac
,aceites_vegetales_refinados_inter
,margarina_y_preparaciones_similares_inter
,demanda_energia
,null as aceitepalma
,null as palmiste
,null as totfrutoprocesado
,null as ciudad_nombre
,null as depart_nombre
,null as rendimiento
,null as areaproduccion
,null as areadesarrollo
,year(now()) as year
from proceso_riesgos.Tabla_palma_produccion UNION

SELECT
year(now()) as ingestion_year
,month(now()) as ingestion_month
,day(now()) as ingestion_day

,case 
when anio is not null then cast(anio as bigint)
when anio_mes is not null then cast(anio_mes/100 as bigint)
else null end as anio

,anio_mes
,null as fecha
,null as ipc_aceites_combustibles
,null as ipc_margarinas_y_grasas
,null as precio_inter_petro
,null as var_precios_petroleo
,null as precio_inter_soya
,null as var_precios_soya
,null as precio_inter_aceite_crudo
,null as var_precios_aceite_palma
,null as precio_nacional_del_aceite_crudo
,null as precio_biodiesel
,null as precio_diesel
,null as venta_biodiesel
,null as venta_empresas_tradicionales
,null as venta_alimentos_conce
,null as venta_jaboneros
,null as venta_otras_industrias
,null as venta_aceites_grasas
,null as clase_industrial
,null as produccion_nominal
,null as produccion_real
,null as ventas_nominales
,productos_internacionales
,unidades as unidad
,precio_proyectado_producto 
,zona
,produccion
,null as aceite_de_palma_crudo
,null as aceites_vegetales_refinados_nac
,null as margarina_y_preparaciones_similares_nac
,null as aceites_vegetales_refinados_inter
,null as margarina_y_preparaciones_similares_inter
,null as demanda_energia
,aceitepalma
,palmiste
,totfrutoprocesado
,ciudad_nombre
,depart_nombre
,rendimiento
,areaproduccion
,areadesarrollo
,year(now()) as year
from proceso_riesgos.Areas_Product_rendi_plm2;


----------------formato final 7------------------------------------------------------------------------------------

drop table if exists proceso_riesgos.tbl_ficha_sector_palma purge;
create table proceso_riesgos.tbl_ficha_sector_palma stored as parquet as 
SELECT
cast(ingestion_year as smallint) as ingestion_year
,cast(ingestion_month as smallint) as ingestion_month
,cast(ingestion_day as smallint) as ingestion_day
,cast(anio as bigint) as anio
,to_timestamp(concat(cast(anio as STRING),"0101"),"yyyyMMdd") as anio_timestamp
,cast(anio_mes as double) as anio_mes
,to_timestamp(concat(cast(anio_mes as STRING),"01"),"yyyyMMdd") as anio_mes_timestamp
,cast(fecha as timestamp) as fecha
,cast(ipc_aceites_combustibles as double) as ipc_aceites_combustibles
,cast(ipc_margarinas_y_grasas as double) as ipc_margarinas_y_grasas
,cast(precio_inter_petro as double) as precio_inter_petro
,cast(var_precios_petroleo as string) as var_precios_petroleo
,cast(precio_inter_soya as double) as precio_inter_soya
,cast(var_precios_soya as string) as var_precios_soya
,cast(precio_inter_aceite_crudo as double) as precio_inter_aceite_crudo
,cast(var_precios_aceite_palma as string) as var_precios_aceite_palma
,cast(precio_nacional_del_aceite_crudo as double) as precio_nacional_del_aceite_crudo
,cast(precio_biodiesel as double) as precio_biodiesel
,cast(precio_diesel as double) as precio_diesel
,cast(venta_biodiesel as double) as venta_biodiesel
,cast(venta_empresas_tradicionales as double) as venta_empresas_tradicionales
,cast(venta_alimentos_conce as double) as venta_alimentos_conce
,cast(venta_jaboneros as double) as venta_jaboneros
,cast(venta_otras_industrias as double) as venta_otras_industrias
,cast(venta_aceites_grasas as double) as venta_aceites_grasas
,cast(clase_industrial as string) as clase_industrial
,cast(produccion_nominal as double) as produccion_nominal
,cast(produccion_real as double) as produccion_real
,cast(ventas_nominales as double) as ventas_nominales
,cast(productos_internacionales as string) as productos_internacionales
,cast(unidad as string) as unidad
,cast(precio_proyectado_producto as double) as precio_proyectado_producto
,cast(zona as string) as zona
,cast(produccion as double) as produccion
,cast(aceite_de_palma_crudo as double) as aceite_de_palma_crudo
,cast(aceites_vegetales_refinados_nac as double) as aceites_vegetales_refinados_nac
,cast(margarina_y_preparaciones_similares_nac as double) as margarina_y_preparaciones_similares_nac
,cast(aceites_vegetales_refinados_inter as double) as aceites_vegetales_refinados_inter
,cast(margarina_y_preparaciones_similares_inter as double) as margarina_y_preparaciones_similares_inter
,cast(demanda_energia as double) as demanda_energia
,cast(aceitepalma as bigint) as aceitepalma
,cast(palmiste as bigint) as palmiste
,cast(totfrutoprocesado as bigint) as totfrutoprocesado
,cast(ciudad_nombre as string) as ciudad_nombre
,cast(depart_nombre as string) as depart_nombre
,cast(rendimiento as double) as rendimiento
,cast(areaproduccion as bigint) as areaproduccion
,cast(areadesarrollo as bigint) as areadesarrollo
,cast(year as smallint) as year
from proceso_riesgos.tabla_palma_cera ;