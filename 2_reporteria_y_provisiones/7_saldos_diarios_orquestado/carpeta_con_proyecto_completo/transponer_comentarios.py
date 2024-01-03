"""
Proyecto - Automatizacion de comentarios de saldos diarios orquestado



1) Todo esto se hace en Python

2) Las consultas SQL las ejecutas desde el Python

3) Leer los Excel q estan en la carpeta ...\excel\1_insumo,
   se leen las hojas principales_vencidos y principales_recuperados

4) Hacer codigo para leer N Excel de ...\excel\1_insumo

5) Crear una nueva columna llamada tipo
   con el nombre de la hoja de Excel de la q se extrajo el dato

6) Transponer la tabla
   haciendo coincidir en las columnas
   los nombres (num_doc), fechas y comentarios

7) Al hacer lo anterior 6) los nombres (num_doc) se van a repetir

8) cuando se vuelva a ejecutar el codigo 
   se tienen q conservar TODOS los registros 
   (fechas y comentarios viejos y nuevos)

9) El formato de la fecha tiene q ser dia/mes/año

10) Crear un Excel en ...\excel\2_respuesta

11) Hacer codigo para leer N Excel de ...\excel\2_respuesta

12) Verificar q se haya transpuesto bien la tabla

13) Subir tabla a LZ en proceso_generadores

14) Des-comentar lo que hay en 5_reportes.sql

15) Ejecutar 5_reportes.sql
    esto NO hay q programarlo 
    solo es para verificar q funcione

16) Verificar q funcione 5_reportes.sql
"""

########################################################################
#%%

import pandas as pd

########################################################################
#%%
