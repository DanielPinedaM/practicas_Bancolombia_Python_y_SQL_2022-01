# -*- coding: utf-8 -*-
"""

@author: cagalind, vaosorio 

Controles a Resultados Asignación de Garantías con Seguridades 
y escenarios de simulación

"""

import pyodbc
import pandas as pd
import unittest
from helper.helper import Helper
from sparky_bc import Sparky
import os
import getpass

usr=getpass.getuser()
pass_usr=os.environ['PWD']

sp = Sparky(usr,"IMPALA_PROD", False, pass_usr)
hp = sp.helper

ResultZone = 'resultados_riesgos'

## SQL que calcula las fechas del reporte:hgjhgjhg





hp.ejecutar_archivo('../../1_ingestiones.sql')

asigna_df_sd = hp.obtener_dataframe('select * from proceso.fechas_saldos')
fecha_corte = asigna_df_sd.iloc[0]['fecha_corte'] # Es equivalente a fecha_corte = asigna_df_sd.loc[0,'fecha_corte']
fecha_corte_ant = asigna_df_sd.iloc[0]['fecha_corte_ant']
asigna_df_cn = hp.obtener_dataframe('select * from proceso.fechas_cierre')
fecha_cierre = asigna_df_cn.iloc[0]['fecha_corte']


# fecha_corte = '2022-02-02'
# fecha_corte_ant = '2022-02-01'
# fecha_cierre = '2022-01-31'



class pre_asig_garant(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        CONN_STR = "DSN=impala_prod"
        cls.conn = pyodbc.connect( CONN_STR, autocommit = True )

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_ingestion_sd_actual(self):
        """Valida que exista la ingestión de la fecha actual
        """
        query = f"""
                select 
                count(*) as cuenta
                from {ResultZone}.saldo_diario t1
                where t1.year=year(cast('{fecha_corte}' as timestamp)) and t1.month=month(cast('{fecha_corte}' as timestamp)) and t1.ingestion_day = day(cast('{fecha_corte}' as timestamp))
                """
        df = pd.read_sql(query, self.conn)
        self.assertGreater(df.iloc[0, 0], 0)
    
    def test_duplicados_sd_actual(self):
        """Valida que no existan Llaves duplicadas diferentes a las validadas
        """
        query = f"""
WITH a AS
  ( SELECT concat(trim(t1.obl17), trim(t1.apli), trim(if(t1.moneda IS NULL,'0',cast(t1.moneda AS string))), if(cast(cast(t1.num_doc AS bigint) AS string) IS NULL,'0',cast(cast(t1.num_doc AS bigint) AS string)), trim(if(t1.tipo_doc IS NULL,'0',t1.tipo_doc))) AS llave1 ,
           count(*) AS cuenta
   FROM resultados_riesgos.saldo_diario t1
   WHERE t1.year=year(cast('{fecha_corte}' as timestamp)) and t1.month=month(cast('{fecha_corte}' as timestamp)) and t1.ingestion_day = day(cast('{fecha_corte}' as timestamp))
   GROUP BY 1 )
SELECT COUNT(*)
FROM a
WHERE cuenta <> 1
  AND llave1 NOT IN( /*llaves identificadas y validadas que no afectan el reporte*/ '00000000000000100109009338363',
                                                                                    '00000000002020082Lea08909039383',
                                                                                    '00000096181021188L0326491371',
                                                                                    '00000000000274379Lea09001664743'
                                                                                    ,'00000000000279303Lea010265645921')
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 0)
    
    def test_duplicados_sd_anterior(self):
        """Valida que no existan Llaves duplicadas diferentes a las validadas
        """
        query = f"""
                with a as (
                select 
                    concat(trim(t1.obl17), trim(t1.apli), trim(if(t1.moneda is null,'0',cast(t1.moneda as string))), if(cast(cast(t1.num_doc as bigint) as string) is null,'0',cast(cast(t1.num_doc as bigint) as string)), trim(if(t1.tipo_doc is null,'0',t1.tipo_doc))) as llave1
                    , count(*) as cuenta
                from {ResultZone}.saldo_diario t1
                where t1.year=year(cast('{fecha_corte}' as timestamp)) and t1.month=month(cast('{fecha_corte}' as timestamp)) and t1.ingestion_day = day(cast('{fecha_corte}' as timestamp))
                group by 1
                )
                select count(*)
                from a where cuenta <> 1 and llave1 not in( /*llaves identificadas y validadas que no afectan el reporte*/
                    '00000000000000100109009338363' 
                    ,'00000000002020082Lea08909039383', '00000096181021188L0326491371',
                                                                                    '00000000000274379Lea09001664743','00000000000279303Lea010265645921')
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 0)
    
    def test_duplicados_ceniegarc(self):
        """Valida que no existan Llaves duplicadas diferentes a las validadas
        """
        query = f"""
                with a as (
                select 
                    concat(trim(t1.obl341), trim(t1.apl), trim(cast(t1.md3411 as string)), if(cast(cast(t1.id as bigint) as string) is null,'0',cast(cast(t1.id as bigint) as string)), trim(if(cast(t1.tid as string) is null,'0',cast(t1.tid as string)))) as llave1
                    , count(*) as cuenta
                from {ResultZone}.ceniegarc_lz t1
                where t1.year=year(cast('{fecha_cierre}' as timestamp)) and t1.ingestion_month=month(cast('{fecha_cierre}' as timestamp))
                group by 1
                )
                select count(*)
                from a where cuenta <> 1 and llave1 not in( /*llaves identificadas y validadas que no afectan el reporte*/
                    '00000000000000100109009338363' 
                    ,'00000000002020082Lea08909039383','00000096181021188L0326491371',
                                                                                    '00000000000274379Lea09001664743','00000000000279303Lea010265645921')
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 0)

if __name__ == '__main__':
    unittest.main()
    print("Pruebas exitosas. La base cumple con los controles de calidad establecidos.")