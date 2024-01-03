# -*- coding: utf-8 -*-
"""

@author: vaosorio, dulondon  

Controles a resultados del reporte "Saldos Diarios"

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

processZone = 'proceso'
processZone2 = 'proceso_riesgos'
ResultZone = 'resultados_riesgos'

### El archivo 1_ingestiones.sql para calcular las fechas a continuación, se ejecuta en el programa unitest/pre-test/tes_pre_SD.py:
asigna_df_sd = hp.obtener_dataframe('select * from proceso.fechas_saldos')
fecha_corte = asigna_df_sd.iloc[0]['fecha_corte'] # Es equivalente a fecha_corte = asigna_df_sd.loc[0,'fecha_corte']
fecha_corte_ant = asigna_df_sd.iloc[0]['fecha_corte_ant']
fecha_corte_nmb = asigna_df_sd.iloc[0]['fecha_corte_nmb']

asigna_df_cn = hp.obtener_dataframe('select * from proceso.fechas_cierre')
fecha_cierre = asigna_df_cn.iloc[0]['fecha_corte']

# fecha_corte = '2021-06-27'
# fecha_corte_nmb = '20210627'
# fecha_corte_ant = '2021-06-24'
# fecha_cierre = '2021-05-31' #'2021-03-31'
# fecha_trm = '2021-05-01'

class asig_garant(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        CONN_STR = "DSN=IMPALA_PROD"
        cls.conn = pyodbc.connect( CONN_STR, autocommit = True )

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_actualiza_apl_1 (self):
        """Valida que la información del aplicativo 1 si esté actualizada respecto al día anterior
        """
        query = f"""
                select actualizados 
                from proceso.valida_saldos_var
                where apli = '1'
                """
        df = pd.read_sql(query, self.conn)
        self.assertGreater(df.iloc[0, 0], 0)
    
    def test_var_apl_1 (self):
        """Valida que la variación de saldo del aplicativo 1, respecto al día anterior, si esté en los rangos esperados
        """
        query = f"""
                select en_rango_var 
                from proceso.valida_saldos_var
                where apli = '1'
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 1)
    
    def test_actualiza_apl_4 (self):
        """Valida que la información del aplicativo 4 si esté actualizada respecto al día anterior
        """
        query = f"""
                select actualizados 
                from proceso.valida_saldos_var
                where apli = '4'
                """
        df = pd.read_sql(query, self.conn)
        self.assertGreater(df.iloc[0, 0], 0)
    
    def test_var_apl_4 (self):
        """Valida que la variación de saldo del aplicativo 4, respecto al día anterior, si esté en los rangos esperados
        """
        query = f"""
                select en_rango_var 
                from proceso.valida_saldos_var
                where apli = '4'
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 1)
    
    def test_actualiza_apl_7 (self):
        """Valida que la información del aplicativo 7 si esté actualizada respecto al día anterior
        """
        query = f"""
                select actualizados 
                from proceso.valida_saldos_var
                where apli = '7'
                """
        df = pd.read_sql(query, self.conn)
        self.assertGreater(df.iloc[0, 0], 0)
    
    def test_var_apl_7 (self):
        """Valida que la variación de saldo del aplicativo 7, respecto al día anterior, si esté en los rangos esperados
        """
        query = f"""
                select en_rango_var 
                from proceso.valida_saldos_var
                where apli = '7'
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 1)
    
    def test_actualiza_apl_9 (self):
        """Valida que la información del aplicativo 9 si esté actualizada respecto al día anterior
        """
        query = f"""
                select actualizados 
                from proceso.valida_saldos_var
                where apli = '9'
                """
        df = pd.read_sql(query, self.conn)
        self.assertGreater(df.iloc[0, 0], 0)
    
    def test_var_apl_9 (self):
        """Valida que la variación de saldo del aplicativo 9, respecto al día anterior, si esté en los rangos esperados
        """
        query = f"""
                select en_rango_var 
                from proceso.valida_saldos_var
                where apli = '9'
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 1)

    def test_actualiza_apl_D (self):
        """Valida que la información del aplicativo D si esté actualizada respecto al día anterior
        """
        query = f"""
                select actualizados 
                from proceso.valida_saldos_var
                where apli = 'D'
                """
        df = pd.read_sql(query, self.conn)
        self.assertGreater(df.iloc[0, 0], 0)
    
    def test_var_apl_D (self):
        """Valida que la variación de saldo del aplicativo D, respecto al día anterior, si esté en los rangos esperados
        """
        query = f"""
                select en_rango_var 
                from proceso.valida_saldos_var
                where apli = 'D'
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 1)
    
    def test_actualiza_apl_K (self):
        """Valida que la información del aplicativo K si esté actualizada respecto al día anterior
        """
        query = f"""
                select actualizados 
                from proceso.valida_saldos_var
                where apli = 'K'
                """
        df = pd.read_sql(query, self.conn)
        self.assertGreater(df.iloc[0, 0], 0)
    
    def test_var_apl_K (self):
        """Valida que la variación de saldo del aplicativo K, respecto al día anterior, si esté en los rangos esperados
        """
        query = f"""
                select en_rango_var 
                from proceso.valida_saldos_var
                where apli = 'K'
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 1)
    
    def test_actualiza_apl_L (self):
        """Valida que la información del aplicativo L si esté actualizada respecto al día anterior
        """
        query = f"""
                select actualizados 
                from proceso.valida_saldos_var
                where apli = 'L'
                """
        df = pd.read_sql(query, self.conn)
        self.assertGreater(df.iloc[0, 0], 0)
    
    def test_var_apl_L (self):
        """Valida que la variación de saldo del aplicativo L, respecto al día anterior, si esté en los rangos esperados
        """
        query = f"""
                select en_rango_var 
                from proceso.valida_saldos_var
                where apli = 'L'
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 1)
    
    def test_actualiza_apl_3 (self):
        """Valida que la información del aplicativo 3 si esté actualizada respecto al día anterior
        """
        query = f"""
                select actualizados 
                from proceso.valida_saldos_var
                where apli = 'Lea'
                """
        df = pd.read_sql(query, self.conn)
        self.assertGreater(df.iloc[0, 0], 0)
    
    def test_var_apl_3 (self):
        """Valida que la variación de saldo del aplicativo 3, respecto al día anterior, si esté en los rangos esperados
        """
        query = f"""
                select en_rango_var 
                from proceso.valida_saldos_var
                where apli = 'Lea'
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 1)
    
    def test_actualiza_apl_M (self):
        """Valida que la información del aplicativo M si esté actualizada respecto al día anterior
        """
        query = f"""
                select actualizados 
                from proceso.valida_saldos_var
                where apli = 'M'
                """
        df = pd.read_sql(query, self.conn)
        self.assertGreater(df.iloc[0, 0], 0)
    
    def test_var_apl_M (self):
        """Valida que la variación de saldo del aplicativo M, respecto al día anterior, si esté en los rangos esperados
        """
        query = f"""
                select en_rango_var 
                from proceso.valida_saldos_var
                where apli = 'M'
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 1)
    
    def test_actualiza_apl_V (self):
        """Valida que la información del aplicativo V si esté actualizada respecto al día anterior
        """
        query = f"""
                select actualizados 
                from proceso.valida_saldos_var
                where apli = 'V'
                """
        df = pd.read_sql(query, self.conn)
        self.assertGreater(df.iloc[0, 0], 0)
    
    def test_var_apl_V (self):
        """Valida que la variación de saldo del aplicativo V, respecto al día anterior, si esté en los rangos esperados
        """
        query = f"""
                select en_rango_var 
                from proceso.valida_saldos_var
                where apli = 'V'
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 1)

    def test_duplicados(self):
        """Valida que no existan llaves duplicadas en el reporte
        """
        query = f"""
            with a as (
            select llave1, count(*) as cuenta
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            group by 1)

            select count(*) from a
            where cuenta <> 1
                """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(df.iloc[0, 0], 0)

    def test_var_anterior_saldo_banca_ccio(self):
        """Valida que la variación del saldo con el día anterior, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Comercio, Manufactura, Agro y Bienes de Consumo'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.005)
    
    def test_var_anterior_saldo_banca_recursos_nat(self):
        """Valida que la variación del saldo con el día anterior, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Infraestructura y Recursos Naturales'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.005)
    
    def test_var_anterior_saldo_banca_grandes_corp(self):
        """Valida que la variación del saldo con el día anterior, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Grandes Corporativos'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.005)
    
    def test_var_anterior_saldo_banca_gob_ssfros(self):
        """Valida que la variación del saldo con el día anterior, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Gobierno, Servicios Financieros, Salud y Educación'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.005)
    
    def test_var_anterior_saldo_banca_constructor(self):
        """Valida que la variación del saldo con el día anterior, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Inmobiliario y Constructor'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.005)
    
    def test_var_anterior_saldo_banca_otros_terr(self):
        """Valida que la variación del saldo con el día anterior, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Otros Territorios'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.01)
    
    def test_var_anterior_saldo_banca_corr_otros(self):
        """Valida que la variación del saldo con el día anterior, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Corresponsales y Otros'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.005)
    
    def test_var_anterior_saldo_segm_Empresarial(self):
        """Valida que la variación del saldo con el día anterior, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where desc_segm_rpte = 'EMPRESARIAL'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.005)
    
    def test_var_anterior_saldo_segm_PYMES(self):
        """Valida que la variación del saldo con el día anterior, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where desc_segm_rpte = 'PYMES'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.005)
    
    def test_var_anterior_saldo_segm_NeI(self):
        """Valida que la variación del saldo con el día anterior, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where desc_segm_rpte = 'NeI'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.005)
    
    def test_var_anterior_saldo_segm_GOBIERNO_RED(self):
        """Valida que la variación del saldo con el día anterior, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where desc_segm_rpte = 'GOBIERNO DE RED'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.005)
    
    def test_var_anterior_saldo_pdto_consumo(self):
        """Valida que la variación del saldo con el día anterior, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where producto_agr = 'Consumo'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.005)
    
    def test_var_anterior_saldo_pdto_sln_Inmobiliaria(self):
        """Valida que la variación del saldo con el día anterior, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where producto_agr = 'Solucion Inmobiliaria'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.005)
    
    def test_var_anterior_saldo_pdto_ccial_otros(self):
        """Valida que la variación del saldo con el día anterior, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where producto_agr = 'Comercial y otros'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.02)

    def test_var_cierre_saldo_banca_ccio(self):
        """Valida que la variación del saldo con el día cierre, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Comercio, Manufactura, Agro y Bienes de Consumo'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.05)
    
    def test_var_cierre_saldo_banca_recursos_nat(self):
        """Valida que la variación del saldo con el día cierre, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Infraestructura y Recursos Naturales'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.07)
    
    def test_var_cierre_saldo_banca_grandes_corp(self):
        """Valida que la variación del saldo con el día cierre, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Grandes Corporativos'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.01)
    
    def test_var_cierre_saldo_banca_gob_ssfros(self):
        """Valida que la variación del saldo con el día cierre, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Gobierno, Servicios Financieros, Salud y Educación'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.2)
    
    def test_var_cierre_saldo_banca_constructor(self):
        """Valida que la variación del saldo con el día cierre, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Inmobiliario y Constructor'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.01)
    
    def test_var_cierre_saldo_banca_otros_terr(self):
        """Valida que la variación del saldo con el día cierre, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Otros Territorios'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.08)
    
    def test_var_cierre_saldo_banca_corr_otros(self):
        """Valida que la variación del saldo con el día cierre, para esta Banca, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where vicepresidencia = 'Corresponsales y Otros'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 1.1)
    
    def test_var_cierre_saldo_segm_Empresarial(self):
        """Valida que la variación del saldo con el día cierre, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where desc_segm_rpte = 'EMPRESARIAL'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.01)
    
    def test_var_cierre_saldo_segm_PYMES(self):
        """Valida que la variación del saldo con el día cierre, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where desc_segm_rpte = 'PYMES'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.01)
    
    def test_var_cierre_saldo_segm_NeI(self):
        """Valida que la variación del saldo con el día cierre, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where desc_segm_rpte = 'NeI'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.01)
    
    def test_var_cierre_saldo_segm_GOBIERNO_RED(self):
        """Valida que la variación del saldo con el día cierre, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where desc_segm_rpte = 'GOBIERNO DE RED'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.01)
    
    def test_var_cierre_saldo_pdto_consumo(self):
        """Valida que la variación del saldo con el día cierre, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where producto_agr = 'Consumo'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.02)
    
    def test_var_cierre_saldo_pdto_sln_Inmobiliaria(self):
        """Valida que la variación del saldo con el día cierre, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where producto_agr = 'Solucion Inmobiliaria'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.01)
    
    def test_var_cierre_saldo_pdto_ccial_otros(self):
        """Valida que la variación del saldo con el día cierre, para este Segmento, se encuentre en niveles aceptables
        """
        query = f"""
            select abs(sum(var_sld_cap_cierre)/sum(sld_cap_act))
            from {processZone2}.saldos_diarios_new_{fecha_corte_nmb}
            where producto_agr = 'Comercial y otros'
                """
        df = pd.read_sql(query, self.conn)
        self.assertLessEqual(df.iloc[0, 0], 0.1)

if __name__ == '__main__':
    unittest.main()
    print("Pruebas exitosas. La base cumple con los controles de calidad establecidos.")