from conexion_sap import connect_to_sap
from db.conexion_db import crear_conexion, cerrar_conexion
from db.querys_db import insert_data, consult_data

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account

import pandas as pd
import numpy as np
import subprocess
import datetime
import sys
import time
import os
import json


def main():

    def get_db_config(path="config/credenciales_sap.json"):
        """Carga la configuración de la base de datos desde un archivo JSON."""
        with open(path, "r") as config_file:
            return json.load(config_file)
        
    config = get_db_config()    
    connection_name = config["sap_connection_name"]
    user_sap = config["sap_user"]
    pwa_sap = config["sap_password"]

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    KEY = r'C:\Users\CGM\Projects\Generar_OC_Automatico\config\key_cloud.json'
    # Escribe aquí el ID de tu documento:
    SPREADSHEET_ID = '1McMSLQAGpRq12vME_3CBuyKny64B31-0IeJuy6Jm_fQ'

    creds = None
    creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    # Llamada a la api - Extraccion C154 - C152
    result_lurin = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Lurin - La Victoria!A:G').execute()
    # Extraemos values del resultado
    values_lurin = result_lurin.get('values',[])

    # Llamada a la api - Extraccion C200
    result_piura = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Piura!A:G').execute()
    # Extraemos values del resultado
    values_piura = result_piura.get('values',[])

    # Llamada a la api - Extraccion C040 - C080
    result_arq_cus = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Arequipa - Cusco!A:G').execute()
    # Extraemos values del resultado
    values_arq_cus = result_arq_cus.get('values',[])

    precios_lurin = pd.DataFrame(values_lurin)
    precios_piura = pd.DataFrame(values_piura)
    precios_arq_cus = pd.DataFrame(values_arq_cus)

    precios_lurin = precios_lurin.rename(columns=precios_lurin.iloc[1])
    precios_lurin = precios_lurin.drop(precios_lurin.index[0:2])

    precios_piura = precios_piura.rename(columns=precios_piura.iloc[1])
    precios_piura = precios_piura.drop(precios_piura.index[0:2])

    precios_arq_cus = precios_arq_cus.rename(columns=precios_arq_cus.iloc[1])
    precios_arq_cus = precios_arq_cus.drop(precios_arq_cus.index[0:2])

    lista_precios = pd.concat([precios_lurin, precios_piura, precios_arq_cus], ignore_index=True)

    lista_precios['identificador'] = lista_precios['Material'] + lista_precios['Centro']
    
    col_lista_precios = lista_precios[['identificador','Cod_Proveedor','Precio Unitario']].copy()
    col_lista_precios['Precio Unitario'] = col_lista_precios['Precio Unitario'].astype(float)

    connection = crear_conexion()
    por_comprar = consult_data(connection, "SELECT material,reserva_centro,q_pendiente FROM tbl_abastecimiento where ate_accion = 'MERCADERIA' and estado_compra='Por_Pedir'")
    cerrar_conexion(connection)

    df_por_comprar = pd.DataFrame(por_comprar)
    df_por_comprar['q_pendiente'] = df_por_comprar['q_pendiente'].astype(float)

    df_por_comprar_agrupado = df_por_comprar.groupby(['material','reserva_centro']).agg({'q_pendiente':'sum'}).reset_index()

    df_por_comprar_agrupado['identificador'] = df_por_comprar_agrupado['material'] + df_por_comprar_agrupado['reserva_centro']

    data_por_comprar = pd.merge(df_por_comprar_agrupado, col_lista_precios, on='identificador', how='inner')

    data_prueba = data_por_comprar[['material','q_pendiente','reserva_centro','Cod_Proveedor','Precio Unitario']].copy()

    session = connect_to_sap(connection_name=connection_name,user_sap=user_sap,pwa_sap=pwa_sap)
    if session:
        session.findById("wnd[0]").maximize()
        session.findById("wnd[0]/tbar[0]/okcd").text = "ME21N"
        session.findById("wnd[0]").sendVKey(0)
        grupos = data_prueba.groupby(['reserva_centro','Cod_Proveedor'])
        scroll = 1
        fecha = (datetime.datetime.now() + datetime.timedelta(days=5)).strftime('%d.%m.%Y')

        for (centro, proveedor), pedidos_grupo in grupos:

            #print(f'Procesando{centro}, {proveedor}, cantidad sku {len(pedidos_grupo)}')
            
            if proveedor == '1200000011' and centro == 'C154':
                
                session.findById("wnd[0]/usr/subSUB0:SAPLMEGUI:0013/subSUB0:SAPLMEGUI:0030/subSUB1:SAPLMEGUI:1105/cmbMEPO_TOPLINE-BSART").key = "ZNAC"
                session.findById("wnd[0]/usr/subSUB0:SAPLMEGUI:0013/subSUB0:SAPLMEGUI:0030/subSUB1:SAPLMEGUI:1105/ctxtMEPO_TOPLINE-SUPERFIELD").text = proveedor
                session.findById("wnd[0]/usr/subSUB0:SAPLMEGUI:0013/subSUB1:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:1102/tabsHEADER_DETAIL/tabpTABHDT9/ssubTABSTRIPCONTROL2SUB:SAPLMEGUI:1221/ctxtMEPO1222-EKORG").text = "PE02"
                session.findById("wnd[0]/usr/subSUB0:SAPLMEGUI:0013/subSUB1:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:1102/tabsHEADER_DETAIL/tabpTABHDT9/ssubTABSTRIPCONTROL2SUB:SAPLMEGUI:1221/ctxtMEPO1222-EKGRP").text = "C32"
                session.findById("wnd[0]/usr/subSUB0:SAPLMEGUI:0013/subSUB1:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:1102/tabsHEADER_DETAIL/tabpTABHDT9/ssubTABSTRIPCONTROL2SUB:SAPLMEGUI:1221/ctxtMEPO1222-BUKRS").text = "PE02"
                session.findById("wnd[0]").sendVKey(0)

                pedidos_grupo.reset_index(drop=True, inplace=True)
                
                print(pedidos_grupo)

                for i, row in pedidos_grupo.iterrows():

                    ventana_actual = "0013" if i == 0 else "0010"
                    indice = "0" if i == 0 else "1"
                    #scroll = 1 if i <= 1 else scroll +1
                     
                    path_base = f"wnd[0]/usr/subSUB0:SAPLMEGUI:{ventana_actual}/subSUB2:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:1211/tblSAPLMEGUITC_1211"
                    
                    session.findById(f"{path_base}/ctxtMEPO1211-EMATN[4,{indice}]").text = row['material']
                    session.findById(f"{path_base}/txtMEPO1211-MENGE[6,{indice}]").text = row['q_pendiente']
                    session.findById(f"{path_base}/ctxtMEPO1211-EEIND[9,{indice}]").text = '15.04.2025'
                    session.findById(f"{path_base}/txtMEPO1211-NETPR[10,{indice}]").text = row['Precio Unitario']
                    session.findById(f"{path_base}/ctxtMEPO1211-NAME1[14,{indice}]").text = row['reserva_centro']
                    session.findById(f"{path_base}/ctxtMEPO1211-LGOBE[15,{indice}]").text = "0002"
                    session.findById(f"{path_base}/ctxtMEPO1211-AFNAM[16,{indice}]").text = "C012"
                    session.findById(f"{path_base}/ctxtMEPO1211-AFNAM[16,{indice}]").setFocus()
                    session.findById(f"{path_base}/ctxtMEPO1211-AFNAM[16,{indice}]").caretPosition = 4 
                    session.findById("wnd[0]").sendVKey(0)  
                    session.findById(f"wnd[0]/usr/subSUB0:SAPLMEGUI:0010/subSUB2:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:1211/tblSAPLMEGUITC_1211").verticalScrollbar.position = scroll
                    print('Primera vuelta',i, scroll,indice)
                    scroll += 1
              
    else:
        print(f"No se pudo establecer conexión con {connection_name,user_sap,pwa_sap}.")
        session.findById("wnd[0]").close()

if __name__ == "__main__":
    main()
    