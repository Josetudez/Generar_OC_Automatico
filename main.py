from conexion_sap import connect_to_sap

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

    ruta_data= r'C:\Users\CGM\Projects\Generar_OC_Automatico\data_prueba.xlsx'

    

    data_prueba = pd.read_excel(ruta_data,header=0,dtype=str)

    session = connect_to_sap(connection_name=connection_name,user_sap=user_sap,pwa_sap=pwa_sap)
    if session:
        session.findById("wnd[0]").maximize()
        session.findById("wnd[0]/tbar[0]/okcd").text = "ME21N"
        session.findById("wnd[0]").sendVKey(0)
        grupos = data_prueba.groupby(['centro','cod_proveedor'])
        scroll = 1

        for (centro, proveedor), pedidos_grupo in grupos:
            session.findById("wnd[0]/usr/subSUB0:SAPLMEGUI:0013/subSUB0:SAPLMEGUI:0030/subSUB1:SAPLMEGUI:1105/cmbMEPO_TOPLINE-BSART").key = "ZNAC"
            session.findById("wnd[0]/usr/subSUB0:SAPLMEGUI:0013/subSUB0:SAPLMEGUI:0030/subSUB1:SAPLMEGUI:1105/ctxtMEPO_TOPLINE-SUPERFIELD").text = proveedor
            session.findById("wnd[0]/usr/subSUB0:SAPLMEGUI:0013/subSUB1:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:1102/tabsHEADER_DETAIL/tabpTABHDT9/ssubTABSTRIPCONTROL2SUB:SAPLMEGUI:1221/ctxtMEPO1222-EKORG").text = "PE02"
            session.findById("wnd[0]/usr/subSUB0:SAPLMEGUI:0013/subSUB1:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:1102/tabsHEADER_DETAIL/tabpTABHDT9/ssubTABSTRIPCONTROL2SUB:SAPLMEGUI:1221/ctxtMEPO1222-EKGRP").text = "C32"
            session.findById("wnd[0]/usr/subSUB0:SAPLMEGUI:0013/subSUB1:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:1102/tabsHEADER_DETAIL/tabpTABHDT9/ssubTABSTRIPCONTROL2SUB:SAPLMEGUI:1221/ctxtMEPO1222-BUKRS").text = "PE02"
            session.findById("wnd[0]").sendVKey(0)   

            for i, row in pedidos_grupo.iterrows():

                ventana_actual = "0013" if i == 0 else "0010"
                indice = "0" if i == 0 else "1"
                path_base = f"wnd[0]/usr/subSUB0:SAPLMEGUI:{ventana_actual}/subSUB2:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:1211/tblSAPLMEGUITC_1211"
                session.findById(f"{path_base}/ctxtMEPO1211-EMATN[4,{indice}]").text = row['material']
                session.findById(f"{path_base}/txtMEPO1211-MENGE[6,{indice}]").text = row['cantidad']
                session.findById(f"{path_base}/ctxtMEPO1211-EEIND[9,{indice}]").text = "15.04.2025"
                session.findById(f"{path_base}/txtMEPO1211-NETPR[10,{indice}]").text = row['precio']
                session.findById(f"{path_base}/ctxtMEPO1211-NAME1[14,{indice}]").text = row['centro']
                session.findById(f"{path_base}/ctxtMEPO1211-LGOBE[15,{indice}]").text = "0002"
                session.findById(f"{path_base}/ctxtMEPO1211-AFNAM[16,{indice}]").text = "C012"
                session.findById("wnd[0]").sendVKey(0)
                session.findById("wnd[0]/usr/subSUB0:SAPLMEGUI:0010/subSUB2:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:1211/tblSAPLMEGUITC_1211").verticalScrollbar.position = scroll
                print('Primera vuelta',i, scroll,indice)
                scroll += 1
                
    else:
        print(f"No se pudo establecer conexión con {connection_name,user_sap,pwa_sap}.")
        session.findById("wnd[0]").close()

    
if __name__ == "__main__":
    main()