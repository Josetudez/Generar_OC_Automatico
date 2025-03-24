import win32com.client
import subprocess
import time
import os
from winreg import OpenKey, QueryValueEx, HKEY_LOCAL_MACHINE

def connect_to_sap(open_if_not_active=True, connection_name="CGM S4 HANA PRD" , user_sap="JTUDELANO", pwa_sap="Jtudel@no.Z2024"):
    """
    Conecta a SAP GUI. Valida si hay una sesión activa o abre el aplicativo si no lo está,
    y luego establece la conexión especificada.
    :param open_if_not_active: Indica si debe abrir el aplicativo si no hay sesiones activas.
    :param connection_name: Nombre de la conexión en SAP GUI.
    :return: Sesión activa de SAP o None si no se puede conectar.
    """
    try:
        # Intentar conectar a una sesión activa
        SapGuiAuto = win32com.client.GetObject("SAPGUI")
        if not SapGuiAuto:
            raise Exception("SAP GUI no está disponible.")

        application = SapGuiAuto.GetScriptingEngine
        if len(application.Children) == 0:
            raise Exception("No hay conexiones activas de SAP.")

        connection = application.Children(0)  # Usar la primera conexión disponible
        if len(connection.Children) == 0:
            raise Exception("No hay sesiones activas en la conexión.")

        session = connection.Children(0)  # Usar la primera sesión activa
        print("Sesión activa de SAP encontrada.")
        return session

    except Exception as e:
        print(f"Error al validar conexión activa: {e}")

        if open_if_not_active:
            print("Intentando abrir SAP GUI y establecer conexión...")
            saplogon_path = get_saplogon_path()
            if saplogon_path and open_sap_gui(saplogon_path, connection_name, user_sap, pwa_sap):
                # Intentar nuevamente la conexión
                time.sleep(5)  # Dar tiempo para que el aplicativo cargue
                return connect_to_sap(open_if_not_active=False, connection_name=connection_name , user_sap=user_sap, pwa_sap=pwa_sap)
        return None


def get_saplogon_path():
    """
    Obtiene dinámicamente el path de saplogon.exe.
    Busca en el Registro de Windows o en el PATH del sistema.
    :return: Ruta completa de saplogon.exe o None si no se encuentra.
    """
    try:
        # Buscar en el Registro de Windows
        with OpenKey(HKEY_LOCAL_MACHINE, r"SOFTWARE\SAP\SAPGUI Front\SAP Frontend Server") as key:
            path, _ = QueryValueEx(key, "InstallationPath")
            saplogon_path = os.path.join(path, "saplogon.exe")
            if os.path.exists(saplogon_path):
                return saplogon_path
    except Exception as e:
        print(f"No se pudo obtener la ruta desde el registro: {e}")

    return find_saplogon_exe()


def find_saplogon_exe(search_path="C:\\"):
    """
    Busca el archivo saplogon.exe en el sistema utilizando os.walk.
    """
    print(f"Buscando 'saplogon.exe' en {search_path}...")
    for root, dirs, files in os.walk(search_path):
        if "saplogon.exe" in files:
            saplogon_path = os.path.join(root, "saplogon.exe")
            print(f"'saplogon.exe' encontrado: {saplogon_path}")
            return saplogon_path
    print("'saplogon.exe' no encontrado.")
    return None


def open_sap_gui(saplogon_path, connection_name, user_sap, pwa_sap):
    """
    Abre el aplicativo de SAP GUI usando la ruta proporcionada y establece la conexión especificada.
    :param saplogon_path: Ruta completa a saplogon.exe.
    :param connection_name: Nombre de la conexión en SAP GUI.
    :return: True si se abre correctamente, False en caso contrario.
    """
    try:
        # Iniciar SAP GUI
        subprocess.Popen([saplogon_path])
        print(f"SAP GUI iniciado exitosamente desde: {saplogon_path}")

        # Esperar a que se cargue SAP GUI y abrir la conexión
        time.sleep(5)  # Tiempo para que el programa cargue
        SapGuiAuto = win32com.client.GetObject("SAPGUI")
        application = SapGuiAuto.GetScriptingEngine
        connection = application.OpenConnection(connection_name, True)
        time.sleep(3)
        session = connection.Children(0)
        session.findById("wnd[0]").maximize()
        session.findById("wnd[0]/usr/txtRSYST-BNAME").text = user_sap  ########################## Usuario de SAP
        session.findById("wnd[0]/usr/pwdRSYST-BCODE").text = pwa_sap ################## Contraseña de SAP
        session.findById("wnd[0]/usr/pwdRSYST-BCODE").setFocus
        session.findById("wnd[0]/usr/pwdRSYST-BCODE").caretPosition = 15
        session.findById("wnd[0]").sendVKey(0)
        print(f"Conexión a {connection_name} establecida correctamente.")
        return True
    except Exception as e:
        print(f"Error al intentar abrir SAP GUI o establecer conexión: {e}")
        return False
