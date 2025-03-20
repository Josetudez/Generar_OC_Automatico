import mysql.connector as mysql
from mysql.connector import Error
import json

def get_db_config(path="config/credenciales_db.json"):
    """Carga la configuración de la base de datos desde un archivo JSON."""
    with open(path, "r") as config_file:
        return json.load(config_file)

def crear_conexion():
    try:
        config = get_db_config()
        conn = mysql.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"]
        )
        if conn.is_connected():
            print("Conexion Exitosa")
            return conn
    except Error as e:
        print("Error al conectar a la base de datos:",e )
        return None

def cerrar_conexion(conn):
    if conn.is_connected():
        conn.close()
        print("conexion cerrada")
