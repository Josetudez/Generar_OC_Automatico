from db.conexion_db import cerrar_conexion
from datetime import datetime
import numpy as np

def insert_data(connection, tabla, registros, cabezeras, batch_size):
    """
    Inserta datos de un DataFrame en la tabla especificada por bloques.
    :param connection: Conexión activa a la base de datos.
    :param tabla: Nombre de la tabla.
    :param datos: DataFrame con los datos a insertar.
    :param batch_size: Número de filas por bloque (lote) a insertar.
    """
    try:
        cursor = connection.cursor()

        datos = registros.copy()

        datos['fechahora'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cols = ['fechahora'] + [col for col in datos if col != 'fechahora'] 

        num_registros = datos.shape[0]

        datos = datos[cols]
        
        datos = datos.replace({np.nan : None})
        
        columnas = ", ".join([f"`{columna}`" for columna in cabezeras])
        placeholders = ", ".join(["%s"] * len(datos.columns))
        
        # Crear la consulta de inserción
        query = f"INSERT INTO {tabla} ({columnas}) VALUES ({placeholders})"
        
        # Iterar sobre el DataFrame en bloques
        for i in range(0, len(datos), batch_size):
            # Obtener el bloque de datos (un subconjunto del DataFrame)
            bloque = datos.iloc[i:i+batch_size]

            # Convertir el DataFrame del bloque en una lista de tuplas
            valores = [tuple(fila) for fila in bloque.values]
            
            # Ejecutar la inserción del bloque
            cursor.executemany(query, valores)

        
        connection.commit()
        print(f"Se insertaron: {num_registros} registros. en la tabla {tabla}")
    except Exception as e:
        print(f"Error al insertar datos: {e}")
        #enviar_mensaje(f"Error al cargar los registros a la tabla {tabla}")
    finally:
        cerrar_conexion(connection)


def consult_data(connection, query):
    """
    Ejecuta una consulta y devuelve los resultados.
    :param connection: Conexión activa a la base de datos.
    :param query: Consulta SQL.
    """
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        print(f"Error al obtener datos: {e}")
        return []

def procedure_data(connection, query):

    try:
        with connection.cursor() as cursor:

            cursor.callproc(query)
            print("El procedimiento se ah ejecutado correctamente")

            connection.commit()
        
    except Exception as e:  # Captura excepciones más específicas si es posible

        print(f"Error al ejecutar el procedimiento: {e}")
        return []  # Devuelve una lista vacía en caso de error


def update_data(connection, query):
    """
    Ejecuta una consulta y devuelve los resultados.
    :param connection: Conexión activa a la base de datos.
    :param query: Consulta SQL.
    """
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        connection.commit()
        return cursor.rowcount
    except Exception as e:
        print(f"Error al ejecutar la consulta de actualización: {e}")
        connection.rollback()  # Si hay un error, deshacer la transacción
        return 0
    finally:
        cerrar_conexion(connection)