import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import pandas as pd

def main(df_estado=None, df_limpieza=None, df_errores=None):

    load_dotenv()
    
    # Load environment variables
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    TABLE_ESTADO = os.getenv("DB_TABLE_ESTADO")
    TABLE_LIMPIEZA = os.getenv("DB_TABLE_LIMPIEZA")
    TABLE_ERRORES = os.getenv("DB_TABLE_ERRORES")

    def insertar_df_en_tabla(df, tabla):
        if df is None:
            return False
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
            )
            conn.autocommit = True
            cursor = conn.cursor()

            if df.empty:
                # Insertar solo id_estado cuando está vacío
                cursor.execute(f"SELECT MAX(id) FROM {TABLE_ESTADO}")
                id_estado = cursor.fetchone()[0]
                query = f"INSERT INTO {tabla} (id_estado) VALUES (%s)"
                cursor.execute(query, (id_estado,))
            else:
                # Inserción masiva con execute_values
                cols = list(df.columns)
                values = [tuple(row) for row in df.to_numpy()]
                insert_query = f"INSERT INTO {tabla} ({','.join(cols)}) VALUES %s"
                execute_values(cursor, insert_query, values)

            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print(f"Error al insertar en {tabla}: {e}")
            return False

    # 1. Insertar df_estado
    estado_ok = True
    if df_estado is not None:
        estado_ok = insertar_df_en_tabla(df_estado, TABLE_ESTADO)

    # 2. Insertar df_limpieza y df_errores
    limpieza_ok = True
    errores_ok = True

    if df_limpieza is not None:
        limpieza_ok = insertar_df_en_tabla(df_limpieza, TABLE_LIMPIEZA)

    if df_errores is not None:
        errores_ok = insertar_df_en_tabla(df_errores, TABLE_ERRORES)

    # 3. Mensaje final
    if estado_ok and limpieza_ok and errores_ok:
        print("Paso 5 = Ok")
    else:
        print("Paso 5 = Error")

if __name__ == "__main__":
    main()



