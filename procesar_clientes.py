# Import necessary libraries
import os
import glob
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime 

# Modules for processing the Clients file
import step_1
import step_2
import step_3
import step_4
import step_5

def procesar_clientes():

    # Imprimir fecha y hora de inicio del proceso
    print(f"\n=== Proceso ejecutado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

    # Start of the transformation process for the customer file
    print("Fichero Customers...")
    
    load_dotenv()
    # Load environment variables
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")

    origen = "./Version 2/Archivos Enwis/Customers/"
    destino = "./Version 2/Entrada/Customers/"

    # Find CSV files in the input folder
    archivos_csv = glob.glob(os.path.join(origen, "*.csv"))
    if not archivos_csv:
        print('Proceso no iniciado, no se encontraron archivos en la carpeta de entrada')
        
        df_estado = pd.DataFrame([{
            "nombre_fichero": "Customers",
            "empresas_fichero": "",
            "Limpieza": "",
            "fichero_enviado": "",
        }])
        
        step_5.main(df_estado=df_estado)
        
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(id) FROM estado;")
        estado_id = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        df_limpieza = pd.DataFrame([{
            "estado_id": estado_id,
            "fichero": "Customers",
            "codigo_cliente": " ",
            "campo_afectado": " ",
            "motivo": " "
        }])
        
        df_errores = pd.DataFrame([{
            "estado_id": estado_id,
            "fichero": "Customers",
            "paso": " ",
            "detalle": " "
        }])

        step_5.main(df_limpieza=df_limpieza, df_errores=df_errores)
        
        return False

    print('Inicializando Proceseso...')

    # Step 1: Merge files arriving from Enwis
    errors_step_1 = step_1.main()

    # Find the most recent processed CSV in the destination folder
    archivos_destino = glob.glob(os.path.join(destino, "*.csv"))
    archivo_combinado = max(archivos_destino, key=os.path.getmtime)
    df = pd.read_csv(archivo_combinado)

    # Step 2: data cleaning
    cleaning_step_2, errors_step_2, result_step_2 = step_2.main()
    
    # Variable para registrar si hubo limpieza
    if not cleaning_step_2.empty:
        result_cleaning_2 = "Si"
    else:
        result_cleaning_2 = "No"
    
    # Step 3: File transformation
    errors_step_3 = step_3.main()

    # Step 4: Sending file to FSTP
    result_step_4, errors_step_4 = step_4.main()
    
    # Step 5: Loading data into the database        
    df_estado = pd.DataFrame([{
        "nombre_fichero": "Customers",
        "empresas_fichero": result_step_2,
        "limpieza": result_cleaning_2,
        "fichero_enviado": result_step_4
    }])

    step_5.main(df_estado=df_estado)
    
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(id) FROM estado;")
    estado_id = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    
    df_limpieza = cleaning_step_2.copy()

    if not df_limpieza.empty:
        df_limpieza["estado_id"] = estado_id
        df_limpieza = df_limpieza[["estado_id", "fichero", "codigo_cliente", "campo_afectado", "motivo"]]
    else:
        df_limpieza = pd.DataFrame(columns=["estado_id", "fichero", "codigo_cliente", "campo_afectado", "motivo"])
    
    # Concatenar todos los errores
    df_errores = pd.concat(
        [errors_step_1, errors_step_2, errors_step_3, errors_step_4],
        ignore_index=True
    )
    
    df_errores["estado_id"] = estado_id
    df_errores = df_errores[["estado_id", "fichero", "paso", "detalle"]]
    
    if df_limpieza.empty:
        df_limpieza = pd.DataFrame([{
            "estado_id": estado_id,
            "fichero": "Customers",
            "codigo_cliente": "",
            "campo_afectado": "",
            "motivo": ""
        }])
    else:
        df_limpieza["estado_id"] = estado_id
        df_limpieza = df_limpieza[["estado_id", "fichero", "codigo_cliente", "campo_afectado", "motivo"]]

    if df_errores.empty:
        df_errores = pd.DataFrame([{
            "estado_id": estado_id,
            "fichero": "Customers",
            "paso": "",
            "detalle": ""
        }])
    else:
        df_errores["estado_id"] = estado_id
        df_errores = df_errores[["estado_id", "fichero", "paso", "detalle"]]

    # Step 5: Loading data into the database 
    step_5.main(df_limpieza=df_limpieza, df_errores=df_errores)
    
    return True

if __name__ == "__main__":
    procesar_clientes()



