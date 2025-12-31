import pandas as pd
import os
from datetime import date

def main():
    
    # Cargar rutas
    carpeta_entrada = "./Version 2/Archivos Enwis/Customers/"
    carpeta_salida = "./Version 2/Entrada/Customers/"

    os.makedirs(carpeta_salida, exist_ok=True)

    # DataFrame de errores
    errors_step_1 = pd.DataFrame(columns=["paso", "detalle", "fichero"])
    errores_detectados = False
    df_total = None

    # ---------------------------
    # Validación: listar archivos
    # ---------------------------
    try:
        archivos = [f for f in os.listdir(carpeta_entrada) if f.endswith(".csv")]
        archivos.sort()
        if not archivos:
            raise FileNotFoundError("No se encontraron archivos CSV en la carpeta de entrada")
    except Exception as e:
        errores_detectados = True
        errors_step_1 = pd.concat(
            [errors_step_1, pd.DataFrame([{
                "paso": "Paso 1",
                "detalle": str(e),
                "fichero": "Customers"
            }])],
            ignore_index=True
        )
        print("Paso 1 = Error")
        return errors_step_1

    # ---------------------------
    # Validación: primer archivo
    # ---------------------------
    ruta_primer_archivo = os.path.join(carpeta_entrada, archivos[0])
    try:
        df_total = pd.read_csv(ruta_primer_archivo, on_bad_lines="skip")
    except Exception as e:
        errores_detectados = True        
        errors_step_1 = pd.concat(
            [errors_step_1, pd.DataFrame([{
                "paso": "Paso 1",
                "detalle": f"No se pudo leer el primer archivo {archivos[0]}. Detalle: {e}",
                "fichero": "Customers"
            }])],
            ignore_index=True
        )
        print("Paso 1 = Error")
        return errors_step_1

    columnas_validas = df_total.columns.tolist()

    # ---------------------------
    # Leer y concatenar resto de archivos
    # ---------------------------
    for archivo in archivos[1:]:
        ruta_archivo = os.path.join(carpeta_entrada, archivo)
        try:
            df_temp = pd.read_csv(ruta_archivo, usecols=columnas_validas, on_bad_lines="skip")
            df_total = pd.concat([df_total, df_temp], ignore_index=True)
        except Exception as e:
            errores_detectados = True
            errors_step_1 = pd.concat(
                [errors_step_1, pd.DataFrame([{
                    "paso": "Paso 1",
                    "detalle": f"No se pudo leer {archivo}. Detalle: {e}",
                    "fichero": "Customers"
                }])],
                ignore_index=True
            )

    # ---------------------------
    # Validación: guardado archivo combinado
    # ---------------------------
    fecha_actual = date.today().strftime("%Y-%m-%d")
    nombre_salida = f"Customers_{fecha_actual}.csv"
    ruta_salida = os.path.join(carpeta_salida, nombre_salida)

    try:
        df_total.to_csv(ruta_salida, index=False)
    except Exception as e:
        errores_detectados = True        
        errors_step_1 = pd.concat(
            [errors_step_1, pd.DataFrame([{
                "paso": "Paso 1",
                "detalle": f"No se pudo guardar el archivo combinado. Detalle: {e}",
                "fichero": "Customers"
            }])],
            ignore_index=True
        )

    # ---------------------------
    # Validación: eliminación de archivos originales
    # ---------------------------
    for archivo in archivos:
        ruta_a_eliminar = os.path.join(carpeta_entrada, archivo)
        try:
            os.remove(ruta_a_eliminar)
        except Exception as e:
            errores_detectados = True
            
            mensaje = f"No se pudo eliminar {archivo}. Detalle: {e}"
            
            errors_step_1 = pd.concat(
                [errors_step_1, pd.DataFrame([{
                    "paso": "Paso 1",
                    "detalle": f"No se pudo eliminar {archivo}. Detalle: {e}",
                    "fichero": "Customers"
                }])],
                ignore_index=True
            )

    # ---------------------------
    # Mensaje final
    # ---------------------------
    if not errores_detectados and errors_step_1.empty:
        print("Paso 1 = Ok")
    else:
        print("Paso 1 = Error")

    return errors_step_1


if __name__ == "__main__":
    errors_step_1 = main()



