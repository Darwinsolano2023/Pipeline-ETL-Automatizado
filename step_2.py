import pandas as pd
import os
import re

def main():
    
    # Cargar rutas
    carpeta_entrada = "./Version 2/Entrada/Customers/"
    carpeta_salida = "./Version 2/Entrada/Customers/"

    os.makedirs(carpeta_salida, exist_ok=True)

    # DataFrames de salida
    errors_step_2 = pd.DataFrame(columns=["paso", "detalle", "fichero"])
    cleaning_step_2 = pd.DataFrame(columns=["codigo_cliente", "campo_afectado", "motivo", "fichero"])

    errores_detectados = False
    empresas_procesadas = set()

    # ---------------------------
    # Validación: listar archivos
    # ---------------------------
    try:
        archivos = [f for f in os.listdir(carpeta_entrada) if f.endswith('.csv')]
        archivos.sort()
        if not archivos:
            raise FileNotFoundError("No se encontraron archivos CSV en la carpeta de entrada")
    except Exception as e:  
        errores_detectados = True
        errors_step_2 = pd.concat(
            [errors_step_2, pd.DataFrame([{
                "paso": "Paso 2",
                "detalle": f"No se pudo acceder a la carpeta de entrada. Detalle: {e}",
                "fichero": "Customers"
            }])],
            ignore_index=True
        )
        print("Paso 2 = Error")
        return cleaning_step_2, errors_step_2, list(empresas_procesadas)

    # ---------------------------
    # Validación: primer archivo
    # ---------------------------
    ruta_primer_archivo = os.path.join(carpeta_entrada, archivos[0])
    try:
        pd.read_csv(ruta_primer_archivo, on_bad_lines="skip")
    except Exception as e:
        errores_detectados = True
        errors_step_2 = pd.concat(
            [errors_step_2, pd.DataFrame([{
                "paso": "Paso 2",
                "detalle": f"No se pudo leer el primer archivo {archivos[0]}. Detalle: {e}",
                "fichero": "Customers"
            }])],
            ignore_index=True
        )
        print("Paso 2 = Error")
        return cleaning_step_2, errors_step_2, list(empresas_procesadas)

    # ---------------------------
    # Configuración de validación
    # ---------------------------
    patron = re.compile(r"[§^;]")

    # Columnas de fichero
    columnas_clientes = [
        "Nº", "CIF/NIF", "Código de la empresa", "Nombre", "Dirección",
        "Código postal", "Población", "Cód. país/región", "Nº teléfono",
        "Nº fax", "Cód. forma pago", "Cód. términos pago", "Cód. divisa",
        "Grupo registro cliente", "Cód. vendedor", "Segmento Negocio",
        "Crédito máximo (DL)", "PRY", "Centro responsabilidad"
    ]

    # Columnas que SI deben eliminar filas si están vacías
    columnas_obligatorias = [
        "Nº", "CIF/NIF", "Nombre", "Dirección", "Población", "Cód. país/región", "Cód. forma pago"
    ]
    
    # Columnas que solo se validan, pero no se eliminan si están vacías
    columnas_valida_sin_eliminar = [
        "Segmento Negocio", "Cód. vendedor", "Centro responsabilidad",
        "Cód. términos pago", "Crédito máximo (DL)", "Grupo registro cliente"
    ]

    # ---------------------------
    # Procesar archivos
    # ---------------------------
    for archivo in archivos:
        ruta_csv = os.path.join(carpeta_entrada, archivo)

        try:
            df_clientes = pd.read_csv(
                ruta_csv,
                sep=",",
                dtype=str,
                engine="python",
                on_bad_lines="skip",
                quotechar='"'
            )
        except Exception as e:
            errores_detectados = True            
            errors_step_2 = pd.concat(
                [errors_step_2, pd.DataFrame([{
                    "paso": "Paso 2",
                    "detalle": f"No se pudo leer {archivo}. Detalle: {e}",
                    "fichero": "Customers"
                }])],
                ignore_index=True
            )
            continue

        # Empresas únicas
        if "Código de la empresa" in df_clientes.columns:
            empresas_unicas = df_clientes["Código de la empresa"].dropna().unique()
            empresas_procesadas.update(empresas_unicas)

        errores = []
        filas_invalidas = set()

        for col in columnas_clientes:
            if col in df_clientes.columns:
                for idx, val in df_clientes[col].items():
                    valor = str(val).strip() if pd.notna(val) else ""
                    motivo = None

                    # Validación 1: columnas obligatorias → vacío elimina fila
                    if col in columnas_obligatorias and (valor == "" or valor.lower() == "nan"):
                        motivo = "vacío"
                        filas_invalidas.add(idx)

                    # Validación 2: columnas solo validación → vacío no elimina
                    elif col in columnas_valida_sin_eliminar and (valor == "" or valor.lower() == "nan"):
                        motivo = "vacío (sin eliminar)"

                    # Validación 3: caracteres inválidos
                    elif patron.search(valor):
                        motivo = "caracter inválido"
                        nuevo_valor = patron.sub(" ", valor)
                        df_clientes.at[idx, col] = nuevo_valor

                    # Guardar error si existe
                    if motivo:
                        errores.append({
                            "codigo_cliente": df_clientes.at[idx, "Nº"] if "Nº" in df_clientes.columns else "",
                            "campo_afectado": col,
                            "motivo": motivo,
                            "fichero": "Customers"
                        })

        # Eliminar filas inválidas
        if filas_invalidas:
            df_clientes = df_clientes.drop(index=list(filas_invalidas))

        # ---------------------------
        # Validación: reemplazo archivo
        # ---------------------------
        try:
            os.remove(ruta_csv)
            df_clientes.to_csv(ruta_csv, index=False)
        except Exception as e:
            errores_detectados = True
            errors_step_2 = pd.concat(
                [errors_step_2, pd.DataFrame([{
                    "paso": "Paso 2",
                    "detalle": f"No se pudo reemplazar {archivo}. Detalle: {e}",
                    "fichero": "Customers"
                }])],
                ignore_index=True
            )
            continue

        # Registrar errores de contenido
        if errores:
            cleaning_step_2 = pd.concat(
                [cleaning_step_2, pd.DataFrame(errores)],
                ignore_index=True
            )

    # ---------------------------
    # Mensaje final
    # ---------------------------
    if not errores_detectados and errors_step_2.empty:
        print("Paso 2 = Ok")
    else:
        print("Paso 2 = Error")

    return cleaning_step_2, errors_step_2, list(empresas_procesadas)


if __name__ == "__main__":
    cleaning_step_2, errors_step_2, result_step_2 = main()
    




