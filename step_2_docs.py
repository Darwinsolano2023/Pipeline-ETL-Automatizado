import pandas as pd
import os
import re

def main():
    
    # Cargar rutas
    carpeta_entrada = "./Version 2/Entrada/Items/"
    carpeta_salida = "./Version 2/Entrada/Items/"

    os.makedirs(carpeta_salida, exist_ok=True)

    archivos = [f for f in os.listdir(carpeta_entrada) if f.endswith('.csv')]
    archivos.sort()

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
                "fichero": "Items"
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
                "fichero": "Items"
            }])],
            ignore_index=True
        )
        print("Paso 2 = Error")
        return cleaning_step_2, errors_step_2, list(empresas_procesadas)
    
    # ---------------------------
    # Configuración de validación
    # ---------------------------
    patron = re.compile(r'[§^;\'"]')

    # Columnas de fichero
    columnas_items = [
        "Tipo documento", "Venta a-Nº cliente", "Nº documento", "Fecha registro", "Fecha vencimiento",
        "Fecha vencimiento", "Cód. divisa", "Cód. Centro de Costo", "Importe", "Importe pendiente",
        "Cód. forma pago", "Descripción del trabajo", "Nombre Proyecto", "Nombre del Municipio", "ID Usuario Preasignado",
        "Cód. vendedor", "Nº de documento externo", "Cód. términos pago"
    ]

    # Columnas que SI deben eliminar filas si están vacías
    columnas_obligatorias = [
        "Tipo documento", "Venta a-Nº cliente", "Nº documento", "Fecha registro", "Fecha registro", "Importe"
    ]
    
    # Columnas que solo se validan, pero no se eliminan si están vacías
    columnas_valida_sin_eliminar = [
        "Centro responsabilidad", "Grupo registro cliente"
    ]
    
    # ---------------------------
    # Procesar archivos
    # ---------------------------
    for archivo in archivos:
        ruta_csv = os.path.join(carpeta_entrada, archivo)

        try:
            df_items = pd.read_csv(
                ruta_csv,
                sep=",",
                dtype=str,
                engine="python",
                on_bad_lines="skip",
                quotechar='"'
            )

            # -----------------------------------------------------------------
            # LIMPIEZA NUEVA: elimina saltos de línea LF y CR dentro de celdas
            # -----------------------------------------------------------------
            for col in df_items.columns:
                df_items[col] = df_items[col].astype(str).str.replace("\n", " ", regex=False).str.replace("\r", " ", regex=False)

        except Exception as e:
            errores_detectados = True            
            errors_step_2 = pd.concat(
                [errors_step_2, pd.DataFrame([{
                    "paso": "Paso 2",
                    "detalle": f"No se pudo leer {archivo}. Detalle: {e}",
                    "fichero": "Items"
                }])],
                ignore_index=True
            )
            continue

        # Derivar "Código de la empresa" a partir de "Venta a-Nº cliente"
        if "Venta a-Nº cliente" in df_items.columns:
            # Tomar primeros 3 dígitos y anteponer un 1
            df_items["Código de la empresa"] = df_items["Venta a-Nº cliente"].astype(str).str[:3].apply(lambda x: "1" + x if x.isdigit() else None)

            # Acumular empresas únicas
            empresas_unicas = df_items["Código de la empresa"].dropna().unique()
            empresas_procesadas.update(empresas_unicas)

        errores = []
        filas_invalidas = set()
        
        # ---------------------------
        # Validaciones por columna
        # ---------------------------
        for col in columnas_items:
            if col in df_items.columns:
                for idx, val in df_items[col].items():
                    valor = str(val).strip() if pd.notna(val) else ""
                    motivo = None

                    # Validación 1: columnas obligatorias → vacío elimina fila
                    if col in columnas_obligatorias and (valor == "" or valor.lower() == "nan"):
                        motivo = "vacío"
                        filas_invalidas.add(idx)

                    # Validación 2: columnas solo validación → vacío no elimina
                    elif col in columnas_valida_sin_eliminar and (valor == "" or valor.lower() == "nan"):
                        motivo = "vacío (sin eliminar)"

                    # Validación 3: eliminar todos los espacios en "Nº documento"
                    elif col == "Nº documento":
                        nuevo_valor = re.sub(r"\s+", "", valor)  # elimina todos los espacios (inicio, medio y fin)
                        if nuevo_valor != valor:
                            df_items.at[idx, col] = nuevo_valor
                            motivo = "espacios eliminados en Nº documento"

                    # Validación 4: caracteres inválidos
                    elif patron.search(valor):
                        motivo = "caracter inválido"
                        nuevo_valor = patron.sub(" ", valor)
                        df_items.at[idx, col] = nuevo_valor

                    # Guardar error si existe
                    if motivo:
                        errores.append({
                            "codigo_cliente": df_items.at[idx, "Venta a-Nº cliente"] if "Venta a-Nº cliente" in df_items.columns else "",
                            "campo_afectado": col,
                            "motivo": motivo,
                            "fichero": "Items"
                        })

        # Eliminar filas inválidas
        if filas_invalidas:
            df_items = df_items.drop(index=list(filas_invalidas))
        
        # ---------------------------
        # Filtro especial SOLO para empresa 1477 (no afecta otras)
        # ---------------------------

        # Asegurar que la columna exista
        if "Código de la empresa" in df_items.columns:

            # Identificar filas de empresa 1477
            mask_1477 = df_items["Código de la empresa"].astype(str) == "1477"

            if mask_1477.any():  # Si existe la empresa 1477 en este archivo
                # Mantener solo los registros de 1477 cuyo centro de costo empieza en W
                mask_cc_w = df_items["Cód. Centro de Costo"].astype(str).str.startswith("W", na=False)

                # Eliminar SOLO las filas de 1477 que no cumplen la condición "W"
                df_items = df_items[
                    (~mask_1477) | (mask_1477 & mask_cc_w)
                ]

        # ---------------------------
        # Validación: reemplazo archivo
        # ---------------------------
        try:
            os.remove(ruta_csv)
            df_items.to_csv(ruta_csv, index=False)
        except Exception as e:
            errores_detectados = True
            errors_step_2 = pd.concat(
                [errors_step_2, pd.DataFrame([{
                    "paso": "Paso 2",
                    "detalle": f"No se pudo reemplazar {archivo}. Detalle: {e}",
                    "fichero": "Items"
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
          
    # --------------
    # Mensaje final
    # --------------
    if not errores_detectados and errors_step_2.empty:
        print("Paso 2 = Ok")
    else:
        print("Paso 2 = Error")

    return cleaning_step_2, errors_step_2, list(empresas_procesadas)


if __name__ == "__main__":
    cleaning_step_2, errors_step_2, result_step_2 = main()



