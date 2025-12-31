import pandas as pd
import os
from datetime import datetime

def main():
    
    # Cargar rutas y variables
    carpeta_entrada = "./Version 2/Entrada/Items/"
    ruta_exportacion_final = "./Version 2/Salida/"
    destino = "./Version 2/Entrada/Items/"
    fecha_actual = datetime.today().strftime('%Y%m%d')
    
    # DataFrame de errores
    errores_detectados = False
    errors_step_3 = pd.DataFrame(columns=["paso", "detalle", "fichero"])
    
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
        errors_step_3 = pd.concat(
            [errors_step_3, pd.DataFrame([{
                "paso": "Paso 3",
                "detalle": str(e),
                "fichero": "Items"
            }])],
            ignore_index=True
        )
        print("Paso 3 = Error")
        return errors_step_3

    # ---------------------------
    # Validación: primer archivo
    # ---------------------------
    ruta_primer_archivo = os.path.join(carpeta_entrada, archivos[0])
    try:
        data = pd.read_csv(ruta_primer_archivo, on_bad_lines="skip")
    except Exception as e:
        errores_detectados = True        
        errors_step_3 = pd.concat(
            [errors_step_3, pd.DataFrame([{
                "paso": "Paso 3",
                "detalle": f"No se pudo leer el primer archivo {archivos[0]}. Detalle: {e}",
                "fichero": "Items"
            }])],
            ignore_index=True
        )
        print("Paso 3 = Error")
        return errors_step_3
    
    # Renombrar las columnas de entrada para la manipulacion
    nuevos_nombres = [f'Columna_{i + 1}' for i in range(len(data.columns))]
    data.columns = nuevos_nombres

    # Duplicar la columna "Nº documento" para asignarla como "Item functional number"
    data = data.assign(columna_duplicada=data['Columna_3'].copy())
    
    # Asignacion de CLI y CPA a la "Venta a-Nº cliente" para asignar a "Buyer_code" y "Payment_center_code"
    data['Buyer_code'] = 'CLI-' +''+data['Columna_2']
    data['Payment_center_code'] = 'CPA-' +''+data['Columna_2']
    data['Assignment_code_6'] = '1' + data['Columna_2'].astype(str).str[:3]
    data = data.drop('Columna_2', axis=1)
    
    # Preparar columnas "Importe" y "Importe pendiente" para convertir a decimal
    data['Columna_8'] = data['Columna_8'].astype(str)
    data['Columna_9'] = data['Columna_9'].astype(str)
    # Función para limpiar valores
    def convertir_a_entero(valor):
        if pd.isna(valor):
            return '0'
        valor = str(valor).replace('.', '').replace(',', '')
        return valor
    # Aplicar la función de limpieza
    data['Columna_8'] = data['Columna_8'].apply(convertir_a_entero)
    data['Columna_9'] = data['Columna_9'].apply(convertir_a_entero)
    # Eliminar columna auxiliar si existe
    data = data.drop(['Columna_6'], axis=1, errors='ignore')
    # Preparar columnas para tratamiento de datos
    data['Columna_8'] = pd.to_numeric(data['Columna_8'], errors='coerce')
    data['Columna_9'] = pd.to_numeric(data['Columna_9'], errors='coerce')
    # Crear columna vacía (sin espacios en el nombre)
    data['Debit_or_Credit'] = ''
    # Metodo para asignacion de D o C según el valor a la columna "Debit_or_Credit"
    for i in range(len(data)):
        if data.loc[i, 'Columna_8'] < 0:
            data.loc[i, 'Debit_or_Credit'] = 'C'
        else:
            data.loc[i, 'Debit_or_Credit'] = 'D'
            
    # Convertir las columnas a positivo después de la asignación
    data['Columna_8'] = data['Columna_8'].abs()
    data['Columna_9'] = data['Columna_9'].abs()
    
    # Duplicar las columnas para asignacion
    data = data.assign(columna_duplicada_transaction=data['Columna_8'].copy())
    data = data.assign(columna_duplicada_accounting=data['Columna_9'].copy())
    
    # Conversión final a enteros puros para evitar .0 en CSV
    data['Columna_8'] = data['Columna_8'].fillna(0).astype('int64')
    data['Columna_9'] = data['Columna_9'].fillna(0).astype('int64')
    data['columna_duplicada_transaction'] = data['columna_duplicada_transaction'].fillna(0).astype('int64')
    data['columna_duplicada_accounting'] = data['columna_duplicada_accounting'].fillna(0).astype('int64')
    # Convertir columnas de fecha a formato deseado
    data['Columna_4'] = pd.to_datetime(data['Columna_4'], format='%d/%m/%y', errors='coerce')
    data['Columna_5'] = pd.to_datetime(data['Columna_5'], format='%d/%m/%y', errors='coerce')
    # Convert the date column to the desired format
    data['Columna_4'] = data['Columna_4'].dt.strftime('%Y%m%d')
    data['Columna_5'] = data['Columna_5'].dt.strftime('%Y%m%d')
    
    # Asignar el tipo a la columna "Record_type" segun el tipo de registro de la columna "Tipo documento"
    for i in range(len(data)):
        if data.loc[i, 'Columna_1'] == 'Abono' :
            data.loc[i, 'Record_type'] = 'AVO'
        elif data.loc[i, 'Columna_1'] == 'Pago' :
            data.loc[i, 'Record_type'] = 'RGL'
        elif data.loc[i, 'Columna_1'] == 'Reembolso' :
            data.loc[i, 'Record_type'] = 'AVO'
        else:
            data.loc[i, 'Record_type'] = 'FAC'
    data = data.drop('Columna_1', axis=1)
    
    # Asignar el tipo a la columna "Method_of_payment" segun el tipo de registro de la columna "Cód. forma pago"
    for i in range(len(data)):
        if data.loc[i, 'Columna_10'] == 'CH':
            data.loc[i, 'Method_of_payment'] = 'CHQ'
        elif data.loc[i, 'Columna_10'] in ['CG', 'Paypal', 'TR']:
            data.loc[i, 'Method_of_payment'] = 'VIR'
        else:
            data.loc[i, 'Method_of_payment'] = 'VIR'
    data = data.drop('Columna_10', axis=1)
    
    # Asignar el tipo a la columna "Assignment_code_5" segun el tipo de registro de la columna "Cód. términos pago"
    def rango_dias(dias):
        if dias == '15 DIAS':
            return "015"
        elif dias == '30 DIAS':
            return "030"
        elif dias == '45 DIAS':
            return "045"
        elif dias == '60 DIAS':
            return "060"
        elif dias == '75 DIAS':
            return "075"
        elif dias == '90 DIAS':
            return "090"
        elif dias == '120 DIAS':
            return "120"
        elif dias == 'CONTADO':
            return "000"
        else:
            return "000"
    data['Assignment_code_5'] = data['Columna_17'].apply(rango_dias)
    data = data.drop('Columna_17', axis=1)
    data = data.drop(['Columna_7', 'Columna_14', 'Columna_15', 'Columna_18'], axis=1)
    
    # Cambiar el nombre de las columnas
    data.columns = ['Item_technical_number', 'Item_date ', 'Due_date ', 'Initial_amount_including_taxes_in_transaction_currency', 
    'Remaining_amount_in_transaction_currency', 'Assignment_code_4', 'Assignment_code_1', 'Assignment_code_2',   
    'Assignment_code_3', 'Item_functional_number', 'Buyer_code', 'Payment_center_code', 'Assignment_code_6', 'Debit_or_Credit', 'Initial_amount_including_taxes_in_accounting_currency',
    'Remaining_amount_in_accounting_currency', 'Record_type', 'Method_of_payment', 'Assignment_code_5']

    # Extraer el codigo de la empresa
    data["Empresa"] = data["Buyer_code"].str.extract(r"CLI-(\d+)-")

    # Filtro original: AVO o RGL
    mask_avorgl = data["Record_type"].isin(["AVO", "RGL"])

    # Filtro adicional: FAC y Item_technical_number empieza por NDA
    mask_facnda = (data["Record_type"] == "FAC") & (data["Item_technical_number"].str.startswith("NDA"))

    # Máscara final (cualquiera de las dos condiciones)
    mask = mask_avorgl | mask_facnda

    # Agregar el código a Item_technical_number y Item_functional_number
    data.loc[mask, "Item_technical_number"] = (
        data.loc[mask, "Item_technical_number"].astype(str) + "-" + data.loc[mask, "Empresa"]
    )
    data.loc[mask, "Item_functional_number"] = (
        data.loc[mask, "Item_functional_number"].astype(str) + "-" + data.loc[mask, "Empresa"]
    )

    # Eliminar la columna temporal
    data = data.drop(columns=["Empresa"])

    # Reordenar el orden de las columnas
    data = data.reindex(columns=['Record_type', 'Buyer_code', 'Payment_center_code', 'Item_technical_number', 'Item_functional_number',
                                 'Item_date ', 'Due_date ', 'Debit_or_Credit', 'Initial_amount_including_taxes_in_transaction_currency', 
                                 'Initial_amount_including_taxes_in_accounting_currency', 'Remaining_amount_in_transaction_currency',
                                 'Remaining_amount_in_accounting_currency', 'Method_of_payment', 'Assignment_code_1', 'Assignment_code_2',
                                 'Assignment_code_3', 'Assignment_code_4', 'Assignment_code_5', 'Assignment_code_6'])             
    
    # Adicionar columnas faltantes
    data.insert(1,"Member_code ",303194)
    data.insert(8,"Currency","COP")
    data.insert(14,"Latest_balance_change","")
    data.insert(15,"Item_reference","")
    data.insert(17,"General_ledger_account","")
    data.insert(18,"Order_number","")
    data.insert(19,"Order_date ","")
    data.insert(20,"Statement_number","")
    data.insert(21,"Item_status_code","")
    data.insert(22,"Item_status_date","")
    data.insert(23,"Rejected_payment",0)
    data.insert(24,"Rejected_payment_date ","")
    data.insert(31,"Assignment_code_7","")
    data.insert(32,"Assignment_code_8","")
    data.insert(33,"Assignment_code_9","")
    data.insert(34,"Assignment_code_10","")
    data.insert(35,"Initial_amount_without_taxes_in_transaction_currency","")
    data.insert(36,"Initial_amount_without_taxes_in_accounting_currency","")
    data.insert(37,"Remaining_amount_without_taxes_in_transaction_currency","")
    data.insert(38,"Remaining_amount_without_taxes_in_accounting_currency","")
    data.insert(39,"Draft_prorogation",0)
    data.insert(40,"Prorogation_date","")
    data.insert(41,"securitization_module_only:_not_OK_for_securitization","")
    data.insert(42,"Securitization_module_only:_to_resolve","")
    data.insert(43,"Leader_invoice",1)
    data.insert(44,"Eligible_for_default_interests","")
    data.insert(45,"Insurable","")
    data.insert(44,"Local_currency_code","")
    data.insert(47,"Initial_amount_in_local_currency","")
    data.insert(48,"Remaining_amount_in_local_currency","")
    data.insert(49,"Initial_amount_without_taxes_in_local_currency","")
    data.insert(50,"Remaining_amount_without_taxes_in_local_currency","")

    # ---------------------------
    # Exportar fichero final
    # ---------------------------
    try:
        fichero = f"303194-ITEMS-{fecha_actual}.csv"
        ruta_fichero = os.path.join(ruta_exportacion_final, fichero)
        data.to_csv(ruta_fichero, sep=";", index=False, encoding="utf-8")
    except Exception as e:
        errores_detectados = True
        errors_step_3 = pd.concat(
            [errors_step_3, pd.DataFrame([{
                "paso": "Paso 3",
                "detalle": f"Error exportando fichero final. Detalle: {e}",
                "fichero": "Items"
            }])],
            ignore_index=True
        )
        errores_detectados = True 
    
    # Eliminar los CSV procesados ​​de la carpeta de destino
    for archivo in os.listdir(destino):
        archivo_path = os.path.join(destino, archivo)
        if os.path.isfile(archivo_path):
            os.remove(archivo_path)
        
    # ---------------------------
    # Mensaje final
    # ---------------------------
    if not errores_detectados and errors_step_3.empty:
        print("Paso 3 = Ok")
    else:
        print("Paso 3 = Error")
        
    return errors_step_3

if __name__ == "__main__":
    errors_step_3 = main()




