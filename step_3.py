import pandas as pd
import os
from datetime import datetime

def main():
    
    # Cargar rutas y variables
    carpeta_entrada = "./Version 2/Entrada/Customers/"
    ruta_exportacion_final = "./Version 2/Salida/"
    destino = "./Version 2/Entrada/Customers/"
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
                "fichero": "Customers"
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
                "fichero": "Customers"
            }])],
            ignore_index=True
        )
        print("Paso 3 = Error")
        return errors_step_3
    
    # Generate new column names from the input file, and remove columns that are not required
    nuevos_nombres = [f'Columna_{i + 1}' for i in range(len(data.columns))]
    data.columns = nuevos_nombres

    # Process column No.
    data['TECHNICAL CUSTOMER CODE'] = 'CLI-' +''+data['Columna_1']

    # Asignar columna "Assignment_code_8"
    data['Assignment_code_8'] = data['Columna_3']

    # Step 4: Clear column_2
    data['Columna_2'] = data['Columna_2'].apply(lambda x: str(x).replace('.0', '') if str(x).endswith('.0') else str(x))

    #Step 5. Duplicate the column
    data = data.assign(columna_duplicada=data['Columna_2'].copy())

    #Step 6. Split the address column into 3 columns according to the allowed limit of 40 characters
    data = data.assign(ADDRESS_UNO=data['Columna_5'].str[:40],
                        ADDRESS_DOS=data['Columna_5'].str[40:80],
                        ADDRESS_TRES=data['Columna_5'].str[80:120])
    data = data.drop('Columna_5', axis=1)
    data = data.drop('Columna_11', axis=1)

    #Step 7. Assign the days in the corresponding format (000)
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

    data['rango_dias'] = data['Columna_12'].apply(rango_dias)
    data = data.drop('Columna_12', axis=1)
    data = data.assign(new_column_dias=data['rango_dias'].copy())

    #Step 8. Assign the type of currency to the CURRENCY column according to the country code of the COUNTRY CODE column
    for i in range(len(data)):
        if data.loc[i, 'Columna_13'] == 'USD' :
            data.loc[i, 'CURRENCY_A'] = 'USD'
        else:
            data.loc[i, 'CURRENCY_A'] = 'COP'

    data = data.drop('Columna_13', axis=1)

    #Step 9. Change column names
    data.columns = ['Columna_1', 'Functional_customer_code', 'Selleres_entity', 'Customer_name', 'Zip_code', 'City',
                    'Country_code', 'Telefone', 'fax', 'Assignment_code_6', 'Assignment_code_2', 'Assignment_code_1', 'Assignment_code_5',
                    'Assignment_code_7', 'Assignment_code_3', 'Technical_customer_code', 'Assignment_code_8', 'National_id_code', 'Address_1', 
                    'Address_2', 'Address_3', 'Delay_dranted', 'Assignment_code_4', 'Currency']

    #Step 10. Add columns
    data.insert(0,"Record_type","CLI")
    data.insert(1,"Member_code","303194")
    data.insert(7,"Comercial_trade_mark","")
    data.insert(16,"European_act_code","")
    data.insert(17,"Lenguaje_code","ES")
    data.insert(18,"Payment_method","VIR")
    data.insert(20,"End_of_month",0)
    data.insert(31,"Assignment_code_9","")
    data.insert(32,"Assignment_code_10","")
    data.insert(33,"Number_of_items_not_yet_invoiced","")
    data.insert(34,"Amount_of_items_not_yet_invoiced","")
    data.insert(35,"Order_Status","")
    data.insert(36,"Order_status_comment ","")
    data.insert(37,"Order_status_date","")
    data.insert(38,"Eligible_for_default_interests","")
    data.insert(39,"Non_B2B_customer","")
    data.insert(40,"Linked_company","")
    data.insert(41,"Local_currency_code","COP")
    data.insert(42,"Invoices_not_yet_issued_amount_in_local_currency","")
    
    # Step 11: Remove the digits after the . in the columns
    data['Zip_code'] = data['Zip_code'].apply(lambda x: str(x).replace('.0', '') if str(x).endswith('.0') else str(x))
    data['Telefone'] = data['Telefone'].apply(lambda x: str(x).replace('.0', '') if str(x).endswith('.0') else str(x))
    
    # Step 12: Reorder columns
    columnas_finales = ['Record_type', 'Member_code', 'Technical_customer_code', 'Functional_customer_code', 'Selleres_entity', 'National_id_code',
                        'Customer_name', 'Comercial_trade_mark', 'Address_1', 'Address_2', 'Address_3', 'Zip_code', 'City', 'Country_code',
                        'Telefone', 'fax', 'European_act_code', 'Lenguaje_code', 'Payment_method', 'Delay_dranted', 'End_of_month', 'Currency', 
                        'Assignment_code_1', 'Assignment_code_2', 'Assignment_code_3', 'Assignment_code_4', 'Assignment_code_5', 'Assignment_code_6', 
                        'Assignment_code_7', 'Assignment_code_8', 'Assignment_code_9', 'Assignment_code_10', 'Number_of_items_not_yet_invoiced',  
                        'Amount_of_items_not_yet_invoiced', 'Order_Status', 'Order_status_comment', 'Order_status_date', 'Eligible_for_default_interests', 
                        'Non_B2B_customer', 'Linked_company', 'Local_currency_code', 'Invoices_not_yet_issued_amount_in_local_currency', 'Columna_1'
                        ] 
    data = data.reindex(columns=columnas_finales)
    
    # Step 13: Validation: If Zip_code is empty, assign a point
    data['Zip_code'] = data['Zip_code'].fillna('')            
    data['Zip_code'] = data['Zip_code'].astype(str).str.strip()
    data['Zip_code'] = data['Zip_code'].apply(lambda x: '.' if x == '' else x)
    
    data['Zip_code'] = data['Zip_code'].astype(str)
    data['Zip_code'] = data['Zip_code'].replace(['', 'nan', 'NaN', 'None'], '.')
    data['Zip_code'] = data['Zip_code'].fillna('.')

    # Step 14: CPA assignment from CLIs
    data_dos = data.copy()
    data_dos['Technical_customer_code'] = 'CPA-' + data_dos['Columna_1']
    data_dos = data_dos.drop('Columna_1', axis=1)
    columnas_dos = columnas_finales.copy()
    columnas_dos.remove('Columna_1')
    data_dos = data_dos.reindex(columns=columnas_dos)
    data = data.drop('Columna_1', axis=1)

    # Merge both dataframes
    dataframe = pd.concat([data_dos, data], ignore_index=True)
    
    # Asignar Record_type según el prefijo de Technical_customer_code
    dataframe['Record_type'] = dataframe['Technical_customer_code'].apply(
        lambda x: 'CPA' if str(x).startswith('CPA') else 'CLI'
    )
    
    # Clean critical fields that should not have NaN
    columnas_a_limpiar = ['Functional_customer_code', 'National_id_code', 'Telefone']
    for col in columnas_a_limpiar:
        if col in dataframe.columns:
            dataframe[col] = dataframe[col].astype(str)
            dataframe[col] = dataframe[col].replace(['nan', 'NaN', 'None'], '')
            dataframe[col] = dataframe[col].str.strip()
    
    # Clean up processed CSVs from destination folder
    for archivo in os.listdir(destino):
        archivo_path = os.path.join(destino, archivo)
        if os.path.isfile(archivo_path):
            os.remove(archivo_path)

    # ---------------------------
    # Exportar fichero final
    # ---------------------------
    try:
        fichero = f"303194-CUSTOMERS-{fecha_actual}.csv"
        ruta_fichero = os.path.join(ruta_exportacion_final, fichero)
        dataframe.to_csv(ruta_fichero, sep=";", index=False, encoding="utf-8")
    except Exception as e:
        errores_detectados = True
        errors_step_3 = pd.concat(
            [errors_step_3, pd.DataFrame([{
                "paso": "Paso 3",
                "detalle": f"Error exportando fichero final. Detalle: {e}",
                "fichero": "Customers"
            }])],
            ignore_index=True
        )
        errores_detectados = True        
    
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

