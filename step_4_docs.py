import os
import paramiko
from dotenv import load_dotenv
import pandas as pd

def main():
    load_dotenv()

    # Variables de entorno
    hostname_sftp = os.getenv('HOSTNAME_SFTP')
    port_sftp = int(os.getenv('PORT_SFTP'))
    username_sftp = os.getenv('USERNAME_SFTP')
    private_key_path_sftp = os.getenv('PRIVATE_KEY_PATH_SFTP')
    private_key_password_sftp = os.getenv('PRIVATE_KEY_PASSWORD_SFTP')

    # Cargar rutas
    csv_directory = "./Version 2/Salida/"

    # DataFrame de errores
    errors_step_4 = pd.DataFrame(columns=["paso", "detalle", "fichero"])
    errores_detectados = False

    # ---------------------------
    # Validar si hay archivos CSV
    # ---------------------------
    try:
        csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]
        if not csv_files:
            raise FileNotFoundError("No se encontraron archivos CSV en la carpeta de envío.")
    except Exception as e:
        errores_detectados = True
        errors_step_4 = pd.concat(
            [errors_step_4, pd.DataFrame([{
                "paso": "Paso 4",
                "detalle": str(e),
                "fichero": "Items"
            }])],
            ignore_index=True
        )
        result_step_4 = "Error"
        print("Paso 4 = Error")
        return result_step_4, errors_step_4

    # ---------------------------
    # Validar conexión SFTP
    # ---------------------------
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        private_key = paramiko.RSAKey.from_private_key_file(
            private_key_path_sftp, password=private_key_password_sftp
        )
        client.connect(hostname_sftp, port=port_sftp, username=username_sftp, pkey=private_key)

        # Solo validar conexión
        sftp = client.open_sftp()
        sftp.close()
        client.close()
        result_step_4 = "Ok"

    except Exception as e:
        errores_detectados = True
        errors_step_4 = pd.concat(
            [errors_step_4, pd.DataFrame([{
                "paso": "Paso 4",
                "detalle": f"Error al conectar al servidor SFTP: {e}",
                "fichero": "Items"
            }])],
            ignore_index=True
        )
        result_step_4 = "Error"

    # ---------------------------
    # Subir al SFTP (solo si conexión fue Ok)
    # ---------------------------
    if result_step_4 == "Ok":
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            private_key = paramiko.RSAKey.from_private_key_file(
                private_key_path_sftp, password=private_key_password_sftp
            )
            client.connect(hostname_sftp, port=port_sftp, username=username_sftp, pkey=private_key)

            sftp = client.open_sftp()
            for csv_file in csv_files:
                origen = os.path.join(csv_directory, csv_file)
                destino = f"/{csv_file}"
                sftp.put(origen, destino)

            sftp.close()
            client.close()

        except:
            pass
    
    # ---------------------------
    # Eliminar CSVs de la carpeta (si existen)
    # ---------------------------
    #for csv_file in csv_files:
        #try:
            #file_path = os.path.join(csv_directory, csv_file)
            #if os.path.exists(file_path):
                #os.remove(file_path)
        #except:
            #pass

    # ---------------------------
    # Mensaje final
    # ---------------------------
    if not errores_detectados and errors_step_4.empty:
        print("Paso 4 = Ok")
        result_step_4 = "Ok"
    else:
        print("Paso 4 = Error")
        result_step_4 = "Error"

    return result_step_4, errors_step_4


if __name__ == "__main__":
    result_step_4, errors_step_4 = main()
