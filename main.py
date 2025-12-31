# main.py
from dotenv import load_dotenv
from procesar_clientes import procesar_clientes
from procesar_documentos import procesar_documentos

def main():
    print("Iniciando automatismo...\n")
    load_dotenv() 

    procesado_clientes = procesar_clientes()
    
    if not procesado_clientes:
        procesado_documentos = procesar_documentos()
        if not procesado_documentos:
            print('\nAutomatismo finalizado.\n')
            return
    else:
        procesar_documentos()

    print("\nAutomatismo finalizado.\n")

if __name__ == "__main__":
    main()



