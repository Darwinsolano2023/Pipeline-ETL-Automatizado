# 📊 Pipeline Automatizado – Python ETL

Pipeline automatizado desarrollado en **Python** para la **extracción, limpieza, transformación y entrega de datos**, con control de ejecución por pasos, trazabilidad del proceso y preparación para ejecución programada.

El proyecto está diseñado bajo un enfoque **modular y escalable**, permitiendo ejecutar procesos de **clientes** y **documentos** de forma independiente, controlada y mantenible.

---

## 🚀 Características principales

- Pipeline ETL dividido en **5 pasos claramente definidos**
- Control de dependencias entre procesos
- Limpieza y validación de datos
- Transformación estructurada de información
- Preparación de archivos para envío externo (SFTP / integraciones)
- Registro del estado de ejecución del proceso
- Arquitectura modular y reutilizable
- Preparado para ejecución automática (cron / scheduler)

---

## 🧩 Arquitectura del Pipeline

El sistema se organiza en **dos flujos principales**:

### 🔹 Flujo de Clientes
Orquestado desde `procesar_clientes.py`, ejecuta los siguientes pasos:

- `step_1.py`
- `step_2.py`
- `step_3.py`
- `step_4.py`
- `step_5.py`

Cada archivo representa una **etapa específica del proceso ETL**, facilitando mantenimiento y depuración.

---

### 🔹 Flujo de Documentos
Orquestado desde `procesar_documentos.py`, ejecuta los pasos:

- `step_1_docs.py`
- `step_2_docs.py`
- `step_3_docs.py`
- `step_4_docs.py`

Este flujo depende del resultado exitoso del proceso de clientes.

---

## 📂 Estructura del Proyecto

```
Algoritmo - SUC/
│
├── main.py
├── procesar_clientes.py
├── procesar_documentos.py
│
├── step_1.py
├── step_2.py
├── step_3.py
├── step_4.py
├── step_5.py
│
├── step_1_docs.py
├── step_2_docs.py
├── step_3_docs.py
├── step_4_docs.py
│
├── .env
```

---

## ⚙️ Requisitos

- Python **3.10 o superior**
- Entorno virtual recomendado
- Acceso a servicios externos (DB, SFTP, SMTP)

---

## 🔐 Configuración

Crear un archivo `.env` en la raíz del proyecto:

```env
DB_HOST=localhost
DB_NAME=database_name
DB_USER=user
DB_PASSWORD=password

SFTP_HOST=host
SFTP_USER=user
SFTP_PASSWORD=password

SMTP_HOST=smtp_host
SMTP_USER=smtp_user
SMTP_PASSWORD=smtp_password
```

---

## ▶️ Ejecución

```bash
python main.py
```

---

## 🧠 Buenas Prácticas

- Separación clara de responsabilidades
- Código modular y escalable
- Preparado para automatización y cloud

---

## 👨‍💻 Autor

**Darwin Solano**  
Ingeniero de Software | Data & Automation
