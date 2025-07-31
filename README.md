# VALD Data Extraction App

Esta aplicación permite extraer datos de la API de VALD Performance y guardarlos tanto en archivos CSV locales como en Google Sheets.

## 📋 Funcionalidades

- Extracción de datos de la API de VALD Performance
  - Tenants
  - Categorías
  - Grupos
  - Perfiles de usuarios
  - Datos de pruebas NordBord
  - Datos de pruebas ForceFrame
- Guardado en archivos CSV
- Integración con Google Sheets
- Interfaz gráfica con Streamlit

## 🛠️ Instalación

1. Clona este repositorio:
```bash
git clone https://github.com/fideito10/vald.git
cd vald
```

2. Instala las dependencias:
```bash
pip install -r requirements.txt
```

3. Configura las credenciales de Google:
   - Crea un proyecto en [Google Cloud Console](https://console.cloud.google.com/)
   - Habilita las APIs de Google Sheets y Google Drive
   - Crea una cuenta de servicio y descarga las credenciales JSON
   - Guarda el archivo como `credentials.json` en la raíz del proyecto

## 🚀 Uso

Ejecuta la aplicación con Streamlit:

```bash
streamlit run app.py
```

La aplicación se abrirá en tu navegador donde podrás:
1. Iniciar el proceso de extracción
2. Visualizar los datos extraídos
3. Descargar los archivos CSV generados

## 📊 Estructura del proyecto

```
vald-extracion-app/
├── app.py                  # Aplicación principal con Streamlit
├── credentials.json        # Credenciales de Google (no incluido en el repo)
├── requirements.txt        # Dependencias del proyecto
├── output_data/            # Carpeta donde se guardan los archivos CSV
└── utils/
    ├── __init__.py
    ├── extractor.py        # Lógica de extracción de datos
    └── Extracion.ipynb     # Notebook para pruebas
```

## 📝 Configuración

Las principales configuraciones se encuentran en `utils/extractor.py`:

- `CLIENT_ID` y `CLIENT_SECRET`: Credenciales para autenticación con VALD API
- `FECHA_DESDE`: Fecha desde la cual extraer datos
- `SHEET_URL`: URL de la hoja de Google Sheets donde se guardarán los datos

## Contribuciones

Las contribuciones son bienvenidas. Si deseas contribuir, por favor abre un issue o envía un pull request.

## Licencia

Este proyecto está bajo la Licencia MIT.