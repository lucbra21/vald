import requests
import pandas as pd
import json
import base64
import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import traceback
from dotenv import load_dotenv

# from utils.extractor_v2 import df_all_forcedecks

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de VALD
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
FECHA_DESDE = os.getenv('FECHA_DESDE')

# Directorio ABSOLUTO para guardar CSV (relativo a este archivo)
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output_data"
OUTPUT_DIR.mkdir(exist_ok=True)
print(f"📁 Directorio de salida: {OUTPUT_DIR}")

# Configuración para Google Sheets
#CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE', 'credentials.json')
SHEET_URL = os.getenv('SHEET_URL')




# Función para guardar DataFrame en Google Sheets
def save_to_google_sheets(df, sheet_name):
    try:
        print(f"🔄 Guardando datos en Google Sheets (hoja: {sheet_name})...")
        
        # Convertir DataFrame a una copia para no modificar el original
        df_copy = df.copy()
        
        # Convertir todos los tipos de datos a string para asegurar compatibilidad
        for col in df_copy.columns:
            # Primero manejar columnas con tipos específicos
            if df_copy[col].dtype.name.startswith('datetime'):
                df_copy[col] = df_copy[col].astype(str)
            elif 'object' in str(df_copy[col].dtype):
                # Para columnas tipo object, convertir cada valor individualmente
                df_copy[col] = df_copy[col].apply(lambda x: str(x) if x is not None else '')
        
        # Convertir valores NaN, None o NaT a cadenas vacías
        df_copy = df_copy.fillna('')
        
        # Definir el scope
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # 🔄 AUTENTICACIÓN FLEXIBLE - funciona local y en Render
        credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
        
        if credentials_json:
            # Desde variable de entorno (para Render.com)
            import json
            credentials_dict = json.loads(credentials_json)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
            print("ℹ️ Usando credenciales desde variable de entorno")
        else:
            # Desde archivo local (para desarrollo)
            creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
            print("ℹ️ Usando credenciales desde archivo local")
        
        client = gspread.authorize(creds)
        
        # Abrir la hoja de cálculo por URL (eliminar el fragmento #gid=0 si está presente)
        clean_url = SHEET_URL.split('#')[0]
        spreadsheet = client.open_by_url(clean_url)
        
        # Verificar si la hoja ya existe
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            # Si existe, limpiar contenido
            worksheet.clear()
            print(f"ℹ️ Hoja '{sheet_name}' encontrada y limpiada")
        except gspread.exceptions.WorksheetNotFound:
            # Si no existe, crearla
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=min(df_copy.shape[0]+1, 5000), cols=min(df_copy.shape[1], 26))
            print(f"ℹ️ Hoja '{sheet_name}' creada")
        
        # Preparar los datos (headers + valores)
        headers = df_copy.columns.tolist()
        
        # Convertir manualmente cada fila a lista de strings
        values = []
        for _, row in df_copy.iterrows():
            # Convertir cada valor en la fila a string
            row_values = [str(val) if val is not None else '' for val in row]
            values.append(row_values)
        
        # Verificar límites de Google Sheets
        if len(values) > 50000:
            print(f"⚠️ El dataset es muy grande ({len(values)} filas). Se guardarán las primeras 50000 filas.")
            values = values[:50000]
        
        # Actualizar la hoja con los datos
        worksheet.update([headers] + values)
        
        print(f"✅ {len(values)} registros guardados exitosamente en Google Sheets (hoja: {sheet_name})")
        return True
    except Exception as e:
        print(f"❌ Error al guardar en Google Sheets: {str(e)}")
        import traceback
        traceback.print_exc()  # Imprimir el traceback completo para debug
        return False



# Función para obtener token
def get_token():
    token_url = "https://security.valdperformance.com/connect/token"
    
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    
    try:
        response = requests.post(token_url, data=payload)
        
        if response.status_code == 200:
            token_data = response.json()
            print("✅ Autenticación exitosa")
            return token_data.get('access_token')
        else:
            print(f"❌ Error en la autenticación: {response.status_code}")
            print(response.text)
            return None
            
    except Exception as e:
        print(f"❌ Error inesperado: {str(e)}")
        return None

# Función para obtener tenants
def get_tenants(token):
    url = "https://prd-use-api-externaltenants.valdperformance.com/tenants"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            tenants_data = response.json()
            df_tenants = pd.DataFrame(tenants_data['tenants'])
            
            # Guardar a CSV
            csv_path = os.path.join(OUTPUT_DIR, "tenants.csv")
            df_tenants.to_csv(csv_path, index=False)
            print(f"✅ Datos de tenants guardados en {csv_path}")
            
            return df_tenants
        else:
            print(f"❌ Error al obtener tenants: {response.status_code}")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error inesperado: {str(e)}")
        return pd.DataFrame()

# Función para obtener categorías
def get_categories(tenant_id, token):
    url = "https://prd-use-api-externaltenants.valdperformance.com/categories"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    params = {
        "TenantId": tenant_id
    }
    
    try:
        print("🔄 Solicitando categories a la API...")
        response_categories = requests.get(url, headers=headers, params=params)
        
        if response_categories.status_code != 200:
            print(f"❌ Error al obtener categorías: {response_categories.status_code}")
            return pd.DataFrame()
            
        categories = json.loads(response_categories.content.decode('utf-8'))
        df_categories_ = pd.DataFrame(categories['categories'])
        df_categories_['tenant_id'] = tenant_id
        
        # Guardar a CSV
        csv_path = os.path.join(OUTPUT_DIR, f"categories_{tenant_id}.csv")
        df_categories_.to_csv(csv_path, index=False)
        print(f"✅ Categorías para tenant {tenant_id} guardadas en {csv_path}")
        
        return df_categories_
        
    except Exception as e:
        print(f"❌ Error al obtener categorías: {str(e)}")
        return pd.DataFrame()

# Función para obtener grupos
def get_groups(tenant_id, token):
    url = "https://prd-use-api-externaltenants.valdperformance.com/groups"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    params = {
        "TenantId": tenant_id
    }
    
    try:
        print("🔄 Solicitando grupos a la API...")
        response_groups = requests.get(url, headers=headers, params=params)
        
        if response_groups.status_code != 200:
            print(f"❌ Error al obtener grupos: {response_groups.status_code}")
            return pd.DataFrame()
            
        groups = json.loads(response_groups.content.decode('utf-8'))
        df_groups_ = pd.DataFrame(groups['groups'])
        df_groups_['tenant_id'] = tenant_id
        
        # Guardar a CSV
        csv_path = os.path.join(OUTPUT_DIR, f"groups_{tenant_id}.csv")
        df_groups_.to_csv(csv_path, index=False)
        print(f"✅ Grupos para tenant {tenant_id} guardados en {csv_path}")
        
        return df_groups_
        
    except Exception as e:
        print(f"❌ Error al obtener grupos: {str(e)}")
        return pd.DataFrame()

# Función para obtener perfiles
def get_profiles(token, tenant_id, groupId, groupName, categoryId, categoryName, df_all_profiles=None):
    url = "https://prd-use-api-externalprofile.valdperformance.com/profiles"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    params = {
        "TenantId": tenant_id,
        "groupId": groupId
    }
    
    try:
        # print(f"🔄 Solicitando perfiles para grupo {groupName}...")
        response_profiles = requests.get(url, headers=headers, params=params)
        
        if response_profiles.status_code != 200:
            print(f"⚠️ Error HTTP {response_profiles.status_code} para grupo {groupName}")
            return df_all_profiles if df_all_profiles is not None else pd.DataFrame()
        
        if not response_profiles.content or response_profiles.content == b'':
            # print(f"⚠️ Respuesta vacía para grupo {groupName}")
            return df_all_profiles if df_all_profiles is not None else pd.DataFrame()
        
        content_str = response_profiles.content.decode('utf-8')
        
        if not content_str.strip():
            # print(f"⚠️ Contenido vacío para grupo {groupName}")
            return df_all_profiles if df_all_profiles is not None else pd.DataFrame()
            
        profiles = json.loads(content_str)
        
        if 'profiles' not in profiles:
            # print(f"⚠️ Estructura JSON inesperada para grupo {groupName}")
            return df_all_profiles if df_all_profiles is not None else pd.DataFrame()
        
        if not profiles['profiles']:
            # print(f"ℹ️ No hay perfiles para el grupo {groupName}")
            return df_all_profiles if df_all_profiles is not None else pd.DataFrame()
        
        df_profiles_ = pd.DataFrame(profiles['profiles'])
        
        df_profiles_['tenant_id'] = tenant_id
        df_profiles_['groupId'] = groupId
        df_profiles_['groupName'] = groupName
        df_profiles_['categoryId'] = categoryId
        df_profiles_['categoryName'] = categoryName
        
        # Si ya tenemos un DataFrame de todos los perfiles, concatenamos
        if df_all_profiles is not None:
            df_all_profiles = pd.concat([df_all_profiles, df_profiles_], ignore_index=True)
        else:
            df_all_profiles = df_profiles_
        
        # print(f"✅ {len(df_profiles_)} perfiles obtenidos para grupo {groupName}")
        
        return df_all_profiles
        
    except json.JSONDecodeError as e:
        print(f"❌ Error JSON para grupo {groupName}: {e}")
        return df_all_profiles if df_all_profiles is not None else pd.DataFrame()
    except Exception as e:
        print(f"❌ Error inesperado para grupo {groupName}: {e}")
        return df_all_profiles if df_all_profiles is not None else pd.DataFrame()

def get_nordbord_complete(token, tenant_id, fecha_desde, profile_id=None):
    """
    Obtiene TODOS los datos de NordBord usando la paginación correcta del endpoint /tests/v2
    """
    base_url = "https://prd-use-api-externalnordbord.valdperformance.com"
    endpoint = "/tests/v2"
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    all_tests = []  # Cambiado para ser más específico
    current_modified_from = fecha_desde
    page_count = 0
    
    print(f"🔄 Iniciando obtención completa de datos NordBord para tenant {tenant_id}...")
    
    while True:
        page_count += 1
        print(f"📄 Procesando página {page_count} (desde: {current_modified_from})")
        
        params = {
            "tenantId": tenant_id,
            "modifiedFromUtc": current_modified_from
        }
        
        if profile_id:
            params["profileId"] = profile_id
        
        try:
            response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
            
            if response.status_code == 200:
                datos = response.json()
                
                # Determinar la estructura de los datos
                if isinstance(datos, list):
                    current_batch = datos
                elif isinstance(datos, dict):
                    # Buscar diferentes posibles keys
                    if 'tests' in datos:
                        current_batch = datos['tests']
                    elif 'items' in datos:
                        current_batch = datos['items']
                    elif 'data' in datos:
                        current_batch = datos['data']
                    else:
                        # Si es un dict sin keys conocidas, tratarlo como un solo elemento
                        current_batch = [datos]
                else:
                    current_batch = [datos] if datos else []
                
                if not current_batch:
                    print("✅ No hay más datos en esta respuesta")
                    break
                
                print(f"📊 Obtenidos {len(current_batch)} registros en esta página")
                
                # Agregar datos a la lista principal
                all_tests.extend(current_batch)
                
                # CLAVE: Buscar el campo de fecha de modificación para paginación
                last_record = current_batch[-1]
                next_modified_date = None
                
                # Buscar diferentes posibles nombres para el campo de fecha
                possible_date_fields = ['modifiedDateUtc', 'modifiedDate', 'lastModified', 
                                      'updatedAt', 'dateModified', 'modified']
                
                for field in possible_date_fields:
                    if field in last_record:
                        next_modified_date = last_record[field]
                        print(f"🔄 Encontrado campo '{field}': {next_modified_date}")
                        break
                
                if next_modified_date:
                    # Verificar si la fecha es la misma que la anterior (bucle infinito)
                    if next_modified_date == current_modified_from:
                        print("⚠️ Detectado bucle infinito: misma fecha de modificación")
                        print("🔄 Agregando 1 milisegundo para avanzar...")
                        
                        # Convertir a datetime, agregar 1 milisegundo, y volver a string
                        from datetime import datetime, timedelta
                        try:
                            dt = datetime.fromisoformat(next_modified_date.replace('Z', '+00:00'))
                            dt_next = dt + timedelta(milliseconds=1)
                            current_modified_from = dt_next.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                            print(f"🔄 Nueva fecha: {current_modified_from}")
                        except Exception as e:
                            print(f"❌ Error al procesar fecha: {e}")
                            break
                    else:
                        current_modified_from = next_modified_date
                        print(f"🔄 Siguiente página desde: {current_modified_from}")
                else:
                    print("⚠️ No se encontró campo de fecha de modificación en el último registro")
                    print(f"⚠️ Campos disponibles: {list(last_record.keys())}")
                    break
                
                # Protección adicional contra bucles infinitos
                if page_count > 1000:  # Máximo 1000 páginas
                    print("⚠️ Alcanzado límite máximo de páginas (1000). Deteniendo...")
                    break
                
            elif response.status_code == 204:
                print("✅ Código 204 - No hay más registros para obtener")
                break
                
            else:
                print(f"❌ Error {response.status_code}: {response.text}")
                break
                
        except Exception as e:
            print(f"❌ Error en la solicitud: {str(e)}")
            break
    
    # Convertir a DataFrame
    if all_tests:
        df = pd.DataFrame(all_tests)
        
        # Convertir columnas de fecha (sin el warning)
        date_columns = [col for col in df.columns 
                       if 'date' in col.lower() or 'time' in col.lower()]
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass  # Si no se puede convertir, mantener el formato original
        
        # Añadir información del tenant
        df['tenant_id'] = tenant_id
        
        print(f"🎉 Proceso completado: {len(df)} registros totales obtenidos en {page_count} páginas")
        return df
    else:
        print("⚠️ No se obtuvieron datos")
        return pd.DataFrame()
        
# Función para obtener datos de ForceFrame
def get_ForceFrame_complete(token, tenant_id, fecha_desde, profile_id=None):
    """
    Obtiene TODOS los datos de ForceFrame usando paginación correcta
    """
    base_url = "https://prd-use-api-externalforceframe.valdperformance.com"
    endpoint = "/tests/v2"
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    all_tests = []
    current_modified_from = fecha_desde
    page_count = 0
    
    print(f"🔄 Iniciando obtención completa de datos ForceFrame para tenant {tenant_id}...")
    
    while True:
        page_count += 1
        print(f"📄 Procesando página {page_count} (desde: {current_modified_from})")
        
        params = {
            "tenantId": tenant_id,
            "modifiedFromUtc": current_modified_from
        }
        
        if profile_id:
            params["profileId"] = profile_id
        
        try:
            response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
            
            if response.status_code == 200:
                datos = response.json()
                
                # Determinar la estructura de los datos
                if isinstance(datos, list):
                    current_batch = datos
                elif isinstance(datos, dict):
                    if 'tests' in datos:
                        current_batch = datos['tests']
                    elif 'items' in datos:
                        current_batch = datos['items']
                    elif 'data' in datos:
                        current_batch = datos['data']
                    else:
                        current_batch = [datos]
                else:
                    current_batch = [datos] if datos else []
                
                if not current_batch:
                    print("✅ No hay más datos en esta respuesta")
                    break
                
                print(f"📊 Obtenidos {len(current_batch)} registros en esta página")
                
                # Agregar datos a la lista principal
                all_tests.extend(current_batch)
                
                # Buscar el campo de fecha de modificación para paginación
                last_record = current_batch[-1]
                next_modified_date = None
                
                # Buscar diferentes posibles nombres para el campo de fecha
                possible_date_fields = ['modifiedDateUtc', 'modifiedDate', 'lastModified', 
                                      'updatedAt', 'dateModified', 'modified']
                
                for field in possible_date_fields:
                    if field in last_record:
                        next_modified_date = last_record[field]
                        print(f"🔄 Encontrado campo '{field}': {next_modified_date}")
                        break
                
                if next_modified_date:
                    # Verificar si la fecha es la misma (bucle infinito)
                    if next_modified_date == current_modified_from:
                        print("⚠️ Detectado bucle infinito: misma fecha de modificación")
                        print("🔄 Agregando 1 milisegundo para avanzar...")
                        
                        from datetime import datetime, timedelta
                        try:
                            dt = datetime.fromisoformat(next_modified_date.replace('Z', '+00:00'))
                            dt_next = dt + timedelta(milliseconds=1)
                            current_modified_from = dt_next.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                            print(f"🔄 Nueva fecha: {current_modified_from}")
                        except Exception as e:
                            print(f"❌ Error al procesar fecha: {e}")
                            break
                    else:
                        current_modified_from = next_modified_date
                        print(f"🔄 Siguiente página desde: {current_modified_from}")
                else:
                    print("⚠️ No se encontró campo de fecha de modificación")
                    print(f"⚠️ Campos disponibles: {list(last_record.keys())}")
                    break
                
                # Protección contra bucles infinitos
                if page_count > 1000:
                    print("⚠️ Alcanzado límite máximo de páginas (1000)")
                    break
                
            elif response.status_code == 204:
                print("✅ Código 204 - No hay más registros")
                break
                
            else:
                print(f"❌ Error {response.status_code}: {response.text}")
                break
                
        except Exception as e:
            print(f"❌ Error en la solicitud: {str(e)}")
            break
    
    # Convertir a DataFrame
    if all_tests:
        df = pd.DataFrame(all_tests)
        
        # Convertir columnas de fecha
        date_columns = [col for col in df.columns 
                       if 'date' in col.lower() or 'time' in col.lower()]
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass
        
        df['tenant_id'] = tenant_id
        
        print(f"🎉 Proceso completado: {len(df)} registros totales en {page_count} páginas")
        return df
    else:
        print("⚠️ No se obtuvieron datos")
        return pd.DataFrame()

def get_forcedecks_complete(token, tenant_id, fecha_desde, profile_id=None):
    """
    Obtiene TODOS los datos de NordBord usando la paginación correcta del endpoint /tests/v2
    """
    base_url = "https://prd-use-api-extforcedecks.valdperformance.com"
    endpoint = "/tests"
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    all_tests = []  # Cambiado para ser más específico
    current_modified_from = fecha_desde
    page_count = 0
    
    print(f"🔄 Iniciando obtención completa de datos ForceDecks para tenant {tenant_id}...")
    
    while True:
        page_count += 1
        print(f"📄 Procesando página {page_count} (desde: {current_modified_from})")
        
        params = {
            "tenantId": tenant_id,
            "modifiedFromUtc": current_modified_from
        }
        
        if profile_id:
            params["profileId"] = profile_id
        
        try:
            response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
            
            if response.status_code == 200:
                datos = response.json()
                
                # Determinar la estructura de los datos
                if isinstance(datos, list):
                    current_batch = datos
                elif isinstance(datos, dict):
                    # Buscar diferentes posibles keys
                    if 'tests' in datos:
                        current_batch = datos['tests']
                    elif 'items' in datos:
                        current_batch = datos['items']
                    elif 'data' in datos:
                        current_batch = datos['data']
                    else:
                        # Si es un dict sin keys conocidas, tratarlo como un solo elemento
                        current_batch = [datos]
                else:
                    current_batch = [datos] if datos else []
                
                if not current_batch:
                    print("✅ No hay más datos en esta respuesta")
                    break
                
                print(f"📊 Obtenidos {len(current_batch)} registros en esta página")
                
                # Agregar datos a la lista principal
                all_tests.extend(current_batch)
                
                # CLAVE: Buscar el campo de fecha de modificación para paginación
                last_record = current_batch[-1]
                next_modified_date = None
                
                # Buscar diferentes posibles nombres para el campo de fecha
                possible_date_fields = ['modifiedDateUtc', 'modifiedDate', 'lastModified', 
                                      'updatedAt', 'dateModified', 'modified']
                
                for field in possible_date_fields:
                    if field in last_record:
                        next_modified_date = last_record[field]
                        print(f"🔄 Encontrado campo '{field}': {next_modified_date}")
                        break
                
                if next_modified_date:
                    # Verificar si la fecha es la misma que la anterior (bucle infinito)
                    if next_modified_date == current_modified_from:
                        print("⚠️ Detectado bucle infinito: misma fecha de modificación")
                        print("🔄 Agregando 1 milisegundo para avanzar...")
                        
                        # Convertir a datetime, agregar 1 milisegundo, y volver a string
                        from datetime import datetime, timedelta
                        try:
                            dt = datetime.fromisoformat(next_modified_date.replace('Z', '+00:00'))
                            dt_next = dt + timedelta(milliseconds=1)
                            current_modified_from = dt_next.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                            print(f"🔄 Nueva fecha: {current_modified_from}")
                        except Exception as e:
                            print(f"❌ Error al procesar fecha: {e}")
                            break
                    else:
                        current_modified_from = next_modified_date
                        print(f"🔄 Siguiente página desde: {current_modified_from}")
                else:
                    print("⚠️ No se encontró campo de fecha de modificación en el último registro")
                    print(f"⚠️ Campos disponibles: {list(last_record.keys())}")
                    break
                
                # Protección adicional contra bucles infinitos
                if page_count > 1000:  # Máximo 1000 páginas
                    print("⚠️ Alcanzado límite máximo de páginas (1000). Deteniendo...")
                    break
                
            elif response.status_code == 204:
                print("✅ Código 204 - No hay más registros para obtener")
                break
                
            else:
                print(f"❌ Error {response.status_code}: {response.text}")
                break
                
        except Exception as e:
            print(f"❌ Error en la solicitud: {str(e)}")
            break
    
    # Convertir a DataFrame
    if all_tests:
        df = pd.DataFrame(all_tests)
        
        # Convertir columnas de fecha (sin el warning)
        date_columns = [col for col in df.columns 
                       if 'date' in col.lower() or 'time' in col.lower()]
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass  # Si no se puede convertir, mantener el formato original
        
        # Añadir información del tenant
        df['tenant_id'] = tenant_id
        
        print(f"🎉 Proceso completado: {len(df)} registros totales obtenidos en {page_count} páginas")
        return df
    else:
        print("⚠️ No se obtuvieron datos")
        return pd.DataFrame()


# ======== NUEVA FUNCIÓN CON LOGS EN TIEMPO REAL ========

def run_extraction_with_realtime_logs(log_cb, progress_cb):
    """Ejecuta la extracción usando callbacks para logs y progreso en vivo."""
    total_steps = 8
    step = 1

    # 1. Autenticación
    log_cb("🔐 Paso 1/8: Autenticando...")
    progress_cb(step, total_steps, "Autenticación")
    token = get_token()
    step += 1

    # 2. Obtener tenants
    log_cb("🏢 Paso 2/8: Obteniendo tenants...")
    progress_cb(step, total_steps, "Tenants")
    df_tenants = get_tenants(token)
    tenant_id = df_tenants.iloc[0]['id'] if not df_tenants.empty else None
    step += 1

    # 3. Configuración (categories, groups, profiles)
    log_cb("⚙️ Paso 3/8: Procesando categorías, grupos y perfiles...")
    progress_cb(step, total_steps, "Config")
    df_categories = get_categories(tenant_id, token)
    # Mantener solo la categoría CBMM
    df_categories = df_categories[df_categories['name'].str.upper() == 'CBMM']
    df_groups = get_groups(tenant_id, token)
    df_profiles = pd.DataFrame()
    if not df_categories.empty and not df_groups.empty:
        # filtrar grupos dentro de las categorías CBMM si aplica
        if not df_categories.empty:
            df_groups = df_groups[df_groups['categoryId'].isin(df_categories['id'])]
        total_grps = len(df_groups)
        for idx, grp in df_groups.iterrows():
            log_cb(f"   👥 Grupo {idx+1}/{total_grps}: {grp['name']}")
            progress_cb(step, total_steps, f"Perfiles {idx+1}/{total_grps}")
            df_profiles = get_profiles(
                token,
                tenant_id,
                grp['id'],       # groupId
                grp['name'],     # groupName
                grp['categoryId'],
                df_categories[df_categories['id']==grp['categoryId']]['name'].values[0] if not df_categories.empty else '',
                df_profiles
            )
    step += 1

    # 4. Extraer NordBord
    log_cb("🦵 Paso 4/8: Extrayendo NordBord...")
    progress_cb(step, total_steps, "NordBord")
    FECHA_DESDE = "2020-01-01T00:00:00Z"
    df_all_nordbord = get_nordbord_complete(token, tenant_id, FECHA_DESDE)
    step += 1

    # 5. Extraer ForceFrame
    log_cb("🏋️ Paso 5/8: Extrayendo ForceFrame...")
    progress_cb(step, total_steps, "ForceFrame")
    df_all_forceframe = get_ForceFrame_complete(token, tenant_id, FECHA_DESDE)
    step += 1

    # 5. Extraer ForceDescks
    log_cb("🏋️ Paso 6/8: Extrayendo ForceDecks...")
    progress_cb(step, total_steps, "ForceDecks")
    df_all_forcedecks = get_forcedecks_complete(token, tenant_id, FECHA_DESDE)
    step += 1

    # 6. Guardar CSV
    log_cb("💾 Paso 7/8: Guardando CSV...")
    progress_cb(step, total_steps, "Guardar CSV")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if not df_profiles.empty:
        df_profiles.to_csv(os.path.join(OUTPUT_DIR, "all_profiles.csv"), index=False)
    if not df_all_nordbord.empty:
        df_all_nordbord.to_csv(os.path.join(OUTPUT_DIR, "all_nordbord.csv"), index=False)
    if not df_all_forceframe.empty:
        df_all_forceframe.to_csv(os.path.join(OUTPUT_DIR, "all_forceframe.csv"), index=False)
    if not df_all_forcedecks.empty:
        df_all_forcedecks.to_csv(os.path.join(OUTPUT_DIR, "all_forcedecks.csv"), index=False)
    step += 1

    # 7. Guardar en Google Sheets
    log_cb("📤 Paso 8/8: Guardando en Google Sheets...")
    progress_cb(step, total_steps, "Google Sheets")
    if not df_profiles.empty:
        save_to_google_sheets(df_profiles, "Perfiles_VALD")
    if not df_all_nordbord.empty:
        save_to_google_sheets(df_all_nordbord, "NordBord_VALD")
    if not df_all_forceframe.empty:
        save_to_google_sheets(df_all_forceframe, "ForceFrame_VALD")
    if not df_all_forcedecks.empty:
        save_to_google_sheets(df_all_forcedecks, "ForceDecks_VALD")

    log_cb("✅ Extracción completada")
    progress_cb(total_steps, total_steps, "Completado")

# Función principal para ejecutar todo el proceso
def run_extraction():
    # Obtener token
    token = get_token()
    if not token:
        print("❌ No se pudo obtener el token. Proceso cancelado.")
        return
    
    # Obtener tenants
    df_tenants = get_tenants(token)
    if df_tenants.empty:
        print("❌ No se pudieron obtener los tenants. Proceso cancelado.")
        return
    
    # Inicializar DataFrames para consolidar datos
    df_all_profiles = pd.DataFrame()
    df_all_nordbord = pd.DataFrame()
    df_all_forceframe = pd.DataFrame()
    df_all_forcedecks = pd.DataFrame()
    df_all_dynamo = pd.DataFrame()
    df_all_humantrak = pd.DataFrame()
    df_all_smartspeed = pd.DataFrame()

    # Procesar cada tenant
    for index, tenant in df_tenants.iterrows():
        tenant_id = tenant['id']
        tenant_name = tenant['name']
        print(f"\n🔍 Procesando tenant {index+1}/{len(df_tenants)}: {tenant_name}")
        
        # Obtener categorías
        df_categories = get_categories(tenant_id, token)
        # Mantener solo la categoría CBMM
        df_categories = df_categories[df_categories['name'].str.upper() == 'CBMM']

        # Obtener grupos
        df_groups = get_groups(tenant_id, token)
        if not df_categories.empty:
            df_groups = df_groups[df_groups['categoryId'].isin(df_categories['id'])]

        # Si hay categorías y grupos, combinarlos
        if not df_categories.empty and not df_groups.empty:
            # Combinar grupos con categorías
            df_groups_with_category = df_groups.merge(
                df_categories[['id', 'name']].rename(columns={'name': 'category_name'}),
                left_on='categoryId',
                right_on='id',
                how='left'
            )
            
            # Limpiar columnas duplicadas
            if 'id_y' in df_groups_with_category.columns:
                df_groups_with_category = df_groups_with_category.drop(columns=['id_y'])
                df_groups_with_category = df_groups_with_category.rename(columns={'id_x': 'id'})
            
            # Guardar grupos con categorías
            csv_path = os.path.join(OUTPUT_DIR, f"groups_with_categories_{tenant_id}.csv")
            df_groups_with_category.to_csv(csv_path, index=False)
            print(f"✅ Grupos con categorías guardados en {csv_path}")
            
            # Obtener perfiles por grupo
            print("\n📊 Obteniendo perfiles para cada grupo...")
            for idx, group in df_groups_with_category.iterrows():
                df_all_profiles = get_profiles(
                    token, 
                    tenant_id, 
                    group['id'], 
                    group['name'], 
                    group['categoryId'], 
                    group.get('category_name', ''),
                    df_all_profiles
                )
        
        # Obtener datos de NordBord
        print("\n📊 Obteniendo datos de NordBord...")
        #df_all_nordbord = get_nordbord(token, tenant_id, FECHA_DESDE, df_all_nordbord)
        df_all_nordbord = get_nordbord_complete(token, tenant_id, FECHA_DESDE)
        print("\n📊 Fin de Obtener datos de NordBord...")
        if not df_all_nordbord.empty:
            
            # Guardar en CSV local
            csv_path = os.path.join(OUTPUT_DIR, "all_nordbord.csv")
            df_all_nordbord.to_csv(csv_path, index=False)
            print(f"✅ Total de {len(df_all_nordbord)} datos NordBord guardados en {csv_path} llll")
            
            # Guardar en Google Sheets
            save_to_google_sheets(df_all_nordbord, "NordBord_VALD")

        # Obtener datos de ForceFrame
        print("\n📊 Obteniendo datos de ForceFrame...")
        df_all_forceframe = get_ForceFrame_complete(token, tenant_id, FECHA_DESDE)
        print("\n📊 Fin de Obtener datos de ForceFrame...")
        if not df_all_forceframe.empty:
            # Guardar en CSV local
            csv_path = os.path.join(OUTPUT_DIR, "all_forceframe.csv")
            df_all_forceframe.to_csv(csv_path, index=False)
            print(f"✅ Total de {len(df_all_forceframe)} datos ForceFrame guardados en {csv_path}")
            
            # Guardar en Google Sheets
            save_to_google_sheets(df_all_forceframe, "ForceFrame_VALD")

        # Obtener datos de ForceDecks
        print("\n📊 Obteniendo datos de ForceDecks...")
        df_all_forcedecks = get_forcedecks_complete(token, tenant_id, FECHA_DESDE)
        print("\n📊 Fin de Obtener datos de ForceDecks...")
        if not df_all_forcedecks.empty:
            # Guardar en CSV local
            csv_path = os.path.join(OUTPUT_DIR, "all_forcedecks.csv")
            df_all_forcedecks.to_csv(csv_path, index=False)
            print(f"✅ Total de {len(df_all_forcedecks)} datos ForceDecks guardados en {csv_path}")
            
            # Guardar en Google Sheets
            save_to_google_sheets(df_all_forcedecks, "ForceDecks_VALD")

    # Guardar todos los DataFrames consolidados
    if not df_all_profiles.empty:
        # Guardar en CSV local
        csv_path = os.path.join(OUTPUT_DIR, "all_profiles.csv")
        df_all_profiles.to_csv(csv_path, index=False)
        print(f"✅ Total de {len(df_all_profiles)} perfiles guardados en {csv_path}")
        
        # Guardar en Google Sheets
        save_to_google_sheets(df_all_profiles, "Perfiles_VALD")
    
    
    
    # También guardar los tenants en Google Sheets
    if not df_tenants.empty:
        save_to_google_sheets(df_tenants, "Tenants_VALD")
    
    print("\n✅ Proceso de extracción completado")

# Si se ejecuta directamente este archivo
if __name__ == "__main__":
    run_extraction()