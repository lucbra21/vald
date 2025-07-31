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

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de VALD
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
FECHA_DESDE = os.getenv('FECHA_DESDE')

# Crear directorio para guardar los CSV si no existe
OUTPUT_DIR = "output_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

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
    url = "https://prd-aue-api-externaltenants.valdperformance.com/tenants"
    
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
    url = "https://prd-aue-api-externaltenants.valdperformance.com/categories"
    
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
    url = "https://prd-aue-api-externaltenants.valdperformance.com/groups"
    
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
    url = "https://prd-aue-api-externalprofile.valdperformance.com/profiles"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    params = {
        "TenantId": tenant_id,
        "groupId": groupId
    }
    
    try:
        print(f"🔄 Solicitando perfiles para grupo {groupName}...")
        response_profiles = requests.get(url, headers=headers, params=params)
        
        if response_profiles.status_code != 200:
            print(f"⚠️ Error HTTP {response_profiles.status_code} para grupo {groupName}")
            return df_all_profiles if df_all_profiles is not None else pd.DataFrame()
        
        if not response_profiles.content or response_profiles.content == b'':
            print(f"⚠️ Respuesta vacía para grupo {groupName}")
            return df_all_profiles if df_all_profiles is not None else pd.DataFrame()
        
        content_str = response_profiles.content.decode('utf-8')
        
        if not content_str.strip():
            print(f"⚠️ Contenido vacío para grupo {groupName}")
            return df_all_profiles if df_all_profiles is not None else pd.DataFrame()
            
        profiles = json.loads(content_str)
        
        if 'profiles' not in profiles:
            print(f"⚠️ Estructura JSON inesperada para grupo {groupName}")
            return df_all_profiles if df_all_profiles is not None else pd.DataFrame()
        
        if not profiles['profiles']:
            print(f"ℹ️ No hay perfiles para el grupo {groupName}")
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
        
        print(f"✅ {len(df_profiles_)} perfiles obtenidos para grupo {groupName}")
        
        return df_all_profiles
        
    except json.JSONDecodeError as e:
        print(f"❌ Error JSON para grupo {groupName}: {e}")
        return df_all_profiles if df_all_profiles is not None else pd.DataFrame()
    except Exception as e:
        print(f"❌ Error inesperado para grupo {groupName}: {e}")
        return df_all_profiles if df_all_profiles is not None else pd.DataFrame()

# Función para obtener datos de NordBord
def get_nordbord(token, tenant_id, fecha_desde, df_all_nordbord=None):
    base_url = "https://prd-aue-api-externalnordbord.valdperformance.com"
    
    endpoint = "/tests/v2"
    params = {
        "tenantId": tenant_id,
        "modifiedFromUtc": fecha_desde
    }
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    try:
        print(f"🔄 Solicitando datos NordBord para tenant {tenant_id}...")
        response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
        
        if response.status_code == 200:
            print(f"✅ Datos obtenidos correctamente")
            datos = response.json()
        
            if isinstance(datos, dict) and 'items' in datos:
                df = pd.DataFrame(datos['items'])
            elif isinstance(datos, list):
                df = pd.DataFrame(datos)
            else:
                df = pd.DataFrame([datos])
            
            if 'tests' in df.columns and len(df) > 0 and isinstance(df['tests'].iloc[0], (list, dict)):
                try:
                    expanded_df = pd.json_normalize(df['tests'].iloc[0])
                    
                    date_columns = [col for col in expanded_df.columns 
                                    if 'date' in col.lower() or 'time' in col.lower()]
                    for col in date_columns:
                        expanded_df[col] = pd.to_datetime(expanded_df[col], errors='ignore')
                    
                    df = expanded_df
                    print("✅ Normalización aplicada a la columna 'tests'")
                except Exception as e:
                    print(f"⚠️ No se pudo normalizar la columna 'tests': {e}")
            
            # Añadir información del tenant
            df['tenant_id'] = tenant_id
            
            # Si ya tenemos un DataFrame, concatenamos
            if df_all_nordbord is not None and not df.empty:
                df_all_nordbord = pd.concat([df_all_nordbord, df], ignore_index=True)
            elif not df.empty:
                df_all_nordbord = df
            
            return df_all_nordbord
            
        elif response.status_code == 204:
            print("⚠️ No hay más registros para obtener.")
            return df_all_nordbord if df_all_nordbord is not None else pd.DataFrame()
        else:
            print(f"❌ Error al obtener datos: {response.status_code}")
            print(response.text)
            return df_all_nordbord if df_all_nordbord is not None else pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error al obtener datos NordBord: {str(e)}")
        return df_all_nordbord if df_all_nordbord is not None else pd.DataFrame()

# Función para obtener datos de ForceFrame
def get_ForceFrame(token, tenant_id, fecha_desde, df_all_forceframe=None):
    base_url = "https://prd-aue-api-externalforceframe.valdperformance.com"
    
    endpoint = "/tests/v2"
    params = {
        "tenantId": tenant_id,
        "modifiedFromUtc": fecha_desde
    }
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    try:
        print(f"🔄 Solicitando datos ForceFrame para tenant {tenant_id}...")
        response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
        
        if response.status_code == 200:
            print(f"✅ Datos obtenidos correctamente")
            datos = response.json()
        
            if isinstance(datos, dict) and 'items' in datos:
                df = pd.DataFrame(datos['items'])
            elif isinstance(datos, list):
                df = pd.DataFrame(datos)
            else:
                df = pd.DataFrame([datos])
            
            if 'tests' in df.columns and len(df) > 0 and isinstance(df['tests'].iloc[0], (list, dict)):
                try:
                    expanded_df = pd.json_normalize(df['tests'].iloc[0])
                    
                    date_columns = [col for col in expanded_df.columns 
                                    if 'date' in col.lower() or 'time' in col.lower()]
                    for col in date_columns:
                        expanded_df[col] = pd.to_datetime(expanded_df[col], errors='ignore')
                    
                    df = expanded_df
                    print("✅ Normalización aplicada a la columna 'tests'")
                except Exception as e:
                    print(f"⚠️ No se pudo normalizar la columna 'tests': {e}")
            
            # Añadir información del tenant
            df['tenant_id'] = tenant_id
            
            # Si ya tenemos un DataFrame, concatenamos
            if df_all_forceframe is not None and not df.empty:
                df_all_forceframe = pd.concat([df_all_forceframe, df], ignore_index=True)
            elif not df.empty:
                df_all_forceframe = df
            
            return df_all_forceframe
            
        elif response.status_code == 204:
            print("⚠️ No hay más registros para obtener.")
            return df_all_forceframe if df_all_forceframe is not None else pd.DataFrame()
        else:
            print(f"❌ Error al obtener datos: {response.status_code}")
            print(response.text)
            return df_all_forceframe if df_all_forceframe is not None else pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error al obtener datos ForceFrame: {str(e)}")
        return df_all_forceframe if df_all_forceframe is not None else pd.DataFrame()

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
        df_categories = df_categories[df_categories['name'] == 'CBMM']

        # Obtener grupos
        df_groups = get_groups(tenant_id, token)
        df_groups = df_groups[df_groups['categoryId'] == '0e7bfdff-aabb-4efa-ad13-ecb419e0beef']

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
        df_all_nordbord = get_nordbord(token, tenant_id, FECHA_DESDE, df_all_nordbord)
        
        # Obtener datos de ForceFrame
        print("\n📊 Obteniendo datos de ForceFrame...")
        df_all_forceframe = get_ForceFrame(token, tenant_id, FECHA_DESDE, df_all_forceframe)
    
    # Guardar todos los DataFrames consolidados
    if not df_all_profiles.empty:
        # Guardar en CSV local
        csv_path = os.path.join(OUTPUT_DIR, "all_profiles.csv")
        df_all_profiles.to_csv(csv_path, index=False)
        print(f"✅ Total de {len(df_all_profiles)} perfiles guardados en {csv_path}")
        
        # Guardar en Google Sheets
        save_to_google_sheets(df_all_profiles, "Perfiles_VALD")
        
    if not df_all_nordbord.empty:
        # Guardar en CSV local
        csv_path = os.path.join(OUTPUT_DIR, "all_nordbord.csv")
        df_all_nordbord.to_csv(csv_path, index=False)
        print(f"✅ Total de {len(df_all_nordbord)} datos NordBord guardados en {csv_path}")
        
        # Guardar en Google Sheets
        save_to_google_sheets(df_all_nordbord, "NordBord_VALD")
        
    if not df_all_forceframe.empty:
        # Guardar en CSV local
        csv_path = os.path.join(OUTPUT_DIR, "all_forceframe.csv")
        df_all_forceframe.to_csv(csv_path, index=False)
        print(f"✅ Total de {len(df_all_forceframe)} datos ForceFrame guardados en {csv_path}")
        
        # Guardar en Google Sheets
        save_to_google_sheets(df_all_forceframe, "ForceFrame_VALD")
    
    # También guardar los tenants en Google Sheets
    if not df_tenants.empty:
        save_to_google_sheets(df_tenants, "Tenants_VALD")
    
    print("\n✅ Proceso de extracción completado")

# Si se ejecuta directamente este archivo
if __name__ == "__main__":
    run_extraction()