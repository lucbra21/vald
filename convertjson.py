#!/usr/bin/env python3
"""
Script para convertir credentials.json a formato de variable de entorno
"""

import json

def convert_json_to_env_format(json_file_path):
    """Convierte un archivo JSON a formato de variable de entorno"""
    try:
        # Leer el archivo JSON
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        # Convertir a JSON compactado (una línea)
        json_string = json.dumps(data, separators=(',', ':'))
        
        # Mostrar el resultado
        print("📋 Copia esta línea para tu .env:")
        print("=" * 50)
        print(f'GOOGLE_CREDENTIALS_JSON={json_string}')
        print("=" * 50)
        
        # También crear el archivo .env automáticamente
        with open('.env.example', 'w') as f:
            f.write("# Variables de entorno para VALD\n")
            f.write("CLIENT_ID=tu_client_id\n")
            f.write("CLIENT_SECRET=tu_client_secret\n")
            f.write("FECHA_DESDE=2023-01-01T00:00:00.000Z\n")
            f.write("SHEET_URL=tu_google_sheets_url\n")
            f.write(f"GOOGLE_CREDENTIALS_JSON={json_string}\n")
        
        print("✅ También se creó .env.example con el formato correcto")
        
    except FileNotFoundError:
        print(f"❌ No se encontró el archivo {json_file_path}")
    except json.JSONDecodeError:
        print(f"❌ El archivo {json_file_path} no es un JSON válido")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    # Usar con tu archivo de credenciales
    convert_json_to_env_format("credentials.json")
    
    # O si tienes el archivo en otra ubicación:
    # convert_json_to_env_format("~/credentials-valt.json")