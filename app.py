import streamlit as st
import pandas as pd
from utils.extractor import run_extraction
import os
import sys

# Configurar página
st.set_page_config(
    page_title="VALD Data Extraction App",
    page_icon="📊",
    layout="centered"
)

def show_home():
    """Muestra la página principal con la descripción de la aplicación."""
    st.title("🌟 VALD Data Extraction App")
    st.write("Esta aplicación permite extraer datos de la API de VALD Performance y guardarlos en archivos CSV.")

    


def show_download():
    """Página de extracción y visualización de datos."""
    st.header("📥 Extracción de Datos")

    # Botón para iniciar extracción
    if st.button("🚀 Iniciar Proceso de Extracción", type="primary"):
        # Crear contenedor para logs
        log_container = st.empty()

        # Redirigir la salida estándar a Streamlit para mostrar logs en tiempo real
        import sys
        import time

        log_container = st.empty()
        class StreamlitWriter:
            """Escribe cada mensaje en un contenedor Streamlit en tiempo real."""
            def __init__(self, container):
                self.container = container
                self.logs = ""
            def write(self, message):
                # Añadir el nuevo mensaje y actualizar la UI
                self.logs += str(message)
                # Utilizamos code() para formato monoespaciado
                self.container.code(self.logs)
                # Pequeña pausa para permitir al frontend renderizar
                time.sleep(0.05)
            def flush(self):
                pass  # Necesario para compatibilidad con sys.stdout

        old_stdout = sys.stdout
        writer = StreamlitWriter(log_container)
        sys.stdout = writer

        status = st.status("Ejecutando el proceso de extracción…", expanded=True)
        try:
            run_extraction()
            sys.stdout = old_stdout
            status.update(label="Proceso completado ✅", state="complete")
            show_extracted_data()
        except Exception as e:
            sys.stdout = old_stdout
            status.update(label=f"Error: {e}", state="error")


def login():
    """Pantalla de inicio de sesión simple."""
    # Ocultar sidebar mientras se muestra el login
    hide_sidebar_style = """
        <style>
            [data-testid="stSidebar"] {display: none !important;}
            [data-testid="collapsedControl"] {display: none !important;}
        </style>
    """
    st.markdown(hide_sidebar_style, unsafe_allow_html=True)

    placeholder = st.empty()
    with placeholder.container():
        # Logo centrado
        cols = st.columns(3)
        with cols[1]:
            st.image("img/logo.png", use_container_width=True)

        st.subheader("Iniciar sesión")
        username = st.text_input("Usuario")
        password = st.text_input("Contraseña", type="password")

        if st.button("Entrar"):
            if username == "admin" and password == "admin":
                st.session_state.authenticated = True
                # Limpiar la UI de login y redirigir a Home
                placeholder.empty()
                st.switch_page("pages/home.py")
            else:
                st.error("❌ Credenciales incorrectas")


def main():
    """Solo muestra login y redirige a home.py tras autenticarse."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        login()
    else:
        # Redirigir a la página Home para que quede seleccionada en la navegación automática
        st.switch_page("pages/home.py")

def show_extracted_data():
    """Muestra los datos extraídos si existen"""
    output_dir = "output_data"
    
    if not os.path.exists(output_dir):
        st.warning("No se encontró la carpeta de datos extraídos.")
        return
    
    # Obtener lista de archivos CSV
    csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
    
    if not csv_files:
        st.info("No se encontraron archivos CSV en la carpeta de datos extraídos.")
        return
    
    st.header("📊 Datos Extraídos")
    
    # Mostrar selector de archivos
    selected_file = st.selectbox("Seleccionar archivo para visualizar:", csv_files)
    
    if selected_file:
        file_path = os.path.join(output_dir, selected_file)
        try:
            df = pd.read_csv(file_path)
            st.write(f"**{selected_file}** - {len(df)} filas:")
            st.dataframe(df)
            
            # Opción para descargar
            st.download_button(
                label="⬇️ Descargar archivo CSV",
                data=df.to_csv(index=False).encode('utf-8'),
                file_name=selected_file,
                mime='text/csv',
            )
            
        except Exception as e:
            st.error(f"Error al cargar el archivo {selected_file}: {str(e)}")

if __name__ == "__main__":
    main()
