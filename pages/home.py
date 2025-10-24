import streamlit as st
import pandas as pd
import os
import altair as alt
import time
from datetime import datetime
from utils.extractor import run_extraction_with_realtime_logs

# Configurar página
st.set_page_config(
    page_title="VALD Data Extraction App",
    page_icon="📊",
    layout="wide"
)

def show_home():
    """Página principal con información de la aplicación."""
    st.title("🌟 VALD Data Extraction App")
    st.write("Esta aplicación permite extraer datos de la API de VALD Performance y guardarlos en archivos CSV.")


def show_extracted_data():
    """Muestra los datos extraídos si existen."""
    output_dir = "output_data"
    if not os.path.exists(output_dir):
        st.warning("No se encontró la carpeta de datos extraídos.")
        return

    csv_files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
    if not csv_files:
        st.info("No se encontraron archivos CSV en la carpeta de datos extraídos.")
        return

    st.header("📊 Datos Extraídos")
    selected_file = st.selectbox("Seleccionar archivo para visualizar:", csv_files)
    if selected_file:
        file_path = os.path.join(output_dir, selected_file)
        try:
            df = pd.read_csv(file_path)
            st.write(f"**{selected_file}** - {len(df)} filas:")
            st.dataframe(df)
            st.download_button(
                label="⬇️ Descargar archivo CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name=selected_file,
                mime="text/csv",
            )
        except Exception as e:
            st.error(f"Error al cargar el archivo {selected_file}: {str(e)}")


def show_download():
    """Página de extracción y descarga de datos."""
    st.header("📥 Extracción de Datos")
    with st.expander("ℹ️ Información de la aplicación", expanded=False):
        st.markdown("""
        Esta aplicación extrae datos de la API de VALD Performance incluyendo:
        - Tenants
        - Categorías
        - Grupos
        - Perfiles de usuarios
        - Datos de pruebas NordBord
        - Datos de pruebas ForceFrame

        Los datos se guardan en archivos CSV en la carpeta `output_data`.
        """)
    if st.button("🚀 Iniciar Proceso de Extracción", type="primary"):
        log_area = st.empty()
        progress_bar = st.progress(0.0)

        def log_cb(message: str):
            timestamp = datetime.now().strftime("%H:%M:%S")
            log_area.write(f"{timestamp} | {message}")

        def progress_cb(current: int, total: int, text: str):
            progress_bar.progress(current / total, text)

        try:
            run_extraction_with_realtime_logs(log_cb, progress_cb)
            progress_bar.progress(1.0, "Completado")
            st.success("✅ Proceso de extracción completado con éxito")
            show_extracted_data()
        except Exception as e:
            st.error(f"❌ Error durante la extracción: {e}")


def show_nordbord():
    """Muestra los datos del archivo all_nordbord.csv"""
    csv_path = "utils/output_data/all_nordbord.csv"
    st.header("🦵 Datos NordBord")
    if not os.path.exists(csv_path):
        st.warning("El archivo all_nordbord.csv no existe. Ejecuta la extracción primero.")
        return
    try:
        df_tests = pd.read_csv(csv_path)
        profiles_path = "utils/output_data/all_profiles.csv"
        if os.path.exists(profiles_path):
            df_profiles = pd.read_csv(profiles_path)[["profileId","givenName","familyName","dateOfBirth","groupName"]]
            df = df_tests.merge(df_profiles, on="profileId", how="left")
            # Eliminar tests sin perfil asociado
            df = df.dropna(subset=["givenName","familyName","dateOfBirth"])
            # Forzar columna Grupo a partir de groupName y limpiar
            df["Grupo"] = df["groupName"]
            # Renombrar columnas
            df = df.rename(columns={"givenName": "Nombre", "familyName": "Apellido", "dateOfBirth": "Fecha de nacimiento", "testTypeName": "Test", "device": "Dispositivo", "leftAvgForce": "L Avg Force (N)", "rightAvgForce": "R Avg Force (N)", "leftMaxForce": "L Max Force (N)", "rightMaxForce": "R Max Force (N)", "leftTorque": "L Max Torque (Nm)", "rightTorque": "R Max Torque (Nm)", "leftRepetitions": "L Reps", "rightRepetitions": "R Reps", "leftImpulse": "L Max Impulse (Ns)", "rightImpulse": "R Max Impulse (Ns)","notes": "Notas"})

            # ------------------ Filtros ------------------
            # Fecha del test como columna fecha
            df["Fecha Test"] = pd.to_datetime(df_tests["testDateUtc"], errors="coerce").dt.date
            
            # ---- Filtros (primera fila) ----
            fecha_series = df["Fecha Test"].dropna()
            min_date = fecha_series.min() if not fecha_series.empty else None
            max_date = fecha_series.max() if not fecha_series.empty else None

            grupos = sorted(df["Grupo"].dropna().unique()) if "Grupo" in df.columns else []
            test_types = sorted(df["Test"].dropna().unique())

            col1, col2, col3 = st.columns(3)
            with col1:
                fecha_inicio, fecha_fin = st.date_input("Rango de fechas", (min_date, max_date), key="nb_fecha")
            with col2:
                sel_grupo = st.multiselect("Plantel", options=grupos, default=[], key="nb_grupo")
            with col3:
                sel_tests = st.multiselect("Tipo de test", options=test_types, default=[], key="nb_test")

            # Aplicar filtros seleccionados
            if fecha_inicio and fecha_fin:
                df = df[df["Fecha Test"].notna() & (df["Fecha Test"] >= fecha_inicio) & (df["Fecha Test"] <= fecha_fin)]
            if sel_grupo:
                df = df[df["Grupo"].isin(sel_grupo)]
            if sel_tests:
                df = df[df["Test"].isin(sel_tests)]

            # ---- Filtro Jugador (segunda fila) ----
            df["Jugador"] = (df["Nombre"] + " " + df["Apellido"]).str.strip()
            jugadores = sorted(df["Jugador"].unique())
            sel_jug = st.multiselect("Jugador", options=jugadores, default=[])
            if sel_jug:
                df = df[df["Jugador"].isin(sel_jug)]

            # Selector plantel (groupName)
            grupos = sorted(df["Grupo"].dropna().unique()) if "Grupo" in df.columns else []
            
            if sel_grupo:
                df = df[df["Grupo"].isin(sel_grupo)]
            # Formatear fecha y calcular edad
            df["Fecha de nacimiento"] = pd.to_datetime(df["Fecha de nacimiento"]).dt.date
            today = pd.to_datetime("today").date()
            df["Edad"] = df["Fecha de nacimiento"].apply(lambda d: today.year - d.year - ((today.month, today.day) < (d.month, d.day)))
            # Calcular Max Imbalance (%) si existen las columnas de fuerza
            if {"L Max Force (N)", "R Max Force (N)"}.issubset(df.columns):
                df["Max Imbalance (%)"] = (df["R Max Force (N)"] - df["L Max Force (N)"]).abs() / df[["L Max Force (N)", "R Max Force (N)"]].max(axis=1) * 100
            if {"L Avg Force (N)", "R Avg Force (N)"}.issubset(df.columns):
                df["Avg Imbalance (%)"] = (df["R Avg Force (N)"] - df["L Avg Force (N)"]).abs() / df[["L Avg Force (N)", "R Avg Force (N)"]].max(axis=1) * 100
            if {"L Max Impulse (Ns)", "R Max Impulse (Ns)"}.issubset(df.columns):
                df["Impulse Imbalance (%)"] = (df["R Max Impulse (Ns)"] - df["L Max Impulse (Ns)"]).abs() / df[["L Max Impulse (Ns)", "R Max Impulse (Ns)"]].max(axis=1) * 100
            # Asegurar columnas numéricas y redondear a 2 decimales
            for col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            num_cols = df.select_dtypes(include='number').columns
            df[num_cols] = df[num_cols].round(2)
            # Eliminar columnas no deseadas
            cols_to_drop = ["profileId", "testId", "modifiedDateUtc", "testDateUtc", "testTypeId", "tenant_id","leftCalibration", "rightCalibration", "categoryName", "groupName"]
            df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
            # Reordenar columnas según preferencia
            desired_order = [
                "Fecha Test", "Nombre", "Apellido", "Grupo", "Fecha de nacimiento", "Edad", "Dispositivo", "Test", "L Reps", "R Reps",
                "L Max Force (N)", "R Max Force (N)", "Max Imbalance (%)", "L Max Torque (Nm)", "R Max Torque (Nm)",
                "L Avg Force (N)", "R Avg Force (N)", "Avg Imbalance (%)", "L Max Impulse (Ns)", "R Max Impulse (Ns)",
                "Impulse Imbalance (%)", "Notas"
            ]
            existing_cols = [c for c in desired_order if c in df.columns]
            remaining_cols = [c for c in df.columns if c not in existing_cols]
            df = df[existing_cols + remaining_cols]
        else:
            df = df_tests
        # Conservar columnas relevantes
        # Name,"ExternalId","Date UTC","Time UTC","Device","Test","L Reps","R Reps","L Max Force (N)","R Max Force (N)","Max Imbalance (%)","L Max Torque (Nm)","R Max Torque (Nm)","L Avg Force (N)","R Avg Force (N)","Avg Imbalance (%)","L Max Impulse (Ns)","R Max Impulse (Ns)","Impulse Imbalance (%)","Notes"
        # cols = [
        #     "Name","ExternalId","Date UTC","Time UTC","Device","Test","L Reps","R Reps",
        #     "L Max Force (N)","R Max Force (N)","Max Imbalance (%)","L Max Torque (Nm)","R Max Torque (Nm)",
        #     "L Avg Force (N)","R Avg Force (N)","Avg Imbalance (%)","L Max Impulse (Ns)","R Max Impulse (Ns)",
        #     "Impulse Imbalance (%)","Notes"
        # ]
        # df = df[cols]
        with st.expander(f"🗒️ Tabla completa ({len(df)} filas)"):
            st.dataframe(df, use_container_width=True)

        # ---------------- Estadísticas por edad ----------------
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in ["Edad"]]
        if numeric_cols:
            var_sel = st.selectbox("Variable para analizar:", numeric_cols, index=0, key="nb_var")
            stats_df = (
                df.groupby("Edad")[var_sel]
                .agg(Min="min", Max="max")
                .reset_index()
                .sort_values("Edad")
            )
            st.subheader("📊 Valores mínimo y máximo por edad")
            # Tabs para mostrar gráfico (por defecto) y tabla
            tab_chart, tab_table = st.tabs(["📈 Gráfico", "📋 Tabla"])

            # ---- Gráfico ----
            mean_df = df.groupby("Edad")[var_sel].mean().reset_index(name="Media")
            overall_mean = df[var_sel].mean()
            chart_mean = (
                alt.Chart(mean_df)
                .mark_bar(color="#2ca02c")
                .encode(
                    x=alt.X("Edad:O", sort="x"),
                    y=alt.Y("Media:Q", title=f"Media de {var_sel}"),
                    tooltip=["Edad", "Media"]
                )
                .properties(height=300)
            )
            rule = (
                alt.Chart(pd.DataFrame({"y":[overall_mean]}))
                .mark_rule(color="red", strokeDash=[4,4])
                .encode(y="y:Q")
            )
            with tab_chart:
                st.altair_chart(chart_mean + rule, use_container_width=True)

            # ---- Tabla ----
            with tab_table:
                st.dataframe(stats_df, use_container_width=True)

        st.subheader("⚖️ Dispersión Fuerza Máxima Izquierda vs Derecha")
        # Gráfico comparativo de fuerzas máximas izquierda vs derecha
        tooltip_candidates = ["Nombre","Apellido","Date UTC","L Max Force (N)","R Max Force (N)"]
        tooltip_cols = [c for c in tooltip_candidates if c in df.columns]
        chart = (
            alt.Chart(df)
            .mark_circle(size=60, color="#1f77b4")
            .encode(
                x=alt.X("L Max Force (N):Q", title="L Max Force (N)"),
                y=alt.Y("R Max Force (N):Q", title="R Max Force (N)"),
                tooltip=tooltip_cols
            )
            .interactive()
        )
        st.altair_chart(chart, use_container_width=True)

        st.download_button(
            label="⬇️ Descargar NordBord CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="all_nordbord.csv",
            mime="text/csv",
        )
    except Exception as e:
        st.error(f"Error al leer el CSV: {e}")


def show_forceframe():
    """Muestra los datos del archivo all_forceframe.csv"""
    csv_path = "utils/output_data/all_forceframe.csv"
    st.header("🏋️‍♂️ Datos ForceFrame")
    if not os.path.exists(csv_path):
        st.warning("El archivo all_forceframe.csv no existe. Ejecuta la extracción primero.")
        return
    try:
        df_tests = pd.read_csv(csv_path)
        profiles_path = "utils/output_data/all_profiles.csv"
        if os.path.exists(profiles_path):
            df_profiles = pd.read_csv(profiles_path)[["profileId","givenName","familyName","dateOfBirth","groupName"]]
            df_merged = df_tests.merge(df_profiles, on="profileId", how="left")
            # Mantener solo tests con perfil asociado completo
            df = df_merged.dropna(subset=["givenName","familyName","dateOfBirth"])
            if df.empty:
                st.warning("No se encontraron tests con perfil asociado.")
                return
            df = df.rename(columns={"givenName":"Nombre","familyName":"Apellido","dateOfBirth":"Fecha de nacimiento"})
            df["Fecha de nacimiento"] = pd.to_datetime(df["Fecha de nacimiento"]).dt.date
            today = pd.to_datetime("today").date()
            df["Edad"] = df["Fecha de nacimiento"].apply(lambda d: today.year - d.year - ((today.month, today.day) < (d.month, d.day)))
            # ¿Hay filas con las tres columnas de perfil completas?
            has_complete_profile = df_merged[["givenName","familyName","dateOfBirth"]].notna().all(axis=1).any()
            if has_complete_profile:
                df = df_merged.dropna(subset=["givenName","familyName","dateOfBirth"])
                df = df.rename(columns={"givenName":"Nombre","familyName":"Apellido","dateOfBirth":"Fecha de nacimiento"})
                df["Fecha de nacimiento"] = pd.to_datetime(df["Fecha de nacimiento"]).dt.date
                today = pd.to_datetime("today").date()
                df["Edad"] = df["Fecha de nacimiento"].apply(lambda d: today.year - d.year - ((today.month, today.day) < (d.month, d.day)))
            else:
                st.info("No se encontraron coincidencias de perfiles; se muestran todos los tests.")
                df = df_tests
        else:
            df = df_tests
        with st.expander(f"🗒️ Tabla completa ({len(df)} filas)"):
            st.dataframe(df, use_container_width=True)
        st.download_button(
            label="⬇️ Descargar ForceFrame CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="all_forceframe.csv",
            mime="text/csv",
        )
    except Exception as e:
        st.error(f"Error al leer el CSV: {e}")

def show_forcedecks():
    """Muestra los datos del archivo all_forcedecks.csv"""
    csv_path = "utils/output_data/all_forcedecks.csv"
    st.header("🏋️‍♂️ Datos ForceDecks")
    if not os.path.exists(csv_path):
        st.warning("El archivo all_forcedecks.csv no existe. Ejecuta la extracción primero.")
        return
    try:
        df_tests = pd.read_csv(csv_path)
        profiles_path = "utils/output_data/all_profiles.csv"
        if os.path.exists(profiles_path):
            df_profiles = pd.read_csv(profiles_path)[["profileId","givenName","familyName","dateOfBirth","groupName"]]
            df_merged = df_tests.merge(df_profiles, on="profileId", how="left")
            # Mantener solo tests con perfil asociado completo
            df = df_merged.dropna(subset=["givenName","familyName","dateOfBirth"])
            if df.empty:
                st.warning("No se encontraron tests con perfil asociado.")
                return
            df = df.rename(columns={"givenName":"Nombre","familyName":"Apellido","dateOfBirth":"Fecha de nacimiento"})
            df["Fecha de nacimiento"] = pd.to_datetime(df["Fecha de nacimiento"]).dt.date
            today = pd.to_datetime("today").date()
            df["Edad"] = df["Fecha de nacimiento"].apply(lambda d: today.year - d.year - ((today.month, today.day) < (d.month, d.day)))
            # ¿Hay filas con las tres columnas de perfil completas?
            has_complete_profile = df_merged[["givenName","familyName","dateOfBirth"]].notna().all(axis=1).any()
            if has_complete_profile:
                df = df_merged.dropna(subset=["givenName","familyName","dateOfBirth"])
                df = df.rename(columns={"givenName":"Nombre","familyName":"Apellido","dateOfBirth":"Fecha de nacimiento"})
                df["Fecha de nacimiento"] = pd.to_datetime(df["Fecha de nacimiento"]).dt.date
                today = pd.to_datetime("today").date()
                df["Edad"] = df["Fecha de nacimiento"].apply(lambda d: today.year - d.year - ((today.month, today.day) < (d.month, d.day)))
            else:
                st.info("No se encontraron coincidencias de perfiles; se muestran todos los tests.")
                df = df_tests
        else:
            df = df_tests
        with st.expander(f"🗒️ Tabla completa ({len(df)} filas)"):
            st.dataframe(df, use_container_width=True)
        st.download_button(
            label="⬇️ Descargar ForceDecks CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="all_forcedecks.csv",
            mime="text/csv",
        )
    except Exception as e:
        st.error(f"Error al leer el CSV: {e}")


def show_profiles():
    """Muestra lista de perfiles"""
    csv_path = "utils/output_data/all_profiles.csv"
    st.header("🧑‍💼 Perfiles")
    if not os.path.exists(csv_path):
        st.warning("El archivo all_profiles.csv no existe. Ejecuta la extracción primero.")
        return
    try:
        df = pd.read_csv(csv_path)[["groupName","givenName","familyName","dateOfBirth"]]
        df = df.rename(columns={"groupName":"Plantel","givenName":"Nombre","familyName":"Apellido","dateOfBirth":"Fecha de nacimiento"})
        df["Fecha de nacimiento"] = pd.to_datetime(df["Fecha de nacimiento"]).dt.date
        today = pd.to_datetime("today").date()
        df["Edad"] = df["Fecha de nacimiento"].apply(lambda d: today.year - d.year - ((today.month, today.day) < (d.month, d.day)))
        df["Edad"] = df["Edad"].astype(int)
        # Filtro por Plantel
        planteles = sorted(df["Plantel"].dropna().unique())
        seleccion = st.selectbox("Filtrar por plantel:", ["Todos"] + planteles, index=0)
        if seleccion != "Todos":
            df = df[df["Plantel"] == seleccion]
        # Order columns
        df = df[["Plantel","Nombre","Apellido","Fecha de nacimiento","Edad"]]

        # Gráfico: cantidad de perfiles por plantel
        chart_plantel = (
            alt.Chart(df.groupby("Plantel").size().reset_index(name="Perfiles"))
            .mark_bar()
            .encode(
                x=alt.X("Plantel:N", sort="-y"),
                y="Perfiles:Q",
                tooltip=["Plantel", "Perfiles"]
            )
            .properties(height=300, title="Perfiles por plantel")
        )
        st.altair_chart(chart_plantel, use_container_width=True)

        # Gráfico: cantidad de jugadores por edad
        chart_edad = (
            alt.Chart(df.groupby("Edad").size().reset_index(name="Jugadores"))
            .mark_bar()
            .encode(
                x=alt.X("Edad:O", sort="x"),
                y="Jugadores:Q",
                tooltip=["Edad", "Jugadores"]
            )
            .properties(height=300, title="Jugadores por edad")
        )
        st.altair_chart(chart_edad, use_container_width=True)

        st.write(f"{len(df)} perfiles cargados")
        st.dataframe(df, use_container_width=True)
        st.download_button(
            label="⬇️ Descargar Perfiles CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="perfiles.csv",
            mime="text/csv",
        )
    except Exception as e:
        st.error(f"Error al leer el CSV: {e}")


def logout():
    """Cierra la sesión y vuelve al login."""
    st.session_state.authenticated = False
    st.switch_page("app.py")


def main():
    if "authenticated" not in st.session_state or not st.session_state.authenticated:
        st.switch_page("app.py")  # volver al login

    # Custom HTML/CSS for the banner
    ## Add background image
    st.markdown(
        """
        <style>
        header[data-testid="stHeader"]{
            background-image: url(https://i.ibb.co/N6z3GQ1K/header.png);
            background-repeat: repeat;
            background-size: contain;
            height: 10%;
        }
        
        section[data-testid="stSidebar"] {
            top: 0%; 
        }
        </style>""",
        unsafe_allow_html=True,
    )

    # Logo en la parte superior del sidebar
    st.sidebar.image("img/logo.png", use_container_width=True)

    # Ocultar el selector de páginas automático de Streamlit
    hide_default_nav = """
    <style>
        /* Oculta la lista de páginas que Streamlit crea automáticamente */
        [data-testid="stSidebarNav"] {
            display: none;
        }
        [data-testid="stSidebarHeader"] {
            padding: 0 !important;
            margin: 0 !important;
            height: 0 !important;
            min-height: 0 !important;
            max-height: 0 !important;
        }
    </style>
    """
    st.markdown(hide_default_nav, unsafe_allow_html=True)
    # Estilos para los botones de menú (borde inferior y ancho completo)
    menu_button_style = """
    <style>
    [data-testid='stSidebar'] button {
        width: 100% !important;
        background: transparent !important;
        border: none !important;
        border-bottom: 1px solid rgba(0,0,0,0.15) !important;
        border-radius: 0 !important;
        color: inherit !important;
        text-align: left !important;
        padding: 0.6rem 1rem !important;
    }
    [data-testid='stSidebar'] button:hover {
        background: rgba(0,0,0,0.05) !important;
    }
    [data-testid='stSidebar'] button:disabled {
        font-weight: 600;
        color: #ff8800 !important;
        background: rgba(0,0,0,0.04) !important;
    }
    </style>
    """
    st.markdown(menu_button_style, unsafe_allow_html=True)

    st.sidebar.title("Navegación")

    # Diccionario de secciones
    secciones = {
        "Home": show_home,
        "Descargar Datos": show_download,
        "Perfiles": show_profiles,
        "NordBord": show_nordbord,
        "Force Frame": show_forceframe,
        "Force Decks": show_forcedecks,
        "Cerrar Sesión": logout,
    }

    # Página seleccionada en sesión
    if "page" not in st.session_state:
        st.session_state.page = "Home"

    # Renderizar botones como menú (botón de página actual deshabilitado para resaltar)
    for nombre in secciones.keys():
        if nombre == st.session_state.page:
            st.sidebar.button(nombre, key=f"menu_{nombre}", disabled=True)
        else:
            if st.sidebar.button(nombre, key=f"menu_{nombre}"):
                st.session_state.page = nombre
                st.rerun()

    # Llamar a la función de la página seleccionada
    secciones[st.session_state.page]()


if __name__ == "__main__":
    main()
