# app.py
# --------------------------------------------
# App: Enriquecimiento de resultados de query con catálogo por CId (2G/3G)
# Elaborado por: Angelo Olmedo Camacho - Pasante Prevención de Fraude 
# 2025-09-01
# --------------------------------------------

import io
import re
from datetime import datetime

import pandas as pd
import streamlit as st

# ==========================
# Configuración de la página
# ==========================
st.set_page_config(
    page_title="Cruce de Query vs CId (2G/3G)",
    page_icon="📊",
    layout="wide"
)

# ==========================
# Parámetros ajustables
# ==========================
DATE_FORMATS_IN = [
    "%m/%d/%Y %H:%M:%S",  # 08/25/2025 18:14:03
    "%m/%d/%y %H:%M:%S",  # 08/25/25 18:14:03 (por si acaso)
    "%Y-%m-%d %H:%M:%S",  # ISO común
]
INCLUDE_TECNOLOGIA_COLS = True   # Mostrar columnas Tecnologia A/B
SORT_ASCENDING_BY_TIME = True    # Ordenar por fecha ascendente
FILTER_MODE_DEFAULT_EXACT = False  # Filtro por contains (False) vs exacto (True)

# Normalización de CId: si True, se extraen solo dígitos al preparar CId (robustece casos "34806.0")
CLEAN_CID_KEEP_DIGITS_ONLY = True

# ==========================
# Utilidades
# ==========================
def normalize_text(s):
    if pd.isna(s):
        return ""
    return str(s).strip()


def normalize_cid(value):
    """
    Normaliza un CId o celda A/B:
      - Quita espacios.
      - Maneja strings tipo '34806.0' -> '34806'.
      - Si CLEAN_CID_KEEP_DIGITS_ONLY=True, extrae solo dígitos.
      - Devuelve '' si no hay valor válido o si es '0'.
    """
    s = normalize_text(value)
    if s == "":
        return ""

    # Caso típico Excel: '34806.0'
    if re.fullmatch(r"\d+(\.0+)?", s):
        try:
            n = int(float(s))
            return str(n) if n > 0 else ""
        except Exception:
            pass

    if CLEAN_CID_KEEP_DIGITS_ONLY:
        digits = re.sub(r"\D+", "", s)
        if digits == "":
            return ""
        try:
            return str(int(digits)) if int(digits) > 0 else ""
        except Exception:
            return ""

    # Alternativa conservadora: devolver tal cual (si es distinto de '0')
    try:
        if int(s) == 0:
            return ""
        return s
    except Exception:
        # No numérico y no usamos "solo dígitos": lo dejamos si no es '0'
        return s if s != "0" else ""

def pick_final_cell(primary, fallback):
    """
    Regla: usar primary si > 0, si no usar fallback (equivalente a: SI(A_CELL>0;A_CELL;A_FIRST_SAC))
    Nota: Se evalúa tras normalización con normalize_cid()
    """
    p = normalize_cid(primary)
    f = normalize_cid(fallback)
    return p if p != "" else f


def parse_datetime_maybe(s):
    s = normalize_text(s)
    if s == "":
        return pd.NaT
    for fmt in DATE_FORMATS_IN:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    # Intento automático final
    try:
        return pd.to_datetime(s, errors="coerce")
    except Exception:
        return pd.NaT


def ensure_required_columns(df, required, label_for_error):
    missing = [c for c in required if c not in df.columns]
    if missing:
        st.error(
            f"El archivo **{label_for_error}** no contiene las columnas requeridas: {missing}"
        )
        return False
    return True

# ==========================
# Carga y preparación datos
# ==========================
def load_excel_catalog(file_bytes) -> pd.DataFrame:
    """
    Lee el Excel con hojas 'Tabla2g' y 'Tabla3g', normaliza columnas clave y
    concatena en un catálogo con esquema: CId, Nombre Estacion, Dirección, Tecnologia.
    """
    try:
        excel = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")
    except Exception as e:
        st.error(f"No se pudo leer el Excel: {e}")
        return pd.DataFrame()

    required_cols = ["CId", "Nombre Estacion", "Dirección"]
    catalog_parts = []

    for sheet_name, tech in [("Base2G", "2G"), ("Base3G", "3G")]:
        if sheet_name not in excel.sheet_names:
            st.error(f"El Excel no contiene la hoja requerida: **{sheet_name}**.")
            return pd.DataFrame()

        df = pd.read_excel(excel, sheet_name=sheet_name, dtype=str)
        # Normalización básica
        df.columns = [c.strip() for c in df.columns]
        if not ensure_required_columns(df, required_cols, sheet_name):
            return pd.DataFrame()

        df = df.copy()
        df["CId"] = df["CId"].map(normalize_cid)
        df["Nombre Estacion"] = df["Nombre Estacion"].map(normalize_text)
        df["Dirección"] = df["Dirección"].map(normalize_text)
        df["Tecnologia"] = tech

        # Filtrar registros sin CId válido
        df = df.loc[df["CId"] != ""]
        catalog_parts.append(df[["CId", "Nombre Estacion", "Dirección", "Tecnologia"]])

    if not catalog_parts:
        return pd.DataFrame()

    catalog = pd.concat(catalog_parts, ignore_index=True).drop_duplicates()
    return catalog


def load_query_csv(file_bytes) -> pd.DataFrame:
    """
    Lee el CSV de la query y fuerza columnas a str para no perder ceros.
    """
    # Intento lectura flexible con separador auto (pandas detecta comas)
    try:
        df = pd.read_csv(io.BytesIO(file_bytes), dtype=str)
    except Exception as e:
        st.error(f"No se pudo leer el CSV de la query: {e}")
        return pd.DataFrame()

    # Limpieza de cabeceras
    df.columns = [c.strip() for c in df.columns]

    required = [
        "A_DIRECTION_NUMBER", "CALL_START_TIME", "B_DIRECTION_NUMBER",
        "A_IMEI", "B_IMEI", "CHARGING_END_TIME",
        "A_CELL", "B_CELL", "A_FIRST_SAC", "B_FIRST_SAC"
    ]
    if not ensure_required_columns(df, required, "CSV de la query"):
        return pd.DataFrame()

    # Normalización de strings
    for c in required:
        df[c] = df[c].map(normalize_text)

    # Parse de fechas
    df["CALL_START_TIME_DT"] = df["CALL_START_TIME"].map(parse_datetime_maybe)
    df["CHARGING_END_TIME_DT"] = df["CHARGING_END_TIME"].map(parse_datetime_maybe)

    # Cálculo de A/B_CELL_FINAL
    df["A_CELL_FINAL"] = df.apply(
        lambda r: pick_final_cell(r["A_CELL"], r["A_FIRST_SAC"]), axis=1
    )
    df["B_CELL_FINAL"] = df.apply(
        lambda r: pick_final_cell(r["B_CELL"], r["B_FIRST_SAC"]), axis=1
    )

    return df


def enrich_with_catalog(df_query: pd.DataFrame, catalog: pd.DataFrame) -> pd.DataFrame:
    """
    Hace merge 1->N:
      - A: left_on A_CELL_FINAL == CId
      - B: left_on B_CELL_FINAL == CId
    Devuelve todas las combinaciones (si CId está en 2G y 3G, se duplicará la fila, como requerido).
    """
    if df_query.empty or catalog.empty:
        return pd.DataFrame()

    # Catálogo versionado para A
    cat_A = catalog.rename(columns={
        "Nombre Estacion": "NOMBRE ESTACION A",
        "Dirección": "DIRECCION A",
        "Tecnologia": "TECNOLOGIA A",
        "CId": "CId_A"
    })

    # Catálogo versionado para B
    cat_B = catalog.rename(columns={
        "Nombre Estacion": "NOMBRE ESTACION B",
        "Dirección": "DIRECCION B",
        "Tecnologia": "TECNOLOGIA B",
        "CId": "CId_B"
    })

    # Merge para A
    merged_A = df_query.merge(
        cat_A,
        how="left",
        left_on="A_CELL_FINAL",
        right_on="CId_A"
    )

    # Merge para B
    merged_AB = merged_A.merge(
        cat_B,
        how="left",
        left_on="B_CELL_FINAL",
        right_on="CId_B"
    )

    # Orden de columnas final
    base_cols = [
        "A_DIRECTION_NUMBER", "CALL_START_TIME", "B_DIRECTION_NUMBER",
        "A_IMEI", "B_IMEI", "CHARGING_END_TIME",
        "A_CELL_FINAL", "B_CELL_FINAL"
    ]

    a_cols = ["NOMBRE ESTACION A", "DIRECCION A"]
    b_cols = ["NOMBRE ESTACION B", "DIRECCION B"]

    if INCLUDE_TECNOLOGIA_COLS:
        a_cols.append("TECNOLOGIA A")
        b_cols.append("TECNOLOGIA B")

    # Asegurar existencia (en caso de no match se crean columnas vacías)
    for c in a_cols + b_cols:
        if c not in merged_AB.columns:
            merged_AB[c] = ""

    final_cols = base_cols + a_cols + b_cols + ["CALL_START_TIME_DT"]
    final = merged_AB.copy()

    # Ordenar por fecha
    if "CALL_START_TIME_DT" in final.columns:
        final = final.sort_values("CALL_START_TIME_DT", ascending=SORT_ASCENDING_BY_TIME)

    return final[final_cols]


def apply_filters(df: pd.DataFrame, numeros_raw: str, imeis_raw: str, exact_mode: bool) -> pd.DataFrame:
    """
    Aplica filtros por número (A/B_DIRECTION_NUMBER) y por IMEI (A/B_IMEI).
    - exact_mode=False => 'contains' (subcadenas)
    - exact_mode=True  => coincidencia exacta (igualdad)
    Permite múltiples valores separados por coma.
    """
    if df.empty:
        return df

    filtered = df

    def tokenize(s):
        return [t.strip() for t in s.split(",") if t.strip() != ""]

    # Filtro por números
    nums = tokenize(numeros_raw)
    if nums:
        if exact_mode:
            mask = (
                filtered["A_DIRECTION_NUMBER"].isin(nums)
                | filtered["B_DIRECTION_NUMBER"].isin(nums)
            )
        else:
            pattern = "|".join([re.escape(x) for x in nums])
            mask = (
                filtered["A_DIRECTION_NUMBER"].str.contains(pattern, na=False)
                | filtered["B_DIRECTION_NUMBER"].str.contains(pattern, na=False)
            )
        filtered = filtered[mask]

    # Filtro por IMEIs
    imeis = tokenize(imeis_raw)
    if imeis:
        if exact_mode:
            mask = (
                filtered["A_IMEI"].isin(imeis)
                | filtered["B_IMEI"].isin(imeis)
            )
        else:
            pattern = "|".join([re.escape(x) for x in imeis])
            mask = (
                filtered["A_IMEI"].str.contains(pattern, na=False)
                | filtered["B_IMEI"].str.contains(pattern, na=False)
            )
        filtered = filtered[mask]

    return filtered

# ==========================
# UI
# ==========================
st.title("📊 Cruce de Query (CSV) con Catálogo por CId (Excel 2G/3G)")
st.caption("Sube el Excel mensual (Base2G / Base3G) y el CSV de resultados de la query. Luego presiona **Procesar**.")

col1, col2 = st.columns(2)
with col1:
    excel_file = st.file_uploader(
        "Excel mensual (.xlsx) con hojas 'Base2G' y 'Base3G'",
        type=["xlsx"],
        accept_multiple_files=False
    )
with col2:
    csv_file = st.file_uploader(
        "CSV de resultados de la query",
        type=["csv"],
        accept_multiple_files=False
    )

st.write("---")
# query_text = st.text_area(
#     "Opcional: pega aquí la query (solo informativo/bitácora, no se ejecuta):",
#     height=150,
#     placeholder="SELECT A_DIRECTION_NUMBER, CALL_START_TIME, ... FROM ... WHERE ...;"
# )

process = st.button("🚀 Empezar / Procesar", type="primary", use_container_width=True)
st.write("")

if process:
    if not excel_file or not csv_file:
        st.warning("Por favor sube **ambos** archivos: el Excel mensual y el CSV de la query.")
    else:
        with st.spinner("Leyendo Excel y construyendo catálogo..."):
            catalog = load_excel_catalog(excel_file.getvalue())

        if catalog.empty:
            st.stop()

        with st.spinner("Leyendo CSV de la query y preparando datos..."):
            df_query = load_query_csv(csv_file.getvalue())

        if df_query.empty:
            st.stop()

        with st.spinner("Enriqueciendo resultados con el catálogo (CId -> Estación/Dirección)..."):
            df_final = enrich_with_catalog(df_query, catalog)

        if df_final.empty:
            st.info("No hay resultados para mostrar (verifica que A/B_CELL_FINAL tengan valores y coincidan con CId del Excel).")
            st.stop()

        # Guardamos en sesión para filtros/descarga sin re-procesar
        st.session_state["df_final"] = df_final

# Si ya hay df en sesión, mostramos filtros, tabla y descarga
if "df_final" in st.session_state and not st.session_state["df_final"].empty:
    df_final = st.session_state["df_final"]

    st.subheader("🔎 Filtros")
    fc1, fc2, fc3 = st.columns([2, 2, 1])
    with fc1:
        numeros_filtro = st.text_input(
            "Filtrar por Número (A/B_DIRECTION_NUMBER). Puedes ingresar varios separados por coma.",
            placeholder="Ej: 98681019, 7804"
        )
    with fc2:
        imei_filtro = st.text_input(
            "Filtrar por IMEI (A/B_IMEI). Puedes ingresar varios separados por coma.",
            placeholder="Ej: 350034570070400"
        )
    with fc3:
        exact_mode = st.toggle("Coincidencia exacta", value=FILTER_MODE_DEFAULT_EXACT, help="Si está desactivado, se usa 'contiene' (subcadena).")

    df_view = apply_filters(df_final, numeros_filtro, imei_filtro, exact_mode)

    st.write("")
    st.subheader("📄 Resultados")
    # Ocultamos la columna de apoyo para ordenamiento si no se desea mostrar
    show_cols = [c for c in df_view.columns if c != "CALL_START_TIME_DT"]
    st.dataframe(
        df_view[show_cols],
        use_container_width=True,
        hide_index=True
    )

    st.write("")
    # Exportar CSV (vista filtrada)
    csv_buffer = io.StringIO()
    df_view[show_cols].to_csv(csv_buffer, index=False, encoding="utf-8-sig")
    st.download_button(
        label="💾 Descargar CSV (vista actual)",
        data=csv_buffer.getvalue(),
        file_name="resultado_cruzado.csv",
        mime="text/csv"
    )

    # Métricas simples
    cA, cB, cC, cD = st.columns(4)
    with cA:
        st.metric("Filas mostradas", len(df_view))
    with cB:
        st.metric("Total filas procesadas", len(df_final))
    with cC:
        st.metric("Catálogo (CIds únicos)", catalog["CId"].nunique() if 'catalog' in globals() else "-")
    with cD:
        st.metric("Hojas (2G/3G) combinadas", catalog["Tecnologia"].nunique() if 'catalog' in globals() else "-")
