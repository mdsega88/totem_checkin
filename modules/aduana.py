import pandas as pd
import random
from typing import Dict, Any

COUNTRIES = [
    "ARGENTINA", "BRASIL", "ESPAÑA", "ESTADOS UNIDOS", "ITALIA", 
    "FRANCIA", "ALEMANIA", "MÉXICO", "COLOMBIA", "URUGUAY",
    "CHILE", "JAPÓN", "CHINA", "RUSIA", "INGLATERRA"
]

def build_aduana_payload(df: pd.DataFrame, rotate_seconds: int = 5) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"rows": [], "rotate_seconds": rotate_seconds}
    
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    # 1) ADUANA: Filtrar por Selfie
    df["Selfie Aduana"] = df["Selfie Aduana"].astype(str).str.strip()
    df_aduana = df[df["Selfie Aduana"].str.contains("http", case=False, na=False)].copy()
    
    # Column detection (accent robust)
    import unicodedata
    def normalize_str(s):
        s = s.lower().strip()
        return "".join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

    norm_cols = {normalize_str(c): c for c in df.columns}
    
    def get_col(candidates):
        for cand in candidates:
            norm_cand = normalize_str(cand)
            if norm_cand in norm_cols:
                return norm_cols[norm_cand]
        return None

    cap_col = get_col(["Capitan", "Capitán", "Captain"]) or "Capitán"
    
    # Prioridad: Foto Capitán de Mesa -> Foto Capitán -> Selfie Aduana
    photo_cap_col = get_col(["Foto Capitan de Mesa", "Foto Capitán de Mesa", "Foto Capitan", "Foto Capitán"])
    if not photo_cap_col:
        photo_cap_col = get_col(["Selfie Aduana", "Foto"]) or "Selfie Aduana"
    
    if cap_col in df.columns:
        df_cap = df[df[cap_col].astype(str).str.upper() == "X"].copy()
    else:
        df_cap = pd.DataFrame()
    
    def fmt_age(val):
        try:
            return str(int(float(val)))
        except:
            return str(val)

    def clean_val(val):
        if pd.isna(val) or str(val).lower() == "nan": return ""
        return str(val).strip()

    # Preparar datos Aduana
    aduana_rows = []
    for _, row in df_aduana.iterrows():
        aduana_rows.append({
            "Pasajero": clean_val(row.get("Pasajero", "")),
            "Selfie Aduana": clean_val(row.get("Selfie Aduana", "")),
            "Edad": fmt_age(row.get("Edad", "")),
            "Estado Civil": clean_val(row.get("Estado Civil", "")).upper(),
            "Peligrosidad": clean_val(row.get("Peligrosidad", "")),
            "En": clean_val(row.get("Buscado en:", "")).upper(),
            "Por": clean_val(row.get("Buscado", "")),
        })

    # Preparar datos Capitanes
    capitanes_rows = []
    for _, row in df_cap.iterrows():
        pasajero_val = clean_val(row.get("Pasajero", ""))
        if not pasajero_val: continue
        
        capitanes_rows.append({
            "Pasajero": pasajero_val,
            "Foto": clean_val(row.get(photo_cap_col, "")),
            "Mesa": clean_val(row.get("Mesa", "")),
        })

    # Ordenar alfabéticamente por Pasajero
    aduana_rows.sort(key=lambda x: x["Pasajero"].upper())
    capitanes_rows.sort(key=lambda x: x["Pasajero"].upper())

    return {
        "aduana": aduana_rows,
        "capitanes": capitanes_rows,
        "rotate_seconds": rotate_seconds
    }
