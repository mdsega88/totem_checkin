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
    print(f"DEBUG ADUANA COLS: {df.columns.tolist()}")
    
    # Columnas esperadas
    # Agregamos Pais, Peligrosidad, Codigo (según debug names)
    expected_columns = ["Pasajero", "Selfie Aduana", "Checkin", "Hora", "Mesa", "Edad", "Buscado", "Pais Buscado", "Peligrosidad", "Codigo Delito"]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = ""
    
    # Filtrar solo los que tienen foto de Aduana
    df["Selfie Aduana"] = df["Selfie Aduana"].astype(str).str.strip()
    # Filtrar solo links validos (contienen http)
    df_filtered = df[df["Selfie Aduana"].str.contains("http", case=False, na=False)].copy()
    
    if df_filtered.empty:
        return {"rows": [], "rotate_seconds": rotate_seconds}
    
    # Helper para formatear edad
    def fmt_age(val):
        try:
            return str(int(float(val)))
        except:
            return str(val)

    # Preparar datos
    rows = []
    for _, row in df_filtered.iterrows():
        rows.append({
            "Pasajero": row.get("Pasajero", ""),
            "Selfie Aduana": row.get("Selfie Aduana", ""),
            "Edad": fmt_age(row.get("Edad", "")),
            "En": str(row.get("Pais Buscado", "")).upper(), # Corrected column name
            "Por": str(row.get("Buscado", "")),
            "Peligrosidad": str(row.get("Peligrosidad", "")),
            "Codigo": str(row.get("Codigo Delito", "")), # Corrected column name
            "Checkin": row.get("Checkin", ""),
            "Hora": row.get("Hora", ""),
            "Mesa": row.get("Mesa", ""),
        })

    return {
        "rows": rows,
        "rotate_seconds": rotate_seconds
    }
