import pandas as pd
from typing import Dict, Any

def build_aduana_payload(df: pd.DataFrame, rotate_seconds: int = 5) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"rows": [], "rotate_seconds": rotate_seconds}
    
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    
    # Columnas esperadas
    expected_columns = ["Pasajero", "Selfie Aduana", "Checkin", "Hora", "Mesa"]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = ""
    
    # Filtrar solo los que tienen foto de Aduana
    df["Selfie Aduana"] = df["Selfie Aduana"].astype(str).str.strip()
    df_filtered = df[df["Selfie Aduana"] != ""].copy()
    
    if df_filtered.empty:
        return {"rows": [], "rotate_seconds": rotate_seconds}
    
    # Preparar datos para el formato "buscado por interpol"
    df_filtered = df_filtered[[
        "Pasajero", "Selfie Aduana", "Checkin", "Hora", "Mesa"
    ]].fillna("")
    
    return {
        "rows": df_filtered.to_dict(orient="records"),
        "rotate_seconds": rotate_seconds
    }
