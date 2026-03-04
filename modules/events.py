import pandas as pd
from typing import Dict, Any

def build_events_payload(df: pd.DataFrame, page_size: int, rotate_seconds: int) -> Dict[str, Any]:
    df = df.copy()

    # Normalizar nombres de columnas (evita problemas de espacios/mayúsculas)
    norm = {c: str(c).strip().lower() for c in df.columns}
    df.rename(columns={old: norm[old] for old in df.columns}, inplace=True)

    # Mapear a nombres canónicos esperados por el frontend
    # Acepta variantes comunes
    col_hora = None
    for c in ["hora", "horario", "time"]:
        if c in df.columns:
            col_hora = c
            break

    col_evento = None
    for c in ["evento", "event", "actividad"]:
        if c in df.columns:
            col_evento = c
            break

    col_loc = None
    for c in ["locación", "locacion", "lugar", "location"]:
        if c in df.columns:
            col_loc = c
            break

    # Buscar columna Orden
    col_orden = None
    for c in ["orden", "order"]:
        if c in df.columns:
            col_orden = c
            break

    # Si faltan, las creamos vacías
    if col_hora is None:
        df["hora"] = ""
        col_hora = "hora"
    if col_evento is None:
        df["evento"] = ""
        col_evento = "evento"
    if col_loc is None:
        df["locación"] = ""
        col_loc = "locación"

    # Armar DF final con nombres exactos que usa el HTML
    out_cols = {
        "Hora": df[col_hora].fillna("").astype(str),
        "Evento": df[col_evento].fillna("").astype(str),
        "Locación": df[col_loc].fillna("").astype(str),
    }
    
    if col_orden:
         # Convertir a numerico para ordenar bien
         out_cols["_orden"] = pd.to_numeric(df[col_orden], errors='coerce').fillna(9999)
    
    out = pd.DataFrame(out_cols)

    # Ordenar
    if col_orden:
        out = out.sort_values(by=["_orden"], ascending=[True]).drop(columns=["_orden"])
    else:
        # Fallback a orden por hora si no hay columna Orden
        out["_hora_ord"] = out["Hora"].astype(str)
        out = out.sort_values(by=["_hora_ord"], ascending=[True]).drop(columns=["_hora_ord"])

    # Override page_size to show ALL
    final_page_size = len(out) if not out.empty else page_size
    # Asegurar minimo 1 para evitar division por cero en frontend si estuviera vacio (aunque .empty lo cubre)
    if final_page_size < 1: final_page_size = 1

    return {
        "rows": out.to_dict(orient="records"),
        "page_size": final_page_size,
        "rotate_seconds": rotate_seconds
    }
