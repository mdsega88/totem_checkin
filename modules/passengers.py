import pandas as pd
from typing import Dict, Any

def build_passengers_payload(
    df: pd.DataFrame,
    page_size: int,
    rotate_seconds: int
) -> Dict[str, Any]:

    # Columnas esperadas mínimas
    expected_columns = ["Hora", "Pasajero", "Mesa", "Checkin", "Selfie Aduana"]

    for col in expected_columns:
        if col not in df.columns:
            df[col] = ""

    df = df.copy()

    # Orden: ON TIME arriba, luego por Hora desc (string)
    df["_estado_ord"] = (df["Checkin"] != "ON TIME").astype(int)
    df["_hora_ord"] = df["Hora"].astype(str)

    df = df.sort_values(
        by=["_estado_ord", "_hora_ord"],
        ascending=[True, False]
    )

    # Orden final de columnas (Selfie incluida pero no visible si no se usa)
    df = df[
        ["Pasajero", "Hora", "Mesa", "Checkin", "Selfie Aduana"]
    ].fillna("")

    return {
        "rows": df.to_dict(orient="records"),
        "page_size": page_size,
        "rotate_seconds": rotate_seconds
    }
