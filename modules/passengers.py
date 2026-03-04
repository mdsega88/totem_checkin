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

    # Transform names: "SURNAME(S) NAME" -> "SURNAME(S), NAME"
    # User confirmed input is "SURNAME... NAME"
    def format_name(val):
        val = str(val).strip().upper()
        parts = val.rsplit(" ", 1)
        if len(parts) > 1:
            return f"{parts[0]}, {parts[1]}"
        return val

    df["Pasajero"] = df["Pasajero"].apply(format_name)

    # Sort alphabetically by name
    df = df.sort_values(by=["Pasajero"], ascending=[True])

    # Orden final de columnas (Selfie incluida pero no visible si no se usa)
    df = df[
        ["Pasajero", "Hora", "Mesa", "Checkin", "Selfie Aduana"]
    ].fillna("")

    return {
        "rows": df.to_dict(orient="records"),
        "page_size": page_size,
        "rotate_seconds": rotate_seconds
    }
