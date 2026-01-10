from typing import Dict, Any, List
import pandas as pd
import time

_state = {"idx": 0, "last": 0.0}

def _generation(age: int) -> str:
    if age < 27: return "GEN Z"
    if age <= 42: return "MILLENNIALS"
    if age <= 58: return "GEN X"
    return "BABY BOOMERS"

def build_metrics_payload(df: pd.DataFrame, rotate_seconds: int = 12) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"rotate_seconds": rotate_seconds, "active_metric": None, "all": []}

    d = df.copy()
    d.columns = [c.strip() for c in d.columns]

    for col in ["Edad", "Sexo", "Estado Civil", "Mesa", "Checkin", "Pasajero"]:
        if col not in d.columns:
            d[col] = ""

    # --- Normalizaciones robustas ---
    # Edad: convierte todo lo posible a número; lo demás -> NaN
    d["Edad_num"] = pd.to_numeric(d["Edad"], errors="coerce")

    # Filtramos edades válidas
    ages = d["Edad_num"].dropna().astype(int).tolist()

    # Texto normalizado
    d["Sexo_norm"] = d["Sexo"].astype(str).str.strip().str.upper()
    d["Estado_norm"] = d["Estado Civil"].astype(str).str.strip().str.upper()
    d["Mesa_str"] = d["Mesa"].astype(str).str.strip()
    d["Checkin_norm"] = d["Checkin"].astype(str).str.strip().str.upper()
    d["Pasajero_str"] = d["Pasajero"].astype(str).str.strip()

    def metric(icon, title, value, subtitle=""):
        return {"icon": icon, "title": title, "value": value, "subtitle": subtitle}

    metrics: List[Dict[str, Any]] = []

    # 1) Promedio de edad
    if ages:
        avg = sum(ages) / len(ages)
        metrics.append(metric("🎂", "Edad promedio del vuelo", f"{avg:.0f} años", "Promedio general"))
    else:
        metrics.append(metric("🎂", "Edad promedio del vuelo", "N/D", "Faltan edades"))

    # 2) Generación predominante
    if ages:
        gens = [_generation(a) for a in ages]
        top_gen = pd.Series(gens).value_counts().index[0]
        metrics.append(metric("🧬", "Generación predominante", top_gen, "Según edades"))
    else:
        metrics.append(metric("🧬", "Generación predominante", "N/D", ""))

    # 3) Pasajero más joven
    if ages:
        min_age = min(ages)
        row = d[d["Edad_num"].astype("float").eq(float(min_age))].iloc[0]
        metrics.append(metric("🍼", "Pasajero más joven", f'{row["Pasajero_str"]} — {min_age}', "A bordo"))
    else:
        metrics.append(metric("🍼", "Pasajero más joven", "N/D", ""))

    # 4) Pasajero más experimentado
    if ages:
        max_age = max(ages)
        row = d[d["Edad_num"].astype("float").eq(float(max_age))].iloc[0]
        metrics.append(metric("🎩", "Pasajero más experimentado", f'{row["Pasajero_str"]} — {max_age}', "A bordo"))
    else:
        metrics.append(metric("🎩", "Pasajero más experimentado", "N/D", ""))

    # 5) Mercado activo (solteros/solteras)
    is_soltero = d["Estado_norm"].str.contains("SOLTER", na=False)
    solteras = int(((d["Sexo_norm"] == "F") & is_soltero).sum())
    solteros = int(((d["Sexo_norm"] == "M") & is_soltero).sum())
    metrics.append(metric("💘", "Mercado activo", f"{solteras} solteras / {solteros} solteros", "El destino hace escala 😄"))

    # 6) Mesa puntual (mejor ratio ON TIME)
    ontime = d["Checkin_norm"].eq("ON TIME")
    mesa_stats = d.groupby("Mesa_str").agg(
        total=("Mesa_str", "count"),
        ontime=("Mesa_str", lambda s: int(ontime.loc[s.index].sum()))
    ).reset_index()

    if not mesa_stats.empty:
        mesa_stats["ratio"] = mesa_stats["ontime"] / mesa_stats["total"]
        best = mesa_stats.sort_values(by=["ratio", "ontime", "total"], ascending=[False, False, False]).iloc[0]
        metrics.append(metric("🚀", "Mesa más puntual", best["Mesa_str"], f'{int(round(best["ratio"]*100))}% ON TIME'))
    else:
        metrics.append(metric("🚀", "Mesa más puntual", "N/D", ""))

    # 7) Mesa más joven / 8) más experimentada (por promedio edad)
    mesa_age = d.dropna(subset=["Edad_num"]).groupby("Mesa_str")["Edad_num"].mean()
    if not mesa_age.empty:
        metrics.append(metric("🔥", "Mesa más joven", str(mesa_age.sort_values().index[0]), "Menor edad promedio"))
        metrics.append(metric("🎓", "Mesa más experimentada", str(mesa_age.sort_values(ascending=False).index[0]), "Mayor edad promedio"))
    else:
        metrics.append(metric("🔥", "Mesa más joven", "N/D", ""))
        metrics.append(metric("🎓", "Mesa más experimentada", "N/D", ""))

    # Rotación backend
    now = time.time()
    if now - _state["last"] >= rotate_seconds:
        _state["idx"] = (_state["idx"] + 1) % len(metrics)
        _state["last"] = now

    return {
        "rotate_seconds": rotate_seconds,
        "active_metric": metrics[_state["idx"]],
        "all": metrics
    }
