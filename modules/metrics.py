from typing import Dict, Any, List
import pandas as pd
import numpy as np
import time
from datetime import datetime

_state = {"idx": 0, "last": 0.0}

def _generation(age: int) -> str:
    if age < 27: return "GEN Z"
    if age <= 42: return "MILLENNIALS"
    if age <= 58: return "GEN X"
    return "BABY BOOMERS"

def _parse_time(time_str: str) -> int:
    """Convierte hora string (HH:MM) a minutos desde medianoche"""
    try:
        parts = str(time_str).strip().split(':')
        if len(parts) >= 2:
            return int(parts[0]) * 60 + int(parts[1])
        return 0
    except:
        return 0

def _to_native(val):
    """Convierte valores de pandas (int64, float64) a tipos nativos de Python"""
    try:
        if isinstance(val, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            return int(val)
        elif isinstance(val, (np.floating, np.float64, np.float32, np.float16)):
            return float(val)
        elif pd.isna(val):
            return None
        elif isinstance(val, (int, float, str, bool, type(None))):
            return val
        else:
            # Para otros tipos, intentar convertir a string
            return str(val)
    except:
        return val

def build_metrics_payload(df: pd.DataFrame, df_events: pd.DataFrame = None, rotate_seconds: int = 12) -> Dict[str, Any]:
    if df is None or df.empty:
        return {
            "rotate_seconds": rotate_seconds,
            "active_metric": None,
            "dynamic_metrics": [],
            "fixed_metrics": {
                "embarque_completado": {"porcentaje": 0, "completados": 0, "total": 0},
                "ritmo_embarque": {"promedio_minutos": 0, "promedio_texto": "N/D"},
                "viaje_actual": {"evento": "N/D", "hora": ""},
                "proxima_escala": {"evento": "N/D", "hora": ""}
            }
        }

    d = df.copy()
    d.columns = [c.strip() for c in d.columns]

    for col in ["Edad", "Sexo", "Estado Civil", "Mesa", "Checkin", "Pasajero", "Hora"]:
        if col not in d.columns:
            d[col] = ""

    # --- Normalizaciones robustas ---
    # Edad: convierte todo lo posible a número; lo demás -> NaN
    d["Edad_num"] = pd.to_numeric(d["Edad"], errors="coerce")

    # Filtramos edades válidas (convertir a int nativo de Python)
    ages = [int(x) for x in d["Edad_num"].dropna().tolist()]

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
        matching = d[d["Edad_num"].astype("float").eq(float(min_age))]
        if not matching.empty:
            row = matching.iloc[0]
            metrics.append(metric("🍼", "Pasajero más joven", f'{row["Pasajero_str"]} — {min_age}', "A bordo"))
        else:
            metrics.append(metric("🍼", "Pasajero más joven", "N/D", ""))
    else:
        metrics.append(metric("🍼", "Pasajero más joven", "N/D", ""))

    # 4) Pasajero más experimentado
    if ages:
        max_age = max(ages)
        matching = d[d["Edad_num"].astype("float").eq(float(max_age))]
        if not matching.empty:
            row = matching.iloc[0]
            metrics.append(metric("🎩", "Pasajero más experimentado", f'{row["Pasajero_str"]} — {max_age}', "A bordo"))
        else:
            metrics.append(metric("🎩", "Pasajero más experimentado", "N/D", ""))
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
        ratio_pct = int(round(float(best["ratio"]) * 100))
        mesa_name = str(best["Mesa_str"]) if pd.notna(best["Mesa_str"]) else "N/D"
        metrics.append(metric("🚀", "Mesa más puntual", mesa_name, f'{ratio_pct}% ON TIME'))
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

    # Rotación backend para métricas dinámicas
    if metrics:
        now = time.time()
        if now - _state["last"] >= rotate_seconds:
            _state["idx"] = (_state["idx"] + 1) % len(metrics)
            _state["last"] = now
        # Asegurar que el índice esté dentro del rango válido
        if _state["idx"] >= len(metrics):
            _state["idx"] = 0

    # ========== MÉTRICAS FIJAS ==========
    fixed_metrics = {}
    
    # 1) Embarque completado (%)
    total_pasajeros = len(d)
    checkin_count = int(d["Checkin_norm"].eq("ON TIME").sum())  # Solo contar ON TIME
    if total_pasajeros > 0:
        porcentaje = float((checkin_count / total_pasajeros) * 100)
        fixed_metrics["embarque_completado"] = {
            "porcentaje": float(round(porcentaje, 1)),
            "completados": int(checkin_count),
            "total": int(total_pasajeros)
        }
    else:
        fixed_metrics["embarque_completado"] = {
            "porcentaje": 0.0,
            "completados": 0,
            "total": 0
        }
    
    # 2) Ritmo de embarque (promedio de tiempo entre checkins)
    horas_checkin = []
    if "Hora" in d.columns:
        horas_checkin = d[d["Hora"].astype(str).str.strip().ne("")]["Hora"].tolist()
    if len(horas_checkin) >= 2:
        tiempos = sorted([_parse_time(h) for h in horas_checkin if _parse_time(h) > 0])
        if len(tiempos) >= 2:
            diferencias = [tiempos[i+1] - tiempos[i] for i in range(len(tiempos)-1)]
            if diferencias:
                promedio_min = float(sum(diferencias) / len(diferencias))
                fixed_metrics["ritmo_embarque"] = {
                    "promedio_minutos": float(round(promedio_min, 1)),
                    "promedio_texto": f"{int(promedio_min)} min"
                }
            else:
                fixed_metrics["ritmo_embarque"] = {"promedio_minutos": 0.0, "promedio_texto": "N/D"}
        else:
            fixed_metrics["ritmo_embarque"] = {"promedio_minutos": 0.0, "promedio_texto": "N/D"}
    else:
        fixed_metrics["ritmo_embarque"] = {"promedio_minutos": 0.0, "promedio_texto": "N/D"}
    
    # 3) Viaje Actual y 4) Próxima Escala (basado en eventos)
    now_time = datetime.now()
    current_minutes = now_time.hour * 60 + now_time.minute
    
    fixed_metrics["viaje_actual"] = {"evento": "N/D", "hora": ""}
    fixed_metrics["proxima_escala"] = {"evento": "N/D", "hora": ""}
    
    try:
        if df_events is not None and not df_events.empty:
            df_ev = df_events.copy()
            df_ev.columns = [c.strip() for c in df_ev.columns]
            
            # Buscar columnas de eventos
            col_hora = None
            col_evento = None
            for c in df_ev.columns:
                c_lower = c.lower()
                if col_hora is None and any(x in c_lower for x in ["hora", "horario", "time"]):
                    col_hora = c
                if col_evento is None and any(x in c_lower for x in ["evento", "event", "actividad"]):
                    col_evento = c
            
            if col_hora and col_evento:
                # Ordenar eventos por hora
                df_ev["_hora_min"] = df_ev[col_hora].apply(_parse_time)
                df_ev = df_ev[df_ev["_hora_min"] > 0].sort_values("_hora_min")
                
                if not df_ev.empty:
                    # Buscar evento actual (el que está en curso o el más reciente pasado)
                    current_event = None
                    for _, row in df_ev.iterrows():
                        if row["_hora_min"] <= current_minutes:
                            current_event = row
                        else:
                            break
                    
                    if current_event is not None:
                        fixed_metrics["viaje_actual"] = {
                            "evento": str(current_event[col_evento]),
                            "hora": str(current_event[col_hora])
                        }
                    
                    # Buscar próxima escala (primer evento futuro)
                    next_event = df_ev[df_ev["_hora_min"] > current_minutes]
                    if not next_event.empty:
                        next_row = next_event.iloc[0]
                        fixed_metrics["proxima_escala"] = {
                            "evento": str(next_row[col_evento]),
                            "hora": str(next_row[col_hora])
                        }
    except Exception:
        # Si hay error procesando eventos, dejamos los valores por defecto
        pass

    # 5) Último check-in
    fixed_metrics["ultimo_checkin"] = {"nombre": "N/D", "hora": ""}
    
    # Filtrar solo los ON TIME
    ontime_df = d[d["Checkin_norm"].eq("ON TIME")]
    if not ontime_df.empty and "Hora" in ontime_df.columns:
        # Intentar ordenar por hora
        # Usamos _parse_time para obtener minutos y ordenar
        # IMPORTANTE: Usamos .copy() para evitar SettingWithCopyWarning
        ontime_df = ontime_df.copy()
        ontime_df["_minutos"] = ontime_df["Hora"].astype(str).apply(_parse_time)
        # Ordenar descendente (mayor minuto = mas tarde)
        last_pax = ontime_df.sort_values("_minutos", ascending=False).iloc[0]
        
        fixed_metrics["ultimo_checkin"] = {
            "nombre": str(last_pax["Pasajero_str"]),
            "hora": str(last_pax["Hora"])
        }

    # Obtener métrica activa de forma segura
    active_metric = None
    if metrics and _state["idx"] < len(metrics):
        active_metric = metrics[_state["idx"]]
        # Convertir valores numéricos a tipos nativos
        if active_metric:
            active_metric = {k: _to_native(v) if isinstance(v, (pd.Series, pd.DataFrame)) == False else str(v) for k, v in active_metric.items()}

    # Convertir todos los valores numéricos en fixed_metrics a tipos nativos
    for key in fixed_metrics:
        if isinstance(fixed_metrics[key], dict):
            fixed_metrics[key] = {k: _to_native(v) for k, v in fixed_metrics[key].items()}

    return {
        "rotate_seconds": int(rotate_seconds),
        "active_metric": active_metric,
        "dynamic_metrics": metrics,  # Solo las dinámicas que rotan
        "fixed_metrics": fixed_metrics  # Las fijas que siempre se muestran
    }
