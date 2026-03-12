from typing import Dict, Any, List
import pandas as pd
import numpy as np
import time
from datetime import datetime

_state = {"idx": 0, "last": 0.0}

def _generation(age: int) -> str:
    if age < 27: return "GEN Z (0 a 26 años)"
    if age <= 42: return "MILLENNIALS (27 a 42 años)"
    if age <= 58: return "GEN X (43 a 58 años)"
    return "BABY BOOMERS (59+ años)"

def _parse_time(time_str: str) -> int:
    """Convierte hora string (HH:MM o HH:MM:SS o HH.MM) a segundos desde medianoche"""
    try:
        t = str(time_str).strip().replace(',', '.')
        if not t or t.lower() == 'nan': return 0
        
        # Caso HH:MM:SS o HH:MM
        if ':' in t:
            parts = t.split(':')
            if len(parts) >= 3:
                return int(float(parts[0])) * 3600 + int(float(parts[1])) * 60 + int(float(parts[2]))
            if len(parts) == 2:
                return int(float(parts[0])) * 3600 + int(float(parts[1])) * 60
        
        # Caso HH.MM (Punto en lugar de dos puntos)
        if '.' in t:
            parts = t.split('.')
            if len(parts) >= 2:
                # Limitamos a 2 decimales para HH.MM
                h = int(float(parts[0]))
                m = int(float(parts[1][:2]))
                return h * 3600 + m * 60
        
        return 0
    except:
        return 0

def _format_duration(sec: float) -> str:
    """Formatea segundos en MM:SS mns"""
    m = int(sec // 60)
    s = int(sec % 60)
    return f"{m:02d}:{s:02d} mins"

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
                "embarque_completado": {"porcentaje": 0, "status_text": "Esperando pasajeros...", "status_color": "#EF4444", "completados": 0, "total": 0},
                "ritmo_embarque": {"promedio_segundos": 0.0, "promedio_texto": "N/D"},
                "viaje_actual": {"evento": "N/D", "hora": ""},
                "proxima_escala": {"evento": "N/D", "hora": ""}
            }
        }

    d = df.copy()
    d.columns = [c.strip() for c in d.columns]
    
    # Normalización de nombres de columnas (ignorando mayúsculas)
    col_map = {c.lower().strip(): c for c in d.columns}
    def get_col(candidates):
        for c in candidates:
            if c.lower() in col_map:
                return col_map[c.lower()]
        return None

    # Normalizaciones clave
    actual_hora_col = get_col(["Hora", "Horario", "Time"]) or "Hora"
    if actual_hora_col not in d.columns: d[actual_hora_col] = ""
    
    d["Checkin_norm"] = d[get_col(["Checkin", "Status"]) or "Checkin"].astype(str).str.strip().str.upper()
    d["Pasajero_str"] = d[get_col(["Pasajero", "Name", "Guest"]) or "Pasajero"].astype(str).str.strip()
    d["Mesa_str"] = d[get_col(["Mesa", "Table"]) or "Mesa"].astype(str).str.strip()
    d["Sexo_norm"] = d[get_col(["Sexo", "Gender"]) or "Sexo"].astype(str).str.strip().str.upper()
    d["Estado_norm"] = d[get_col(["Estado Civil", "Status"]) or "Estado Civil"].astype(str).str.strip().str.upper()

    # --- NORMALIZACIÓN DE DATOS ---
    def extract_risk(val):
        try:
            if pd.isna(val) or str(val).strip() == "": return 0.0
            val_str = str(val).strip()
            # Contar estrellas emoji ⭐
            stars = val_str.count("⭐")
            if stars > 0: return float(stars)
            for char in val_str:
                if char.isdigit(): return float(char)
            return 0.0
        except: return 0.0

    d["Peligrosidad_num"] = d[get_col(["Peligrosidad", "Interpol"]) or "Peligrosidad"].apply(extract_risk)
    d["Edad_num"] = pd.to_numeric(d[get_col(["Edad", "Age"]) or "Edad"], errors="coerce")
    
    ages = [int(x) for x in d["Edad_num"].dropna().tolist()]

    def metric(icon, title, value, subtitle=""):
        return {"icon": icon, "title": title, "value": value, "subtitle": subtitle}

    metrics: List[Dict[str, Any]] = []

    # 1) Mesa más puntual (Excluimos MESA M&M)
    d_puntual = d[d["Mesa_str"].str.upper() != "MESA M&M"].copy()
    ontime = d_puntual["Checkin_norm"].eq("ON TIME")
    mesa_stats = d_puntual.groupby("Mesa_str").agg(
        total=("Mesa_str", "count"),
        ontime=("Mesa_str", lambda s: int(ontime.loc[s.index].sum()))
    ).reset_index()
    if not mesa_stats.empty:
        mesa_stats["ratio"] = mesa_stats["ontime"] / mesa_stats["total"]
        best = mesa_stats.sort_values(by=["ratio", "ontime"], ascending=False).iloc[0]
        ratio_pct = int(round(float(best["ratio"]) * 100))
        metrics.append(metric("🚀", "Mesa más puntual", str(best["Mesa_str"]), f"{ratio_pct}% ON TIME"))

    # Métricas de Edad y Generación
    if ages:
        avg = sum(ages) / len(ages)
        metrics.append(metric("🎂", "Edad promedio del vuelo", f"{avg:.0f} AÑOS", "Vuelo SZ2803"))

        total_exp = sum(ages)
        metrics.append(metric("⏳", "EXPERIENCIA TOTAL DEL VUELO", f"{total_exp} AÑOS", "Suma de todas las vivencias"))

        # Orden cronológico (Menores primero)
        gen_order = {
            "GEN Z (0 a 26 años)": 0,
            "MILLENNIALS (27 a 42 años)": 1,
            "GEN X (43 a 58 años)": 2,
            "BABY BOOMERS (59+ años)": 3
        }
        gens_list = [_generation(a) for a in ages]
        gen_counts = pd.Series(gens_list).value_counts(normalize=True) * 100
        
        # Ordenar según el mapa de orden
        sorted_gens = sorted(gen_counts.items(), key=lambda x: gen_order.get(x[0], 99))
        for g_name, g_pct in sorted_gens:
            metrics.append(metric("🧬", f"GENERACIÓN {g_name}", f"{g_pct:.1f}%", "Distribución de pasajeros"))

    # Mercado activo
    is_soltero = d["Estado_norm"].str.contains("SOLTER", na=False)
    solteras = int(((d["Sexo_norm"] == "F") & is_soltero).sum())
    solteros = int(((d["Sexo_norm"] == "M") & is_soltero).sum())
    metrics.append(metric("💘", "Mercado activo", f"{solteras} SOLTERAS / {solteros} SOLTEROS", "Cantidad confirmada"))

    # Estado civil (3 métricas separadas)
    total_val = len(d)
    if total_val > 0:
        s_count = int(is_soltero.sum())
        p_count = int(d["Estado_norm"].str.contains("PAREJA|NOVIO", na=False).sum())
        c_count = int(d["Estado_norm"].str.contains("CASADO|CONYUGE", na=False).sum())
        
        metrics.append(metric("💍", "Pasajeros Solteros", f"{(s_count/total_val*100):.1f}%", f"{s_count} invitados"))
        metrics.append(metric("🥂", "En Pareja / Novios", f"{(p_count/total_val*100):.1f}%", f"{p_count} invitados"))
        metrics.append(metric("👰", "Pasajeros Casados", f"{(c_count/total_val*100):.1f}%", f"{c_count} invitados"))

    # Mesa Joven/Exp
    mesa_age = d.dropna(subset=["Edad_num"]).groupby("Mesa_str")["Edad_num"].mean()
    if not mesa_age.empty:
        metrics.append(metric("🔥", "Mesa más joven", str(mesa_age.sort_values().index[0]), "Menor edad promedio"))
        metrics.append(metric("🎓", "Mesa más experimentada", str(mesa_age.sort_values(ascending=False).index[0]), "Mayor edad promedio"))

    # Mesa Peligrosa
    mesa_danger = d.groupby("Mesa_str")["Peligrosidad_num"].mean()
    if not mesa_danger.empty:
        metrics.append(metric("🚨", "Mesa más peligrosa", str(mesa_danger.sort_values(ascending=False).index[0]), "Nivel de riesgo acumulado"))

    # Peligrosidad Vuelo
    if not d.empty:
        avg_danger = d["Peligrosidad_num"].mean()
        metrics.append(metric("⭐", "Peligrosidad del vuelo", f"{avg_danger:.1f} ESTRELLAS", "Promedio general de riesgo"))

    # Rotación backend para métricas dinámicas
    if metrics:
        now = time.time()
        if now - _state["last"] >= rotate_seconds:
            _state["idx"] = (_state["idx"] + 1) % len(metrics)
            _state["last"] = now
        if _state["idx"] >= len(metrics):
            _state["idx"] = 0

    # ========== MÉTRICAS FIJAS ==========
    fixed_metrics = {}
    
    # 1) Embarque completado (%)
    total_pasajeros = len(d)
    checkin_count = int(d["Checkin_norm"].eq("ON TIME").sum())  # Solo contar ON TIME
    
    status_text = "Los primeros pasajeros comienzan a llegar."
    status_color = "#EF4444"
    porcentaje = 0.0
    
    if total_pasajeros > 0:
        porcentaje = float((checkin_count / total_pasajeros) * 100)
        p = porcentaje
        if p >= 100:
            status_text = "El capitán autoriza el descontrol."
            status_color = "#16A34A"
        elif p >= 90:
            status_text = "Último llamado para pasajeros rezagados."
            status_color = "#22C55E"
        elif p >= 75:
            status_text = "La pista de baile comienza a activarse."
            status_color = "#4ADE80"
        elif p >= 50:
            status_text = "El vuelo alcanza velocidad social."
            status_color = "#A3E635"
        elif p >= 25:
            status_text = "El vuelo empieza a llenarse."
            status_color = "#FACC15"
        elif p >= 10:
            status_text = "La tripulación comienza a recibir pasajeros."
            status_color = "#F97316"

    fixed_metrics["embarque_completado"] = {
        "porcentaje": float(round(porcentaje, 1)),
        "status_text": status_text,
        "status_color": status_color,
        "completados": int(checkin_count),
        "total": int(total_pasajeros)
    }
    
    # 2) Ritmo de embarque (promedio de tiempo entre checkins)
    horas_checkin = []
    if actual_hora_col in d.columns:
        # Filtrar solo los que tienen Checkin ON TIME para ritmo real de llegada
        d_checkins = d[d["Checkin_norm"] == "ON TIME"]
        horas_checkin = d_checkins[d_checkins[actual_hora_col].astype(str).str.strip().ne("")][actual_hora_col].tolist()
        
    if len(horas_checkin) >= 2:
        # Tiempos en SEGUNDOS
        tiempos = sorted([_parse_time(h) for h in horas_checkin if _parse_time(h) > 0])
        if len(tiempos) >= 2:
            diferencias = [tiempos[i+1] - tiempos[i] for i in range(len(tiempos)-1)]
            if diferencias:
                promedio_seg = float(sum(diferencias) / len(diferencias))
                fixed_metrics["ritmo_embarque"] = {
                    "promedio_segundos": float(round(promedio_seg, 1)),
                    "promedio_texto": _format_duration(promedio_seg)
                }
            else:
                fixed_metrics["ritmo_embarque"] = {"promedio_segundos": 0.0, "promedio_texto": "N/D"}
        else:
            fixed_metrics["ritmo_embarque"] = {"promedio_segundos": 0.0, "promedio_texto": "N/D"}
    else:
        fixed_metrics["ritmo_embarque"] = {"promedio_segundos": 0.0, "promedio_texto": "N/D"}
    
    # 3) Viaje Actual y 4) Próxima Escala (basado en eventos)
    now_time = datetime.now()
    current_seconds = now_time.hour * 3600 + now_time.minute * 60 + now_time.second
    
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
                        if row["_hora_min"] <= current_seconds:
                            current_event = row
                        else:
                            break
                    
                    if current_event is not None:
                        fixed_metrics["viaje_actual"] = {
                            "evento": str(current_event[col_evento]),
                            "hora": str(current_event[col_hora])
                        }
                    
                    # Buscar próxima escala (primer evento futuro)
                    next_event = df_ev[df_ev["_hora_min"] > current_seconds]
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
    if not ontime_df.empty and actual_hora_col in ontime_df.columns:
        # Intentar ordenar por hora
        # Usamos _parse_time para obtener minutos y ordenar
        # IMPORTANTE: Usamos .copy() para evitar SettingWithCopyWarning
        ontime_df = ontime_df.copy()
        ontime_df["_minutos"] = ontime_df[actual_hora_col].astype(str).apply(_parse_time)
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
