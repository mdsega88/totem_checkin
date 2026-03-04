import requests
import time
from typing import Dict, Any

class WeatherService:
    def __init__(self, lat: float, lon: float, refresh_seconds: int = 300):
        self.lat = lat
        self.lon = lon
        self.refresh_seconds = refresh_seconds
        self._cache_data = None
        self._last_fetch = 0.0
        # Open-Meteo URL
        self.url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}&"
            f"current=temperature_2m,precipitation,weather_code,wind_speed_10m&"
            f"hourly=precipitation_probability&"
            f"forecast_days=1&timezone=auto"
        )

    def _get_icon(self, code: int) -> str:
        # WMO Weather interpretation codes (WW)
        # 0: Clear sky
        if code == 0: return "☀️"
        # 1, 2, 3: Mainly clear, partly cloudy, and overcast
        if code in [1, 2, 3]: return "🌥️"
        # 45, 48: Fog
        if code in [45, 48]: return "🌫️"
        # 51, 53, 55: Drizzle
        if code in [51, 53, 55]: return "ddd"
        # 61, 63, 65: Rain
        if code in [61, 63, 65]: return "🌧️"
        # 80, 81, 82: Rain showers
        if code in [80, 81, 82]: return "🌦️"
        # 95, 96, 99: Thunderstorm
        if code in [95, 96, 99]: return "⚡"
        
        return "⛅"

    def get_current(self) -> Dict[str, Any]:
        now = time.time()
        if self._cache_data and (now - self._last_fetch < self.refresh_seconds):
            return self._cache_data

        try:
            # Timeout cortito para no bloquear
            resp = requests.get(self.url, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            
            # Current units
            current = data.get("current", {})
            temp = current.get("temperature_2m", 0)
            wcode = current.get("weather_code", 0)
            wind = current.get("wind_speed_10m", 0)
            
            # Hourly for precipitation probability (take current hour)
            # Find index of current hour
            # Simplificación: tomamos el primer valor de la lista horaria si está disponible
            # Idealmente buscaríamos la hora actual, pero para MVP tomamos el max de las proximas horas o el actual
            rain_prob = 0
            hourly = data.get("hourly", {})
            probs = hourly.get("precipitation_probability", [])
            if probs:
                # Tomamos un promedio o el valor de la hora actual.
                # Como hourly empieza en 00:00 del día, calculamos hora local aproximada
                # O simplemente tomamos el valor máximo del día para ser pesimistas, o el primer valor del array si asumimos que la API devuelve desde 'now' (a veces devuelve todo el día)
                # Open-Meteo 'forecast_days=1' devuelve las 24hs del día actual.
                from datetime import datetime
                curr_hour = datetime.now().hour
                if curr_hour < len(probs):
                    rain_prob = probs[curr_hour]
            
            self._cache_data = {
                "location": "VILLA DE MAYO",
                "temp": f"{round(temp)}°C",
                "icon": self._get_icon(wcode),
                "sky": "Despejado" if wcode == 0 else ("Nublado" if wcode > 2 else "Parcial"), # Simplificado
                "rain_prob": f"{rain_prob}%",
                "wind": f"{round(wind)} km/h"
            }
            self._last_fetch = now
            return self._cache_data
            
        except Exception as e:
            print(f"Weather error: {e}")
            # Retornar caché viejo si existe, sino error
            if self._cache_data:
                return self._cache_data
                
            return {
                "location": "VILLA DE MAYO",
                "temp": "--°C",
                "icon": "❓",
                "sky": "--",
                "rain_prob": "--%",
                "wind": "-- km/h"
            }
