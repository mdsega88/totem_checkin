from flask import Flask, jsonify, render_template
from data_source import CsvSheetCache
import config

from modules.passengers import build_passengers_payload
from modules.events import build_events_payload
from modules.metrics import build_metrics_payload
from modules.aduana import build_aduana_payload

app = Flask(__name__)

display_cache = CsvSheetCache(config.DISPLAY_CSV_URL, refresh_seconds=config.REFRESH_SECONDS)
events_cache = CsvSheetCache(config.EVENTS_CSV_URL, refresh_seconds=config.REFRESH_SECONDS)

@app.get("/")
def home():
    return render_template("index.html")

@app.get("/events")
def events_page():
    return render_template("events.html")

@app.get("/dashboard")
def dashboard_page():
    return render_template("dashboard.html")


@app.get("/data/passengers")
def data_passengers():
    df = display_cache.get()
    payload = build_passengers_payload(df, page_size=config.PAGE_SIZE, rotate_seconds=config.ROTATE_SECONDS)
    return jsonify(payload)

@app.get("/data/events")
def data_events():
    df = events_cache.get()
    payload = build_events_payload(df, page_size=config.EVENTS_PAGE_SIZE, rotate_seconds=config.EVENTS_ROTATE_SECONDS)
    return jsonify(payload)

@app.get("/data/metrics")
def data_metrics():
    try:
        df = display_cache.get()
        df_events = events_cache.get()
        return jsonify(build_metrics_payload(df, df_events, rotate_seconds=12))
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.get("/data/aduana")
def data_aduana():
    df = display_cache.get()
    return jsonify(build_aduana_payload(df, rotate_seconds=5))

from modules.weather import WeatherService

# Coordenadas Villa de Mayo
weather_service = WeatherService(lat=-34.50, lon=-58.68)

@app.get("/data/weather")
def data_weather():
    return jsonify(weather_service.get_current())

if __name__ == "__main__":
    import webbrowser
    webbrowser.open("http://127.0.0.1:5000/dashboard")
    app.run(host="127.0.0.1", port=5000, debug=False)
