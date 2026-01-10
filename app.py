from flask import Flask, jsonify, render_template
from data_source import CsvSheetCache
import config

from modules.passengers import build_passengers_payload
from modules.events import build_events_payload
from modules.metrics import build_metrics_payload
from modules.aduana import build_aduana_payload

app = Flask(__name__)

display_cache = CsvSheetCache(config.DISPLAY_CSV_URL, refresh_seconds=config.REFRESH_SECONDS)

@app.get("/")
def home():
    return render_template("index.html")

@app.get("/data/passengers")
def data_passengers():
    df = display_cache.get()
    payload = build_passengers_payload(df, page_size=config.PAGE_SIZE, rotate_seconds=config.ROTATE_SECONDS)
    return jsonify(payload)

# Stubs (para completar luego)
@app.get("/data/events")
def data_events():
    return jsonify(build_events_payload())

@app.get("/data/metrics")
def data_metrics():
    return jsonify(build_metrics_payload())

@app.get("/data/aduana")
def data_aduana():
    return jsonify(build_aduana_payload())

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
