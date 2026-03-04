@echo off
echo Iniciando Checkin Totem V1...

:: Verificamos si existe el entorno virtual
if not exist .venv (
    echo Creando entorno virtual...
    python -m venv .venv
)

:: Activamos el entorno virtual e instalamos dependencias
echo Validando dependencias...
call .venv\Scripts\activate
pip install -r requirements.txt --quiet

:: Abrimos el navegador (opcional, pero ayuda al usuario)
start http://127.0.0.1:5000/dashboard

:: Ejecutamos la app
echo Ejecutando aplicacion...
python app.py

pause
