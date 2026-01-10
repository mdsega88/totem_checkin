SHEET_ID = "101WhDSA5C3Sh-_ke7k2pzWQQMB0kKMSxdR_CgcjLtvE"
GID_DISPLAY = "1460795540"

# NUEVO: hoja Horarios (cambiá este gid por el de tu hoja "Horarios")
GID_EVENTS = "1682462321"

REFRESH_SECONDS = 5
PAGE_SIZE = 10
ROTATE_SECONDS = 5

# NUEVO: settings específicos de Events (podés dejarlos iguales si querés)
EVENTS_PAGE_SIZE = 10
EVENTS_ROTATE_SECONDS = 6

def csv_url(sheet_id: str, gid: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

DISPLAY_CSV_URL = csv_url(SHEET_ID, GID_DISPLAY)

# NUEVO
EVENTS_CSV_URL = csv_url(SHEET_ID, GID_EVENTS)
