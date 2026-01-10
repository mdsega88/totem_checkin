SHEET_ID = "101WhDSA5C3Sh-_ke7k2pzWQQMB0kKMSxdR_CgcjLtvE"
GID_DISPLAY = "1460795540"

REFRESH_SECONDS = 5
PAGE_SIZE = 10
ROTATE_SECONDS = 5

def csv_url(sheet_id: str, gid: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

DISPLAY_CSV_URL = csv_url(SHEET_ID, GID_DISPLAY)
