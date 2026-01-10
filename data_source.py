import time
import pandas as pd

class CsvSheetCache:
    def __init__(self, url: str, refresh_seconds: int = 5):
        self.url = url
        self.refresh_seconds = refresh_seconds
        self._last_fetch = 0.0
        self._cache_df = pd.DataFrame()

    def get(self) -> pd.DataFrame:
        now = time.time()
        if now - self._last_fetch < self.refresh_seconds and not self._cache_df.empty:
            return self._cache_df

        df = pd.read_csv(self.url)
        df.columns = [c.strip() for c in df.columns]
        self._cache_df = df
        self._last_fetch = now
        return df
