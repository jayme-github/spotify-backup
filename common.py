import os

from pathlib import Path

from spotipy import Spotify
from spotipy.cache_handler import CacheFileHandler
from spotipy.oauth2 import SpotifyOAuth

CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID", "aba916bbd6214fdc8bc993344439c58e")
REDIRECT_URI = "http://localhost/"

SAVED_OBJECT_TYPES = ("albums", "episodes", "shows", "tracks")
TOP_OBJECT_TYPES = ("artists", "tracks")
TOP_RANGES = ("short_term", "medium_term", "long_term")


class SpotifyBase:
    def __init__(self, scopes: tuple, cache_file: str, backup_path: Path):
        self.cache_file = cache_file
        self.scopes = scopes
        self.sp = self.get_client()
        self.user_id = self.sp.me()["id"]
        self.backup_path = backup_path

    def cache_path(self) -> Path:
        fallback_path = Path(os.environ["HOME"], ".config")
        return Path(
            os.environ.get("XDG_CONFIG_HOME", fallback_path),
            self.cache_file,
        )

    def get_client(self) -> Spotify:
        auth_manager = SpotifyOAuth(
            client_id=CLIENT_ID,
            redirect_uri=REDIRECT_URI,
            open_browser=False,
            scope=",".join(self.scopes),
            cache_handler=CacheFileHandler(cache_path=self.cache_path()),
        )
        return Spotify(auth_manager=auth_manager)
