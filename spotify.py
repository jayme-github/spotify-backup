import os
from pathlib import Path
from typing import Callable, Dict

from spotipy import Spotify
from spotipy.cache_handler import CacheFileHandler
from spotipy.oauth2 import SpotifyOAuth

CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID", "aba916bbd6214fdc8bc993344439c58e")
REDIRECT_URI = "http://localhost/"
# SPOTIPY_CLIENT_SECRET must be provided via env variable
SCOPES = (
    "playlist-read-private",
    "playlist-read-collaborative",
    "user-library-read",  # Needed to read saved tracks
    "user-top-read",
    "user-follow-read",
    "user-read-recently-played",
)


class SpotifyClient(Spotify):
    def __init__(self):
        self._auth_manager = SpotifyOAuth(
            client_id=CLIENT_ID,
            redirect_uri=REDIRECT_URI,
            open_browser=False,
            scope=",".join(SCOPES),
            cache_handler=CacheFileHandler(cache_path=self.cache_path()),
        )
        super().__init__(auth_manager=self._auth_manager)
        self.user_id = self.me()["id"]

    @staticmethod
    def cache_path() -> Path:
        fallback_path = Path(os.environ["HOME"], ".config")
        return Path(
            os.environ.get("XDG_CONFIG_HOME", fallback_path),
            "spotify-backup",
        )

    def get_all_items(self, func: Callable, **kwargs) -> Dict:
        """Return all items for a paginated result set of Spotify"""
        result = func(**kwargs)
        items = result["items"]
        while result["next"]:
            result = self.next(result)
            items.extend(result["items"])
        return items
