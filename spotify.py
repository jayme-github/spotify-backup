import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List

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

logger = logging.getLogger(__name__)


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

    @staticmethod
    def chunks(lst: Iterable, n: int) -> Iterable[Any]:
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    def create_playlist(self, name: str, uris: List[str]) -> Dict:
        """Create a copy of the playlist in json_path"""
        uris_count = len(uris)
        logger.info("Copying playlist as %s (%d tracks)", name, uris_count)

        dst = self.user_playlist_create(
            self.user_id,
            name,
            public=False,
            collaborative=False,
        )
        dst_id = dst["id"]
        logger.debug("Destination playlist ID: %s", dst_id)

        count = 0
        for chunk in self.chunks(uris, 100):
            count += len(chunk)
            self.playlist_add_items(dst_id, chunk)
            logger.info(
                "Copied %s of %s tracks",
                count,
                uris_count,
            )

        return dst
