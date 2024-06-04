#!/usr/bin/env python3

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple, get_args

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

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
)


_SAVED_OBJECT_TYPES = Literal["albums", "episodes", "shows", "tracks"]
SAVED_OBJECT_TYPES: Tuple[_SAVED_OBJECT_TYPES, ...] = get_args(_SAVED_OBJECT_TYPES)
_TOP_OBJECT_TYPES = Literal["artists", "tracks"]
TOP_OBJECT_TYPES: Tuple[_TOP_OBJECT_TYPES, ...] = get_args(_TOP_OBJECT_TYPES)
_TOP_RANGES = Literal["short_term", "medium_term", "long_term"]
TOP_RANGES: Tuple[_TOP_RANGES, ...] = get_args(_TOP_RANGES)

logger = logging.getLogger(__name__)


class SpotifyBackup:
    def __init__(self, backup_path: Path, pretty: bool = False):
        self.pretty = pretty
        self.sp = self.get_client()
        self.user_id = self.sp.me()["id"]
        self.backup_path = backup_path

    @staticmethod
    def cache_path() -> Path:
        fallback_path = Path(os.environ["HOME"], ".config")
        return Path(
            os.environ.get("XDG_CONFIG_HOME", fallback_path),
            "spotify-backup",
        )

    def get_client(self) -> Spotify:
        auth_manager = SpotifyOAuth(
            client_id=CLIENT_ID,
            redirect_uri=REDIRECT_URI,
            open_browser=False,
            scope=",".join(SCOPES),
            cache_handler=CacheFileHandler(cache_path=self.cache_path()),
        )
        return Spotify(auth_manager=auth_manager)

    def _dump_json(self, path: Path, j: Dict):
        # Ensure .json suffix
        if not path.suffix == ".json":
            path = path.parent / (path.name + ".json")
        kwargs = {"sort_keys": True}
        if self.pretty:
            kwargs["indent"] = 2
        with open(path, "w") as f:
            json.dump(j, f, **kwargs)

    def _ensure_dir(self, what: Optional[Path] = None) -> Path:
        if what is None:
            bdir = self.backup_path
        else:
            bdir = self.backup_path / what
        bdir.mkdir(parents=True, exist_ok=True)
        return bdir

    def _get_all_items(self, func: Callable, **kwargs) -> Dict:
        """Return all items for a paginated result set of Spotify"""
        result = func(**kwargs)
        items = result["items"]
        while result["next"]:
            result = self.sp.next(result)
            items.extend(result["items"])
        return items

    def backup_everything(self):
        self.backup_playlists()
        self.backup_saved_objects()
        self.backup_top_objects()
        self.backup_followed_artists()

    def _backup_playlist(self, playlist: Dict, path: Path):
        """Backup a playlist, including all track details"""
        # Check if we have a backup already
        playlist_id = playlist["id"]
        playlist_path = path / (playlist_id + ".json")
        # The snapshot_id of spotify playlists changes for every response, so don't
        # bother checking those.
        if not playlist["owner"]["id"] == "spotify":
            try:
                with open(playlist_path, "r") as f:
                    have_snapshot_id = json.load(f).get("snapshot_id", None)
            except:  # noqa: E722
                have_snapshot_id = None
            if playlist["snapshot_id"] == have_snapshot_id:
                # We already have a up to date backup of this playlist
                logger.debug('Playlist "%s": Backup is up to date', playlist["name"])
                return

        playlist_result = self.sp.playlist(playlist_id)
        all_items = playlist_result["tracks"]["items"]
        # Read all tracks and extend the tracks array with all of them.
        # This will keep the order of tracks straight as well.
        while playlist_result["tracks"]["next"]:
            track_result = self.sp.next(playlist_result["tracks"])
            all_items.extend(track_result["items"])
            track_result["items"] = all_items
            playlist_result["tracks"] = track_result

        self._dump_json(playlist_path, playlist_result)
        logger.info('Playlist "%s": Backup created', playlist["name"])

    def backup_playlists(self):
        """Backup all users playlists (own and starred)"""
        path = Path("playlists")
        for playlist in self._get_all_items(self.sp.current_user_playlists):
            if playlist["owner"]["id"] == self.user_id:
                # my playlist
                backup_dir = self._ensure_dir(path / "my")
            else:
                # starred playlist
                backup_dir = self._ensure_dir(path / "starred")
            self._backup_playlist(playlist, backup_dir)

    def backup_saved_objects(
        self, objtype: Optional[_SAVED_OBJECT_TYPES] = None
    ):
        """Backup users saved objects"""

        def _dump(objtype):
            logger.info("Backing up saved %s", objtype)
            func = getattr(self.sp, "current_user_saved_" + objtype)
            result = self._get_all_items(func)
            self._dump_json(self._ensure_dir() / f"saved_{objtype}.json", result)

        if objtype is None:
            for objtype in SAVED_OBJECT_TYPES:
                _dump(objtype)
        else:
            _dump(objtype)

    def backup_top_objects(self, objtype: Optional[_TOP_OBJECT_TYPES] = None):
        """Backup users top objects"""

        def _dump(objtype):
            for top_range in TOP_RANGES:
                logger.info("Backing up top %s %s", objtype, top_range)
                func = getattr(self.sp, "current_user_top_" + objtype)
                result = self._get_all_items(func, time_range=top_range)
                self._dump_json(
                    self._ensure_dir() / f"top_{objtype}_{top_range}.json", result
                )

        if objtype is None:
            for objtype in TOP_OBJECT_TYPES:
                _dump(objtype)
        else:
            _dump(objtype)

    def backup_followed_artists(self):
        """Backup users followed artists"""

        logger.info("Backing up followed artists")
        result = self.sp.current_user_followed_artists()["artists"]
        artists = result["items"]
        while result["next"]:
            result = self.sp.next(result)["artists"]
            artists.extend(result["items"])
        self._dump_json(self._ensure_dir() / "followed_artists.json", artists)


def main():
    parser = argparse.ArgumentParser(
        description="Backup Spotify playlists and liked songs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="count")
    parser.add_argument(
        "--backup-dir",
        type=lambda p: Path(p).absolute(),
        default=Path(__file__).absolute().parent / "backup",
        help="Backup path",
    )
    parser.add_argument(
        "--pretty", action="store_true", help='Create "pretty" JSON files'
    )

    args = parser.parse_args()

    level = logging.WARNING
    if args.verbose is not None:
        if args.verbose > 0:
            level = logging.INFO
        if args.verbose > 1:
            level = logging.DEBUG
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] (%(name)s.%(funcName)s) %(message)s",  # NOQA
        level=level,
    )

    sb = SpotifyBackup(args.backup_dir, args.pretty)
    sb.backup_everything()


if __name__ == "__main__":
    main()
