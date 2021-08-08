#!/bin/env python3

import os
import argparse
import logging
import json
from pathlib import Path
from typing import Dict, List, Optional

from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import CacheFileHandler


SCOPE = "playlist-read-private,playlist-read-collaborative,user-library-read"

logger = logging.getLogger(__name__)


class SpotifyBackup:
    def __init__(self, backup_path: Path):
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
            scope=SCOPE, cache_handler=CacheFileHandler(cache_path=self.cache_path())
        )
        return Spotify(auth_manager=auth_manager)

    @staticmethod
    def _dump_json(path: Path, j: Dict):
        # Ensure .json suffix
        if not path.suffix == ".json":
            path = path.parent / (path.name + ".json")
        with open(path, "w") as f:
            json.dump(j, f, sort_keys=True, indent=2)

    def _ensure_dir(self, what: Optional[Path] = None) -> Path:
        if what is None:
            bdir = self.backup_path
        else:
            bdir = self.backup_path / what
        bdir.mkdir(parents=True, exist_ok=True)
        return bdir

    def get_playlists(self) -> List[Dict]:
        result = self.sp.current_user_playlists()
        playlists = result["items"]

        while result["next"]:
            result = self.sp.next(result)
            playlists.extend(result["items"])

        return playlists

    def backup_everything(self):
        self.backup_playlists()
        self.backup_saved_tracks()

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
            except:
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
        for playlist in self.get_playlists():
            if playlist["owner"]["id"] == self.user_id:
                # my playlist
                backup_dir = self._ensure_dir(path / "my")
            else:
                # starred playlist
                backup_dir = self._ensure_dir(path / "starred")
            self._backup_playlist(playlist, backup_dir)

    def backup_saved_tracks(self):
        """Backup users saved tracks"""
        result = self.sp.current_user_saved_tracks()
        saved_tracks = result["items"]

        while result["next"]:
            result = self.sp.next(result)
            saved_tracks.extend(result["items"])

        self._dump_json(self._ensure_dir() / "saved_tracks.json", saved_tracks)


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

    sb = SpotifyBackup(args.backup_dir)
    sb.backup_everything()


if __name__ == "__main__":
    main()
