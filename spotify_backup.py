#!/bin/env python3

import os
import argparse
import logging
import json
from pathlib import Path
from typing import Dict, List

from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import CacheFileHandler


SCOPE = "playlist-read-private,playlist-read-collaborative"

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

    def get_playlists(self) -> List[Dict]:
        result = self.sp.current_user_playlists()
        playlists = result["items"]

        while result["next"]:
            result = self.sp.next(result)
            playlists.extend(result["items"])

        return playlists

    @staticmethod
    def _dump_json(path: Path, j: Dict):
        with open(path, "w") as f:
            json.dump(j, f, sort_keys=True, indent=2)

    def backup_playlist(self, playlist_id: str, path: Path):
        playlist_result = self.sp.playlist(playlist_id)
        all_items = playlist_result["tracks"]["items"]
        # Read all tracks and extend the tracks array with all of them.
        # This will keep the order of tracks straight as well.
        while playlist_result["tracks"]["next"]:
            track_result = self.sp.next(playlist_result["tracks"])
            all_items.extend(track_result["items"])
            track_result["items"] = all_items
            playlist_result["tracks"] = track_result

        self._dump_json(path / playlist_id, playlist_result)

    def backup_playlists(self):
        path = Path("playlists")
        for playlist in self.get_playlists():
            if playlist["owner"]["id"] == self.user_id:
                # my playlist
                self.backup_playlist(playlist["id"], self.backup_dir(path / "my"))
            else:
                # starred playlist
                self.backup_playlist(playlist["id"], self.backup_dir(path / "starred"))

    def backup_dir(self, what: Path) -> Path:
        bdir = self.backup_path / what
        bdir.mkdir(parents=True, exist_ok=True)
        return bdir


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
    sb.backup_playlists()


if __name__ == "__main__":
    main()
