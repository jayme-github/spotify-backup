#!/usr/bin/env python3

import argparse
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, get_args

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal  # type: ignore

from spotipy.exceptions import SpotifyException

from spotify import SpotifyClient
from spotify_history import SpotifyHistoryDB

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
        self.backup_path = backup_path
        self.sp = SpotifyClient()

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

    def backup_everything(self):
        everything = (
            self.backup_playlists,
            self.backup_saved_objects,
            self.backup_top_objects,
            self.backup_followed_artists,
            self.backup_history,
        )
        for func in everything:
            try:
                func()
            except SpotifyException as e:
                logger.error("Error during backup %s: %s", func.__name__, e)

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
        logger.info("Backing up playlists")
        path = Path("playlists")
        for playlist in self.sp.get_all_items(self.sp.current_user_playlists):
            if playlist is None:
                # The API started returning None/null items alongside SimplifiedPlaylistObjects
                # for some reason.
                logger.warning("Skipping None/null playlist object")
                continue
            if playlist["owner"]["id"] == self.sp.user_id:
                # my playlist
                backup_dir = self._ensure_dir(path / "my")
            else:
                # starred playlist
                backup_dir = self._ensure_dir(path / "starred")
            self._backup_playlist(playlist, backup_dir)

    def backup_saved_objects(self, objtype: Optional[_SAVED_OBJECT_TYPES] = None):
        """Backup users saved objects"""

        def _dump(objtype):
            logger.info("Backing up saved %s", objtype)
            func = getattr(self.sp, "current_user_saved_" + objtype)
            result = self.sp.get_all_items(func)
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
                result = self.sp.get_all_items(func, time_range=top_range)
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

    def backup_history(self):
        """Backup listening history
        This is a special case because the spotify API [1] does not behave as advertised.
        It will only ever return the last 50 songs played by the user, regardless of the
        after/before parameters given.

        So this function is supposed to run more often then the other backup functions
        (like once every hour) to ensure no history is missed.

        [1] https://developer.spotify.com/documentation/web-api/reference/get-recently-played
        """
        logger.info("Backing up listening history")
        db = SpotifyHistoryDB(self._ensure_dir() / "history.sqlite")
        after = db.get_most_recent_timestamp()
        result = self.sp.current_user_recently_played(
            after=after
        )
        items: List = result["items"]
        while result["next"]:
            result = self.sp.next(result)
            items.extend(result["items"])
        if items:
            logger.info("Backing up %d history items", len(items))
            items_added = db.insert_play_history_objects(items, after)
            logger.info("Added %d history items", items_added)
        db.close_connection()


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
    parser.add_argument(
        "--history-only", action="store_true", help="Backup listening history only"
    )

    args = parser.parse_args()

    level = logging.WARNING
    if args.verbose is not None:
        if args.verbose > 0:
            level = logging.INFO
        if args.verbose > 1:
            level = logging.DEBUG
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] (%(name)s.%(funcName)s) %(message)s",
        level=level,
    )

    sb = SpotifyBackup(args.backup_dir, args.pretty)
    if args.history_only:
        sb.backup_history()
    else:
        sb.backup_everything()


if __name__ == "__main__":
    main()
