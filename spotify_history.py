#!/usr/bin/env python3

import argparse
import sys
import json
import logging
import sqlite3
from collections.abc import MutableMapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import List

from tabulate import tabulate

from spotify import SpotifyClient

INIT_TABLES = """
BEGIN;
CREATE TABLE IF NOT EXISTS "history" (
    played_at INTEGER PRIMARY KEY,
    track_id TEXT NOT NULL,
    FOREIGN KEY (track_id)
        REFERENCES tracks(track_id)
            ON UPDATE RESTRICT
            ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS "tracks" (
    track_id TEXT PRIMARY KEY,
    data JSON
);
COMMIT;
"""

logger = logging.getLogger(__name__)


@dataclass
class SpotifyHistory:
    played_at: datetime
    track_name: str
    artist_name: str
    track_id: str


def spotify_history_factory(cursor: sqlite3.Cursor, row: tuple) -> SpotifyHistory:
    return SpotifyHistory(
        datetime.fromtimestamp(row[0], UTC),
        row[1],
        row[2],
        row[3],
    )


def print_history_table(history: List[SpotifyHistory]):
    start = history[-1].played_at
    end = history[0].played_at
    try:
        print(
            f"Listening History from {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}, {len(history)} items:"
        )
        print(tabulate(history, headers=["Played At (UTC)", "Track", "Artist", "Track ID"], tablefmt="plain"))
    except BrokenPipeError:
        pass  # Ignore broken pipe errors (like "head" truncating the output)
    finally:
        sys.stderr.close()


def delete_keys_from_dict(dictionary, keys):
    for key in keys:
        with suppress(KeyError):
            del dictionary[key]
    for value in dictionary.values():
        if isinstance(value, MutableMapping):
            delete_keys_from_dict(value, keys)


# SpotifyHistory class manages the listening history as a sqlite database
class SpotifyHistoryDB:
    def __init__(self, db_file: Path):
        if isinstance(db_file, str):
            db_file = Path(db_file)
        self.db_file = db_file
        self.con = self.create_connection()

    def create_connection(self) -> sqlite3.Connection:
        bootstrap_tables = not self.db_file.is_file()
        con = sqlite3.connect(str(self.db_file))
        con.execute("PRAGMA foreign_keys = ON")
        if bootstrap_tables:
            con.executescript(INIT_TABLES)
        return con

    def close_connection(self):
        self.con.close()

    def _cleanup_history_items(self, items):
        """Remove (probably irrelevant) keys from the history items to reduce the size of the history database"""
        for item in items:
            delete_keys_from_dict(
                item, ("available_markets", "context", "images", "preview_url", "external_urls", "href")
            )

    def _insert_history_item(self, cur: sqlite3.Cursor, played_at: int, track_id: str):
        cur.execute(
            "INSERT OR IGNORE INTO history VALUES (unixepoch(?), ?)",
            (
                played_at,
                track_id,
            ),
        )

    def _insert_track(self, cur: sqlite3.Cursor, track_id: str, track: dict = None):
        if track is None:
            # This uses the "INSERT OR IGNORE" clause to avoid overriding existing tracks data with NULL
            cur.execute("INSERT OR IGNORE INTO tracks (track_id) VALUES (?)", (track_id,))
        else:
            cur.execute("INSERT OR REPLACE INTO tracks VALUES (?, ?)", (track_id, json.dumps(track)))

    def insert_play_history_objects(self, play_history_objects: List):
        self._cleanup_history_items(play_history_objects)
        cur = self.con.cursor()
        for item in play_history_objects:
            track = item["track"]
            track_id = track["id"]
            try:
                self._insert_history_item(cur, item["played_at"], track_id)
            except sqlite3.IntegrityError:
                self._insert_track(cur, track_id, track)
                self._insert_history_item(cur, item["played_at"], track_id)

        cur.close()
        self.con.commit()

    def insert_from_gdpr_json(self, json_file: Path, backfill=False):
        """Parse the listening history from a GDPR request data JSON file
        into a format compatible with what we get from spotify recently played API"""
        history = set()
        with json_file.open("r") as f:
            data = json.load(f)
            for entry in data:
                if any(map(lambda x: entry.get(x) is None, ["ts", "ms_played", "spotify_track_uri"])):
                    # Skip entries missing relevant data (like podcast episodes)
                    continue
                if entry["ms_played"] < 1000:
                    # Skip songs that were played for less than a second
                    continue
                played_at = entry["ts"]
                track_id = entry["spotify_track_uri"].split(":")[-1]
                history.add((played_at, track_id))

        cur = self.con.cursor()
        tracks_added = cur.executemany(
            "INSERT OR IGNORE INTO tracks (track_id) VALUES (?)", [(track_id,) for _, track_id in history]
        ).rowcount
        logger.info(f"Added {tracks_added} tracks")
        history_added = cur.executemany("INSERT OR IGNORE INTO history VALUES (unixepoch(?), ?)", history).rowcount
        logger.info(f"Added {history_added} history items")
        cur.close()
        self.con.commit()

        if backfill:
            self.backfill_track_data()

    def backfill_track_data(self):
        """Fetches missing track data for all tracks in the database"""
        cur = self.con.cursor()
        cur.execute("SELECT track_id FROM tracks WHERE data IS NULL")
        track_ids = [row[0] for row in cur.fetchall()]
        tracks_to_backfill = len(track_ids)
        if tracks_to_backfill == 0:
            return

        spotify = SpotifyClient()

        def _batch(iterable, n=1):
            iter_length = len(iterable)
            for ndx in range(0, iter_length, n):
                yield iterable[ndx : min(ndx + n, iter_length)]  # noqa: E203

        # Fetch track data in batches of 50
        # API docs say maximum of 100, but that cake is a lie
        cur = self.con.cursor()
        backfilled_tracks = 0
        for batch in _batch(track_ids, 50):
            tracks = spotify.sp.tracks(tracks=batch)["tracks"]
            self._cleanup_history_items(tracks)
            backfilled_tracks += cur.executemany(
                "UPDATE tracks SET data=? WHERE track_id=?", [(json.dumps(track), track["id"]) for track in tracks]
            ).rowcount
            # Ensure data is committed to the database after each batch
            self.con.commit()
            logger.info(f"Backfilled {backfilled_tracks} of {tracks_to_backfill} tracks")
        cur.close()

    def get_most_recent_timestamp(self) -> int:
        cur = self.con.execute("SELECT MAX(played_at) FROM history")
        timestamp = cur.fetchone()[0]
        if not timestamp:
            return 0
        return timestamp

    def get_history(self) -> List[SpotifyHistory]:
        cur = self.con.cursor()
        cur.row_factory = spotify_history_factory
        cur.execute(
            """
            SELECT
                history.played_at,
                json_extract(tracks.data, '$.name') AS track_name,
                json_extract(tracks.data, '$.artists[0].name') AS artist_name,
                history.track_id
            FROM history
            JOIN tracks ON history.track_id == tracks.track_id
            ORDER BY history.played_at DESC
            """
        )
        return cur.fetchall()


def main():
    parser = argparse.ArgumentParser(
        description="Spotify history",
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
        "--import-history",
        type=lambda p: Path(p).absolute(),
        nargs="*",
        help="Import listening history (from GDPR request data JSON file)",
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

    db = SpotifyHistoryDB(args.backup_dir / "history.sqlite")
    if args.import_history:
        for json_file in args.import_history:
            logger.info(f"Importing listening history from {json_file}")
            db.insert_from_gdpr_json(json_file, backfill=False)
        logger.info("Backfilling missing track data")
        db.backfill_track_data()
    else:
        history = db.get_history()
        print_history_table(history)


if __name__ == "__main__":
    main()
