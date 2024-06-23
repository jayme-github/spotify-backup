#!/usr/bin/env python3

import argparse
import sys
import json
import logging
import sqlite3
from collections.abc import MutableMapping
from contextlib import suppress
from datetime import UTC, datetime, date, timedelta
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


def dict_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


def print_top_table(history: List[dict], headers: tuple | dict = None):
    def _sorted():
        keys = list(headers.keys())
        for item in history:
            # Yield a dict that only contains keys referenced in the headers
            # ordered by the order of the headers.
            yield dict(sorted(filter(lambda pair: pair[0] in keys, item.items()), key=lambda pair: keys.index(pair[0])))

    if isinstance(headers, tuple):
        # tabulate requires headers to be a dict if table data is a list of dicts
        headers = {h: h for h in headers}

    if headers is None:
        # If headers are None, instruct tabulate to use the the dictionary keys directly
        headers = "keys"
        # No sorting required/possible, so just pass the history as-is
        data = history
    else:
        data = _sorted()

    try:
        print(tabulate(data, headers=headers, tablefmt="plain"))
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


class SpotifyHistoryDB:
    def __init__(self, db_file: Path, sql_debug=False):
        # Convert datetime objects to unix timestamps when inserting into the database
        # sqlite3.register_adapter(datetime, lambda dt: timegm(dt.utctimetuple()))
        # Convert cells with type "unixepoch" to datetime objects
        sqlite3.register_converter("unixepoch", lambda ts: datetime.fromtimestamp(int(ts), UTC))

        if isinstance(db_file, str):
            db_file = Path(db_file)
        self.db_file = db_file
        self.con = self.create_connection()
        if sql_debug:
            self.con.set_trace_callback(logger.getChild("sqlite").debug)

    def create_connection(self) -> sqlite3.Connection:
        bootstrap_tables = not self.db_file.is_file()
        con = sqlite3.connect(str(self.db_file), detect_types=sqlite3.PARSE_COLNAMES)
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
            tracks = spotify.tracks(tracks=batch)["tracks"]
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

    def get_history(self, limit: int = -1) -> List[dict]:
        cur = self.con.cursor()
        cur.row_factory = dict_factory
        cur.execute(
            """
            SELECT
                h.played_at as "played_at [unixepoch]",
                json_extract(t.data, '$.name') AS track_name,
                json_extract(t.data, '$.artists[0].name') AS artist_name,
                h.track_id
            FROM
                history h
            JOIN
                tracks t ON h.track_id == t.track_id
            ORDER BY
                h.played_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return cur.fetchall()

    def get_top_tracks(self, start, end: date, limit: int = -1) -> List[dict]:
        logger.debug(f"Getting top tracks from {start} to {end}")
        cur = self.con.cursor()
        cur.row_factory = dict_factory
        cur.execute(
            """
            SELECT
                t.track_id,
                json_extract(t.data, '$.name') AS track_name,
                json_extract(t.data, '$.artists[0].name') AS artist_name,
                MIN(h.played_at) as "played_first_at [unixepoch]",
                COUNT(h.track_id) AS play_count
            FROM
                history h
            JOIN
                tracks t ON h.track_id = t.track_id
            WHERE
                date(h.played_at, 'unixepoch') BETWEEN date(?) AND date(?)
            GROUP BY
                t.track_id
            ORDER BY
                play_count DESC, played_at ASC
            LIMIT ?
            """,
            (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), limit),
        )
        return cur.fetchall()

    def get_today_last_year(self, limit: int = -1) -> List[dict]:
        today_last_year = date.today().replace(year=date.today().year - 1)
        return self.get_top_tracks(today_last_year, today_last_year, limit)


def cmd_full_history(db: SpotifyHistoryDB, args: argparse.Namespace):
    history = db.get_history()
    start = history[-1]["played_at"]
    end = history[0]["played_at"]
    print(f"Listening History from {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}, {len(history)} items:")
    print_top_table(history)


def cmd_import_history(db: SpotifyHistoryDB, args: argparse.Namespace):
    for json_file in args.files:
        logger.info(f"Importing listening history from {json_file}")
        db.insert_from_gdpr_json(json_file, backfill=False)
    logger.info("Backfilling missing track data")
    db.backfill_track_data()


def cmd_top_tracks(db: SpotifyHistoryDB, args: argparse.Namespace):
    start = args.start
    end = args.end
    top = db.get_top_tracks(start, end, args.limit)
    print(
        f"Top {str(args.limit) + ' ' if args.limit > 0 else ''}tracks from {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}:"
    )
    print_top_table(
        top,
        {
            "play_count": "#",
            "played_first_at": "Played first",
            "track_name": "Name",
            "artist_name": "Artist",
            "track_id": "ID",
        },
    )


def cmd_today_last_year(db: SpotifyHistoryDB, args: argparse.Namespace):
    top = db.get_today_last_year(args.limit)
    print(f"Top {str(args.limit) + ' ' if args.limit > 0 else ''}tracks from today last year:")
    print_top_table(
        top,
        {
            "play_count": "#",
            "played_first_at": "Played first",
            "track_name": "Name",
            "artist_name": "Artist",
            "track_id": "ID",
        },
    )


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
    parser.set_defaults(func=cmd_full_history)
    subparser = parser.add_subparsers()

    import_history = subparser.add_parser(
        "import-history",
        help="Import listening history from GDPR request data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    import_history.add_argument(
        "files",
        type=lambda p: Path(p).absolute(),
        nargs="*",
        help="Import listening history (from GDPR request data JSON file)",
    )

    top_tracks = subparser.add_parser(
        "top-tracks",
        help="Top tracks",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    top_tracks.add_argument(
        "--start", type=date.fromisoformat, default=date.today() - timedelta(days=30), help="Start date"
    )
    top_tracks.add_argument("--end", type=date.fromisoformat, default=date.today(), help="End date (inclusive)")
    top_tracks.add_argument("--limit", type=int, default=10, help="Number of top tracks to show")
    top_tracks.set_defaults(func=cmd_top_tracks)

    today_last_year = subparser.add_parser(
        "today-last-year",
        help="Top tracks from today last year",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    today_last_year.add_argument("--limit", type=int, default=10, help="Number of top tracks to show")
    today_last_year.set_defaults(func=cmd_today_last_year)

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

    try:
        func = args.func
    except AttributeError:
        parser.error("too few arguments")

    db = SpotifyHistoryDB(args.backup_dir / "history.sqlite", sql_debug=args.verbose > 1)
    func(db, args)


if __name__ == "__main__":
    main()
