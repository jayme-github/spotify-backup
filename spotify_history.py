#!/usr/bin/env python3

import argparse
import json
import logging
import sqlite3
import sys
from collections.abc import MutableMapping
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import List

from tabulate import tabulate

from spotify import SpotifyClient

MIGRATIONS = {
    1: (
        """
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
""",
        [],
    ),
    2: (
        "ALTER TABLE history ADD COLUMN ms_played INTEGER;",
        ["backfill_ms_played"],
    ),
}


logger = logging.getLogger(__name__)


def dict_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


def print_table(history: List[dict], headers: tuple | dict = None):
    def _sorted():
        keys = list(headers.keys())
        for item in history:
            # Yield a dict that only contains keys referenced in the headers
            # ordered by the order of the headers.
            yield dict(
                sorted(
                    filter(lambda pair: pair[0] in keys, item.items()),
                    key=lambda pair: keys.index(pair[0]),
                )
            )

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
        sqlite3.register_converter(
            "unixepoch", lambda ts: datetime.fromtimestamp(int(ts), UTC)
        )

        if isinstance(db_file, str):
            db_file = Path(db_file)
        self.db_file = db_file
        self.con = self.create_connection()
        if sql_debug:
            self.con.set_trace_callback(logger.getChild("sqlite").debug)
        # Setup the database
        cur = self.con.execute("PRAGMA user_version")
        self.db_version = cur.fetchone()[0]
        logger.info(f"Database version: {self.db_version}")
        self._apply_migrations()

    def create_connection(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_file), detect_types=sqlite3.PARSE_COLNAMES)
        con.execute("PRAGMA foreign_keys = ON")
        return con

    def close_connection(self):
        self.con.close()

    def _apply_migrations(self):
        for version, migration in MIGRATIONS.items():
            query, funcs = migration
            if version > self.db_version:
                logger.info(f"Applying migration {version}")
                with self.con:
                    self.con.executescript(query)
                    self.con.execute(f"PRAGMA user_version = {version}")
                    self.db_version = version
                    for func in funcs:
                        logger.info(f"Running post-migration function {func}")
                        ret = getattr(self, func)()
                        logger.info(f"Function {func} returned: {ret}")
                    logger.info(f"Applied migration {version}")

    def _cleanup_history_items(self, items):
        """Remove (probably irrelevant) keys from the history items to reduce the size of the history database"""
        for item in items:
            delete_keys_from_dict(
                item,
                (
                    "available_markets",
                    "context",
                    "images",
                    "preview_url",
                    "external_urls",
                    "href",
                ),
            )

    def _insert_history_item(
        self, cur: sqlite3.Cursor, played_at: int, track_id: str, ms_played: int = None
    ) -> int:
        return cur.execute(
            "INSERT OR IGNORE INTO history VALUES (unixepoch(?), ?, ?)",
            (
                played_at,
                track_id,
                ms_played,
            ),
        ).rowcount

    def _insert_track(self, cur: sqlite3.Cursor, track_id: str, track: dict = None):
        if track is None:
            # This uses the "INSERT OR IGNORE" clause to avoid overriding existing tracks data with NULL
            cur.execute(
                "INSERT OR IGNORE INTO tracks (track_id) VALUES (?)", (track_id,)
            )
        else:
            cur.execute(
                "INSERT OR REPLACE INTO tracks VALUES (?, ?)",
                (track_id, json.dumps(track)),
            )

    def insert_play_history_objects(self, play_history_objects: List, backfill_from: int = None) -> int:
        self._cleanup_history_items(play_history_objects)
        cur = self.con.cursor()
        history_items_added = 0
        for item in play_history_objects:
            track = item["track"]
            track_id = track["id"]
            try:
                history_items_added += self._insert_history_item(cur, item["played_at"], track_id)
            except sqlite3.IntegrityError:
                self._insert_track(cur, track_id, track)
                history_items_added += self._insert_history_item(cur, item["played_at"], track_id)

        cur.close()
        self.con.commit()
        if backfill_from:
            # Calculate ms_played for what was previously the last history item
            # as well as all new history items added above
            self.backfill_ms_played(backfill_from=backfill_from)
        return history_items_added

    def insert_from_gdpr_json(self, json_file: Path, backfill=False):
        """Parse the listening history from a GDPR request data JSON file
        into a format compatible with what we get from spotify recently played API"""
        history = set()
        with json_file.open("r") as f:
            data = json.load(f)
            for entry in data:
                if any(
                    map(
                        lambda x: entry.get(x) is None,
                        ["ts", "ms_played", "spotify_track_uri"],
                    )
                ):
                    # Skip entries missing relevant data (like podcast episodes)
                    continue
                ms_played = entry["ms_played"]
                if ms_played == 0:
                    # Skip tracks which have not been played
                    continue
                played_at = entry["ts"]
                track_id = entry["spotify_track_uri"].split(":")[-1]
                history.add((played_at, track_id, ms_played))

        cur = self.con.cursor()
        tracks_added = cur.executemany(
            "INSERT OR IGNORE INTO tracks (track_id) VALUES (?)",
            [(track_id,) for _, track_id, _ in history],
        ).rowcount
        logger.info(f"Added {tracks_added} tracks")
        history_added = cur.executemany(
            "INSERT OR IGNORE INTO history VALUES (unixepoch(?), ?, ?)", history
        ).rowcount
        logger.info(f"Added {history_added} history items")
        history_updated = cur.executemany(
            "UPDATE history SET ms_played=? WHERE played_at=unixepoch(?) and ms_played IS NULL",
            [[h[2], h[0]] for h in history],
        ).rowcount
        logger.info(f"Updated {history_updated} history items with ms_played")
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
                "UPDATE tracks SET data=? WHERE track_id=?",
                [(json.dumps(track), track["id"]) for track in tracks],
            ).rowcount
            # Ensure data is committed to the database after each batch
            self.con.commit()
            logger.info(
                f"Backfilled {backfilled_tracks} of {tracks_to_backfill} tracks"
            )
        cur.close()

    def backfill_ms_played(self, backfill_from: int = 0) -> int:
        cur = self.con.cursor()
        cur.row_factory = dict_factory
        cur.execute(
            """
            SELECT
                h.played_at,
                t.track_id,
                json_extract(t.data, '$.duration_ms') AS duration_ms,
                ms_played
                FROM history h
                JOIN tracks t ON h.track_id = t.track_id
                WHERE played_at >= ?
                ORDER BY h.played_at
            """,
            (backfill_from,),
        )
        data = cur.fetchall()
        # Calculate a plausible ms_played for each track as spotify does only provide that field in the GDPR data export
        for idx, row in enumerate(data):
            if row["ms_played"] is not None:
                # Skip rows that already have a ms_played value
                continue
            try:
                next_row = data[idx + 1]
            except IndexError:
                # last track, nothing to do
                break
            # Calculate the end time of a full playback of the current track
            full_play = row["played_at"] + (row["duration_ms"] / 1000)

            # If the next track was played before the full playback of the current track, calculate the ms_played from
            # the time between the start of the current track and the start of the next track (in milliseconds).
            # Otherwise, assume the track was played in full.
            if next_row["played_at"] < full_play:
                row["ms_played"] = (next_row["played_at"] - row["played_at"]) * 1000
            else:
                row["ms_played"] = row["duration_ms"]

        history_updated = cur.executemany(
            "UPDATE history SET ms_played=? WHERE played_at=? AND ms_played IS NULL",
            [(x["ms_played"], x["played_at"]) for x in data],
        ).rowcount
        logger.info(f"Backfilled {history_updated} ms_played values")
        cur.close()
        self.con.commit()
        return history_updated

    def get_most_recent_timestamp(self) -> int:
        cur = self.con.execute("SELECT MAX(played_at) FROM history")
        timestamp = cur.fetchone()[0]
        if not timestamp:
            return 0
        return timestamp

    def get_history(self, start, end: datetime = None, limit: int = -1) -> List[dict]:
        cur = self.con.cursor()
        cur.row_factory = dict_factory

        args = (limit,)
        where_clause = ""
        if start and end:
            where_clause = """
                WHERE
                    datetime(h.played_at, 'unixepoch') BETWEEN datetime(?) AND datetime(?)
                """
            args = (start, end, limit)
        elif start:
            where_clause = "WHERE datetime(h.played_at, 'unixepoch') >= datetime(?)"
            args = (start, limit)
        elif end:
            where_clause = " AND datetime(h.played_at, 'unixepoch') <= datetime(?)"
            args = (end, limit)

        cur.execute(
            f"""
            SELECT
                h.played_at as "played_at [unixepoch]",
                json_extract(t.data, '$.name') AS track_name,
                json_extract(t.data, '$.artists[0].name') AS artist_name,
                h.track_id
            FROM
                history h
            JOIN
                tracks t ON h.track_id == t.track_id
            {where_clause}
            ORDER BY
                h.played_at ASC
            LIMIT ?
            """,
            args,
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


def cmd_history(db: SpotifyHistoryDB, args: argparse.Namespace):
    history = db.get_history(args.start, args.end)
    start = history[-1]["played_at"].strftime("%Y-%m-%d %H:%M:%s")
    end = history[0]["played_at"].strftime("%Y-%m-%d %H:%M:%s")
    print(f"Listening History from {start} to {end}, {len(history)} items:")
    if args.create_playlist:
        spotify = SpotifyClient()
        track_ids = [item["track_id"] for item in history]
        playlist = spotify.create_playlist(args.create_playlist, track_ids)
        print(
            f"Playlist created with {len(track_ids)} tracks: {playlist['external_urls']['spotify']}"
        )
    print_table(history)


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
        f"Top {str(args.limit) + ' ' if args.limit > 0 else ''}tracks from "
        f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}:"
    )
    print_table(
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
    print(
        f"Top {str(args.limit) + ' ' if args.limit > 0 else ''}tracks from today last year:"
    )
    print_table(
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
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase verbosity"
    )
    parser.add_argument(
        "--backup-dir",
        type=lambda p: Path(p).absolute(),
        default=Path(__file__).absolute().parent / "backup",
        help="Backup path",
    )
    parser.set_defaults(func=cmd_history)
    parser.add_argument(
        "--start",
        type=datetime.fromisoformat,
        help="Start date",
    )
    parser.add_argument(
        "--end",
        type=datetime.fromisoformat,
        help="End date (inclusive)",
    )
    parser.add_argument(
        "--create-playlist", help="Name of the playlist to write results to"
    )
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
        "--start",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=30),
        help="Start date",
    )
    top_tracks.add_argument(
        "--end",
        type=date.fromisoformat,
        default=date.today(),
        help="End date (inclusive)",
    )
    top_tracks.add_argument(
        "--limit", type=int, default=10, help="Number of top tracks to show"
    )
    top_tracks.set_defaults(func=cmd_top_tracks)

    today_last_year = subparser.add_parser(
        "today-last-year",
        help="Top tracks from today last year",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    today_last_year.add_argument(
        "--limit", type=int, default=10, help="Number of top tracks to show"
    )
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

    db = SpotifyHistoryDB(
        args.backup_dir / "history.sqlite", sql_debug=args.verbose > 1
    )
    func(db, args)


if __name__ == "__main__":
    main()
