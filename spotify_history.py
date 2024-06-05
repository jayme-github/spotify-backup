#!/usr/bin/env python3

import argparse
import logging
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import List

from tabulate import tabulate

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
    print(f"Listening History (most recent first), {len(history)} items:")
    print(tabulate(history, headers=["Played At (UTC)", "Track", "Artist", "Track ID"], tablefmt="plain"))


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

    def _insert_history_item(self, cur: sqlite3.Cursor, played_at: int, track: dict):
        cur.execute(
            "INSERT OR IGNORE INTO history VALUES (unixepoch(?), ?, ?, ?)",
            (
                played_at,
                track["name"],
                track["artists"][0]["name"],
                track["id"],
            ),
        )

    def insert_items(self, items: List):
        cur = self.con.cursor()
        for item in items:
            try:
                self._insert_history_item(cur, item)
            except sqlite3.IntegrityError:
                track = item["track"]
                cur.execute("INSERT OR REPLACE INTO tracks VALUES (?, ?)", (track["id"], json.dumps(track)))
                self._insert_history_item(cur, item)

        cur.close()
        self.con.commit()

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

    db = SpotifyHistoryDB(args.backup_dir / "history.sqlite")
    history = db.get_history()
    print_history_table(history)


if __name__ == "__main__":
    main()
