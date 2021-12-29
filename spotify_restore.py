#!/usr/bin/env python3

import logging
import json
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, Any

from common import *

SCOPES = (
    "user-library-modify",
    "playlist-modify-private",
)

logger = logging.getLogger(__name__)


class SpotifyRestore(SpotifyBase):
    def __init__(self, backup_path: Path):
        super().__init__(
            scopes=SCOPES, cache_file="spotify-restore", backup_path=backup_path
        )

    @staticmethod
    def chunks(lst: Iterable, n: int) -> Iterable[Any]:
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    def create_playlist(self, json_path: Path) -> Dict:
        """Create a copy of the playlist in json_path"""
        src = json.load(json_path.open())
        src_count = len(src["tracks"]["items"])
        prefix = date.today().strftime("Restore [%Y-%m-%d]")
        # There might be multiple playlists with the same name
        name = f"{prefix} {src['name']}"
        logger.info("Copying playlist as %s (%d tracks)", name, src_count)

        dst = self.sp.user_playlist_create(
            self.user_id,
            name,
            public=False,
            collaborative=False,
            description=src["description"],
        )
        dst_id = dst["id"]
        logger.debug("Destination playlist ID: %s", dst_id)

        count = 0
        for chunk in self.chunks(src["tracks"]["items"], 100):
            count += len(chunk)
            self.sp.playlist_add_items(dst_id, [c["track"]["id"] for c in chunk])
            logger.info(
                "Copied %s of %s tracks",
                count,
                src_count,
            )

        return dst
