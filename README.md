# spotify-backup
spotify-backup uses the [Spotify Web API](https://developer.spotify.com/web-api/) (via [Spotipy](https://github.com/plamere/spotipy/)) to backup your user data from Spotify. The backed up data consists of:
* Playlists you created
* Playlists you starred
* Your saved albums, episodes, shows and tracks
* Your short, medium and long term top artists and tracks
* Artists you follow

The script does not try to be smart about the backup format, so everything the Spotify API returns will just be dumped as JSON to disk. It does try to prevent backing up unchanged playlists, though (to not update local files for no reason).
The resulting backups might be pretty big (given they are text only) due to all the metadata the Spotify API returns, but compressing the backup directory during your actual backup should do the job.

# Setup
You need to register and application with the [Spotify Developer Portal](https://developer.spotify.com/documentation/general/guides/app-settings/). The name does not matter, but adding http://localhost/ to `Redirect URIs` is important. Make note of *Client ID* and *Client Secret*.

Run `./spotify_backup.py` with environment variables `SPOTIPY_CLIENT_ID` and `SPOTIPY_CLIENT_SECRET` set to whatever you got from the Developer Portal. You'll be asked to visit a specific URL at `spotify.com` to grant the application the required credentials to your account. This will redirect you back to `http:://localhost/...`. Just paste the URL you have been redirected to into the prompt of the script.

The so obtained OAuth credentials are cached in `$XDG_CONFIG_HOME/spotify-backup` and can be refreshed on consecutive runs. So you should only need to do this once.

When the script is finished, a bunch on JSON files should land in a directory called `backup`.