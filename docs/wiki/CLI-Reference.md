# CLI Reference

## Daemon Management

### `pdrive start`

Start the pdrive daemon. Prompts for encryption passphrase on first run. Runs in background by default.

```bash
pdrive start              # Start daemon (background)
pdrive start --foreground # Start in foreground (for debugging)
```

### `pdrive stop`

Stop the running daemon (sends SIGTERM via PID file).

```bash
pdrive stop
```

### `pdrive mount`

Mount the pdrive FUSE filesystem at the configured mountpoint. Requires a running daemon with `--backend fuse`.

```bash
pdrive mount                        # Mount at default ~/pdrive
pdrive mount --mountpoint /mnt/pd   # Mount at custom path
```

### `pdrive unmount`

Unmount the pdrive FUSE filesystem.

```bash
pdrive unmount                        # Unmount default mountpoint
pdrive unmount --mountpoint /mnt/pd   # Unmount specific path
```

### `pdrive health`

Check if the daemon is running and responsive.

```bash
pdrive health
# Output: pdrive daemon is healthy (uptime 2h15m)
```

## File Operations

### `pdrive ls [path]`

List files in a directory. Supports numeric references from previous listings.

```bash
pdrive ls              # List root directory
pdrive ls /photos      # List /photos directory
pdrive ls 3            # Open item #3 from last listing
```

**Columns:** Index, Size, Age, Name

### `pdrive upload <local-path> [virtual-path]`

Upload a local file to pdrive.

```bash
pdrive upload ~/photo.jpg              # Upload to /photo.jpg
pdrive upload ~/photo.jpg /pics/       # Upload to /pics/photo.jpg
```

### `pdrive cat <path>`

Stream file contents to stdout.

```bash
pdrive cat /notes.txt
pdrive cat /data.csv | head -20
```

### `pdrive get <path> [local-dest]`

Download a file to local disk.

```bash
pdrive get /report.pdf              # Download to current directory
pdrive get /report.pdf ~/Desktop/   # Download to specific location
```

### `pdrive rm <path>`

Delete a file or directory.

```bash
pdrive rm /old-file.txt
pdrive rm /old-folder/
```

### `pdrive mv <source> <dest>`

Rename or move a file.

```bash
pdrive mv /old-name.txt /new-name.txt
pdrive mv /file.txt /subdir/file.txt
```

### `pdrive mkdir <path>`

Create a virtual directory.

```bash
pdrive mkdir /projects
pdrive mkdir /projects/2024
```

## Search and Discovery

### `pdrive find <query>`

Search for files by name (fuzzy matching).

```bash
pdrive find report
pdrive find .pdf
```

### `pdrive tree [path]`

Display directory tree.

```bash
pdrive tree
pdrive tree /projects
```

### `pdrive du [path]`

Show disk usage summary.

```bash
pdrive du
pdrive du /photos
```

## Information

### `pdrive info <path>`

Show detailed file info (size, hash, chunks, locations).

```bash
pdrive info /report.pdf
```

### `pdrive status`

Show overall storage status across all providers.

```bash
pdrive status
```

### `pdrive uploads`

Show active upload progress.

```bash
pdrive uploads
```

### `pdrive remotes`

List configured cloud providers with their storage usage.

```bash
pdrive remotes
```

### `pdrive metrics`

Show engine telemetry (upload/download/delete counts).

```bash
pdrive metrics
```

## Interactive Mode

### `pdrive browse`

Launch the interactive TUI file browser.

**Keyboard shortcuts:**

- `↑`/`↓` or `j`/`k` — Navigate
- `Enter` — Open directory / download file
- `u` — Upload file
- `d` — Delete selected
- `r` — Rename selected
- `q` or `Esc` — Quit

## Pin/Unpin (Sync Directory)

### `pdrive pin <path>`

Download a cloud file to the local sync directory.

```bash
pdrive pin /important.pdf
```

### `pdrive unpin <path>`

Remove local copy and replace with a lightweight stub.

```bash
pdrive unpin /large-video.mp4
```

### `pdrive verify <path>`

Verify file integrity by downloading and checking all chunk hashes.

```bash
pdrive verify /critical-data.bin
```

## Global Flags

| Flag           | Default     | Description                             |
| -------------- | ----------- | --------------------------------------- |
| `--config-dir` | `~/.pdrive` | Path to pdrive data directory           |
| `--sync-dir`   | `~/pdrive`  | Local sync directory path               |
| `--backend`    | `webdav`    | Mount backend: `webdav` or `fuse`       |
| `--mountpoint` | `~/pdrive`  | FUSE mountpoint (when `--backend fuse`) |
| `--foreground` | `false`     | Run daemon in foreground                |
| `--debug`      | `false`     | Enable debug logging                    |

Flags can also be set persistently in `~/.pdrive/config.toml`. CLI flags override config file values.
