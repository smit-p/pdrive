#!/usr/bin/env bash
set -euo pipefail

# Downloads the latest rclone binary into ./bin/rclone

VERSION="${1:-current}"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$ARCH" in
    x86_64)  ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *)
        echo "Unsupported architecture: $ARCH" >&2
        exit 1
        ;;
esac

URL="https://downloads.rclone.org/${VERSION}/rclone-${VERSION}-${OS}-${ARCH}.zip"

TMPDIR="$(mktemp -d)"
trap 'command rm -rf "$TMPDIR"' EXIT

echo "Downloading rclone ${VERSION} for ${OS}/${ARCH}..."
curl -fsSL "$URL" -o "$TMPDIR/rclone.zip"

unzip -q "$TMPDIR/rclone.zip" -d "$TMPDIR"

mkdir -p bin
cp "$TMPDIR"/rclone-*/rclone bin/rclone
chmod +x bin/rclone

echo "rclone installed to bin/rclone"
bin/rclone version
