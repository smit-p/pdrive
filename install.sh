#!/usr/bin/env bash
set -euo pipefail

# pdrive installer — downloads the latest release binary for your platform
# and installs it to /usr/local/bin (or a custom location).
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/smit-p/pdrive/main/install.sh | bash
#   curl -fsSL https://raw.githubusercontent.com/smit-p/pdrive/main/install.sh | bash -s -- --dir ~/.local/bin

REPO="smit-p/pdrive"
INSTALL_DIR="/usr/local/bin"

# Parse arguments.
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$ARCH" in
    x86_64)        ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *)
        echo "Error: unsupported architecture: $ARCH" >&2
        exit 1
        ;;
esac

case "$OS" in
    darwin|linux) ;;
    *)
        echo "Error: unsupported OS: $OS" >&2
        exit 1
        ;;
esac

# Get the latest release tag from GitHub.
echo "Fetching latest pdrive release..."
LATEST=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | head -1 | sed -E 's/.*"([^"]+)".*/\1/')

if [[ -z "$LATEST" ]]; then
    echo "Error: could not determine latest release" >&2
    exit 1
fi

VERSION="${LATEST#v}"
ARCHIVE="pdrive_${VERSION}_${OS}_${ARCH}.tar.gz"
URL="https://github.com/${REPO}/releases/download/${LATEST}/${ARCHIVE}"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

echo "Downloading pdrive ${LATEST} for ${OS}/${ARCH}..."
curl -fsSL "$URL" -o "$TMPDIR/pdrive.tar.gz"

echo "Extracting..."
tar -xzf "$TMPDIR/pdrive.tar.gz" -C "$TMPDIR"

# Install.
mkdir -p "$INSTALL_DIR"
if [[ -w "$INSTALL_DIR" ]]; then
    cp "$TMPDIR/pdrive" "$INSTALL_DIR/pdrive"
    chmod +x "$INSTALL_DIR/pdrive"
else
    echo "Need sudo to install to $INSTALL_DIR"
    sudo cp "$TMPDIR/pdrive" "$INSTALL_DIR/pdrive"
    sudo chmod +x "$INSTALL_DIR/pdrive"
fi

echo ""
echo "✓ pdrive ${LATEST} installed to ${INSTALL_DIR}/pdrive"
echo ""
echo "Get started:"
echo "  pdrive --help"
echo ""
echo "rclone will be downloaded automatically on first run if not installed."
echo "To install rclone manually: https://rclone.org/install/"
