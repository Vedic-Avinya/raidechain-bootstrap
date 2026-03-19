#!/usr/bin/env bash
# Build p2pmobile.aar for Android using gomobile bind.
#
# Prerequisites:
#   go install golang.org/x/mobile/cmd/gomobile@latest
#   gomobile init
#   ANDROID_HOME must be set (Android SDK)
#   Android NDK must be installed (sdkmanager "ndk;28.2.13676358")
#
# Usage:
#   cd bootstrap && bash p2pmobile/build.sh
#
# Output:
#   apps/rider-android/libs/p2pmobile.aar
#   apps/rider-android/libs/p2pmobile-sources.jar
#   (also copied to car-android/libs/)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BOOTSTRAP_DIR="$(dirname "$SCRIPT_DIR")"
RIDE_DIR="$(dirname "$BOOTSTRAP_DIR")"

# Use a separate GOPATH to avoid conflict with user's GOPATH (which has a go.mod)
export GOPATH=/tmp/gopath_p2p
export GO111MODULE=on
export PATH="$GOPATH/bin:$PATH"

RIDER_LIBS="$RIDE_DIR/apps/rider-android/libs"
CAR_LIBS="$RIDE_DIR/apps/car-android/libs"
OUT_AAR="$RIDER_LIBS/p2pmobile.aar"
OUT_SRC="$RIDER_LIBS/p2pmobile-sources.jar"

echo "==> Building p2pmobile.aar (gomobile bind)"
echo "    BOOTSTRAP_DIR: $BOOTSTRAP_DIR"
echo "    GOPATH: $GOPATH"
echo "    Output: $OUT_AAR"

cd "$BOOTSTRAP_DIR"

# Ensure gomobile + gobind are installed
if ! command -v gomobile &>/dev/null; then
    echo "Installing gomobile + gobind..."
    go install golang.org/x/mobile/cmd/gomobile@latest
    go install golang.org/x/mobile/cmd/gobind@latest
    gomobile init
fi

# Build AAR — only the p2pmobile package (not the full bootstrap)
# -checklinkname=0 is required for Go 1.23+ because wlynxg/anet uses //go:linkname
# to access net.zoneCache (see https://github.com/wlynxg/anet#how-to-build-with-go-1230-or-later)
gomobile bind \
    -target=android \
    -androidapi 21 \
    -ldflags="-checklinkname=0" \
    -o "$OUT_AAR" \
    ./p2pmobile

echo "==> AAR built: $OUT_AAR"
ls -lh "$OUT_AAR"

# Copy to car-android libs too
mkdir -p "$CAR_LIBS"
cp "$OUT_AAR" "$CAR_LIBS/p2pmobile.aar"
if [ -f "$OUT_SRC" ]; then
    cp "$OUT_SRC" "$CAR_LIBS/p2pmobile-sources.jar"
fi

echo "==> Copied to: $CAR_LIBS/"
echo "==> Done. Rebuild both Android apps to pick up the new AAR."
