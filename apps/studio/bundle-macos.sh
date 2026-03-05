#!/usr/bin/env bash
#
# bundle-macos.sh — Build exdb-studio and package it as a macOS .dmg
#
# Usage:
#   ./apps/studio/bundle-macos.sh            # release build
#   ./apps/studio/bundle-macos.sh --debug    # debug build (faster, larger)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

APP_NAME="exdb studio"
BUNDLE_ID="com.exdb.studio"
BINARY_NAME="exdb-studio"
VERSION="0.1.0"
DMG_NAME="exdb-studio-${VERSION}-$(uname -m)"

PROFILE="release"
CARGO_FLAGS="--release"
if [[ "${1:-}" == "--debug" ]]; then
    PROFILE="debug"
    CARGO_FLAGS=""
fi

BUILD_DIR="$REPO_ROOT/target/bundle"
APP_DIR="$BUILD_DIR/${APP_NAME}.app"
DMG_PATH="$BUILD_DIR/${DMG_NAME}.dmg"

echo "==> Building exdb-studio ($PROFILE)..."
cargo build -p exdb-studio $CARGO_FLAGS

BINARY="$REPO_ROOT/target/$PROFILE/$BINARY_NAME"
if [[ ! -f "$BINARY" ]]; then
    echo "ERROR: binary not found at $BINARY"
    exit 1
fi

echo "==> Creating app bundle..."
rm -rf "$APP_DIR"
mkdir -p "$APP_DIR/Contents/MacOS"
mkdir -p "$APP_DIR/Contents/Resources"

# Copy binary
cp "$BINARY" "$APP_DIR/Contents/MacOS/$BINARY_NAME"

# Info.plist
cat > "$APP_DIR/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>${APP_NAME}</string>
    <key>CFBundleDisplayName</key>
    <string>${APP_NAME}</string>
    <key>CFBundleIdentifier</key>
    <string>${BUNDLE_ID}</string>
    <key>CFBundleVersion</key>
    <string>${VERSION}</string>
    <key>CFBundleShortVersionString</key>
    <string>${VERSION}</string>
    <key>CFBundleExecutable</key>
    <string>${BINARY_NAME}</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleIconFile</key>
    <string>AppIcon</string>
    <key>LSMinimumSystemVersion</key>
    <string>11.0</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>NSSupportsAutomaticGraphicsSwitching</key>
    <true/>
</dict>
</plist>
PLIST

# Generate .icns icon using the built-in sips + iconutil toolchain.
# We render a simple database cylinder icon via a tiny Python script
# that writes raw RGBA PNGs (no third-party deps needed).
echo "==> Generating app icon..."
ICONSET_DIR="$BUILD_DIR/AppIcon.iconset"
rm -rf "$ICONSET_DIR"
mkdir -p "$ICONSET_DIR"

python3 - "$ICONSET_DIR" <<'PYEOF'
import struct, sys, os, zlib

out_dir = sys.argv[1]

def make_icon_rgba(size):
    """Render a database cylinder icon at the given pixel size."""
    pixels = bytearray(size * size * 4)
    cx = size / 2.0
    # Scale geometry proportionally
    s = size / 32.0
    cy_top = 9.0 * s
    cy_bot = 23.0 * s
    rx = 12.0 * s
    ry_top = 5.0 * s
    ry_bot = 5.0 * s
    col = (0x7a, 0xa2, 0xf7)

    for y in range(size):
        for x in range(size):
            fx = x + 0.5
            fy = y + 0.5
            dx = (fx - cx) / rx

            in_top = dx * dx + ((fy - cy_top) / ry_top) ** 2 <= 1.0
            in_bot = dx * dx + ((fy - cy_bot) / ry_bot) ** 2 <= 1.0
            in_body = abs(dx) <= 1.0 and cy_top <= fy <= cy_bot
            # Middle ring highlight
            ry_mid = 4.0 * s
            cy_mid = 16.0 * s
            mid_val = dx * dx + ((fy - cy_mid) / ry_mid) ** 2
            on_mid_ring = in_body and 0.85 <= mid_val <= 1.0

            if in_top:
                a = 255
            elif on_mid_ring:
                a = 220
            elif in_body:
                a = 160
            elif in_bot:
                a = 120
            else:
                a = 0

            if a > 0:
                i = (y * size + x) * 4
                pixels[i] = col[0]
                pixels[i+1] = col[1]
                pixels[i+2] = col[2]
                pixels[i+3] = a
    return bytes(pixels)

def write_png(path, width, height, rgba):
    """Write a minimal RGBA PNG (no dependencies)."""
    def chunk(tag, data):
        c = tag + data
        return struct.pack('>I', len(data)) + c + struct.pack('>I', zlib.crc32(c) & 0xffffffff)

    header = struct.pack('>IIBBBBB', width, height, 8, 6, 0, 0, 0)
    raw = b''
    for y in range(height):
        raw += b'\x00'  # filter: none
        raw += rgba[y * width * 4 : (y + 1) * width * 4]
    compressed = zlib.compress(raw)

    with open(path, 'wb') as f:
        f.write(b'\x89PNG\r\n\x1a\n')
        f.write(chunk(b'IHDR', header))
        f.write(chunk(b'IDAT', compressed))
        f.write(chunk(b'IEND', b''))

# macOS iconutil expects these exact filenames
icon_sizes = [
    (16,   'icon_16x16.png'),
    (32,   'icon_16x16@2x.png'),
    (32,   'icon_32x32.png'),
    (64,   'icon_32x32@2x.png'),
    (128,  'icon_128x128.png'),
    (256,  'icon_128x128@2x.png'),
    (256,  'icon_256x256.png'),
    (512,  'icon_256x256@2x.png'),
    (512,  'icon_512x512.png'),
    (1024, 'icon_512x512@2x.png'),
]

for size, filename in icon_sizes:
    rgba = make_icon_rgba(size)
    write_png(os.path.join(out_dir, filename), size, size, rgba)
    print(f'  {filename} ({size}x{size})')
PYEOF

iconutil -c icns "$ICONSET_DIR" -o "$APP_DIR/Contents/Resources/AppIcon.icns"
rm -rf "$ICONSET_DIR"
echo "  AppIcon.icns created"

# Create DMG
echo "==> Creating DMG..."
rm -f "$DMG_PATH"
hdiutil create \
    -volname "$APP_NAME" \
    -srcfolder "$APP_DIR" \
    -ov \
    -format UDZO \
    "$DMG_PATH" \
    2>/dev/null

echo ""
echo "Done! Output:"
echo "  App:  $APP_DIR"
echo "  DMG:  $DMG_PATH"
echo ""
du -sh "$DMG_PATH"
