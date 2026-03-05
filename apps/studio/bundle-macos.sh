#!/usr/bin/env bash
#
# bundle-macos.sh — Build exdb-studio and package it as a macOS .app + .dmg
#
# Preferred method: uses `dx bundle` (Dioxus CLI).
# Fallback: manual .app bundle + hdiutil if dx is not installed.
#
# Usage:
#   ./apps/studio/bundle-macos.sh            # release build
#   ./apps/studio/bundle-macos.sh --debug    # debug build (faster, larger)
#
# Prerequisites (preferred):
#   cargo install dioxus-cli
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

PROFILE="release"
if [[ "${1:-}" == "--debug" ]]; then
    PROFILE="debug"
fi

# ─── Try dx bundle (the Dioxus-preferred way) ───────────────────────────
if command -v dx &>/dev/null; then
    echo "==> Using dx bundle ($PROFILE)..."
    DX_FLAGS=""
    if [[ "$PROFILE" == "debug" ]]; then
        DX_FLAGS="--skip-optimizations"
    fi
    (cd apps/studio && dx bundle --desktop \
        --package-types macos \
        --package-types dmg \
        $DX_FLAGS)

    echo ""
    echo "Done! Output in target/dx/exdb-studio/bundle/"
    find target/dx/exdb-studio/bundle -name '*.app' -o -name '*.dmg' 2>/dev/null | while read -r f; do
        echo "  $f ($(du -sh "$f" | cut -f1))"
    done
    exit 0
fi

# ─── Fallback: manual bundle ────────────────────────────────────────────
echo "dx CLI not found — using manual bundle."
echo "  (Install with: cargo install dioxus-cli)"
echo ""

APP_NAME="exdb studio"
BUNDLE_ID="com.exdb.studio"
BINARY_NAME="exdb-studio"
VERSION="0.1.0"
DMG_NAME="exdb-studio-${VERSION}-$(uname -m)"

CARGO_FLAGS=""
if [[ "$PROFILE" == "release" ]]; then
    CARGO_FLAGS="--release"
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

cp "$BINARY" "$APP_DIR/Contents/MacOS/$BINARY_NAME"

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

# Generate .icns from the asset icon or from scratch
echo "==> Generating app icon..."
ICONSET_DIR="$BUILD_DIR/AppIcon.iconset"
rm -rf "$ICONSET_DIR"
mkdir -p "$ICONSET_DIR"

ASSET_ICON="$SCRIPT_DIR/assets/icon.png"
if [[ -f "$ASSET_ICON" ]]; then
    # Use sips to resize the asset icon into all required sizes
    SIZES=("16x16" "32x32" "128x128" "256x256" "512x512")
    for s in "${SIZES[@]}"; do
        w="${s%x*}"
        w2=$((w * 2))
        sips -z "$w" "$w" "$ASSET_ICON" --out "$ICONSET_DIR/icon_${s}.png" &>/dev/null
        sips -z "$w2" "$w2" "$ASSET_ICON" --out "$ICONSET_DIR/icon_${s}@2x.png" &>/dev/null
    done
else
    # Generate from scratch with Python
    python3 - "$ICONSET_DIR" <<'PYEOF'
import struct, sys, os, zlib
out_dir = sys.argv[1]

def make_icon_rgba(size):
    pixels = bytearray(size * size * 4)
    cx = size / 2.0
    s = size / 32.0
    cy_top, cy_bot = 9.0*s, 23.0*s
    rx, ry_top, ry_bot = 12.0*s, 5.0*s, 5.0*s
    col = (0x7a, 0xa2, 0xf7)
    for y in range(size):
        for x in range(size):
            fx, fy = x+0.5, y+0.5
            dx = (fx-cx)/rx
            in_top = dx*dx+((fy-cy_top)/ry_top)**2 <= 1.0
            in_bot = dx*dx+((fy-cy_bot)/ry_bot)**2 <= 1.0
            in_body = abs(dx)<=1.0 and cy_top<=fy<=cy_bot
            ry_mid, cy_mid = 4.0*s, 16.0*s
            mid_val = dx*dx+((fy-cy_mid)/ry_mid)**2
            on_mid = in_body and 0.85<=mid_val<=1.0
            a = 255 if in_top else (220 if on_mid else (160 if in_body else (120 if in_bot else 0)))
            if a > 0:
                i = (y*size+x)*4
                pixels[i], pixels[i+1], pixels[i+2], pixels[i+3] = *col, a
    return bytes(pixels)

def write_png(path, w, h, rgba):
    def chunk(tag, data):
        c = tag+data
        return struct.pack('>I',len(data))+c+struct.pack('>I',zlib.crc32(c)&0xffffffff)
    hdr = struct.pack('>IIBBBBB',w,h,8,6,0,0,0)
    raw = b''.join(b'\x00'+rgba[y*w*4:(y+1)*w*4] for y in range(h))
    with open(path,'wb') as f:
        f.write(b'\x89PNG\r\n\x1a\n')
        f.write(chunk(b'IHDR',hdr))
        f.write(chunk(b'IDAT',zlib.compress(raw)))
        f.write(chunk(b'IEND',b''))

for size, name in [(16,'icon_16x16.png'),(32,'icon_16x16@2x.png'),(32,'icon_32x32.png'),
    (64,'icon_32x32@2x.png'),(128,'icon_128x128.png'),(256,'icon_128x128@2x.png'),
    (256,'icon_256x256.png'),(512,'icon_256x256@2x.png'),(512,'icon_512x512.png'),
    (1024,'icon_512x512@2x.png')]:
    write_png(os.path.join(out_dir,name),size,size,make_icon_rgba(size))
PYEOF
fi

iconutil -c icns "$ICONSET_DIR" -o "$APP_DIR/Contents/Resources/AppIcon.icns"
rm -rf "$ICONSET_DIR"
echo "  AppIcon.icns created"

# Create DMG with Applications symlink for drag-to-install
echo "==> Creating DMG..."
rm -f "$DMG_PATH"

STAGING="$BUILD_DIR/dmg-staging"
rm -rf "$STAGING"
mkdir -p "$STAGING"
cp -a "$APP_DIR" "$STAGING/"
ln -s /Applications "$STAGING/Applications"

hdiutil create \
    -volname "$APP_NAME" \
    -srcfolder "$STAGING" \
    -ov \
    -format UDZO \
    "$DMG_PATH" \
    2>/dev/null

rm -rf "$STAGING"

echo ""
echo "Done! Output:"
echo "  App:  $APP_DIR"
echo "  DMG:  $DMG_PATH"
echo ""
du -sh "$DMG_PATH"
