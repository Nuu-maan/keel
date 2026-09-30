#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
chrome=${CHROME:-$(command -v chromium || command -v google-chrome || command -v chromium-browser)}
port=${PORT:-8765}

python3 -m http.server "$port" --bind 127.0.0.1 >/dev/null 2>&1 &
server=$!
trap 'kill $server' EXIT
sleep 1

render() {
  local name=$1 width=$2 height=$3
  "$chrome" --headless --disable-gpu --hide-scrollbars \
    --force-device-scale-factor=2 --window-size="$width,$height" \
    --virtual-time-budget=15000 \
    --screenshot="$PWD/$name.png" "http://127.0.0.1:$port/render.html?d=$name" 2>/dev/null
  echo "rendered $name.png"
}

if [[ $# -gt 0 ]]; then
  grep -E "^$1 " diagrams.txt | while read -r name w h; do render "$name" "$w" "$h"; done
else
  while read -r name w h; do render "$name" "$w" "$h"; done < diagrams.txt
fi
