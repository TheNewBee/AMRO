#!/bin/sh
set -e
input="${INPUT_FILE:-/data/Input.txt}"
mkdir -p "$(dirname "$input")"
if [ ! -s "$input" ]; then
  cp /seed/Input.txt "$input"
fi
exec java -jar /app/app.jar
