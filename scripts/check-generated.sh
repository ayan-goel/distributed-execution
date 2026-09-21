#!/bin/sh
set -eu
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
cp -R gen "$tmp/gen"
sh scripts/generate-protocol.sh
diff -ru "$tmp/gen" gen
