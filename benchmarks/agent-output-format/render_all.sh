#!/bin/sh
# Render every dataset with the new renderer (tsv, markdown) and the 6.1 renderer (markdown).
# render_new / render_old are the binaries built from render_nd.rs (see README.md).
set -e
mkdir -p rendered
for f in data/*.ndjson; do
  n=$(basename "$f" .ndjson)
  ./render_new "$f" "data/$n.schema.tsv" tsv      > "rendered/$n.tsv.txt"
  ./render_new "$f" "data/$n.schema.tsv" markdown > "rendered/$n.md_new.txt"
  ./render_old "$f" "data/$n.schema.tsv"          > "rendered/$n.md_old.txt"
done
