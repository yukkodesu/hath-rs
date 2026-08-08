#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
out="$root/../../target/pcache-java-exporter"
rm -rf "$out"
mkdir -p "$out/classes"
javac --release 8 -d "$out/classes" "$root/src/io/github/yukkodesu/hathrs/PcacheJavaExporter.java"
jar cfe "$out/pcache-java-exporter.jar" io.github.yukkodesu.hathrs.PcacheJavaExporter -C "$out/classes" .
