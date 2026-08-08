#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
out="$root/../../target/pcache-java-exporter-test"
rm -rf "$out"
mkdir -p "$out/classes"
javac --release 8 -d "$out/classes" \
  "$root/src/io/github/yukkodesu/hathrs/PcacheJavaExporter.java" \
  "$root/test/io/github/yukkodesu/hathrs/PcacheJavaExporterTest.java"
java -cp "$out/classes" io.github.yukkodesu.hathrs.PcacheJavaExporterTest
