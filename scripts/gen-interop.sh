#!/usr/bin/env bash
set -euo pipefail

OUT="${1:-/tmp/pxar-interop}"
PXAR_DIR="${PROXMOX_PXAR_DIR:-$HOME/proxmox-pxar}"
PBS_DIR="${PROXMOX_BACKUP_DIR:-$HOME/proxmox-backup}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "$OUT"
echo ">> output: $OUT"
echo ">> pxar:   $PXAR_DIR"
echo ">> pbs:    $PBS_DIR"

install -d "$PXAR_DIR/.cargo"
: > "$PXAR_DIR/.cargo/config.toml"

echo ">> building probe_interop"
cp "$SCRIPT_DIR/rust-refs/probe_interop.rs" "$PXAR_DIR/examples/probe_interop.rs"
( cd "$PXAR_DIR" && cargo build --example probe_interop ) >/dev/null
PROBE="$PXAR_DIR/target/debug/examples/probe_interop"

echo ">> generating pxar references"
"$PROBE" encode-v1 "$OUT/rust_v1.pxar"
"$PROBE" encode-v2 "$OUT/rust_v2.mpxar" "$OUT/rust_v2.ppxar"
"$PROBE" decode    "$OUT/rust_v1.pxar"               >"$OUT/rust_v1.dump"
"$PROBE" decode    "$OUT/rust_v2.mpxar" "$OUT/rust_v2.ppxar" >"$OUT/rust_v2.dump"

( cd "$SCRIPT_DIR/.." && PXAR_INTEROP_DIR="$OUT" go test ./internal/interop/ -run TestWriteGoArchivesForRust ) >/dev/null

echo ">> building chunker probe"
CP=/tmp/pxar-chunker-probe
rm -rf "$CP"; mkdir -p "$CP/src"
cat >"$CP/Cargo.toml" <<'TOML'
[package]
name = "chunker-probe"
version = "0.1.0"
edition = "2021"
[[bin]]
name = "chunker-probe"
path = "src/main.rs"
TOML
sed -n '1,232p' "$PBS_DIR/pbs-datastore/src/chunker.rs" \
  | sed '/^pub struct PayloadChunker/,/^}/d' \
  | sed 's/^use std::sync::mpsc::Receiver;//' >"$CP/src/chunker.rs"
echo '}' >>"$CP/src/chunker.rs"
cat >"$CP/src/main.rs" <<'RUST'
mod chunker;
use chunker::{Chunker, ChunkerImpl, Context};
fn lcg_data(len: usize, seed: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(len); let mut x = seed;
    for _ in 0..len { x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); v.push((x >> 33) as u8); }
    v
}
fn boundaries(data: &[u8], avg: usize, feed: usize) -> Vec<usize> {
    let mut c = ChunkerImpl::new(avg); let ctx = Context::default(); let mut out = Vec::new(); let mut start = 0;
    while start < data.len() {
        let end = std::cmp::min(start + feed, data.len()); let mut off = start;
        loop {
            let pos = c.scan(&data[off..end], &ctx);
            if pos == 0 { break; }
            off += pos; out.push(off); if off >= end { break; }
        }
        start = end;
    }
    out
}
fn main() {
    let cases: Vec<(Vec<u8>, &str)> = vec![
        (lcg_data(1<<22, 1), "lcg-4m-seed1"), (lcg_data(3<<20, 42), "lcg-3m-seed42"),
        (vec![0u8; 1<<21], "zeros-2m"),
        ((0..(1u32<<21)).map(|i| (i % 251) as u8).collect(), "mod251-2m"),
        (lcg_data(70000, 7), "lcg-70k-seed7"),
    ];
    for (data, name) in &cases {
        for avg in [64*1024usize, 128*1024] {
            for feed in [data.len(), 4096, 61, 256*1024] {
                let b = boundaries(data, avg, feed); let sum: usize = b.iter().sum();
                println!("{} avg={} feed={} n={} sum={} first10={:?}", name, avg, feed, b.len(), sum, &b[..std::cmp::min(10, b.len())]);
            }
        }
    }
}
RUST
( cd "$CP" && cargo build --release ) >/dev/null
"$CP/target/release/chunker-probe" >"$OUT/rust_chunks.txt"

echo ">> building catalog probe"
CAT=/tmp/pxar-catalog-probe
rm -rf "$CAT"; mkdir -p "$CAT/src"
cat >"$CAT/Cargo.toml" <<'TOML'
[package]
name = "catalog-probe"
version = "0.1.0"
edition = "2021"
[[bin]]
name = "catalog-probe"
path = "src/main.rs"
TOML
cp "$SCRIPT_DIR/rust-refs/catalog_probe.rs" "$CAT/src/main.rs"
( cd "$CAT" && cargo build --release ) >/dev/null
"$CAT/target/release/catalog-probe" >"$OUT/rust_catalog.hex"

echo
echo ">> reference artifacts generated in $OUT"
ls -1 "$OUT"
echo
echo ">> run Go interop tests with: PXAR_INTEROP_DIR=$OUT go test ./internal/interop/"
