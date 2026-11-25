#!/bin/sh
# Ferpyhost: reproducible static linux-x64 build of this fork (x86_64-unknown-linux-musl).
# Builds a clean clone of the given commit, so the host checkout's line endings
# and untracked files never reach the binary. Run from the repo root on the host:
#
#   docker run --rm -v "$PWD:/src:ro" -v "$PWD/target/fh:/out" \
#     rust:1.99-bookworm@sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c \
#     sh /src/fh/build.sh <commit>
#
# Writes /out/lazymc and prints its sha256.
set -eu
rev=${1:-HEAD}
git config --global --add safe.directory '*'
git clone -q /src /build
cd /build
git checkout -q "$rev"
rustup target add x86_64-unknown-linux-musl
cargo build --release --locked --target x86_64-unknown-linux-musl
cp target/x86_64-unknown-linux-musl/release/lazymc /out/lazymc
sha256sum /out/lazymc
