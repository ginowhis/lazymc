#!/bin/sh
# Ferpyhost: reproducible static linux-x64 build of this fork (x86_64-unknown-linux-musl).
# Builds a clean clone of the given commit, so the host checkout's line endings
# and untracked files never reach the binary. Run from the repo root on the host:
#
#   docker run --rm -v "$PWD:/src:ro" -v "$PWD/target/fh:/out" \
#     rust:1.99-bookworm@sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c \
#     sh /src/fh/build.sh <commit>
#
# Writes /out/lazymc and /out/lazymc-src.tar.gz and prints their sha256.
set -eu
rev=${1:-HEAD}
git config --global --add safe.directory '*'
git clone -q /src /build
cd /build
git checkout -q "$rev"
rustup target add x86_64-unknown-linux-musl
cargo build --release --locked --target x86_64-unknown-linux-musl
cp target/x86_64-unknown-linux-musl/release/lazymc /out/lazymc

# GPL corresponding source for the release: this commit (git archive) plus every
# dependency vendored, with the cargo config that builds it offline:
#   tar xzf lazymc-src.tar.gz && cd lazymc-src
#   cargo build --release --locked --offline --target x86_64-unknown-linux-musl
src=/tmp/lazymc-src
mkdir -p "$src/.cargo"
git archive "$rev" | tar -x -C "$src"
(cd "$src" && cargo vendor --locked --versioned-dirs vendor > .cargo/config.toml)
mtime=$(git log -1 --format=%ct "$rev")
tar --sort=name --mtime="@$mtime" --owner=0 --group=0 --numeric-owner -C /tmp -cf - lazymc-src |
  gzip -n > /out/lazymc-src.tar.gz

sha256sum /out/lazymc /out/lazymc-src.tar.gz
