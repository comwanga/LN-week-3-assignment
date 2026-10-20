#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo run --manifest-path rust/Cargo.toml
