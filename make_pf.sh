#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

gcc pf.c -Wall -Wextra -O3 -o pf

