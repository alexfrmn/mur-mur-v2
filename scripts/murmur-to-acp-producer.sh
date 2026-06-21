#!/usr/bin/env bash
set -euo pipefail

exec /usr/bin/env python3 /opt/lifecoach/mur-mur-v2/scripts/murmur-to-acp-producer.py "$@"
