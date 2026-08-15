#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runtime_uid="${QDL_RUNTIME_UID:-10001}"
runtime_gid="${QDL_RUNTIME_GID:-10001}"

case "${runtime_uid}:${runtime_gid}" in
  *[!0-9:]*|:*|*:) echo "QDL runtime UID/GID must be numeric" >&2; exit 2 ;;
esac

for relative in data logs; do
  directory="${repo_root}/${relative}"
  install -d -m 0750 "${directory}"
  chown -R "${runtime_uid}:${runtime_gid}" "${directory}"
done

printf 'prepared data/logs for qdl runtime uid=%s gid=%s\n' \
  "${runtime_uid}" "${runtime_gid}"
