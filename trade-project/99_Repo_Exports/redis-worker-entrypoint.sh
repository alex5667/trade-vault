#!/bin/sh
# redis-worker-entrypoint.sh
# Seeds the initial ACL file from the read-only bind-mount source
# (/usr/local/etc/redis/redis-worker-1-acl.conf) into the writable data
# volume (/data/redis-worker-1-acl.conf) so that Redis can perform
# ACL SAVE (which requires rename() on the file, impossible on a bind-mount).
#
# Logic:
#   - First boot: copy source → /data/
#   - Subsequent boots: if source is NEWER than /data/ copy → refresh
#     (allows re-applying ACL policy by touching the source file via the
#      exec-health-freeze-acl-policy container which updates via Redis)
#
# Note: Once Redis is running the authoritative copy is /data/; updates
# must happen via `ACL SETUSER` + `ACL SAVE` commands, NOT by editing
# the bind-mount source file.

set -e

ACL_FILE="${ACL_FILENAME:-redis-worker-1-acl.conf}"
SRC="/usr/local/etc/redis/$ACL_FILE"
DST="/data/$ACL_FILE"

if [ ! -f "$DST" ]; then
    echo "[entrypoint] Seeding ACL file from bind-mount to /data/ (first boot)"
    cp "$SRC" "$DST"
    echo "[entrypoint] ACL seeded: $(wc -l < "$DST") lines"
elif [ "$SRC" -nt "$DST" ]; then
    echo "[entrypoint] Source ACL is newer than /data/ copy — refreshing"
    cp "$SRC" "$DST"
    echo "[entrypoint] ACL refreshed: $(wc -l < "$DST") lines"
else
    echo "[entrypoint] /data/ ACL is up-to-date ($(wc -l < "$DST") lines), keeping existing"
fi

# Hand off to the standard Redis entrypoint
exec redis-server /usr/local/etc/redis/redis.conf
