#!/bin/sh
# Copy only the mounted workload identities to tmpfs, then run the client as
# the unprivileged Data Layer runtime UID. The outer container is disposable.
set -eu

umask 077
mkdir -p /tmp/identity
for identity in /identity/*; do
    [ -d "$identity" ] || continue
    name="$(basename "$identity")"
    mkdir -p "/tmp/identity/$name"
    tar -C "$identity" -cf - . | tar --no-same-owner -xf - -C "/tmp/identity/$name"
done
chown -R 10001:10001 /tmp/identity
chmod -R u=rwX,go= /tmp/identity

exec setpriv \
    --reuid=10001 \
    --regid=10001 \
    --keep-groups \
    --inh-caps=-all \
    --ambient-caps=-all \
    --no-new-privs \
    -- python -B /driver/phase3_consumer_load_acceptance.py --inside
