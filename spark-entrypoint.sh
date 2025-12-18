#!/bin/bash
set -e

# Ensure checkpoint directory has proper permissions
if [ -d /tmp/spark-checkpoint ]; then
    chown -R spark:spark /tmp/spark-checkpoint 2>/dev/null || true
    chmod 755 /tmp/spark-checkpoint 2>/dev/null || true
fi

# Execute the original command as spark user
exec gosu spark "$@"

