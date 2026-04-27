#!/bin/bash
# Wrapper script to ensure BuildKit is enabled for Docker builds.
# Set NOBUILDKIT=1 to use the legacy builder (stable for 100+ images).
if [ "${NOBUILDKIT:-0}" = "1" ]; then
    export DOCKER_BUILDKIT=0
    export COMPOSE_DOCKER_CLI_BUILD=0
else
    export DOCKER_BUILDKIT=1
    export COMPOSE_DOCKER_CLI_BUILD=1
fi

# Execute the command passed as arguments
exec "$@"
