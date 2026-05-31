#!/bin/bash
# Enable BuildKit for Docker builds
export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

# Run make with all arguments passed through
exec make "$@"
