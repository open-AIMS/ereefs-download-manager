#!/usr/bin/env bash

#
# Build the Docker image based on Dockerfile. This script assumes the user has previously
# executed the 'build-jar.sh' script.
#

# Identify the directory of this script, and the root directory of the project.
# The path to this script file
SCRIPT="$(readlink -f "$0")"
# The directory where this script file belongs
SCRIPT_PATH="$(dirname "$SCRIPT")"
# The parent directory, aka the project root directory.
PROJECT_ROOT="$(readlink --canonicalize "$SCRIPT_PATH/..")"

# Find the JAR file
ABSOLUTE_PATH_JAR_FILE=$(ls "$PROJECT_ROOT/target/"*"-jar-with-dependencies.jar")
JAR_FILE=$(basename "$ABSOLUTE_PATH_JAR_FILE")

if [ -z "$JAR_FILE" -o "$JAR_FILE" = "" ]; then
    echo "ERROR: Run build-jar.sh before running build-docker-image.sh"
    exit 1
fi

# Build the Docker image.
docker build \
    -t "ereefs-download_manager:latest" \
    -f "$PROJECT_ROOT/Dockerfile" \
    --build-arg JAR_NAME="$JAR_FILE" \
    --force-rm \
    "$PROJECT_ROOT"
