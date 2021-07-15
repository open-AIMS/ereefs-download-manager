#!/usr/bin/env bash

#
# Use a Maven Docker image to compile the source code and package as a .jar file.
#

# The version of the Maven Docker image to use.
MAVEN_DOCKER_TAG="3.6.1-jdk-8-alpine"

# Identify the directory of this script.
SCRIPT="$(readlink -f "$0")"
SCRIPT_PATH="$(dirname "$SCRIPT")"
PROJECT_ROOT="$(readlink --canonicalize "$SCRIPT_PATH/..")"

# Ensure the ${HOME}/.m2 exists in the current user home directory.
mkdir -p ${HOME}/.m2

# Package the app via Maven using current user.
# NOTE:
#   If root user is used (no "-u" option), the files created in the local maven repo and in the target directory
#     will be attributed to root, preventing other users (such as your IDE) from running maven commands
#     such as "mvn clean", "mvn test", "mvn package", etc.
#   If another user is used, you will get a permission denied when it will try to download new dependency into
#     your maven repository or when it will try to access the target directory.
#   The maven repository is map to "/tmp/maven" in the Docker container to be sure the user will be able to
#     access it within Docker.
docker run \
    -u $(id -u):$(id -g) \
    --rm \
    --name "maven-downloadmanager-buildjar" \
    -v "$HOME/.m2:/tmp/maven/.m2" \
    -v "$PROJECT_ROOT:/workspace" \
    -w "/workspace" \
    maven:${MAVEN_DOCKER_TAG} \
    mvn -Duser.home="/tmp/maven" clean package
