#!/usr/bin/env bash

echo "ERROR: Deprecated script!"
echo "This script have not been tested in a very long time and is likely to not work. It was kept for reference only."
exit 1

#
# Run the Docker image locally.
#

# ## IMPORTANT ##
# Set the AWS_ACCESS_KEY_ID and the AWS_SECRET_ACCESS_KEY before running
# Use the credentials of a role or user with the following access:
#   - AmazonS3FullAccess
#   - AmazonSNSFullAccess


docker run \
    --rm \
    --env EXECUTION_ENVIRONMENT=test \
    --env AWS_REGION="ap-southeast-2" \
    --env AWS_ACCESS_KEY_ID="************" \
    --env AWS_SECRET_ACCESS_KEY="************" \
    ereefs-download_manager:latest
