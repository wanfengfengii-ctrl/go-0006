#!/bin/bash
set -e

IMAGE_NAME=${1:-quotaraft}
DOCKER_PLATFORM=${2:-linux/amd64}

docker build --platform "$DOCKER_PLATFORM" -f benzhi.Dockerfile -t "$IMAGE_NAME:latest" .

echo ""
echo "Docker image '$IMAGE_NAME:latest' built successfully."
echo ""
echo "Next step: docker run --rm -it $IMAGE_NAME:latest"
