#!/usr/bin/env bash
# Run the zarr-s3 integration tests against a throwaway MinIO in Docker.
# The test binary is built with day10 and run on the host so that it can
# reach localhost:9000.
set -euo pipefail
cd "$(dirname "$0")/.."

CONTAINER=zarr-itest-minio
PORT="${PORT:-9010}"  # 9000 is often taken by a local MinIO
ENDPOINT="http://localhost:${PORT}"

cleanup() { docker rm -f "$CONTAINER" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

echo ">> starting MinIO on $ENDPOINT"
docker run -d --name "$CONTAINER" -p "${PORT}:9000" \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data >/dev/null
for _ in $(seq 1 30); do
  curl -s --max-time 2 "$ENDPOINT/minio/health/ready" >/dev/null && break
  sleep 1
done

echo ">> building"
day10 build --with-test . test_s3/test_s3.exe

echo ">> running"
S3_ENDPOINT="$ENDPOINT" S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin \
  ./_build/default/test_s3/test_s3.exe
