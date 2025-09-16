#!/bin/bash

set -eou pipefail

shopt -s globstar

PROTO_DIR="${1:-pkg/proto}"
OUT_DIR="${2:-pkg/pb}"
APIDOCS_DIR="${4:-gateway/apidocs}"

# Ensure output directory exists
mkdir -p "${OUT_DIR}"

# Clean previously generated files
find "${OUT_DIR}" -type f \( -name '*.go' \) -delete

# Generate protobuf files.
protoc-wrapper \
  --proto_path="${PROTO_DIR}" \
  --go_out="${OUT_DIR}" \
  --go_opt=paths=source_relative \
  --go-grpc_out="${OUT_DIR}" \
  --go-grpc_opt=paths=source_relative \
  "${PROTO_DIR}"/**/*.proto

# Clean previously generated files.
rm -rf "${APIDOCS_DIR}"/* && \
  mkdir -p "${APIDOCS_DIR}"

# Generate the swagger.json
protoc-wrapper \
  --proto_path="${PROTO_DIR}" \
  --grpc-gateway_out="${OUT_DIR}" \
  --grpc-gateway_opt=logtostderr=true \
  --grpc-gateway_opt=paths=source_relative \
  --openapiv2_out="${APIDOCS_DIR}" \
  --openapiv2_opt=logtostderr=true \
  "${PROTO_DIR}"/service.proto
