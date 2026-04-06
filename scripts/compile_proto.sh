#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# compile_proto.sh
# Compiles sentinel.proto into Python gRPC stubs.
# Re-run this whenever sentinel.proto changes.
# ─────────────────────────────────────────────────────────────────────────────

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="$REPO_ROOT/.venv/bin/python"

if [ ! -f "$PYTHON" ]; then
  echo "ERROR: virtualenv not found at $REPO_ROOT/.venv"
  echo "Run: python -m venv .venv && .venv/bin/pip install -r requirements.txt"
  exit 1
fi

PROTO_DIR="$REPO_ROOT/agent_sentinel/grpc_layer/protos"
OUT_DIR="$REPO_ROOT/agent_sentinel/grpc_layer"

echo "Compiling sentinel.proto..."

$PYTHON -m grpc_tools.protoc \
  -I "$PROTO_DIR" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$PROTO_DIR/sentinel.proto"

# Fix the import in the generated grpc file to use the full package path
# (grpc_tools generates a bare `import sentinel_pb2` which breaks when
#  the file is inside a package)
sed -i '' \
  's/import sentinel_pb2/import agent_sentinel.grpc_layer.sentinel_pb2/' \
  "$OUT_DIR/sentinel_pb2_grpc.py"

echo "Done. Generated:"
echo "  $OUT_DIR/sentinel_pb2.py"
echo "  $OUT_DIR/sentinel_pb2_grpc.py"
