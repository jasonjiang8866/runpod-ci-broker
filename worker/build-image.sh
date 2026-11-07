#!/usr/bin/env bash
set -euo pipefail
source_sha="${1:?full source SHA required}"
[[ "$source_sha" =~ ^[a-f0-9]{40}$ ]] || exit 2
[[ "$(git rev-parse HEAD)" == "$source_sha" ]] || exit 2
base_tag='python:3.12.12-slim-bookworm'
base_digest="$(docker buildx imagetools inspect "$base_tag" --format '{{json .Manifest.Digest}}' | tr -d '"')"
[[ "$base_digest" =~ ^sha256:[a-f0-9]{64}$ ]] || exit 2
base_image="python@${base_digest}"
mkdir -p out/image
printf '%s\n' "$base_image" > out/image/base-image.txt
printf '%s\n' "$source_sha" > out/image/source-sha.txt
docker build --build-arg "PYTHON_BASE=$base_image" --build-arg "SOURCE_SHA=$source_sha" -f worker/Dockerfile -t "rcb-worker:$source_sha" .
docker run --rm "rcb-worker:$source_sha" --help
# CPU CI must refuse GPU verification; no fake CPU success is allowed.
set +e
docker run --rm --entrypoint python "rcb-worker:$source_sha" -m runpod_ci_broker.cuda_probe > out/image/cpu-refusal.json
probe_status=$?
set -e
[[ "$probe_status" == 2 ]] || exit 2
python3 -c 'import json; from pathlib import Path; d=json.loads(Path("out/image/cpu-refusal.json").read_text()); assert d["gpu_verified"] is False'
docker image inspect "rcb-worker:$source_sha" --format '{{.Id}}' > out/image/local-image-id.txt
