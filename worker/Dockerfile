# Base must be resolved to a digest by build-image.sh before building.
ARG PYTHON_BASE
FROM ${PYTHON_BASE}
ARG SOURCE_SHA
LABEL org.opencontainers.image.source="https://github.com/jasonjiang8866/runpod-ci-broker"
LABEL org.opencontainers.image.revision="${SOURCE_SHA}"
WORKDIR /opt/broker
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ ./src/
RUN python -m pip install --no-cache-dir uv==0.13.5 \
 && uv sync --locked --no-dev --no-editable --python /usr/local/bin/python \
 && printf '%s\n' "${SOURCE_SHA}" > /opt/broker/SOURCE_SHA
ENV PATH="/opt/broker/.venv/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/nvidia/lib:/usr/local/nvidia/lib64"
ENV NVIDIA_DRIVER_CAPABILITIES="compute,utility"
ENTRYPOINT ["python", "-m", "runpod_ci_broker.worker"]
