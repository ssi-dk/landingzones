FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client openssh-server rsync tar util-linux uuid-runtime ca-certificates \
    && uuidgen --version \
    && rm -rf /var/lib/apt/lists/* /etc/ssh/ssh_host_*
WORKDIR /opt/landingzones
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .
COPY tests/container_lab /opt/lab
ENTRYPOINT ["python", "/opt/lab/node.py", "boot"]
