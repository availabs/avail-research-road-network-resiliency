# SEE: https://docs.astral.sh/uv/guides/integration/docker/

FROM python:3.12-slim-bookworm

# e.g., using a hash from a previous release
COPY --from=ghcr.io/astral-sh/uv@sha256:2381d6aa60c326b71fd40023f921a0a3b8f91b14d5db6b90402e65a635053709 /uv /uvx /bin/

# Necessities: required for analysis
#   NOTE: We are adding ssh so we can easily ssh into and scp file to/from containers on remote servers.
RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential \
  default-jre \
  gcc \
  gdal-bin \
  git \
  openssh-server \
  libgdal-dev \
  libpq-dev \
  postgresql-client \
  python3-dev \
  sqlite3 \
  sudo \
  time \
  tmux \
  unzip \
  wget \
  zip

# Utilities: dev-ops ergonomics when ssh'ing into the containers.
RUN apt-get update && apt-get install -y --no-install-recommends \
  htop \
  jq \
  less \
  neovim \
  silversearcher-ag \
  tmux \
  tree

# Configure the ssh server to only accept keys for credentials.
#   Cannot use passwords because "availien" user does not have one.
#   SEE: https://askubuntu.com/a/346863
RUN echo "\
# To disable tunneled clear text passwords, change to no here!\n\
PasswordAuthentication no\n\
\n\
PubkeyAuthentication yes\n\
" >> /etc/ssh/sshd_config

# Because the entrypoint includes "&&", we must use this workaround for the ENTRYPOINT.
#   SEE: https://docs.docker.com/reference/build-checks/json-args-recommended/#workarounds
COPY --chmod=755 <<EOT /entrypoint.sh
#!/usr/bin/env bash
set -Eeou pipefail

sudo service ssh restart && sleep infinity
EOT

# Create the non-root user "availien"
RUN adduser --disabled-password --gecos "" availien
RUN adduser availien sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER availien

# SEE: https://github.com/astral-sh/uv-docker-example/blob/1c593aa32621eacd0125b55bd8d2796b86e8ea73/Dockerfile#L7C1-L11C22
# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1
# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Adding these aliases because they are so engrained in muscle memory that not having them is extremely disruptive.
RUN echo "\
# some more ls aliases\n\
alias ll='ls -alF'\n\
alias la='ls -A'\n\
alias l='ls -CF'\
" >> /home/availien/.bash_aliases

RUN mkdir /home/availien/rnr-analysis
WORKDIR /home/availien/rnr-analysis
RUN mkdir /home/availien/rnr-analysis/.venv/
ENV PATH="/home/availien/rnr-analysis/.venv/bin:$PATH"

ENTRYPOINT ["/entrypoint.sh"]
