FROM mcr.microsoft.com/devcontainers/base:jammy

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y awscli openjdk-17-jre-headless && \
    apt-get install -y gdal-bin libgdal-dev && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

COPY environment.yml /tmp/env.yaml