# originally based on outdated dockerfile from https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-postgres/docker/Dockerfile
# trixie 3.12 to use latest debian stable + get 3.12 + patches.
# packages updated to current trixie versions https://www.debian.org/distrib/packages
# https://pypi.org/project/dbt-postgres/ 1.10 (current) instead of from github because AFAIK there's no release for 1.10 on git
# ca-certificates pin removed

ARG py_version=3.12

FROM python:$py_version-slim-trixie AS base

RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    build-essential=12.12 \
    ca-certificates \
    git=1:2.47.3-0+deb13u1 \
    libpq-dev=17.9-0+deb13u1 \
    make=4.4.1-2 \
    openssh-client=1:10.0p1-7+deb13u2 \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

RUN python -m pip install --upgrade "pip==26.1" "setuptools==82.0.1" "wheel==0.47.0" --no-cache-dir


FROM base AS dbt-postgres

HEALTHCHECK CMD dbt --version || exit 1

WORKDIR /usr/app
ENTRYPOINT ["dbt"]

RUN python -m pip install --no-cache-dir "dbt-core==1.10.20" "dbt-postgres==1.10.0"