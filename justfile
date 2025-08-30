dat_version := "0.0.3"

# basic setup
setup:
  uvx pre-commit install

load-dat:
    rm -rf dat/
    curl -OL https://github.com/delta-incubator/dat/releases/download/v{{ dat_version }}/deltalake-dat-v{{ dat_version }}.tar.gz
    mkdir -p dat
    tar --no-same-permissions -xzf deltalake-dat-v{{ dat_version }}.tar.gz --directory dat
    rm deltalake-dat-v{{ dat_version }}.tar.gz

notebooks:
  uv run --directory notebooks marimo edit
