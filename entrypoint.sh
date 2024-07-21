#!/bin/bash

# Install the necessary Python packages
pip install clickhouse-driver

# Execute the original entrypoint command
exec "$@"
